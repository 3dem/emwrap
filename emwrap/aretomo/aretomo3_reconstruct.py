from .aretomo3_modular import Aretomo3ModularBase
from emtools.jobs import TsStarBatchManager
from .aretomo3 import AreTomo3


class AreTomo3ReconstructPipeline(Aretomo3ModularBase):
    name = 'emw-aretomo3-reconstruct'

    def __init__(self, args, output):
        super().__init__(args, output)
        self._args.setdefault('aretomo3.CorrCTF', False)

    def _ctf_requested(self):
        return str(self._args.get('aretomo3.CorrCTF', False)).lower() in ('1', 'true', 'yes')

    def _write_synthetic_aln(self, path, table, row, pixel_size):
        rot = float(getattr(row, 'rlnTomoNominalTiltAxisAngle', 0) or 0)
        with open(path, 'w') as handle:
            handle.write('# Synthetic ALN generated from RELION5 metadata; no local alignment.\n')
            for index, tilt in enumerate(table, start=1):
                x_tilt = float(getattr(tilt, 'rlnTomoXTilt', 0) or 0)
                z_rot = float(getattr(tilt, 'rlnTomoZRot', 0) or 0)
                if x_tilt or z_rot:
                    self.log(f'WARNING: {row.rlnTomoName} row {index}: synthetic ALN drops '
                             f'rlnTomoXTilt={x_tilt} and rlnTomoZRot={z_rot}.')
                tx = float(getattr(tilt, 'rlnTomoXShiftAngst', 0) or 0) / pixel_size
                ty = float(getattr(tilt, 'rlnTomoYShiftAngst', 0) or 0) / pixel_size
                angle = getattr(tilt, 'rlnTomoYTilt', '') or getattr(tilt, 'rlnTomoNominalStageTiltAngle', '')
                if angle in ('', None):
                    raise ValueError(f'Missing tilt angle at row {index}')
                handle.write(f'{index} {rot:.6f} 1 {tx:.6f} {ty:.6f} 0 0 1 0 {float(angle):.6f}\n')

    def _write_synthetic_ctf(self, path, table):
        required = ('rlnDefocusU', 'rlnDefocusV', 'rlnDefocusAngle')
        with open(path, 'w') as handle:
            for index, row in enumerate(table, start=1):
                if any(getattr(row, key, '') in ('', None) for key in required):
                    raise ValueError(f'CTF correction requires {", ".join(required)} at row {index}')
                fom = getattr(row, 'rlnCtfFigureOfMerit', 0) or 0
                resolution = getattr(row, 'rlnCtfMaxResolution', 0) or 0
                handle.write(f'{index} {float(row.rlnDefocusU):.6f} {float(row.rlnDefocusV):.6f} '
                             f'{float(row.rlnDefocusAngle):.6f} 0 {float(fom):.6f} {float(resolution):.6f} 0\n')

    def get_aretomo3_proc(self, gpu):
        def process(batch):
            ts_name = batch['tsName']
            batch.create()
            row = next(row for row in self.inputTsTable if row.rlnTomoName == ts_name)
            table, _, _ = self._stage_stack_and_tlt(batch, ts_name, row, aligned_angles=True)
            self._write_synthetic_aln(batch.join(f'{ts_name}.aln'), table, row, self._pixel_size(row))
            if self._ctf_requested():
                self._write_synthetic_ctf(batch.join(f'{ts_name}_CTF.txt'), table)
            at3 = AreTomo3(self.acq, **self._args)
            at3.process_batch(batch, gpu=gpu, cmd=2, input_prefix=f'./{ts_name}',
                              input_suffix='.mrc', ts_name=ts_name, expect_tilt_series=False,
                              expect_split_tilt_series=False, expect_ctf_output=False)
            return batch
        return process

    def _registerOutputs(self):
        tomograms_star = self.join('tomograms.star')
        failed_star = self.join('failed_tilt_series.star')
        input_cols = self.inputTsTable.getColumnNames()
        extras = [col for col in self._tomogram_extra_cols() if col not in input_cols]
        tomograms, failed = Table(input_cols + extras), Table(input_cols)
        input_by_name = {row.rlnTomoName: row for row in self.inputTsTable}
        for ts_name, result in self._allResults.items():
            row = input_by_name[ts_name]
            tomo = result.get('rlnTomoReconstructedTomogram')
            if 'error' in result or not tomo or not os.path.exists(tomo):
                failed.addRowValues(**row._asdict())
                continue
            dims = Image.get_dimensions(tomo)
            values = row._asdict()
            values.update({
                'rlnTomoReconstructedTomogram': tomo,
                'rlnTomoTomogramBinning': self.newTargetTomBinning(),
                'rlnTomoSizeX': dims[0], 'rlnTomoSizeY': dims[1], 'rlnTomoSizeZ': dims[2],
                'rlnEtomoDirectiveFile': create_dummy_edf_file(os.path.dirname(tomo), ts_name),
                'rlnTomoReconstructedTomogramHalf1': result.get('rlnTomoNameEvn', ''),
                'rlnTomoReconstructedTomogramHalf2': result.get('rlnTomoNameOdd', ''),
            })
            tomograms.addRowValues(**values)
        self.write_ts_table('global', tomograms, tomograms_star)
        nodes = [[tomograms_star, 'TomogramGroupMetadata.star.relion.tomo.tomograms']]
        if len(failed):
            self.write_ts_table('global', failed, failed_star)
            nodes.append([failed_star, 'TomogramGroupMetadata.star.relion.tomo.tomograms-failed'])
        self.writeRelionOutputNodes(nodes)

    def _register_existing_final_outputs(self):
        """Cmd 2 has no tilt-series output to discover in register-only mode."""
        self._allResults = {}
        for row in self.inputTsTable:
            ts_name = row.rlnTomoName
            folder = self._getOutputTomFolder(ts_name)
            tomo = folder.join(f'{ts_name}_Vol.mrc')
            result = {'rlnTomoName': ts_name}
            if os.path.exists(tomo):
                result['rlnTomoReconstructedTomogram'] = tomo
                for filename, key in ((f'{ts_name}_ODD_Vol.mrc', 'rlnTomoNameOdd'),
                                      (f'{ts_name}_EVN_Vol.mrc', 'rlnTomoNameEvn')):
                    path = folder.join(filename)
                    if os.path.exists(path):
                        result[key] = path
            else:
                result['error'] = f'Missing reconstructed tomogram: {tomo}'
            self._allResults[ts_name] = result
        self._registerOutputs()
        self.info['register_only'] = True

    def _output(self, batch):
        ts_name = batch['tsName']
        if batch.error:
            self._allResults[ts_name] = {'error': batch.error}
        else:
            result = batch['results'][0] if batch['results'] else {}
            self._allResults[ts_name] = self._copy_result(result, ts_name, include_tilt=False)
        batch.info['tsName'] = ts_name
        self._registerOutputs()
        self.updateBatchInfo(batch)
        return batch

    # -------- Pipeline lifecycle ---------- 
    def prerun(self):
        self.inputTsTable = self._getInputTsTable()
        self.inputTs =  self._args['input_tiltseries']
        print(f"Input tilt-series: {len(self.inputTsTable)}")  

        if self.registerOnly:
            self._register_existing_final_outputs()
            return
        
        self.mkdir(self.outputTomDir)

        batchMgr = TsStarBatchManager(self.inputTsTable, self.tmpDir)
        g = self.addGenerator(batchMgr.generate)
        
        self.addGpuProcessors(g, self.get_aretomo3_proc, self._output)


if __name__ == '__main__':
    AreTomo3ReconstructPipeline.main()
