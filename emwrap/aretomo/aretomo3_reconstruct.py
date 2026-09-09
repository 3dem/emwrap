import os
import shutil

from .aretomo3_modular import Aretomo3ModularBase
from .aretomo3 import AreTomo3
from emtools.image import Image

from emtools.metadata import Table
from .utils import create_dummy_edf_file

# TODO: Implement handling the synthetic ALN generation and ensure consistency with AreTomo3 input requirements.

class AreTomo3ReconstructPipeline(Aretomo3ModularBase):
    name = 'emw-aretomo3-reconstruct'

    def _output_directories(self):
        return (self.outputTomDir,)

    def _include_tilt_outputs(self):
        return False

    def __init__(self, args, output):
        super().__init__(args, output)

    def _ctf_requested(self):
        return self._args.get('aretomo3.CorrCTF', False)
    
    def _use_previous_alignment(self):
        return self._args.get('UsePreviousAlignment', False)

    def _install_previous_alignment(self, batch, ts_name):
        if self._ctf_requested():
            _, staged = self._stage_previous_alignment(batch, ts_name, required=('stack', 'tlt', 'aln', 'ctf'))
        else:
            _, staged = self._stage_previous_alignment(batch, ts_name, required=('stack', 'tlt', 'aln'))
        return staged

    def _write_synthetic_aln(self, path, table, row, pixel_size, raw_size=None):
        rot = float(getattr(row, 'rlnTomoNominalTiltAxisAngle', 0) or 0)
        raw_x, raw_y = raw_size if raw_size else (0, 0)
        with open(path, 'w') as handle:
            handle.write('# AreTomo Alignment / Priims bprmMn (synthetic, from RELION5 metadata; no local alignment)\n')
            handle.write(f'# RawSize = {raw_x} {raw_y} {len(table)}\n')
            handle.write('# NumPatches = 0\n')
            handle.write('# AlphaOffset =     0.00\n')
            handle.write('# BetaOffset =     0.00\n')
            handle.write('# Thickness = 0\n')
            handle.write('# SEC     ROT         GMAG       TX          TY      SMEAN     SFIT    SCALE     BASE     TILT\n')
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
        self._write_aretomo3_ctf(path, table)

    def get_aretomo3_proc(self, gpu):
        def process(batch):
            ts_name = batch['tsName']
            batch.create()
            row = self._input_row(ts_name)
            if self._use_previous_alignment():
                self.log(f'{ts_name}: Using previous AreTomo3 alignment from tilt_series/{ts_name}/')
                self._install_previous_alignment(batch, ts_name)
            else:
                raise NotImplementedError(
                    f'{ts_name}: Cmd 2 reconstruction without a previous AreTomo3 '
                    'alignment is not implemented yet. Enable UsePreviousAlignment '
                    'and provide the previous tilt_series/<TS_NAME>/ files.'
                )
                self.log(f'{ts_name}: Synthesizing ALN file from RELION5 aligned_tilt_series.star')
                table, stack, _ = self._stage_stack_and_tlt(batch, ts_name, row, aligned_angles=True)
                raw_size = Image.get_dimensions(stack)[:2]
                self._write_synthetic_aln(batch.join(f'{ts_name}.aln'), table, row, self._pixel_size(row),
                                          raw_size=raw_size)
                if self._ctf_requested():
                    self._write_synthetic_ctf(batch.join(f'{ts_name}_CTF.txt'), table)
            
            at3 = AreTomo3(self.acq, **self._args)
            at3.process_batch(batch, gpu=gpu, cmd=2, input_prefix=f'./{ts_name}',
                              input_suffix='.mrc', input_skips='_ODD,_EVN', ts_name=ts_name, expect_tilt_series=False,
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

if __name__ == '__main__':
    AreTomo3ReconstructPipeline.main()
