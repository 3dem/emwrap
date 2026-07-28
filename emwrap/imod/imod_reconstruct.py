# **************************************************************************
# *
# * Authors:     J.M. de la Rosa Trevin (delarosatrevin@gmail.com)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# **************************************************************************

import os
import shutil

from emtools.utils import Color, FolderManager
from emtools.image import Image
from emtools.jobs import TsStarBatchManager
from emtools.metadata import StarFile, Table

from emwrap.base import ProcessingPipeline
from .imod import ImodTomoReconstruct


class ImodReconstructPipeline(ProcessingPipeline):
    """Apply IMOD alignment and reconstruct tomograms from aligned tilt series."""

    name = 'emw-imod-reconstruct'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        cpus = self._args.get('cpus', 1)
        self.cpuList = list(range(int(cpus))) if cpus else [0]
        self.outputTomDir = 'tomograms'
        self.outputTsDir = 'tilt_series'
        self.inputLen = 0
        self.inputTsTable = None
        self.inputTs = None
        self._allResults = {}
        self.registerOnly = self._register_output_only()

    def _tomogram_extra_cols(self):
        return [
            'rlnTomoReconstructedTomogram',
            'rlnTomoTomogramBinning',
            'rlnTomoSizeX',
            'rlnTomoSizeY',
            'rlnTomoSizeZ',
            'rlnTiltSeriesAligned',
        ]

    @staticmethod
    def _resolve_path(path, working_dir):
        return ImodTomoReconstruct._resolve_path(path, working_dir)

    def _getInputTsTable(self):
        input_ts = self._args['input_tiltseries']
        if not os.path.exists(input_ts):
            raise Exception(f"Input tilt series STAR file not found: {input_ts}")

        table = StarFile.getTableFromFile('global', input_ts)
        self.inputLen = len(table)

        if not table.hasColumn('rlnEtomoDirectiveFile'):
            raise Exception(
                "Input STAR file must contain rlnEtomoDirectiveFile "
                "(expected from an IMOD alignment job)."
            )

        return table

    def _getOutputTomFolder(self, ts_name):
        return FolderManager(self.join(self.outputTomDir, ts_name))

    def _getOutputTsFolder(self, ts_name):
        return FolderManager(self.join(self.outputTsDir, ts_name))

    def write_ts_table(self, table_name, table, star_file):
        self.log(f"Writing: {star_file}")
        with StarFile(star_file, 'w') as sf_out:
            sf_out.writeTable(table_name, table, computeFormat='left', timeStamp=True)

    def _updateTomogramDict(self, ts_dict, result):
        tomo = result.get('rlnTomoReconstructedTomogram', '')
        aligned = result.get('rlnTiltSeriesAligned', '')

        ok = bool(tomo) and os.path.exists(tomo)
        dims = Image.get_dimensions(tomo) if ok else None

        binning = 1.0

        ts_dict.update({
            'rlnTomoReconstructedTomogram': tomo if ok else '',
            'rlnTomoTomogramBinning': binning if ok else '',
            'rlnTomoSizeX': dims[0] if dims else '',
            'rlnTomoSizeY': dims[1] if dims else '',
            'rlnTomoSizeZ': dims[2] if dims else '',
            'rlnTiltSeriesAligned': aligned if aligned and os.path.exists(aligned) else '',
        })
        return ok, dims

    def _registerOutputs(self):
        self.log("Registering output STAR files.")

        tomograms_star = self.join('tomograms.star')
        failed_star = self.join('failed_tilt_series.star')

        input_cols = self.inputTsTable.getColumnNames()
        tom_extra_cols = [
            c for c in self._tomogram_extra_cols()
            if c not in input_cols
        ]

        tomograms_table = Table(input_cols + tom_extra_cols)
        failed_table = Table(input_cols)

        input_by_name = {row.rlnTomoName: row for row in self.inputTsTable}
        tom_dims = None

        for ts_name, result in self._allResults.items():
            ts_row = input_by_name.get(ts_name)
            if ts_row is None:
                self.log(f"WARNING: Unknown tilt series in results: {ts_name}")
                continue

            if 'error' in result:
                failed_table.addRowValues(**ts_row._asdict())
                continue

            ts_dict = ts_row._asdict()
            ok, dims = self._updateTomogramDict(ts_dict, result)
            if not ok:
                self.log(f"WARNING: Missing reconstructed tomogram for TS {ts_name}")
                failed_table.addRowValues(**ts_row._asdict())
                continue

            if tom_dims is None and dims is not None:
                tom_dims = dims

            ts_dict['rlnTomoTiltSeriesStarFile'] = self.join(
                self.outputTsDir, f'{ts_name}.star'
            )
            tomograms_table.addRowValues(**ts_dict)

        self.write_ts_table('global', tomograms_table, tomograms_star)
        if len(failed_table):
            self.write_ts_table('global', failed_table, failed_star)

        output_nodes = [[tomograms_star, 'TomogramGroupMetadata.star.relion.tomo.Tomograms']]
        self.writeRelionOutputNodes(output_nodes)

    def _register_existing_final_outputs(self):
        self._allResults = {}

        for row in self.inputTsTable:
            ts_name = row.rlnTomoName
            tom_folder = self._getOutputTomFolder(ts_name)
            ts_folder = self._getOutputTsFolder(ts_name)

            tomo = None
            aligned = None
            if tom_folder.exists():
                for fn in os.listdir(tom_folder.path):
                    if fn.endswith('.rec') or fn.endswith('.mrc'):
                        tomo = tom_folder.join(fn)
                        break

            if ts_folder.exists():
                for fn in os.listdir(ts_folder.path):
                    if fn.endswith('.st') or fn.endswith('.mrc'):
                        aligned = ts_folder.join(fn)
                        break

            if tomo and os.path.exists(tomo):
                self._allResults[ts_name] = {
                    'rlnTomoReconstructedTomogram': tomo,
                    'rlnTiltSeriesAligned': aligned or '',
                }
            else:
                self._allResults[ts_name] = {
                    'error': f'Missing tomogram output for {ts_name}'
                }

        self._registerOutputs()
        self.info['register_only'] = True

    def get_imod_proc(self):
        imod = ImodTomoReconstruct(**self._args)

        def _imod(batch):
            ts_name = batch['tsName']
            batch['path'] = os.path.join(self.tmpDir, ts_name)
            imod.process_batch(batch, self.workingDir)
            return batch

        return _imod

    def _output(self, batch):
        ts_name = batch['tsName']
        batch.log(f"Storing output for batch '{ts_name}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
            self._allResults[ts_name] = {'error': batch.error}
        else:
            result = batch['results'][0] if batch.get('results') else {}
            tom_folder = self._getOutputTomFolder(ts_name)
            ts_folder = self._getOutputTsFolder(ts_name)
            tom_folder.create()
            ts_folder.create()

            def _copy(src_key, dest_folder):
                src = result.get(src_key)
                if not src or not os.path.exists(src):
                    return None

                dst = dest_folder.join(os.path.basename(src))
                if os.path.abspath(src) != os.path.abspath(dst):
                    shutil.copy2(src, dst)

                result[src_key] = dst
                return dst

            _copy('rlnTomoReconstructedTomogram', tom_folder)
            _copy('rlnTiltSeriesAligned', ts_folder)

            batch.info['result'] = {k: v for k, v in result.items() if k != 'error'}
            self._allResults[ts_name] = result

        batch.info['tsName'] = ts_name
        self._registerOutputs()
        self.updateBatchInfo(batch)

        if self.inputLen:
            total_output = len(self.info['batches'])
            percent = total_output * 100 / self.inputLen
            batch.log(
                f">>> Processed {Color.green(total_output)} out of "
                f"{Color.red(self.inputLen)} "
                f"({Color.bold('%0.2f' % percent)} %)",
                flush=True,
            )

        return batch

    def prerun(self):
        self.inputTsTable = self._getInputTsTable()
        self.inputTs = self._args['input_tiltseries']
        self.log(f"Input tilt-series: {len(self.inputTsTable)}")

        self.mkdir(self.outputTomDir)
        self.mkdir(self.outputTsDir)

        if self.registerOnly:
            self._register_existing_final_outputs()
            return

        batch_mgr = TsStarBatchManager(self.inputTsTable, self.tmpDir)
        generator = self.addGenerator(batch_mgr.generate)
        output_queue = None

        n_workers = max(1, len(self.cpuList))
        self.log(f"Creating {n_workers} processing threads.", flush=True)

        for _ in self.cpuList:
            processor = self.addProcessor(
                generator.outputQueue,
                self.get_imod_proc(),
                outputQueue=output_queue,
            )
            output_queue = processor.outputQueue

        self.addProcessor(output_queue, self._output)


if __name__ == '__main__':
    ImodReconstructPipeline.main()
