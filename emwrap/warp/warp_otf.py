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
from glob import glob
from datetime import datetime

from emtools.utils import Color, FolderManager, Path, Process
from emtools.jobs import Batch, Args, TsStarBatchManager
from emtools.metadata import StarFile, Table


from .warp import WarpBasePipeline
from .warp_mctf import WarpMotionCtf
from .warp_aretomo import WarpAreTomo
from .warp_ctfrec import WarpCtfReconstruct


class WarpOTF(WarpBasePipeline):
    """ Warp wrapper to the following steps in streaming:
        - warp_mctf
        - warp_aretomo
        - warp_ctfrec
    """
    name = 'emw-warp-otf'

    def get_preprocessing_proc(self, gpu):

        def _preprocessing(batch):
            tsName = batch['tsName']
            batch.mkdir('mdocs')
            mdocFn = batch.join('mdocs', f"{tsName}.mdoc")
            shutil.copy(batch['tsMdoc'], mdocFn)
            # Make a copy to avoid populating the current batch info
            # with all the sub-steps timings
            batchCopy = Batch(batch)

            def _run(_class, args, **kwargs):
                with batch.execute(_class.__name__):
                    # Create processing pipeline of sub-steps, 
                    # but passing the batch path as the output folder 
                    args['input_tiltseries'] = kwargs['inputTs']
                    step = _class(args, batch.path)
                    step.gain = os.path.basename(self.gain) if self.gain else None
                    step.gpuList = [int(gpu)]
                    step.runBatch(batchCopy, importInputs=False, **kwargs)  # Only first do the import

            # 0. Write first the tilt_series.star file expected as input for Mctf
            rowDict = dict(batch['rowDict'])
            tsStarFn = rowDict['rlnTomoTiltSeriesStarFile']
            localTsStarFn = batch.join(os.path.basename(tsStarFn))
            shutil.copy(tsStarFn, localTsStarFn)
            tsTable = StarFile.getTableFromFile(tsName, tsStarFn)

            rowDict['rlnTomoTiltSeriesStarFile'] = localTsStarFn
            rowDict['rlnTomoMdocFile'] = mdocFn
            rowDict['rlnTomoTiltSeriesPixelSize'] = rowDict['rlnMicrographOriginalPixelSize']  # FIXME: take into account motion cor binning
            table = Table.fromDict(rowDict)
            inputTs = batch.join('tilt_series.star')
            with StarFile(inputTs, 'w') as sf:
                sf.writeTable('global', table, timeStamp=True)

            def _subargs(key):
                """ Special subargs to remove the first prefix only. """ 
                return self._args.subset(key, '')

            # 1. Run Motion Correction and CTF Estimation
            mctf_args = _subargs('mctf')
            
            # mctf_args['input_tiltseries'] = inputTs

            self.log(f"OTF - MCTF arguments: {mctf_args}")
            
            mctf_args['fs_motion_and_ctf.perdevice'] = self._args['perdevice']
            # Both here and reconstruct step need to use the same halfmap_frames setting
            mctf_args['fs_motion_and_ctf.out_average_halves'] = self._args['ctfrec.ts_reconstruct.halfmap_frames']
            _run(WarpMotionCtf, mctf_args, inputTs=inputTs)

            # 2. Run Alignment

            wat_args = _subargs('wat')
            wat_args['ts_import.override_axis'] = tsTable[0].rlnTomoNominalTiltAxisAngle

            #mctf_args['input_tiltseries'] = inputTs
            _run(WarpAreTomo, wat_args, inputTs=inputTs)

            # 3. Run CTF Reconstruction
            # Update some CTF parameters from MCTF
            ctf_args = _subargs('ctfrec')
            key_map_exceptions = {
                'range_low': 'range_min',
                'range_high': 'range_max',
            }
            for ctf_key in ['range_low', 'range_high', 'defocus_min', 'defocus_max', 'window']:
                input_key = key_map_exceptions.get(ctf_key, ctf_key)
                ctf_args[f'ts_ctf.{ctf_key}'] = mctf_args[f'fs_motion_and_ctf.c_{input_key}']

            _run(WarpCtfReconstruct, ctf_args, inputTs=inputTs)

            return batch

        return _preprocessing

    def _move_batch_files(self, batch):
        """ Move the outputs from the batch folder to the main output folder. """
        # Files to copy only once
        filesToCopy = ['note.txt', 'warp_tiltseries.settings', 'warp_frameseries.settings']
        for fn in filesToCopy:
            if batch.exists(fn) and not self.exists(fn):
                shutil.copy(batch.join(fn), self.join(fn))

        # Copy results from the batch folder to the main output
        for d in WarpBasePipeline.WARP_FOLDERS:
            for root, dirs, files in os.walk(batch.join(d)):
                relRoot = batch.relpath(root)
                src = FolderManager(batch.join(relRoot))
                dst = FolderManager(self.join(relRoot))
                for d2 in dirs:
                    if not dst.exists(d2):
                        dst.mkdir(d2)
                for fn in files:
                    if fn != 'processed_items.json':
                        shutil.move(src.join(fn), dst.join(fn))

    def _createOutputFM(self, folderName, clean=False):
        fm = FolderManager(self.join(folderName))
        if clean or not fm.exists():
            fm.create()
        return fm

    def _output_single_item(self, tsDict, create=False):
        """ Register Relion metadata for one tilt series into the OTF outputs.

        Args:
            tsDict: tilt-series row dict (updated in place)
            create: if True, recreate tilt_series/ and mdocs/ folders
        """
        # Register MCTF Relion metadata from moved frameseries outputs
        self._createOutputFM('tilt_series', clean=create)
        mdocsFm = self._createOutputFM('mdocs', clean=create)

        tsName = tsDict['rlnTomoName']

        def _append_global_star(starFile, rowDict):
            if create or not os.path.exists(starFile):
                table = Table.fromDict(rowDict)
            else:
                table = StarFile.getTableFromFile('global', starFile)
                table.addRowValues(**rowDict)
            self.write_ts_table('global', table, starFile)

        alignedStar = self.join('aligned_tilt_series.star')
        tomogramsStar = self.join('tomograms.star')

        mdocFile = tsDict.get('rlnTomoMdocFile', '')
        ok, newTsTable, _ = self.updateMctfTsDict(tsDict, mdocFile, mdocsFm)
        if ok:
            self.write_ts_table(tsName, newTsTable, tsDict['rlnTomoTiltSeriesStarFile'])
            # Enrich with AreTomo alignment labels (rewrites the per-TS star)
            ok, _ = self.updateAlignTsDict(tsDict)
            if ok:
                _append_global_star(alignedStar, tsDict)

                ok, _ = self.updateCtfRecTsDict(tsDict)
                if ok:
                    _append_global_star(tomogramsStar, tsDict)
                else:
                    self.log(f"WARNING: Could not register CTF/reconstruction output for TS {tsName}")
            else:
                self.log(f"WARNING: Could not register alignment output for TS {tsName}")
        else:
            self.log(f"WARNING: Could not register MCTF output for TS {tsName}")

        if not self.exists('RELION_OUTPUT_NODES.star'):
            self.writeRelionOutputNodes([
                [alignedStar, 'TomogramGroupMetadata.star.emwrap.TiltSeriesAligned'],
                [tomogramsStar, 'TomogramGroupMetadata.star.relion.tomo.Tomograms']                
            ])

    def _output_all(self):
        """ Method to generated the output only. """
        for i, row in enumerate(self.inputTs):
            tsDict = row._asdict()
            self._output_single_item(tsDict, create=not i)  # Create only the first time

    def _output(self, batch):
        tsName = batch['tsName']
        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
        else:
            self._move_batch_files(batch)
            tsDict = dict(batch['rowDict'])
            # Prefer the original mdoc path from the batch if not already in the row
            if not tsDict.get('rlnTomoMdocFile'):
                tsDict['rlnTomoMdocFile'] = batch.get('tsMdoc', '')
            self._output_single_item(tsDict, create=False)
            batch.info['name'] = tsName
            self.updateBatchInfo(batch)

        return batch

    def prerun(self):
        self.inputTs = StarFile.getTableFromFile('global', self._args['input_tiltseries'])

        if self._register_output_only():
            return self._output_all()

        # Create output folders
        for d in self.WARP_FOLDERS:
            self.mkdir(d)
        
        self.gain = self.acq.get('gain', None)
        batchMgr = TsStarBatchManager(self.inputTs, self.tmpDir)
        g = self.addGenerator(batchMgr.generate, queueMaxSize=len(self.gpuList))


        self.addGpuProcessors(g, self.get_preprocessing_proc, self._output)


if __name__ == '__main__':
    WarpOTF.main()
