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
import re
import json
import time
import shutil
import traceback
from glob import glob

from emtools.utils import FolderManager, Path
from emtools.jobs import Batch, TsStarBatchManager
from emtools.metadata import StarFile, Table


from .warp import WarpBasePipeline
from .warp_mctf import WarpMotionCtf
from .warp_tsalign import WarpTsAlign
from .warp_ctfrec import WarpCtfReconstruct


class WarpOTF(WarpBasePipeline):
    """ Warp wrapper to the following steps in streaming:
        - warp_mctf
        - warp_tsalign
        - warp_ctfrec

    Each tilt series is processed in its own batch folder (tmp/<batch_id>),
    running the three steps one after the other. When a batch is done, its
    Warp files are merged into the job folder (fixing the paths that Warp
    stores pointing to the batch folder) and the Relion STAR files are
    updated. The outputs (aligned_tilt_series.star and tomograms.star) should
    be equivalent to the ones from running WarpMctf + WarpTsAlign + WarpCtfRec.
    """
    name = 'emw-warp-otf'

    # Output STAR files, relative to the job folder
    ALIGNED_STAR = 'aligned_tilt_series.star'
    TOMOGRAMS_STAR = 'tomograms.star'
    FAILED_STAR = 'failed_tilt_series.star'
    LOGS = 'logs'  # run.out of each batch, kept after cleaning tmp

    # Warp XML files store absolute paths to the folder of the input data
    DATA_DIRECTORY_RE = re.compile(r'DataDirectory="([^"]*)"')
    PROCESSED_ITEMS = 'processed_items.json'
    EXIT_CODE_RE = re.compile(r'^# Exit code: (-?\d+)', re.MULTILINE)

    def __init__(self, args, output):
        self._waitInput(args)
        WarpBasePipeline.__init__(self, args, output)
        self._failed = {}  # tsName -> reason

    def _tsalignArgs(self):
        """Build WarpTsAlign args from the OTF wat.* parameter group."""
        tsalign_args = self._args.subset('wat', '')
        # Needed to load the acquisition when creating the step
        tsalign_args['input_tiltseries'] = self._args['input_tiltseries']
        if perdevice := self._args.get('perdevice'):
            tsalign_args['perdevice'] = perdevice
        if launcher := self._args.get('launcher_warp'):
            tsalign_args.setdefault('launcher_warp', launcher)
        return tsalign_args

    def _tsalignStep(self):
        """Return a WarpTsAlign view of this job for alignment output handling."""
        step = getattr(self, '_tsalign_step', None)
        if step is None:
            step = WarpTsAlign(self._tsalignArgs(), self.path)
            self._tsalign_step = step
        return step

    # ------------------- Processing of each tilt series ---------------------
    def _mctfArgs(self):
        mctf_args = self._args.subset('mctf', '')
        if launcher := self._args.get('launcher_warp'):
            mctf_args.setdefault('launcher_warp', launcher)
        mctf_args['fs_motion_and_ctf.perdevice'] = self._args['perdevice']
        # Both here and reconstruct step need to use the same halfmap_frames setting
        mctf_args['fs_motion_and_ctf.out_average_halves'] = self._args.get(
            'ctfrec.ts_reconstruct.halfmap_frames', False)
        return mctf_args

    def _ctfrecArgs(self, mctf_args):
        ctf_args = self._args.subset('ctfrec', '')
        if launcher := self._args.get('launcher_warp'):
            ctf_args.setdefault('launcher_warp', launcher)
        # Update some CTF parameters from MCTF
        key_map_exceptions = {
            'range_low': 'range_min',
            'range_high': 'range_max',
        }
        for ctf_key in ['range_low', 'range_high', 'defocus_min', 'defocus_max', 'window']:
            input_key = key_map_exceptions.get(ctf_key, ctf_key)
            value = mctf_args.get(f'fs_motion_and_ctf.c_{input_key}')
            if value not in (None, ''):
                ctf_args[f'ts_ctf.{ctf_key}'] = value

        if perdevice := self._args.get('perdevice'):
            ctf_args['ts_ctf.perdevice'] = perdevice
        return ctf_args

    def _runStep(self, _class, args, batch, gpu, inputTs):
        """ Run a sub-step (Mctf, TsAlign or CtfRec) in the batch folder.
        Raise an exception if the step failed (Python error or any program
        with a non-zero exit code), so the next steps are not run. """
        args['input_tiltseries'] = inputTs
        step = _class(args, batch.path)
        step.gain = os.path.basename(self.gain) if self.gain else None
        step.gpuList = [int(gpu)]
        # Use a different batch object, to not populate the current batch
        # info with all the sub-steps timings and to check its errors
        stepBatch = Batch(id=batch.id, path=batch.path)
        logFile = batch.join('run.out')
        logStart = os.path.getsize(logFile) if os.path.exists(logFile) else 0
        with batch.execute(_class.__name__):
            step.runBatch(stepBatch, importInputs=False, inputTs=inputTs)

        if batch.error or stepBatch.error:
            raise Exception(f"{_class.__name__} failed:\n"
                            f"{batch.error or stepBatch.error}")

        if os.path.exists(logFile):
            with open(logFile) as f:
                f.seek(logStart)
                codes = [int(c) for c in self.EXIT_CODE_RE.findall(f.read())]
            if any(codes):
                raise Exception(f"{_class.__name__} failed, exit codes {codes}")
        return step

    def _checkMctf(self, step, tsName, tsTable):
        missing = []
        for row in tsTable:
            prefix = Path.removeBaseExt(row.rlnMicrographMovieName)
            for fn in [step.join(step.FS, prefix + '.xml'),
                       step.join(step.FS, 'average', prefix + '.mrc')]:
                if not os.path.exists(fn):
                    missing.append(fn)
        if missing:
            raise Exception(f"WarpMotionCtf: {len(missing)} missing output "
                            f"file(s) for TS {tsName}, e.g. {missing[0]}")

    def _checkTsAlign(self, step, tsName):
        files = [step.alignedTS(tsName), *step.alignmentFiles(tsName)]
        if missing := [fn for fn in files if not os.path.exists(fn)]:
            raise Exception(f"WarpTsAlign: missing alignment output for "
                            f"TS {tsName}: {', '.join(missing)}")

    def _checkCtfRec(self, step, tsName):
        recPath = step.join(step.TS, 'reconstruction')
        if not glob(os.path.join(recPath, f'{tsName}_*.mrc')):
            raise Exception(f"WarpCtfReconstruct: missing reconstructed "
                            f"tomogram for TS {tsName} in {recPath}")

    def get_preprocessing_proc(self, gpu):

        def _preprocessing(batch):
            tsName = batch['tsName']
            try:
                _process(batch, tsName)
            except Exception as e:
                # Never let the exception kill the processing thread
                batch.error = f"{e}\n{traceback.format_exc()}"
            return batch

        def _process(batch, tsName):
            batch.mkdir('mdocs')
            mdocFn = batch.join('mdocs', f"{tsName}.mdoc")
            shutil.copy(batch['tsMdoc'], mdocFn)

            # 0. Write first the tilt_series.star file expected as input for Mctf
            rowDict = dict(batch['rowDict'])
            tsStarFn = rowDict['rlnTomoTiltSeriesStarFile']
            localTsStarFn = batch.join(os.path.basename(tsStarFn))
            shutil.copy(tsStarFn, localTsStarFn)
            tsTable = StarFile.getTableFromFile(tsName, tsStarFn)

            rowDict['rlnTomoTiltSeriesStarFile'] = localTsStarFn
            rowDict['rlnTomoMdocFile'] = mdocFn
            if self.gain:  # Possibly the upsampled EER gain
                rowDict['rlnMicrographGainName'] = self.gain
            # Use the actual Mctf target pixel size (accounts for
            # create_settings.bin_angpix / mctf.create_settings.bin_angpix),
            # not just the raw/original one, so that WarpTsAlign's
            # create_settings call later gets the correct --bin_angpix
            # and produces correct Image/Volume dimensions when Mctf bins.
            # It also accounts for EER upsampling (mctf.eerSampling).
            _, rowDict['rlnTomoTiltSeriesPixelSize'] = self.mctfPs(
                rowDict['rlnMicrographOriginalPixelSize'],
                tsTable[0].rlnMicrographMovieName)
            table = Table.fromDict(rowDict)
            inputTs = batch.join('tilt_series.star')
            with StarFile(inputTs, 'w') as sf:
                sf.writeTable('global', table, timeStamp=True)

            # 1. Run Motion Correction and CTF Estimation
            mctf_args = self._mctfArgs()
            self.log(f"OTF - MCTF arguments: {mctf_args}")
            step = self._runStep(WarpMotionCtf, mctf_args, batch, gpu, inputTs)
            self._checkMctf(step, tsName, tsTable)

            # 2. Run Alignment
            tsalign_args = self._tsalignArgs()
            self.log(f"OTF - TS align arguments: {tsalign_args}")
            step = self._runStep(WarpTsAlign, tsalign_args, batch, gpu, inputTs)
            self._checkTsAlign(step, tsName)

            # 3. Run CTF Reconstruction
            ctf_args = self._ctfrecArgs(mctf_args)
            self.log(f"OTF - CTF/reconstruction arguments: {ctf_args}")
            step = self._runStep(WarpCtfReconstruct, ctf_args, batch, gpu, inputTs)
            self._checkCtfRec(step, tsName)

        return _preprocessing

    # ------------------- Merging of batch results ---------------------------
    def _fixDataDirectory(self, xmlFile):
        """ Warp XML files store the absolute path of the data folder
        (frames for movies, warp_tomostar for tilt series), which points
        to the batch folder. Make it point to the same folder in this job. """
        with open(xmlFile, encoding='utf-8-sig') as f:
            content = f.read()

        def _replace(m):
            folder = os.path.basename(m.group(1).rstrip('/'))
            return f'DataDirectory="{os.path.join(self._absPath, folder)}"'

        newContent = self.DATA_DIRECTORY_RE.sub(_replace, content)
        if newContent != content:
            # Keep the BOM written by Warp
            with open(xmlFile, 'w', encoding='utf-8-sig') as f:
                f.write(newContent)

    def _mergeProcessedItems(self, src, dst):
        """ Merge Warp processed_items.json files (list of items with 'Path')
        """
        def _load(fn):
            if os.path.exists(fn):
                with open(fn, encoding='utf-8-sig') as f:
                    return json.load(f) or []
            return []

        items = {item['Path']: item for item in _load(dst)}
        items.update((item['Path'], item) for item in _load(src))
        with open(dst, 'w') as f:
            json.dump(list(items.values()), f, indent=2)

    def _linkFrames(self, batch):
        """ Link the movies of the batch into the job 'frames' folder,
        used as DataFolder in warp_frameseries.settings. """
        framesFm = FolderManager(self.join(self.FRAMES))
        for item in batch.get('items', []):
            movieFn = item['rlnMicrographMovieName']
            if not os.path.lexists(framesFm.join(os.path.basename(movieFn))):
                framesFm.link(movieFn)

    def _move_batch_files(self, batch):
        """ Move the outputs from the batch folder to the main output folder. """
        # Files to copy only once
        filesToCopy = ['note.txt', self.TSS, self.FSS]
        for fn in filesToCopy:
            if batch.exists(fn) and not self.exists(fn):
                shutil.copy(batch.join(fn), self.join(fn))

        self._linkFrames(batch)

        # Move results from the batch folder to the main output
        for d in self.WARP_FOLDERS:
            for root, dirs, files in os.walk(batch.join(d)):
                relRoot = batch.relpath(root)
                src = FolderManager(batch.join(relRoot))
                dst = FolderManager(self.join(relRoot))
                for d2 in dirs:
                    if not dst.exists(d2):
                        dst.mkdir(d2)
                for fn in files:
                    if fn == self.PROCESSED_ITEMS:
                        self._mergeProcessedItems(src.join(fn), dst.join(fn))
                        continue
                    if fn.endswith('.xml') and relRoot in (self.FS, self.TS):
                        self._fixDataDirectory(src.join(fn))
                    if os.path.lexists(dst.join(fn)):
                        os.remove(dst.join(fn))
                    shutil.move(src.join(fn), dst.join(fn))

    def _saveBatchLog(self, batch, tsName):
        if batch.exists('run.out'):
            shutil.copy(batch.join('run.out'), self.join(self.LOGS, f'{tsName}.out'))

    def _cleanBatch(self, batch):
        if self.do_clean() and batch.exists():
            shutil.rmtree(batch.path, ignore_errors=True)

    # ------------------- Output registration --------------------------------
    def _createOutputFM(self, folderName, clean=False):
        fm = FolderManager(self.join(folderName))
        if clean or not fm.exists():
            fm.create()
        return fm

    def _resetOutputs(self):
        """ Remove outputs from a previous run, rows are appended to the
        STAR files as tilt series are processed. """
        for fn in [self.ALIGNED_STAR, self.TOMOGRAMS_STAR, self.FAILED_STAR,
                   'RELION_OUTPUT_NODES.star']:
            if self.exists(fn):
                os.remove(self.join(fn))
        self._createOutputFM('tilt_series', clean=True)
        self._createOutputFM('mdocs', clean=True)
        self._createOutputFM(self.LOGS)
        self._failed = {}

    def _fail(self, tsName, reason):
        self._failed[tsName] = reason
        self.log(f"WARNING: TS {tsName} failed: {reason}")

    def _output_single_item(self, tsDict):
        """ Register Relion metadata for one tilt series into the OTF outputs.

        Args:
            tsDict: tilt-series row dict (updated in place)
        """
        mdocsFm = self._createOutputFM('mdocs')
        tsName = tsDict['rlnTomoName']

        mdocFile = tsDict.get('rlnTomoMdocFile', '')
        ok, newTsTable, _ = self.updateMctfTsDict(tsDict, mdocFile, mdocsFm)
        if not ok:
            return self._fail(tsName, "missing motion correction/CTF output")

        self.write_ts_table(tsName, newTsTable, tsDict['rlnTomoTiltSeriesStarFile'])
        # Enrich with alignment labels (rewrites the per-TS star)
        ok, _ = self._tsalignStep().updateAlignTsDict(tsDict)
        if not ok:
            return self._fail(tsName, "missing alignment output")
        self.appendGlobalTsRow(self.join(self.ALIGNED_STAR), tsDict)

        ok, _ = self.updateCtfRecTsDict(tsDict)
        if not ok:
            return self._fail(tsName, "missing CTF/reconstruction output")
        self.appendGlobalTsRow(self.join(self.TOMOGRAMS_STAR), tsDict)

        self._write_output_nodes()

    def _write_output_nodes(self):
        """Register Relion output nodes for the existing STAR outputs."""
        nodes = []
        if self.exists(self.ALIGNED_STAR):
            nodes.append([self.fixOutputPath(self.ALIGNED_STAR),
                          'TomogramGroupMetadata.star.emwrap.TiltSeriesAligned'])
        if self.exists(self.TOMOGRAMS_STAR):
            nodes.append([self.fixOutputPath(self.TOMOGRAMS_STAR),
                          'TomogramGroupMetadata.star.relion.tomo.Tomograms'])
        if nodes:
            self.writeRelionOutputNodes(nodes)

    def _resolve_mdoc_file(self, tsDict):
        """Return an existing mdoc path for this tilt series."""
        tsName = tsDict['rlnTomoName']
        for candidate in (
            tsDict.get('rlnTomoMdocFile', ''),
            self.join('mdocs', f'{tsName}.mdoc'),
        ):
            if candidate and os.path.exists(candidate):
                return candidate
        return tsDict.get('rlnTomoMdocFile', '')

    def _output_all(self):
        """ Method to generated the output only. """
        self._resetOutputs()
        for row in self.inputTs:
            tsDict = row._asdict()
            tsDict['rlnTomoMdocFile'] = self._resolve_mdoc_file(tsDict)
            self._output_single_item(tsDict)

    def _output(self, batch):
        tsName = batch['tsName']
        batch.log(f"Storing output for batch '{tsName}'", flush=True)
        batch.info['name'] = tsName

        try:
            self._saveBatchLog(batch, tsName)
            if batch.error:
                self._fail(tsName, f"{batch.error.splitlines()[0]} (check "
                                   f"{self.join(self.LOGS, tsName + '.out')})")
                batch.log(f"ERROR: {batch.error}")
            else:
                self._move_batch_files(batch)
                tsDict = dict(batch['rowDict'])
                tsDict['rlnTomoMdocFile'] = self._resolve_mdoc_file({
                    **tsDict,
                    'rlnTomoMdocFile': tsDict.get('rlnTomoMdocFile') or batch.get('tsMdoc', ''),
                })
                self._output_single_item(tsDict)
        except Exception as e:
            # Never let the exception kill the output thread
            batch.error = f"{e}\n{traceback.format_exc()}"
            self._fail(tsName, f"output error: {e}")
            batch.log(f"ERROR: {batch.error}")

        self.updateBatchInfo(batch)
        self._cleanBatch(batch)
        return batch

    def _sortedTable(self, starFile, order):
        table = StarFile.getTableFromFile('global', starFile, guessType=False)
        rows = sorted(table, key=lambda r: order.get(r.rlnTomoName, len(order)))
        newTable = Table(table.getColumnNames())
        for row in rows:
            newTable.addRowValues(**row._asdict())
        return newTable

    def _writeFinalOutputs(self):
        """ Sort the outputs in the input order, write the failed tilt series
        and check that some output was produced (failing the job if not). """
        inputTable = StarFile.getTableFromFile('global', self._args['input_tiltseries'],
                                               guessType=False)
        order = {row.rlnTomoName: i for i, row in enumerate(inputTable)}

        tables = {}
        for fn in [self.ALIGNED_STAR, self.TOMOGRAMS_STAR]:
            if self.exists(fn):
                tables[fn] = self._sortedTable(self.join(fn), order)
                self.write_ts_table('global', tables[fn], self.join(fn))
            else:
                tables[fn] = Table(['rlnTomoName'])

        failedTable = Table(inputTable.getColumnNames())
        for row in inputTable:
            if row.rlnTomoName in self._failed:
                failedTable.addRowValues(**row._asdict())
        if len(failedTable):
            self.write_ts_table('global', failedTable, self.join(self.FAILED_STAR))
            for row in failedTable:
                self.log(f"  {row.rlnTomoName}: {self._failed[row.rlnTomoName]}")

        self._write_output_nodes()
        self.checkOutputTs(tables[self.TOMOGRAMS_STAR], failedTable, 'tilt-series')

    def _waitInput(self, args, sleep=30):
        """ When running on-the-fly, the input might not exist yet or be
        empty (e.g. the import job just started), so wait until it has
        some tilt series (or 'input_timeout' seconds). It is called before
        the base constructor, that loads the acquisition from the input. """
        inputStar = args['input_tiltseries']
        timeout = int(args.get('input_timeout', 0) or 0)
        start = time.time()
        while True:
            try:
                if len(StarFile.getTableFromFile('global', inputStar)):
                    return
            except Exception:
                pass
            if time.time() - start > timeout:
                raise Exception(f"No input tilt series found in {inputStar}")
            self.log(f"Waiting for input tilt series in {inputStar}", flush=True)
            time.sleep(sleep)

    def prerun(self):
        inputStar = self._args['input_tiltseries']
        self.inputTs = StarFile.getTableFromFile('global', inputStar)
        self._absPath = os.path.abspath(self.path)

        if self._register_output_only():
            return self._output_all()

        self._resetOutputs()
        # Create output folders
        for d in self.WARP_FOLDERS + [self.FRAMES]:
            self.mkdir(d)

        # Upsample the EER gain only once, it is passed to every batch
        first = self.inputTs[0]
        tsTable = StarFile.getTableFromFile(first.rlnTomoName, first.rlnTomoTiltSeriesStarFile)
        self.gain = self.eerGain(self.acq.get('gain', None),
                                 tsTable[0].rlnMicrographMovieName)
        # Link the gain in the job folder, used in warp_frameseries.settings
        if self.gain and not os.path.lexists(self.join(os.path.basename(self.gain))):
            self.link(self.gain)

        # Monitor the input for new tilt series when running on-the-fly
        if self.inputTsStreaming():
            tsItems = self.inputTsMonitor(inputStar).newItems()
        else:
            tsItems = self.inputTs
        batchMgr = TsStarBatchManager(tsItems, self.tmpDir)
        g = self.addGenerator(batchMgr.generate, queueMaxSize=len(self.gpuList))

        self.addGpuProcessors(g, self.get_preprocessing_proc, self._output)

    def postrun(self):
        self._writeFinalOutputs()


if __name__ == '__main__':
    WarpOTF.main()
