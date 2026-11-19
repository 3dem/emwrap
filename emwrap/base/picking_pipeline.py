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
import traceback
from datetime import datetime

from emtools.utils import Color
from emtools.metadata import StarFile, StarMonitor, Table, RelionStar
from emtools.jobs import Batch
from emtools.image import Image

from .processing_pipeline import ProcessingPipeline, print_launcher


class PickingPipeline(ProcessingPipeline):
    """ Base pipeline for template matching (particle picking) in a set of
    tomograms, one tomogram per batch, with programs such as PyTom or pyTME.

    Inputs: input_tomograms (tomograms STAR file), gpus and, optionally, the
    launcher. Each tomogram is picked by the object returned by
    _createPicker, whose process_batch(batch, launcher=...) must write in
    the batch 'output' folder the files ending with OUTPUT_SUFFICES,
    including '<tsName>_particles_default.star' (RELION 4 coordinates) and
    '<tsName>_particles_relion5.star' (RELION 5 centered coordinates).

    Outputs: tomograms.star (input rows with rlnCoordinatesMetadata and
    rlnParticleNumber), particles.star, optimisation_set.star and
    failed_tomograms.star, with the per-tomogram files in Coordinates/.
    Each tomogram is picked in its batch folder tmp/<date>_<index>_<tsName>.
    Tomograms that fail are registered as failed (the job continues), with
    their batch folder moved to failed_logs/, and on continue only the
    tomograms not picked or failed before are picked.
    """
    # Program (EMWRAP_CONFIG['programs'] key), label for the logs and
    # sub-arguments (prefixes of the form params) of the wrapped program
    PROGRAM = None
    LABEL = 'Picking'
    SUBARGS = ()

    # Expected suffices of the program output files in one batch
    OUTPUT_SUFFICES = ['particles_default.star',
                       'particles_relion5.star']

    PARTICLES_COLUMNS = [
        'rlnTomoName',
        'rlnCenteredCoordinateXAngst',
        'rlnCenteredCoordinateYAngst',
        'rlnCenteredCoordinateZAngst',
        'rlnAngleRot',
        'rlnAngleTilt',
        'rlnAnglePsi',
        'rlnLCCmax'
    ]

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        # FIXME add support to comma separated values for parallels in batches
        self.gpuList = self.get_gpu_list(args['gpus'])
        # A launcher given in the form takes precedence over the one in
        # EMWRAP_CONFIG
        self.launcher_form = args.get('launcher', '') or None
        self.launcher = (self.launcher_form
                         or ProcessingPipeline.get_launcher(self.PROGRAM))

        self.inTomoStar = self._args['input_tomograms']
        self.acq = self.loadAcquisition(self.inTomoStar)
        self.outTomoStar = self.join('tomograms.star')
        self.outParticlesStar = self.join('particles.star')
        self.outTomoOptimisationSet = self.join('optimisation_set.star')
        self.outFailedStar = self.join('failed_tomograms.star')
        self.failedTable = None

        # FIXME: Read this from the input arguments
        self.wait = {
            'timeout': int(args.get('wait.timeout', 60)),
            'file_change': int(args.get('wait.file_change', 30)),
            'sleep': int(args.get('wait.sleep', 30)),
        }

        # Program arguments, by sub-arguments prefix (SUBARGS)
        self._programArgs = {k: self.get_subargs(k) for k in self.SUBARGS}

    def _createPicker(self, args, gpu):
        """ Create the object that picks one tomogram (batch), with its
        process_batch(batch, launcher=...) method.
        args: copy of the program arguments (by SUBARGS prefix).
        gpu: space separated GPU ids of the processing thread. """
        raise NotImplementedError

    def _gpuGroups(self):
        """ GPUs used by each processing thread (one tomogram at a time),
        by default one GPU per thread. """
        return [[g] for g in self.gpuList]

    def _coordinateValues(self, coord):
        """ Values of the output particles (PARTICLES_COLUMNS) from a row
        of the program coordinates (relion5) STAR file. """
        return {c: getattr(coord, c) for c in self.PARTICLES_COLUMNS}

    def _getPickerProc(self, gpu):

        def _pick(batch):
            # Any exception is stored in the batch, so only this tomogram is
            # registered as failed. Raising here would kill the processing
            # thread (losing its GPUs for the rest of the run).
            if batch.error:  # e.g. the batch could not be created
                return batch
            try:
                args = {k: dict(v) for k, v in self._programArgs.items()}
                picker = self._createPicker(args, gpu)
                picker.process_batch(batch, launcher=self.launcher)
            except Exception as e:
                batch.error = f"{e}\n{traceback.format_exc()}"
            return batch

        return _pick

    def _moveBatchFiles(self, batch):
        tsName = batch['tsName']
        missing = []
        outFiles = {}
        def _out(s):
            return self.join('Coordinates', f'{tsName}_{s}')

        found = {}  # suffix -> batch output file
        for suffix in self.OUTPUT_SUFFICES:
            if files := batch.glob(f'output/*_{suffix}'):
                found[suffix] = files[0]
            else:
                missing.append(suffix)

        # tmp absolute path -> final project path
        copied = {os.path.abspath(f): _out(s) for s, f in found.items()}
        for suffix, f in found.items():
            dst = _out(suffix)
            self._copyOutputFile(batch, suffix, f, dst, copied)
            outFiles[suffix] = dst
        if missing:
            with open(_out('missing.json'), 'w') as f:
                json.dump(missing, f)
        elif os.path.exists(_out('missing.json')):
            os.remove(_out('missing.json'))

        return outFiles

    def _copyOutputFile(self, batch, suffix, src, dst, copied):
        """ Copy one program output file (with this suffix) from the batch
        to the Coordinates folder. copied maps the absolute paths of all
        the batch outputs to their final (project) path, for programs that
        need to rewrite paths inside them. """
        shutil.copy(src, dst)

    def _coordsFile(self, tsName, suffix):
        return self.join('Coordinates', f'{tsName}_{suffix}')

    def _processedTomoNames(self):
        if self.outTable is None:
            return set()
        return {row.rlnTomoName for row in self.outTable if row is not None}

    def _ensureOptimisationSet(self):
        if os.path.exists(self.outTomoOptimisationSet):
            return

        with StarFile(self.outTomoOptimisationSet, 'w') as sf:
            values = {
                'rlnTomoParticlesFile': self.outParticlesStar,
                'rlnTomoTomogramsFile': self.outTomoStar,
            }
            sf.writeTable('optimisation_set', Table.fromDict(values), timeStamp=True)

        self._writeOutputNodes()

    def _writeOutputNodes(self):
        outputNodes = []
        if os.path.exists(self.outTomoOptimisationSet):
            outputNodes.append([self.outTomoOptimisationSet,
                                'TomogramGroupMetadata.star.emwrap.TomoCoordinates'])
        if os.path.exists(self.outFailedStar):
            outputNodes.append([self.outFailedStar,
                                'TomogramGroupMetadata.star.relion.tomo.tomograms-failed'])
        if outputNodes:
            self.writeRelionOutputNodes(outputNodes)

    def _readParticles(self):
        if os.path.exists(self.outParticlesStar):
            return StarFile.getTableFromFile('particles', self.outParticlesStar)
        return Table(columns=self.PARTICLES_COLUMNS)

    def _writeParticles(self, ptsTable):
        with StarFile(self.outParticlesStar, 'w') as sf:
            sf.writeTable('particles', ptsTable, timeStamp=True)

    def _addCoordinates(self, ptsTable, tsName, coordsStar):
        """ Add the particles from a program (relion5) coordinates file. """
        coordsTable = StarFile.getTableFromFile('particles', coordsStar)
        for coord in coordsTable:
            if coord is None:
                continue
            values = self._coordinateValues(coord)
            values['rlnTomoName'] = tsName
            ptsTable.addRowValues(**values)

    def _setParticles(self, tsName, coordsStar=None):
        """ Set the particles of tomogram tsName in the output particles,
        replacing any previous ones (e.g. from a run stopped before the
        tomogram was registered), so particles are never duplicated.
        If coordsStar is None, the tomogram particles are only removed. """
        oldTable = self._readParticles()
        ptsTable = Table(columns=self.PARTICLES_COLUMNS)
        found = False
        for row in oldTable:
            if row is None:
                continue
            if row.rlnTomoName == tsName:
                found = True
            else:
                ptsTable.addRowValues(**{c: getattr(row, c)
                                         for c in self.PARTICLES_COLUMNS})
        if coordsStar:
            self._addCoordinates(ptsTable, tsName, coordsStar)
        elif not found:
            return
        self._writeParticles(ptsTable)

    def _failedTomoNames(self):
        if self.failedTable is None:
            return set()
        return {row.rlnTomoName for row in self.failedTable if row is not None}

    def _writeFailed(self):
        with StarFile(self.outFailedStar, 'w') as sf:
            sf.writeTable('global', self.failedTable,
                          timeStamp=True, computeFormat='left')

    def _registerFailed(self, batch):
        """ Register a tomogram that could not be picked in
        failed_tomograms.star (with its input row), it will not be picked
        again on continue. Only this tomogram fails, not the whole job. """
        tsName = batch['tsName']
        error = str(batch.error or 'Unknown error')
        if tsName in self._processedTomoNames():
            # e.g. an error after it was registered, it was picked anyway
            batch.log(Color.warn(f"WARNING: error after registering "
                                 f"tomogram '{tsName}': {error}"))
            return
        batch.log(Color.red(f"ERROR: tomogram '{tsName}' failed: {error}"))
        try:
            if tsName not in self._failedTomoNames():
                cols = self.failedTable.getColumnNames()
                rowDict = batch['rowDict']
                self.failedTable.addRowValues(
                    **{c: rowDict.get(c, '') for c in cols})
                self._writeFailed()
                self._writeOutputNodes()
            # Remove particles that might have been written before the error
            if tsName not in self._processedTomoNames():
                self._setParticles(tsName)
            reason = error.strip().splitlines()[0] if error.strip() else error
            self.info.setdefault('failed', {})[tsName] = reason
            self.updateBatchInfo(batch)
        except Exception:
            batch.log(Color.red(f"ERROR: could not register failed tomogram "
                                f"'{tsName}': {traceback.format_exc()}"))

    def _writeTomograms(self):
        with StarFile(self.outTomoStar, 'w') as sfOut:
            sfOut.writeTable('global', self.outTable,
                             timeStamp=True, computeFormat='left')

    def _isPicked(self, row):
        """ A tomogram was picked (maybe with 0 particles) if its
        coordinates files exist. Tomograms registered without them
        (e.g. by older versions when the program failed) were not picked. """
        coords = getattr(row, 'rlnCoordinatesMetadata', 'None')
        return (coords not in ('None', '', None) and os.path.exists(coords)
                and os.path.exists(self._coordsFile(
                    row.rlnTomoName, 'particles_relion5.star')))

    def _loadPreviousFailed(self, inColumns):
        """ Load failed tomograms from a previous run (unless they were
        picked after all, then they are removed from the failed ones). """
        if not os.path.exists(self.outFailedStar):
            self.failedTable = Table(inColumns)
            return

        prevTable = StarFile.getTableFromFile('global', self.outFailedStar,
                                              guessType=False)
        self.failedTable = Table(prevTable.getColumnNames())
        picked = self._processedTomoNames()
        seen = set()
        for row in prevTable:
            if row is None or row.rlnTomoName in seen | picked:
                continue
            seen.add(row.rlnTomoName)
            self.failedTable.addRowValues(**row._asdict())

        if len(self.failedTable) != len(prevTable):
            self._writeFailed()

    def _loadPreviousOutput(self):
        """ When continuing, keep only the tomograms that were really picked
        (the others will be picked again) and rebuild the output particles
        from their coordinates files, removing duplicated or stale ones. """
        prevTable = StarFile.getTableFromFile('global', self.outTomoStar,
                                              guessType=False)
        self.outTable = Table(prevTable.getColumnNames())
        seen = set()
        notPicked = []
        for row in prevTable:
            if row is None or row.rlnTomoName in seen:
                continue
            if self._isPicked(row):
                seen.add(row.rlnTomoName)
                self.outTable.addRowValues(**row._asdict())
            else:
                notPicked.append(row.rlnTomoName)

        if notPicked:
            self.log(f"Tomograms that were not picked, will be picked "
                     f"again: {Color.warn(len(notPicked))}")

        if len(self.outTable) != len(prevTable):
            self._writeTomograms()

        ptsTable = Table(columns=self.PARTICLES_COLUMNS)
        for row in self.outTable:
            self._addCoordinates(ptsTable, row.rlnTomoName, self._coordsFile(
                row.rlnTomoName, 'particles_relion5.star'))
        self._writeParticles(ptsTable)

    def _output(self, batch):
        """ Register the output of one tomogram. Any error (in the program
        or here) only marks this tomogram as failed, the job continues. """
        try:
            self._outputPicked(batch)
        except Exception as e:
            batch.error = f"{e}\n{traceback.format_exc()}"

        if batch.error:
            self._registerFailed(batch)
            self._keepFailedBatch(batch)

        return batch

    def _keepFailedBatch(self, batch):
        """ Move the folder of a failed batch (with its logs and inputs)
        from tmp to failed_logs, so it is kept to check the error after
        the tmp folder is cleaned. The batch folder names contain the
        tomogram name (see _getInputTomograms). """
        path = batch.path
        if not path or not os.path.isdir(path):
            return
        try:
            failedDir = self.join('failed_logs')
            os.makedirs(failedDir, exist_ok=True)
            dst = os.path.join(failedDir, os.path.basename(path))
            if os.path.exists(dst):
                shutil.rmtree(dst)
            # tmp can be a link to a scratch folder in another filesystem
            shutil.move(path, dst)
            batch.log(Color.warn(f"Failed batch folder kept in: {dst}"))
        except Exception:
            batch.log(Color.red(f"ERROR: could not keep failed batch folder "
                                f"'{path}': {traceback.format_exc()}"))

    def _outputPicked(self, batch):
        tsName = batch['tsName']

        if batch.error:
            return

        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if tsName in self._processedTomoNames():
            batch.log(
                f"WARNING: tomogram '{tsName}' already in "
                f"{self.outTomoStar}, skipping output")
            return

        outFiles = self._moveBatchFiles(batch)
        required = ['particles_default.star', 'particles_relion5.star']
        if missing := [s for s in required if s not in outFiles]:
            raise Exception(f"Missing output files: {', '.join(missing)}")

        coordsStar = outFiles['particles_default.star']
        t = StarFile.getTableFromFile('particles', coordsStar)
        rowDict = dict(batch['rowDict'])
        rowDict.update({
            'rlnCoordinatesMetadata': coordsStar,
            'rlnParticleNumber': len(t)
        })

        # The particles are written before registering the tomogram, the
        # tomogram is only considered picked after it is in the output
        # tomograms STAR file (see _loadPreviousOutput)
        self._ensureOptimisationSet()
        self._setParticles(tsName, outFiles['particles_relion5.star'])
        self.outTable.addRowValues(**rowDict)
        try:
            self._writeTomograms()
        except Exception:
            # Not registered, so it is not kept in memory either
            self.outTable = self._removeRow(tsName)
            raise

        self._updateInput()
        self.updateBatchInfo(batch)

    def _removeRow(self, tsName):
        t = Table(self.outTable.getColumnNames())
        for row in self.outTable:
            if row is not None and row.rlnTomoName != tsName:
                t.addRowValues(**row._asdict())
        return t

    def _getInputTomograms(self):
        """ Create a generator for input tomograms. """
        # Let's create a STAR file monitor to check for incoming tomograms
        # Get the tomograms IDs to avoid processing again that ones
        counter = 0
        blacklist = []
        self.outTable = None
        inTable = StarFile.getTableFromFile('global', self.inTomoStar,
                                            guessType=False)
        n = len(inTable)
        if os.path.exists(self.outTomoStar):
            self._loadPreviousOutput()
            self.log(f"Previously processed tomograms: "
                     f"{Color.cyan(len(self.outTable))}")
        else:
            extraLabels = ['rlnCoordinatesMetadata', 'rlnParticleNumber']
            self.outTable = Table(inTable.getColumnNames() + extraLabels)

        # Tomograms in tomograms.star or failed_tomograms.star are not
        # picked again, only new ones (remove failed_tomograms.star or
        # some of its rows to retry them)
        self._loadPreviousFailed(inTable.getColumnNames())
        if nFailed := len(self.failedTable):
            self.log(f"Previously failed tomograms (not picked again): "
                     f"{Color.warn(nFailed)}")
        blacklist = list(self.outTable) + list(self.failedTable)
        counter = len(blacklist)

        self.log(f"Input star file: {Color.bold(self.inTomoStar)}")
        self.log(f"Total input tomograms: {Color.bold(n)}")
        self.log(f"Tomograms to process: {Color.green(n - counter)}")

        monitor = StarMonitor(self.inTomoStar, 'global',
                              lambda row: row.rlnTomoName,
                              timeout=self.wait['timeout'],
                              wait=self.wait['sleep'],
                              blacklist=blacklist,
                              tableKwargs={'guessType': False})

        # This will keep monitor the star files for new tomograms until timed out.
        for row in monitor.newItems():
            tsName = row.rlnTomoName
            counter += 1
            nowPrefix = datetime.now().strftime('%y%m%d-%H%M%S')
            batchId = f"{nowPrefix}_{counter:03}_{tsName}"
            batch = Batch(id=batchId, index=counter, rowDict=row._asdict(),
                          path=os.path.join(self.tmpDir, batchId),
                          tsName=tsName)
            # Errors with this tomogram's input are stored in the batch, so
            # only this tomogram fails (the generator thread keeps running)
            try:
                t = StarFile.getTableFromFile(tsName, row.rlnTomoTiltSeriesStarFile,
                                              guessType=False)
                batch.update(
                    tomogram=RelionStar.getTomogram(row),
                    tilt_angles=[float(r.rlnTomoNominalStageTiltAngle) for r in t],
                    dose_accumulation=[float(r.rlnMicrographPreExposure) for r in t])
                if hasattr(row, 'rlnDefocus'):
                    batch['defocus'] = float(row.rlnDefocus)
            except Exception as e:
                batch.error = f"Invalid input: {e}\n{traceback.format_exc()}"

            yield batch

    def _updateInput(self):
        inputTomoTable = StarFile.getTableFromFile('global', self.inTomoStar)
        first = inputTomoTable[0]
        N = len(inputTomoTable)
        if self._dims is None:
            self._dims = Image.get_dimensions(RelionStar.getTomogram(first))
        x, y, n = self._dims
        ps = RelionStar.getTomoPixelSize(first)
        bin = first.rlnTomoTomogramBinning

    def prerun(self):
        self.log(f"Setting up {self.LABEL} picking pipeline.")
        print_launcher('launcher' if self.launcher_form else self.PROGRAM,
                       self.launcher)

        self._dims = None
        self._updateInput()
        self.writeInfo()

        g = self.addGenerator(self._getInputTomograms)
        outputQueue = None
        self.mkdir('Coordinates')
        n = len(self.gpuList)
        self.log(f"Total GPUs: {n}.", flush=True)
        gpu_groups = self._gpuGroups()

        self.log(f"Creating {len(gpu_groups)} processing threads.")

        for gpus in gpu_groups:
            gpuStr = ' '.join(str(g) for g in gpus)
            p = self.addProcessor(g.outputQueue,
                                  self._getPickerProc(gpuStr),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)
