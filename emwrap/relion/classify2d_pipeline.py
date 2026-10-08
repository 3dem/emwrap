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
import subprocess
import pathlib
import sys
import time
import json
import argparse
from datetime import timedelta, datetime
from glob import glob

from emtools.utils import Color, Timer, Path, Process, FolderManager, Pretty
from emtools.jobs import Batch, Args
from emtools.metadata import Mdoc, StarFile, Table

from emwrap.base import ProcessingPipeline
from .classify2d import RelionClassify2D


class StarBatchManager(FolderManager):
    """ Batch manager for input particles, grouped by Micrograph or GridSquare.
    """

    def __init__(self, outputPath, inputStar, groupColumn, **kwargs):
        """
        Args:
            outputPath: path where the batches folder will be created
            inputStar: input particles star file.
            groupColumn: column used to group particles.
                Usually gridSquare or micrographName
            minSize: minimum size for each batch
        """
        FolderManager.__init__(self, outputPath)
        self._inputStar = inputStar
        self._outputPath = outputPath

        self._minSize = kwargs.get('minSize', 0)
        self._sleep = kwargs.get('sleep', 60)
        self._timeout = timedelta(seconds=kwargs.get('timeout', 7200))
        self._lastCheck = None  # Last timestamp when input was checked
        self._lastUpdate = None  # Last timestamp when new items were found
        self._startIndex = 0  # FIXME We might read this value for restarting
        self._count = 0
        self._rows = []
        self.log = kwargs.get('log', print)
        self._groupColumn = groupColumn
        self._lastValue = None  # used with groupColum to create new batches

    def generate(self):
        """ Generate batches based on the input items. """
        while not self.timedOut():
            if os.path.exists(self._inputStar):
                mTime = datetime.fromtimestamp(os.path.getmtime(self._inputStar))
                now = datetime.now()
                if self._lastCheck is None or mTime > self._lastCheck:
                    self.log(f"Reading star file: {self._inputStar}, "
                             "checking for new batches.", flush=True)
                    for batch in self._createNewBatches():
                        self._lastUpdate = now
                        yield batch
                self._lastCheck = datetime.now()
            time.sleep(self._sleep)

        # After timeout, let's check if there are any remaining items
        # for the last batch, it will take all remaining items
        # We keep the loop for simplicity, but it should be only one batch
        # as this point
        for batch in self._createNewBatches(last=True):
            yield batch

    def _batchCondition(self, row, rows):
        if self._groupColumn is not None:
            value = getattr(row, self._groupColumn)
            r = (self._lastValue is not None and
                 self._lastValue != value and
                 len(rows) > self._minSize)
            self._lastValue = value
        else:  # We are not grouping by any column in this case
            r = len(rows) > self._minSize

        return r

    def _createNewBatches(self, last=False):
        with StarFile(self._inputStar) as sf:
            tOptics = sf.getTable('optics')
            tParticles = sf.getTableInfo('particles')

            if self._groupColumn is None and self._minSize == 0:  # Take all
                rows = [row for row in sf.iterTable('particles', start=self._startIndex)]
                yield self._createBatch(tOptics, tParticles, rows)
            else:
                rows = []

                for row in sf.iterTable('particles', start=self._startIndex):
                    if self._batchCondition(row, rows):
                        yield self._createBatch(tOptics, tParticles, rows)
                        rows = []
                    rows.append(row)

                if rows and last:
                    yield self._createBatch(tOptics, tParticles, rows)

    def _createBatch(self, tOptics, tParticles, rows):
        self._count += 1
        batch_id = f'batch{self._count:02}'
        batch = Batch(id=batch_id,
                      index=self._count,
                      items={'start': self._startIndex, 'count': len(rows)},
                      path=self.join(batch_id))
        batch.create()
        self._startIndex += len(rows)

        outStarFile = batch.join('particles.star')
        with StarFile(outStarFile, 'w') as sfOut:
            sfOut.writeTimeStamp()
            sfOut.writeTable('optics', tOptics)
            sfOut.writeHeader('particles', tParticles)
            for row in rows:
                sfOut.writeRow(row)

        return batch

    def timedOut(self):
        """ Return True when there has been timeout seconds
        since last new items were found. """
        if self._lastCheck is None or self._lastUpdate is None:
            return False
        else:
            self.log(f"Checking timeout: self._lastCheck - self._lastUpdate: "
                     f"{Pretty.delta(self._lastCheck - self._lastUpdate)}")
            return (self._lastCheck - self._lastUpdate) > self._timeout


class Relion2DPipeline(ProcessingPipeline):
    """ Run Relion 2D classification in batches of the input particles.

    The output is a single MultiClasses2D: a STAR file (classes2d.star)
    with one row per batch (a set of 2D classes) and the Relion files of
    its last iteration. """
    name = 'emw-rln2d'

    MULTI_CLASSES2D = 'classes2d.star'
    # Columns of the MultiClasses2D STAR file ('classes2d' table)
    MULTI_CLASSES2D_COLUMNS = ['batchId', 'classesCount', 'particlesCount',
                               'pixelSize', 'optimiserStar', 'modelStar',
                               'dataStar', 'classesStack']

    def __init__(self, input_args, output):
        ProcessingPipeline.__init__(self, input_args, output)
        # One processing thread per GPU group (see get_gpu_groups), with
        # Relion's --gpu syntax, e.g. "0,1 2,3" (two threads with two GPUs each)
        self.gpuList = [','.join(str(g) for g in group)
                        for group in self.get_gpu_groups(self._args.get('gpus', '1'))]
        if not self.gpuList:
            raise Exception("Relion 2D classification requires at least one GPU (gpus param)")
        # Launcher empty means using EMWRAP_CONFIG['programs']['RELION']
        self._rln2d_args = {
            'launcher': self._args.get('launcher_relion') or None,
            'extra_args': Args.fromString(self._args.get('extra_args') or '')
        }

    def get_rln2d_proc(self, gpu):
        def _rln2d(batch):
            try:
                batch.log(f"{Color.warn('Running 2D classification')}. "
                          f"Items: {batch['items']} "
                          f"GPU = {gpu}", flush=True)
                rln2d = RelionClassify2D(**self._rln2d_args)
                rln2d.process_batch(batch, gpu=gpu)
                if self.do_clean():
                    rln2d.clean_iter_files(batch)
            except Exception as e:
                batch['error'] = str(e)
            return batch

        return _rln2d

    def _output(self, batch):
        iterFiles = {}
        if not batch.error:
            # Files of the last iteration (others might be kept if EMWRAP_CLEAN=0)
            allIterFiles = RelionClassify2D.get_iter_files(batch)
            iterFiles = allIterFiles[max(allIterFiles)] if allIterFiles else {}
            if not iterFiles:
                batch.error = f"No output files."
            elif missing := [k for k in ['optimiser', 'model', 'data', 'classes']
                             if k not in iterFiles]:
                batch.error = f"Missing output files: {missing}"
            elif missing := [fn for fn in iterFiles.values() if not batch.exists(fn)]:
                batch.error = f"Missing files: {missing}"

        if batch.error:
            batch.log(Color.red(f"ERROR: {batch.error}"))
        else:
            if self.do_clean():
                Process.system(f"rm {batch.join('*moment.mrcs')}", print=batch.log)
            Process.system(f"mv {batch.path} {self.join('Classes2D')}", print=batch.log)
            # Batch files are now in the Classes2D folder
            classesDir = self.join('Classes2D', os.path.basename(batch.path))

            with self.outputLock:
                batch.info['index'] = batch['index']
                batch.info['items'] = batch['items']
                batch.info['path'] = classesDir
                batch.info['classes2d'] = self._classes2dInfo(classesDir, iterFiles)
                self.updateBatchInfo(batch)
                self._writeMultiClasses2D()
                batch.log(f"Completed batch in {batch.info['_elapsed']},"
                          f"total batches: {len(self.info['batches'])}", flush=True)
        return batch

    def _classes2dInfo(self, classesDir, iterFiles):
        """ Info of the 2D classes of a batch (stored in the batch info),
        with the files of the last iteration (relative to the project). """
        def _path(key):
            return self.fixOutputPath(os.path.join('Classes2D',
                                                   os.path.basename(classesDir),
                                                   iterFiles[key]))

        modelStar = os.path.join(classesDir, iterFiles['model'])
        with StarFile(modelStar) as sf:
            classes = sf.getTableSize('model_classes')
            ps = float(sf.getTable('model_general')[0].rlnPixelSize)
        with StarFile(os.path.join(classesDir, iterFiles['data'])) as sf:
            particles = sf.getTableSize('particles')

        return {
            'classes': classes,
            'particles': particles,
            'pixelSize': ps,
            'optimiser': _path('optimiser'),
            'model': _path('model'),
            'data': _path('data'),
            'stack': _path('classes')
        }

    def _batchClasses2dInfo(self, batchId, batchInfo):
        """ Return the classes 2D info of a batch, computing it from the
        batch folder for batches processed before it was stored. """
        if 'classes2d' not in batchInfo:
            classesDir = self.join('Classes2D', batchId)
            if not os.path.exists(classesDir):
                return None
            allIterFiles = RelionClassify2D.get_iter_files(Batch(id=batchId, path=classesDir))
            iterFiles = allIterFiles[max(allIterFiles)] if allIterFiles else {}
            if any(k not in iterFiles for k in ['optimiser', 'model', 'data', 'classes']):
                return None
            batchInfo['classes2d'] = self._classes2dInfo(classesDir, iterFiles)
        return batchInfo['classes2d']

    def _writeMultiClasses2D(self):
        """ Write the MultiClasses2D STAR file, with one row per batch,
        and register it as the only output of the job. """
        t = Table(columns=self.MULTI_CLASSES2D_COLUMNS)
        for batchId, batchInfo in sorted(self.info['batches'].items()):
            if c := self._batchClasses2dInfo(batchId, batchInfo):
                t.addRowValues(batchId, c['classes'], c['particles'],
                               '%0.5f' % c['pixelSize'], c['optimiser'],
                               c['model'], c['data'], c['stack'])

        multiStar = self.join(self.MULTI_CLASSES2D)
        tmpStar = multiStar + '.tmp'
        with StarFile(tmpStar, 'w') as sf:
            sf.writeTable('classes2d', t, timeStamp=True)
        os.replace(tmpStar, multiStar)

        self.outputs = {
            'MultiClasses2D': {
                'label': 'MultiClasses2D',
                'files': [[multiStar, 'ProcessData.star.emwrap.MultiClasses2D']]
            }
        }
        self.writeRelionOutputNodes(
            [f for o in self.outputs.values() for f in o['files']])
        self.writeInfo()

    def generate_batches(self):
        """ Use a StarBatchManager to generate processing batches from the input
        StarFile. If a given batch was already processed, we will skip it. """
        batchMgr = StarBatchManager(self.tmpDir, self._args['in_particles'],
                                    self._args.get('group_column') or None,
                                    minSize=self._minSize,
                                    timeout=self._timeout,
                                    sleep=self._sleep,
                                    log=self.log)

        batches = {b for b in self.info.get('batches', {})}

        for batch in batchMgr.generate():
            if batch is None:
                break

            if batch['id'] in batches:
                self.log(f"Skipping batch ID: {batch['id']} because it is "
                         f"already processed.")
            else:
                yield batch

    def prerun(self):
        self._minSize = int(self._args['batch_size'])
        # Wait times in seconds
        self._timeout = int(self._args.get('wait.timeout', 3600))
        self._sleep = int(self._args.get('wait.sleep', 60))
        self.log(f"Batch size: {Color.cyan(str(self._minSize))}")
        self.log(f"Input timeout (s): {Color.cyan(str(self._timeout))}")
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}", flush=True)

        g = self.addGenerator(self.generate_batches, queueMaxSize=4)

        if self.exists('Classes2D'):
            if batches := self.info['batches']:
                self.log(f"Existing output batches: {len(batches)}")
                # Update the output from existing batches (e.g. when
                # previous runs registered one Classes2D output per batch)
                with self.outputLock:
                    self._writeMultiClasses2D()
        else:
            self.mkdir('Classes2D')

        outputQueue = None
        self.log(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            self.log(f"Creating processor for gpu: {gpu}")
            p = self.addProcessor(g.outputQueue,
                                  self.get_rln2d_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.log(f"Adding output processor")
        self.addProcessor(outputQueue, self._output)


def create_subset():
    pattern = os.path.join('Classes2D', 'batch*', 'run_it*model.star')
    missing = Color.red('MISSING')

    files = glob(pattern)
    files.sort()

    sfOut = StarFile('particles.star', 'w')
    firstTime = True

    for fn in files:
        print("Found model: ", fn)
        ptsFn = fn.replace('_model', '_data')
        if os.path.exists(ptsFn):
            print("   Data: ", ptsFn)
        else:
            print("   Data: ", missing)
        selFn = fn + '.selection'
        if os.path.exists(selFn):
            with open(selFn) as f:
                selection = json.load(f)
            print("   Selection: ", selFn, f"({len(selection)} classes)")

        else:
            print("   Selection: ", missing)
            selection = []

        with StarFile(ptsFn) as sf:
            if partTable := sf.getTable('particles'):
                if firstTime:
                    sfOut.writeTimeStamp()
                    sfOut.writeTable('optics', sf.getTable('optics'))
                    sfOut.writeHeader('particles', partTable)
                discarded = 0
                for row in partTable:
                    clsNumber = int(row.rlnClassNumber)
                    if not selection or clsNumber in selection:
                        sfOut.writeRow(row)
                    else:
                        discarded += 1
                print(f">>> Discarded {Color.red(discarded)} particles")
        firstTime = False

    sfOut.close()


def register_outputs():
    run = FolderManager(os.getcwd())

    with open('info.json') as f:
        info = json.load(f)

    for batchFolder in sorted(os.listdir(run.join('tmp'))):
        clsBatch = run.join('Classes2D', batchFolder)
        tmpBatch = run.join('tmp', batchFolder)
        classes = os.path.join(tmpBatch, 'run_it200_classes.mrcs')
        if not os.path.exists(clsBatch) and os.path.exists(classes):
            cmd = f"mv {tmpBatch} {clsBatch}"
            print(cmd)
            os.system(cmd)
            info['batches'][batchFolder] = {
                'path': clsBatch
            }

    print("Writing info to info2.json")
    with open('info2.json', 'w') as f:
        json.dump(info, f, indent=4)


if __name__ == '__main__':
    if '--create_subset' in sys.argv:
        create_subset()
    elif '--register_outputs' in sys.argv:
        register_outputs()
    else:
        Relion2DPipeline.main()
