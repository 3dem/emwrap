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
from emtools.jobs import Batch
from emtools.metadata import Mdoc, StarFile

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
                    self.log("fReading star file: {self._inputStar}, "
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
            return (self._lastCheck - self._lastUpdate) > self._timeout


class Relion2DPipeline(ProcessingPipeline):
    """ Pipeline specific to AreTomo processing. """
    name = 'emw-rln2d'
    input_name = 'in_particles'

    def __init__(self, all_args):
        args = all_args[self.name]
        ProcessingPipeline.__init__(self, args)
        self._args = args
        self.gpuList = args['gpu'].split()

    def get_rln2d_proc(self, gpu):
        def _rln2d(batch):
            try:
                batch.log(f"{Color.warn('Running 2D classification')}. "
                          f"Items: {batch['items']} "
                          f"GPU = {gpu}", flush=True)
                rln2d = RelionClassify2D(**self._args)
                rln2d.process_batch(batch, gpu=gpu)
                rln2d.clean_iter_files(batch)
            except Exception as e:
                batch['error'] = str(e)
            return batch

        return _rln2d

    def _output(self, batch):
        iterFiles = {}
        if not batch.error:
            iterFiles = next(iter(RelionClassify2D(**self._args).get_iter_files(batch).values()), {})
            if iterFiles is None:
                batch.error = f"No output files."
                batch.log(Color.red(f"ERROR: {batch['error']}"))
            else:
                if missing := [fn for fn in iterFiles.values() if not batch.exists(fn)]:
                    batch.error = f"Missing files: {missing}"

        if batch.error:
            batch.log(Color.red(f"ERROR: {batch.error}"))
        else:
            Process.system(f"rm {batch.join('*moment.mrcs')}", print=batch.log)
            Process.system(f"mv {batch.path} {self.join('Classes2D')}", print=batch.log)
            self.info['outputs'].append(
                {'label': f'Classes2D_{batch.id}',
                 'files': [
                     [iterFiles.get('data', batch.join('data:None')), 'ParticleGroupMetadata.star.relion.class2d'],
                     [iterFiles.get('optimiser', 'optimiser:None'), 'ProcessData.star.relion.optimiser.class2d']
                 ]})
            with self.outputLock:
                batch.info['index'] = batch['index']
                batch.info['items'] = batch['items']
                batch.info['path'] = self.join('Classes2D', os.pathb.basename(batch['path']))
                self.updateBatchInfo(batch)
                batch.log(f"Completed batch in {batch.info['elapsed']},"
                          f"total batches: {len(self.info['batches'])}", flush=True)
        return batch

    def generate_batches(self):
        """ Use a StarBatchManager to generate processing batches from the input
        StarFile. If a given batch was already processed, we will skip it. """
        batchMgr = StarBatchManager(self.tmpDir, self._args['in_particles'],
                                    self._args.get('group_column', None),
                                    minSize=self._minSize,
                                    timeout=self._timeout)

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
        self._minSize = self._args['batch_size']
        self._timeout = self._args.get('timeout', 3600)
        self.dumpArgs(printMsg="Input args")
        self.log(f"Batch size: {Color.cyan(str(self._minSize))}")
        self.log(f"Input timeout (s): {Color.cyan(str(self._timeout))}")
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}", flush=True)

        g = self.addGenerator(self.generate_batches, queueMaxSize=4)

        if self.exists('Classes2D'):
            if batches := self.info['batches']:
                self.log(f"Existing output batches: {len(batches)}")

                # # DEBUGGING
                # def fake_processing(batch):
                #     self.log(f"Processing batch: {str(batch)}")
                #     time.sleep(30)
                #     return batch
                # self.addProcessor(g.outputQueue, fake_processing)
                # return
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


def main():
    Relion2DPipeline.runFromArgs()


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
        main()
