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
        self._batches = {}
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
                self.updateBatchInfo(batch)
                batch.log(f"Completed batch in {batch.info['elapsed']},"
                          f"total batches: {len(self.info['batches'])}", flush=True)
        return batch

    def prerun(self):
        minSize = self._args['batch_size']
        timeout = self._args.get('timeout', 3600)
        self.dumpArgs(printMsg="Input args")
        self.log(f"Batch size: {Color.cyan(str(minSize))}")
        self.log(f"Input timeout (s): {Color.cyan(str(timeout))}")
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}", flush=True)

        self.mkdir('Classes2D')

        batchMgr = StarBatchManager(self.tmpDir, self._args['in_particles'],
                                    self._args.get('group_column', None),
                                    minSize=minSize, timeout=timeout)
        g = self.addGenerator(batchMgr.generate, queueMaxSize=4)
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


if __name__ == '__main__':
    main()
