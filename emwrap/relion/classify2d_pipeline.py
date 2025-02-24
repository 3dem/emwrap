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
        self._groupColumn = groupColumn
        self._minSize = kwargs.get('minSize', 0)
        self._wait = kwargs.get('wait', 60)
        self._timeout = timedelta(seconds=kwargs.get('timeout', 7200))
        self._lastCheck = None  # Last timestamp when input was checked
        self._lastUpdate = None  # Last timestamp when new items were found
        self._startIndex = 0  # FIXME We might read this value for restarting
        self._count = 0
        self._rows = []
        self._batches = {}

    def generate(self):
        """ Generate batches based on the input items. """
        while not self.timedOut():
            mTime = datetime.fromtimestamp(os.path.getmtime(self._inputStar))
            now = datetime.now()
            if self._lastCheck is None or mTime > self._lastCheck:
                for batch in self._createNewBatches():
                    self._lastUpdate = now
                    yield batch
            self._lastCheck = datetime.now()
            time.sleep(self._wait)

        # After timeout, let's check if there are any remaining items
        # for the last batch, it will take all remaining items
        # We keep the loop for simplicity, but it should be only one batch
        # as this point
        for batch in self._createNewBatches(last=True):
            yield batch

    def _createNewBatches(self, last=False):
        with StarFile(self._inputStar) as sf:
            tOptics = sf.getTable('optics')
            tParticles = sf.getTableInfo('particles')
            rows = []
            lastValue = None

            for i, row in enumerate(sf.iterTable('particles', start=self._startIndex)):
                value = getattr(row, self._groupColumn)
                if lastValue is not None and lastValue != value and len(rows) > self._minSize:
                    yield self._createBatch(tOptics, tParticles, rows)
                    rows = []
                rows.append(row)
                lastValue = value

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
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)
        self._args = args
        self.gpuList = args['gpu'].split()

    def get_rln2d_proc(self, gpu):
        def _rln2d(batch):
            try:
                batch.log(f"{Color.warn('Running 2D classification')}. Items: {batch['items']}")
                rln2d = RelionClassify2D()
                rln2d.process_batch(batch, gpu=gpu)
                rln2d.clean_iter_files(batch)
            except Exception as e:
                batch['error'] = str(e)
            return batch

        return _rln2d

    def _output(self, batch):
        if 'error' in batch:
            batch.log(Color.red(f"ERROR: {batch['error']}"))
        else:
            batch.log(f"Moving output files.")
            iterFiles = RelionClassify2D().get_iter_files(batch)[0]
            print(f">>>>> Iter files: ", iterFiles)
            missing = [fn for fn in iterFiles.values() if not batch.exists(fn)]
            if missing:
                batch['error'] = f"Missing files: {missing}"
                batch.log(Color.red(f"ERROR: {batch['error']}"))
            else:
                Process.system(f"rm {batch.join('*moment.mrcs')}", print=batch.log)
                Process.system(f"mv {batch.path} {self.join('Classes2D')}", print=batch.log)
                self.info['outputs'].append(
                    {'label': f'Classes2D_{batch.id}',
                     'files': [
                         [iterFiles.get('data', 'data:None'), 'ParticleGroupMetadata.star.relion.class2d'],
                         [iterFiles.get('optimiser', 'optimiser:None'), 'ProcessData.star.relion.optimiser.class2d']
                     ]})
        return batch

    def prerun(self):
        args = self._args  # shortcut
        minSize = args['batch_size']
        timeout = args.get('input_timeout', 3600)
        self.log(f"Minimum batch size: {Color.cyan(str(minSize))}")
        self.log(f"Input timeout (s): {Color.cyan(str(timeout))}")
        self.mkdir('Classes2D')
        batchMgr = StarBatchManager(self.tmpDir, args['in_particles'],
                                    'rlnMicrographName',
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
    p = argparse.ArgumentParser(prog='emw-rln2d-pp')
    p.add_argument('--json',
                   help="Input all arguments through this JSON file. "
                        "The other arguments will be ignored. ")
    p.add_argument('--in_particles', '-i')
    p.add_argument('--output', '-o')
    p.add_argument('--scratch', '-s', default='',
                   help="Scratch directory where to store intermediate "
                        "results of the processing. ")
    p.add_argument('--batch_size', '-b', type=int)
    p.add_argument('--j', help="Threads used within each 2D batch")
    p.add_argument('--gpu', '-g', nargs='*')

    args = p.parse_args()

    with open(args.json) as f:
        input_args = json.load(f)

        for key in ['in_particles', 'output', 'scratch', 'batch_size']:
            if value := getattr(args, key):
                input_args[key] = value

        if args.gpu:
            input_args['gpu'] = ' '.join(g for g in args.gpu)

        rln2d = Relion2DPipeline(input_args)
        rln2d.run()


if __name__ == '__main__':
    main()
