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
import json
import argparse
import math
import time
from glob import glob
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import MdocBatchManager, Args, Batch
from emtools.metadata import Mdoc, StarFile, Table

from emwrap.base import ProcessingPipeline


class SlabifyPipeline(ProcessingPipeline):
    """ Pipeline to run Slabify. """
    name = 'emw-slabify'
    input_name = 'in_movies'

    def __init__(self, input_args):
        ProcessingPipeline.__init__(self, input_args)
        args = self._args
        self.loader = args.get('loader', os.environ.get('SLABIFY_LOADER', None))
        self.batchSize = args.get('batch_size', 8)
        self.extraArgs = args['slabify'].get('extra_args', {})
        self.cpus = args['cpus']
        self.tomogramsPattern = self._args['in_movies']

    def _generateBatch(self):
        self._batch_items = []
        self._batch_count = 0

        def _createBatch():
            self._batch_count += 1
            batchId = f"batch_{self._batch_count}"
            b = Batch(id=batchId, index=self._batch_count,
                      path=os.path.join(self.tmpDir, batchId),
                      items=self._batch_items)
            self._batch_items = []
            return b

        for tomoFn in glob(self.tomogramsPattern):
            self._batch_items.append({'tomo': tomoFn})
            if len(self._batch_items) == self.batchSize:
                yield _createBatch()

        if len(self._batch_items):
            yield _createBatch()

    def _slabify(self, batch):
        for item in batch['items']:
            try:
                tomoName = item['tomo']
                slabName = Path.replaceBaseExt(tomoName, '_slab.mrc')
                item['tomoSlab'] = self.join('slabs', slabName)
                batch.log(f"{Color.cyan(batch['id'])}: Running slabify for {Color.bold(tomoName)} ", flush=True)
                # TODO: Run slabify
                # TODO: Parse slab thickness
                args = Args({
                    self.loader: "slabify",
                    "-i":  tomoName,
                    "-o": item['tomoSlab'],
                    "--measure": "",
                })
                args.update(self.extraArgs)
                batch.log(f"Args: {args.toLine()}")
                item['thickness'] = [0, 0]
                p = Process(*args.toList())
                for line in p.lines():
                    # Slab mask thickness with Z offset: 1209.04 Å
                    if 'Slab mask thickness with' in line:
                        i = 0 if 'without Z offset' in line else 1
                        item['thickness'][i] = float(line.split()[-2]) / 10  # In NM
                    batch.log(line)

            except Exception as e:
                batch.log(Color.red(f"ERROR for tomo: {tomoName}: {str(e)}"))
                item['error'] = e

        return batch

    def _output(self, batch):
        batch.log(f"Storing batch {batch['id']} output", flush=True)
        tomoStar = self.join('tomograms.star')
        firstTime = not os.path.exists(tomoStar)
        # Update micrographs.star
        with StarFile(tomoStar, 'a') as sf:
            if firstTime:
                sf.writeTimeStamp()
                tomoTable = Table(columns=['rlnTomoName',
                                           'tomoSlab',
                                           'slabThickness',
                                           'slabThicknessWithOffset'])
                sf.writeHeader('tomograms', tomoTable)
            for item in batch['items']:
                values = [os.path.basename(item['tomo']),
                          item.get('tomoSlab', 'None')]
                values += item.get('thickness', [0, 0])
                sf.writeRowValues(values)
        return batch

    def prerun(self):
        self.log(f"Running Slabify in parallel over {Color.cyan(self.cpus)} threads.", flush=True)
        self.mkdir("slabs")

        g = self.addGenerator(self._generateBatch)
        outputQueue = None
        for _ in range(self.cpus):
            p = self.addProcessor(g.outputQueue, self._slabify,
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue
        self.addProcessor(outputQueue, self._output)


def main():
    SlabifyPipeline.runFromArgs()


if __name__ == '__main__':
    main()
