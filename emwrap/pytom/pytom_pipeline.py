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
import json
import argparse
import time
import sys
from glob import glob
from datetime import datetime, timedelta

from emtools.utils import Color, FolderManager, Path, Process
from emtools.metadata import StarFile, Acquisition
from emtools.jobs import Batch
from emtools.image import Image
from emwrap.base import ProcessingPipeline

from .pytom import PyTom


class PyTomPipeline(ProcessingPipeline):
    """ Pipeline PyTom picking in a set of tomograms. """
    name = 'emw-pytom'
    input_name = 'in_movies'

    def __init__(self, input_args):
        ProcessingPipeline.__init__(self, input_args)
        self.gpuList = self._args['gpu'].split(',')
        self.inputTomograms = self._args['in_movies']
        self.timeout = timedelta(seconds=self._args.get('timeout', 3600))
        self.wait = self._args.get('wait', 60)
        self.acq = self.loadAcquisition()

    def get_pytom_proc(self, gpu):

        def _pytom(batch):
            pytom = PyTom(self.acq, **self._args['pytom'])
            pytom.process_batch(batch, gpu=gpu)
            return batch

        return _pytom

    def _output(self, batch):
        tsName = batch['tsName']
        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
        else:
            Process.system(f"mv {batch.join('output', '*')} {self.join('Coordinates')}")
            self.updateBatchInfo(batch)

        return batch

    def __getTomoDict(self, recFolder, tomoPattern):
        if tomograms := [os.path.basename(fn) for fn in glob(tomoPattern)]:
            # Get tomo suffix to remove from filenames and get the matching tsName
            tomoSuffix = tomograms[0].split('_')[-1]
            return {t.replace(tomoSuffix, ''): os.path.join(recFolder, t) for t in tomograms}
        else:
            return {}

    def _getInputTomograms(self):
        """ Create a generator for input tomograms. """
        # Let's assume that at the same level, there is a warp_tiltseries folder
        baseDir = os.path.dirname(Path.rmslash(self.inputTomograms))
        tiltseriesFolder = os.path.join(baseDir, 'warp_tiltseries')
        recFolder = os.path.join(tiltseriesFolder, 'reconstruction')
        tomoPattern = os.path.join(recFolder, '*Apx.mrc')

        self.info['inputs'] = [
            {'tomograms': self.inputTomograms}
        ]
        self.writeInfo()
        self.log(f">>> Tomograms pattern: {tomoPattern}")


        counter = 0
        # For now, let input with the tomostar folder
        pattern = os.path.join(self.inputTomograms, '*.tomostar')

        last_found = datetime.now()
        now = datetime.now()
        seen = set()

        while now - last_found < self.timeout:
            self.log("Checking for new tomograms.")
            tomoDict = self.__getTomoDict(recFolder, tomoPattern)

            def _newTomo(fn):
                tsName = Path.removeBaseExt(fn)
                return fn not in seen and f"{tsName}_" in tomoDict

            if newFiles := [fn for fn in glob(pattern) if _newTomo(fn)]:
                self.log(f"Found new tomograms: {str(newFiles)}", flush=True)

                for fn in newFiles:
                    # Let's create a batch for this tomogram
                    tsName = Path.removeBaseExt(fn)
                    nowPrefix = datetime.now().strftime('%y%m%d-%H%M%S')
                    counter += 1
                    batchId = f"{nowPrefix}_{counter:02}_{tsName}"

                    tomoFn = tomoDict[f'{tsName}_']

                    # Let's read the tilt_angles and the dose_accumulation from the .tomostar file
                    with StarFile(fn) as sf:
                        t = sf.getTable('', guessType=False)
                    yield Batch(id=batchId, index=counter,
                                path=os.path.join(self.tmpDir, batchId),
                                tsName=tsName, tomogram=tomoFn,
                                tilt_angles=[float(row.wrpAngleTilt) for row in t],
                                dose_accumulation=[float(row.wrpDose) for row in t])
                    last_found = now
                    seen.add(fn)
                    # else:
                    #     pass
                    #     # Note: In streaming we don't know if the tomograms are read
                    #     # In warp all tomostar are generated
                    #     self.log(f"ERROR: Reconstructed tomogram was not found "
                    #              f"for name: {tsName}")

            else:
                self.log("No new tomograms found, sleeping.")

            time.sleep(self.wait)
            now = datetime.now()


    def prerun(self):
        g = self.addGenerator(self._getInputTomograms)
        outputQueue = None
        self.mkdir('Coordinates')
        self.log(f"Creating {len(self.gpuList)} processing threads.", flush=True)

        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_pytom_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


def main():
    PyTomPipeline.runFromArgs()


if __name__ == '__main__':
    main()
