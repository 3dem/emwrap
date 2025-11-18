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
from emtools.metadata import StarFile, Acquisition, StarMonitor
from emtools.jobs import Batch
from emtools.image import Image
from emwrap.base import ProcessingPipeline

from .pytom import PyTom


class PyTomPipeline(ProcessingPipeline):
    """ Pipeline PyTom picking in a set of tomograms. """
    name = 'emw-pytom'
    input_name = 'in_movies'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        # FIXME add support to comma separated values for parallels in batches
        self.gpuList = [self.get_gpu_list(args['gpus'], as_string=True)]

        self.acq = self.loadAcquisition()
        # FIXME: Read this from the input arguments
        self.wait = {
            'timeout': int(args.get('wait.timeout', 60)),
            'file_change': int(args.get('wait.file_change', 30)),
            'sleep': int(args.get('wait.sleep', 30)),
        }

        self.inTomoStar = self._args['input_tomograms']
        self.outTomoStar = self.join('tomograms_coords.star')

        self._pytom_args = {
            'pytom': self.get_subargs('pytom'),
            'pytom_extract': self.get_subargs('pytom_extract')
        }

    def get_pytom_proc(self, gpu):

        def _pytom(batch):
            args = dict(self._pytom_args)
            args['pytom']['g'] = gpu
            pytom = PyTom(self.acq, args)
            pytom.process_batch(batch)
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

    def _getInputTomograms(self):
        """ Create a generator for input tomograms. """
        # Let's create a STAR file monitor to check for incoming tomograms
        # Get the tomograms IDs to avoid processing again that ones
        counter = 0
        blacklist = []
        if os.path.exists(self.outTomoStar):
            blacklist = StarFile.getTableFromFile('global', self.outTomoStar)
            counter = len(blacklist)
            self.log(f"Previously processed tomograms: {Color.cyan(counter)}")

        t = StarFile.getTableFromFile('global', self.inTomoStar)
        n = len(t)
        self.log(f"Input star file: {Color.bold(self.inTomoStar)}")
        self.log(f"Total input tomograms: {Color.bold(n)}")
        self.log(f"Tomograms to process: {Color.green(n - counter)}")

        monitor = StarMonitor(self.inTomoStar, 'global',
                              lambda row: row.rlnTomoName,
                              timeout=self.wait['timeout'],
                              blacklist=blacklist)

        # This will keep monitor the star files for new tomograms until timed out.
        for row in monitor.newItems():
            tsName = row.rlnTomoName
            counter += 1
            nowPrefix = datetime.now().strftime('%y%m%d-%H%M%S')
            batchId = f"{nowPrefix}_{counter:03}_{tsName}"
            # FIXME: Now reading these values from Warp tomostar, but
            # it should be from Relion's star files
            t = StarFile.getTableFromFile('', row.wrpTomostar)
            yield Batch(id=batchId, index=counter,
                        path=os.path.join(self.tmpDir, batchId),
                        tsName=tsName, tomogram=row.rlnTomogram,
                        tilt_angles=[float(r.wrpAngleTilt) for r in t],
                        dose_accumulation=[float(r.wrpDose) for r in t])

        # while now - last_found < self.timeout:
        #     self.log("Checking for new tomograms.")
        #     tomoDict = self.__getTomoDict(recFolder, tomoPattern)
        #
        #     def _newTomo(fn):
        #         tsName = Path.removeBaseExt(fn)
        #         return fn not in seen and f"{tsName}_" in tomoDict
        #
        #     if newFiles := [fn for fn in glob(pattern) if _newTomo(fn)]:
        #         self.log(f"Found new tomograms: {str(newFiles)}", flush=True)
        #
        #         for fn in newFiles:
        #             # Let's create a batch for this tomogram
        #             tsName = Path.removeBaseExt(fn)
        #             nowPrefix = datetime.now().strftime('%y%m%d-%H%M%S')
        #             counter += 1
        #             batchId = f"{nowPrefix}_{counter:02}_{tsName}"
        #
        #             tomoFn = tomoDict[f'{tsName}_']
        #
        #             # Let's read the tilt_angles and the dose_accumulation from the .tomostar file
        #             with StarFile(fn) as sf:
        #                 t = sf.getTable('', guessType=False)
        #             yield Batch(id=batchId, index=counter,
        #                         path=os.path.join(self.tmpDir, batchId),
        #                         tsName=tsName, tomogram=tomoFn,
        #                         tilt_angles=[float(row.wrpAngleTilt) for row in t],
        #                         dose_accumulation=[float(row.wrpDose) for row in t])
        #             last_found = now
        #             seen.add(fn)
                    # else:
                    #     pass
                    #     # Note: In streaming we don't know if the tomograms are read
                    #     # In warp all tomostar are generated
                    #     self.log(f"ERROR: Reconstructed tomogram was not found "
                    #              f"for name: {tsName}")
            #
            # else:
            #     self.log("No new tomograms found, sleeping.")
            #
            # time.sleep(self.wait)
            # now = datetime.now()

    def prerun(self):
        self.info['inputs'] = [
            {'tomograms': self.inTomoStar}
        ]
        self.writeInfo()

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


if __name__ == '__main__':
    PyTomPipeline.main()
