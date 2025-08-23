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
from datetime import datetime

from emtools.utils import Color, FolderManager, Path, Process
from emtools.jobs import Batch, Args, MdocBatchManager

from .warp import WarpBasePipeline
from .warp_mctf import WarpMotionCtf
from .warp_aretomo import WarpAreTomo
from .warp_ctfrec import WarpCtfReconstruct


class WarpPreprocessing(WarpBasePipeline):
    """ Warp wrapper to the following steps in streaming:
        - warp_mctf
        - warp_aretomo
        - warp_ctfrec
    """
    name = 'emw-warp-preprocessing'
    input_name = 'in_movies'

    def get_preprocessing_proc(self, gpu):

        def _preprocessing(batch):

            if self.gain:
                gain = batch.link(self.gain)
            else:
                gain = None

            batch['mdoc'].write(batch.join(f"{batch['tsName']}.mdoc"))
            batch.mkdir('input_frames')
            Process.system(f"mv {batch.join('frames')} {batch.join('*.eer')} "
                           f"{batch.join('input_frames')}/")
            # Make a copy to avoid populating the current batch info
            # with all the sub-steps timings
            batchCopy = Batch(batch)

            def _run(_class, extra_args):
                args = Args(self._input_args)
                args_class = args[_class.name]
                args_class['gpu'] = str(gpu)
                args_class['output'] = batch.path
                args_class['in_movies'] = batch.join('input_frames')
                args_class.update(extra_args)
                step = _class(args)
                step.gain = gain
                step.runBatch(batchCopy, importInputs=False)  # Only first do the import

            # Build sub-pipelines and run them
            with batch.execute('preprocessing'):
                _run(WarpMotionCtf, {'in_movies': 'input_frames/*.eer'})
                ts_import_args = self._input_args["emw-warp-aretomo"]["ts_import"]
                ts_import_args["--mdocs"] = "."
                _run(WarpAreTomo, {"ts_import": ts_import_args})
                _run(WarpCtfReconstruct, {})

            return batch

        return _preprocessing

    def _output(self, batch):
        tsName = batch['tsName']
        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
        else:
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
            #Process.system(f"mv {batch.join('output', '*')} {self.join('Coordinates')}")
            if 'Tomograms' not in self.outputs:
                self.outputs['Tomograms'] = {'label': 'Tomograms', 'names': []}

            self.outputs['Tomograms']['names'].append(batch['tsName'])
            batch.info['name'] = batch['tsName']  # Store TS name
            self.updateBatchInfo(batch)

        return batch

    def prerun(self):
        self.gain = self.acq.get('gain', None)
        batchMgr = MdocBatchManager(self._args['mdocs'], self.tmpDir,
                                    moviesPath=self._args['in_movies'])
        g = self.addGenerator(batchMgr.generate, queueMaxSize=4)
        outputQueue = None

        # Create output folders
        for d in self.WARP_FOLDERS:
            self.mkdir(d)

        self.log(f"Creating {len(self.gpuList)} processing threads.", flush=True)

        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_preprocessing_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


def main():
    WarpPreprocessing.runFromArgs()


if __name__ == '__main__':
    main()
