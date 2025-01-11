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

"""

"""

import os
import subprocess
import shutil
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile

from emwrap.base import ProcessingPipeline
from emwrap.relion import RelionStar
from .preprocessing import Preprocessing


class PreprocessingPipeline(ProcessingPipeline):
    """ Pipeline to run Preprocessing in batches. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)

        self.gpuList = args['gpu_list'].split()
        self.outputMicDir = self.join('Micrographs')
        self.inputStar = args['input_star']
        self.batchSize = args.get('batch_size', 32)
        self.acq = RelionStar.get_acquisition(self.inputStar)
        pp_args = args['preprocessing_args']
        self.preprocessing = Preprocessing(pp_args)

    def _build(self):
        g = self.addMoviesGenerator(self.inputStar, self.batchSize)
        outputQueue = None
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_preprocessing(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)

    def get_preprocessing(self, gpu):
        def _preprocessing(batch):
            batch = self.preprocessing.process_batch(batch, gpu=gpu)
            batch.dump_info()
            return batch
        return _preprocessing

    def _output(self, batch):
        return batch

    def prerun(self):
        if not os.path.exists(self.outputMicDir):
            os.mkdir(self.outputMicDir)

        self._build()
        print(f"Batch size: {self.batchSize}")

    def postrun(self):
        pass


def main():
    p = argparse.ArgumentParser(prog='emw-preprocessing')
    p.add_argument('--json',
                   help="Input all arguments through this JSON file. "
                        "The other arguments will be ignored. ")
    p.add_argument('--in_movies', '-i')
    p.add_argument('--preprocessing_config', '-c',
                   help="JSON configuration file with preprocessing options. ")
    p.add_argument('--output', '-o')
    p.add_argument('--batch_size', '-b', type=int, default=8)
    p.add_argument('--j', help="Just to ignore the threads option from Relion")
    p.add_argument('--gpu')

    args = p.parse_args()

    if args.json:
        raise Exception("JSON input not yet implemented.")
    else:
        with open(args.preprocessing_config) as f:
            preprocessing_args = json.load(f)

        argsDict = {
            'input_star': args.in_movies,
            'output_dir': args.output,
            'gpu_list': args.gpu,
            'batch_size': args.batch_size,
            'preprocessing_args': preprocessing_args
        }
        mc = PreprocessingPipeline(argsDict)
        mc.run()


if __name__ == '__main__':
    main()
