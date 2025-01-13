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

from emtools.utils import Color, Timer, Path, Process
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile

from emwrap.base import ProcessingPipeline
from emwrap.relion import RelionStar
from .preprocessing import Preprocessing


class PreprocessingPipeline(ProcessingPipeline):
    """ Pipeline to run Preprocessing in batches. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)

        self.gpuList = args['gpu_list'].split()
        self.outputDirs = {}
        self.inputStar = args['input_star']
        self.batchSize = args.get('batch_size', 32)
        print(Color.red(f"Batch size: {self.batchSize}"))
        self.acq = RelionStar.get_acquisition(self.inputStar)
        pp_args = args['preprocessing_args']
        self.preprocessing = Preprocessing(pp_args)

    def prerun(self):
        # Create all required output folders
        for d in ['Micrographs', 'CTFs', 'Coordinates', 'Particles', 'Logs']:
            self.outputDirs[d] = p = self.join(d)
            if not os.path.exists(p):
                os.mkdir(p)

        # Define the current pipeline with generator and processors
        g = self.addMoviesGenerator(self.inputStar, self.batchSize)
        outputQueue = None
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_preprocessing(gpu))
            m = self.addProcessor(p.outputQueue,
                                  self._move, outputQueue=outputQueue)
            outputQueue = m.outputQueue

        self.addProcessor(outputQueue, self._output)

    def get_preprocessing(self, gpu):
        def _preprocessing(batch):
            return self.preprocessing.process_batch(batch, gpu=gpu)

        return _preprocessing

    def _move(self, batch):
        """ Move output files from the batch to the final destination. """
        t = Timer()
        # Move output files
        for d in ['Micrographs', 'CTFs', 'Coordinates']:
            Process.system(f"mv {batch.join(d, '*')} {self.join(d)}")

        for root, dirs, files in os.walk(batch.join('Particles')):
            for name in files:
                if name.endswith('.mrcs'):
                    shutil.move(os.path.join(root, name), self.outputDirs['Particles'])

        batch.info.update({
            'move_elapsed': str(t.getElapsedTime())
        })
        return batch

    def _output(self, batch):
        """ Update output STAR files. """
        t = Timer()
        with self.outputLock:
            micsStar = self.join('micrographs.star')
            firstTime = not os.path.exists(micsStar)
            micsStarBatch = batch.join('micrographs.star')

            # Update micrographs.star
            with StarFile(micsStar, 'a') as sf:
                with StarFile(micsStarBatch) as sfBatch:
                    micsTable = sfBatch.getTable('micrographs')
                    if firstTime:
                        sf.writeTimeStamp()
                        sf.writeTable('optics', sfBatch.getTable('optics'))
                        sf.writeHeader('micrographs', micsTable)
                    for row in micsTable:
                        sf.writeRow(row)

            # Update coordinates.star
            # todo
            batch.info.update({
                'output_elapsed': str(t.getElapsedTime())
            })
            self.updateBatchInfo(batch)

        return batch


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
