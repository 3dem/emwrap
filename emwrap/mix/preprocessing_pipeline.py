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
import time
import shutil
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile

from emwrap.base import ProcessingPipeline, Acquisition
from emwrap.relion import RelionStar, RelionImportMovies
from .preprocessing import Preprocessing


class PreprocessingPipeline(ProcessingPipeline):
    """ Pipeline to run Preprocessing in batches. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)
        self._args = args

        self.gpuList = args['gpu_list'].split()
        self.outputDirs = {}
        self.inputStar = args['input_star']
        self.batchSize = args.get('batch_size', 32)
        self.acq = None  # will be read later
        self.moviesImport = None
        self._totalInput = self._totalOutput = 0

        self._pp_args = args['preprocessing_args']
        self.preprocessing = Preprocessing(self._pp_args)

    def prerun(self):
        argStr = json.dumps(self._args, indent=4) + '\n'
        with open(self.join('args.json'), 'w') as f:
            f.write(argStr)
        print(f">>> Args: \n\t{Color.cyan(argStr)}"
              f">>> Batch size: {Color.cyan(str(self.batchSize))}\n"
              f">>> Using GPUs: {Color.cyan(str(self.gpuList))}")

        if '*' in self.inputStar:  # input is a pattern
            self.acq = Acquisition(self._pp_args['acquisition'])
            # Update some of the args for the import
            args = dict(self._args)
            args['movies_pattern'] = self.inputStar
            args['acquisition'] = self.acq
            self.moviesImport = RelionImportMovies(**args)
            self.moviesImport.start()
            self.inputStar = self.moviesImport.outputStar
            while not os.path.exists(self.inputStar):
                time.sleep(30)  # wait until the star file is being generated
        else:
            self.acq = RelionStar.get_acquisition(self.inputStar)

        # Create all required output folders
        for d in ['Micrographs', 'CTFs', 'Coordinates', 'Particles', 'Logs']:
            self.outputDirs[d] = p = self.mkdir(d)

        # Define the current pipeline with generator and processors
        g = self.addMoviesGenerator(self.inputStar, self.batchSize)
        c = self.addProcessor(g.outputQueue, self._count)
        outputQueue = None
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(c.outputQueue,
                                  self.get_preprocessing(gpu))
            m = self.addProcessor(p.outputQueue,
                                  self._move, outputQueue=outputQueue)
            outputQueue = m.outputQueue

        self.addProcessor(outputQueue, self._output)

    def postrun(self):
        """ This method will be called after the run. """
        if self.moviesImport is not None:
            self.moviesImport.join()

    def _count(self, batch):
        """ Just count input items. """
        with self.outputLock:
            self._totalInput += len(batch['items'])

        return batch

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
        def _pair(name):
            return self.join(name), batch.join(name)

        t = Timer()
        with self.outputLock:
            micsStar, micsStarBatch = _pair('micrographs.star')
            firstTime = not os.path.exists(micsStar)
            partStack = {}  # map micrograph to output stack of particles
            # Update micrographs.star
            with StarFile(micsStar, 'a') as sf:
                with StarFile(micsStarBatch) as sfBatch:
                    micsTable = sfBatch.getTable('micrographs')
                    if firstTime:
                        sf.writeTimeStamp()
                        sf.writeTable('optics', sfBatch.getTable('optics'))
                        sf.writeHeader('micrographs', micsTable)
                    for row in micsTable:
                        micName = row.rlnMicrographName
                        stackName = os.path.join('Particles', Path.replaceBaseExt(micName, '.mrcs'))
                        partStack[micName] = self.fixOutputPath(stackName)
                        sf.writeRow(self.fixOutputRow(row,
                                                      'rlnMicrographName',
                                                      'rlnCtfImage',
                                                      'rlnMicrographCoordinates'))

            # Update coordinates.star
            coordStar, coordStarBatch = _pair('coordinates.star')
            with StarFile(coordStar, 'a') as sf:
                with StarFile(coordStarBatch) as sfBatch:
                    coordsTable = sfBatch.getTable('coordinate_files')
                    if firstTime:
                        sf.writeTimeStamp()
                        sf.writeHeader('coordinate_files', coordsTable)
                    for row in coordsTable:
                        sf.writeRow(self.fixOutputRow(row,
                                                      'rlnMicrographName',
                                                      'rlnMicrographCoordinates'))

            # Update particles.star
            partStar, partStarBatch = _pair('particles.star')
            with StarFile(partStar, 'a') as sf:
                with StarFile(partStarBatch) as sfBatch:
                    partTable = sfBatch.getTable('particles')
                    if firstTime:
                        sf.writeTimeStamp()
                        sf.writeHeader('particles', partTable)
                    for row in partTable:
                        micName = row.rlnMicrographName
                        i = row.rlnImageName.split('@')[0]
                        sf.writeRow(row._replace(rlnImageName=f"{i}@{partStack[micName]}",
                                                 rlnMicrographName=self.fixOutputPath(micName)))

            batch.info.update({
                'output_elapsed': str(t.getElapsedTime())
            })
            self.updateBatchInfo(batch)
            self._totalOutput += len(batch['items'])
            percent = self._totalOutput * 100 / self._totalInput
            print(f">>> Processed {Color.green(str(self._totalOutput))} out of "
                  f"{Color.red(str(self._totalInput))} "
                  f"({Color.bold('%0.2f' % percent)}")

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
    p.add_argument('--scratch', '-s',
                   help="Scratch directory where to keep intermediate results. ")
    p.add_argument('--batch_size', '-b', type=int, default=8)
    p.add_argument('--j', help="Just to ignore the threads option from Relion")
    p.add_argument('--gpu', nargs='*')

    args = p.parse_args()

    if args.json:
        raise Exception("JSON input not yet implemented.")
    else:
        with open(args.preprocessing_config) as f:
            preprocessing_args = json.load(f)

        argsDict = {
            'input_star': args.in_movies,
            'output_dir': args.output,
            'scratch_dir': args.scratch,
            'gpu_list': ' '.join(g for g in args.gpu),
            'batch_size': args.batch_size,
            'preprocessing_args': preprocessing_args
        }
        mc = PreprocessingPipeline(argsDict)
        mc.run()


if __name__ == '__main__':
    main()
