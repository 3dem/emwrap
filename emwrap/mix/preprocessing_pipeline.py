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
import threading
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

        self.gpuList = args['gpu'].split()
        self.outputDirs = {}
        self.inputStar = args['in_movies']
        self.batchSize = args.get('batch_size', 32)
        self.inputTimeOut = args.get('input_timeout', 3600)
        self.acq = None  # will be read later
        self.moviesImport = None
        self._totalInput = self._totalOutput = 0
        self._pp_args = args['preprocessing']

        # Create a lock to estimate the extraction args only once,
        # for the first batch processed
        self._particle_size = self._pp_args['picking'].get('particle_size', None)
        self._particle_size_lock = threading.Lock()

    @property
    def particle_size(self):
        return self._pp_args.get('picking', {}).get('particle_size', None)

    @particle_size.setter
    def particle_size(self, value):
        self._pp_args['picking']['particle_size'] = value

    def dumpArgs(self, printMsg=''):
        argStr = json.dumps(self._args, indent=4) + '\n'
        with open(self.join('args.json'), 'w') as f:
            f.write(argStr)
        if printMsg:
            self.log(f"{Color.cyan(printMsg)}: \n\t{Color.bold(argStr)}")

    def prerun(self):
        self.dumpArgs(printMsg="Input args")
        self.log(f"Batch size: {Color.cyan(str(self.batchSize))}")
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}")

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
                self.log(f"Waiting for input star file: {self.inputStar}")
                time.sleep(30)  # wait until the star file is being generated
        else:
            self.acq = RelionStar.get_acquisition(self.inputStar)

        # Create all required output folders
        for d in ['Micrographs', 'CTFs', 'Coordinates', 'Particles', 'Logs']:
            self.outputDirs[d] = self.mkdir(d)

        # Define the current pipeline with generator and processors
        g = self.addMoviesGenerator(self.inputStar, self.batchSize,
                                    inputTimeOut=self.inputTimeOut,
                                    queueMaxSize=4)
        c = self.addProcessor(g.outputQueue, self._count,
                              queueMaxSize=4)
        outputQueue = None
        self.log(f"Creating {len(self.gpuList)} processing threads.")
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
            with self._particle_size_lock:
                if self.particle_size is None:
                    batch.log(f"{Color.warn('Estimating the boxSize.')}")
                    pp = Preprocessing(self._pp_args)
                    result = pp.process_batch(batch, gpu=gpu)
                    # This should update particle_size and other args
                    self._pp_args.update(pp.args)
                    self.dumpArgs('Updated args')

                    return result

            batch.log(f"{Color.warn('Using existing boxSize.')}")

            return Preprocessing(self._pp_args).process_batch(batch, gpu=gpu)

        return _preprocessing

    def _move(self, batch):
        try:
            """ Move output files from the batch to the final destination. """
            batch.log("Moving results.")
            t = Timer()
            # Move output files
            for d in ['Micrographs', 'CTFs', 'Coordinates']:
                Process.system(f"mv {batch.join(d, '*')} {self.join(d)}",
                               print=batch.log, color=Color.bold)

            for root, dirs, files in os.walk(batch.join('Particles')):
                for name in files:
                    if name.endswith('.mrcs'):
                        shutil.move(os.path.join(root, name), self.outputDirs['Particles'])

            batch.info.update({
                'move_elapsed': str(t.getElapsedTime())
            })
            return batch
        except Exception as e:
            print(Color.red('ERROR: ' + str(e)))
            import traceback
            traceback.print_exc()
            sys.exit(1)

    def _output(self, batch):
        """ Update output STAR files. """
        def _pair(name):
            return self.join(name), batch.join(name)

        try:
            batch.log("Storing outputs.")
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
                            sf.writeTable('optics', sfBatch.getTable('optics'))
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
                      f"({Color.bold('%0.2f' % percent)} %)")

            return batch
        except Exception as e:
            print(Color.red('ERROR: ' + str(e)))
            import traceback
            traceback.print_exc()
            sys.exit(1)


def main():
    p = argparse.ArgumentParser(prog='emw-preprocessing')
    p.add_argument('--json',
                   help="Input all arguments through this JSON file. "
                        "Other arguments passed will override the options"
                        "in this file. ")
    p.add_argument('--in_movies', '-i')
    p.add_argument('--output', '-o')
    p.add_argument('--scratch', '-s',
                   help="Scratch directory where to keep intermediate results. ")
    p.add_argument('--batch_size', '-b', type=int)
    p.add_argument('--particle_size', '-p', type=int,
                   help="Size of the particle in A.")
    p.add_argument('--j', help="Just to ignore the threads option from Relion")
    p.add_argument('--gpu', '-g', nargs='*')

    args = p.parse_args()

    with open(args.json) as f:
        input_args = json.load(f)

        for key in ['in_movies', 'output', 'scratch', 'batch_size']:
            if value := getattr(args, key):
                input_args[key] = value

        if args.gpu:
            input_args['gpu'] = ' '.join(g for g in args.gpu)

        mc = PreprocessingPipeline(input_args)
        mc.run()


if __name__ == '__main__':
    main()
