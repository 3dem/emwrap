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
import threading
import time
import shutil
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process
from emtools.metadata import Acquisition, StarFile, RelionStar
from emtools.jobs import Batch

from emwrap.base import ProcessingPipeline
from .preprocessing import Preprocessing


class PreprocessingPipeline(ProcessingPipeline):
    """ Pipeline to run Preprocessing in batches. """
    name = 'emw-preprocessing'
    input_name = 'in_movies'

    def __init__(self, all_args):
        args = all_args[self.name]
        ProcessingPipeline.__init__(self, args)
        self.gpuList = args['gpu'].split()
        self.outputDirs = {}
        self.inputStar = args['in_movies']
        self.batchSize = args.get('batch_size', 32)
        self.inputTimeOut = args.get('input_timeout', 3600)
        self.acq = all_args['acquisition']
        self._totalInput = self._totalOutput = 0
        self._pp_args = args
        self._pp_args['acquisition'] = Acquisition(self.acq)

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

    def prerun(self):
        self.dumpArgs(printMsg="Input args")
        self.log(f"Batch size: {Color.cyan(str(self.batchSize))}")
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}", flush=True)
        inputs = self.info['inputs']
        inputs.append({
            'key': 'input_movies',
            'label': 'Movies',
            'datatype': 'MicrographMovieGroupMetadata.star.relion',
            'files': [self.inputStar]
        })

        # Create all required output folders
        for d in ['Micrographs', 'CTFs', 'Coordinates', 'Particles', 'Logs']:
            self.outputDirs[d] = self.mkdir(d)

        # Define the current pipeline with generator and processors
        g = self.addMoviesGenerator(self.inputStar, self.batchSize,
                                    inputTimeOut=self.inputTimeOut,
                                    queueMaxSize=4, createBatch=False)
        outputQueue = None
        self.log(f"Creating {len(self.gpuList)} processing threads.", flush=True)
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_preprocessing(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)

    def get_preprocessing(self, gpu):
        def _preprocessing(batch):
            # Convert items to dict
            batch['items'] = [row._asdict() for row in batch['items']]
            gpuStr = Color.cyan(f"GPU = {gpu}")
            result = None

            def _runPP():
                pp = Preprocessing(self._pp_args)
                return pp, pp.process_batch(batch, gpu=gpu,
                                            outputFolder=self.path,
                                            tmpFolder=self.tmpDir)
            with self._particle_size_lock:
                if self.particle_size is None:
                    batch.log(f"{Color.warn('Estimating the boxSize.')} "
                              f"Running preprocessing {gpuStr}", flush=True)
                    pp, result = _runPP()
                    # This should update particle_size and other args
                    self._pp_args.update(pp.args)
                    self.dumpArgs('Updated args')

            if result is None:
                batch.log(f"{Color.warn('Using existing boxSize.')} "
                          f"Running preprocessing {gpuStr}", flush=True)

                _, result = _runPP()

            batch.log(f"Preprocessing done.", flush=True)
            return result

        return _preprocessing

    def _output(self, batch):
        """ Update output STAR files. """

        def _pair(name):
            return self.join(name), self.join(f"{batch.id}_{name}")

        try:
            batch.log("Storing outputs.", flush=True)
            t = Timer()
            with self.outputLock:
                micsStar, micsStarBatch = _pair('micrographs.star')
                firstTime = not os.path.exists(micsStar)
                partStack = {}  # map micrograph to output stack of particles
                # Update micrographs.star
                with StarFile(micsStar, 'a') as sf:
                    with StarFile(micsStarBatch) as sfBatch:
                        if micsTable := sfBatch.getTable('micrographs'):
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
                    batch.log(f"Removing {micsStarBatch}", flush=True)
                    os.remove(micsStarBatch)

                # Update coordinates.star
                coordStar, coordStarBatch = _pair('coordinates.star')
                with StarFile(coordStar, 'a') as sf:
                    with StarFile(coordStarBatch) as sfBatch:
                        if coordsTable := sfBatch.getTable('coordinate_files'):
                            if firstTime:
                                sf.writeTimeStamp()
                                sf.writeHeader('coordinate_files', coordsTable)
                            for row in coordsTable:
                                sf.writeRow(self.fixOutputRow(row,
                                                              'rlnMicrographName',
                                                              'rlnMicrographCoordinates'))
                    batch.log(f"Removing {coordStarBatch}", flush=True)
                    os.remove(coordStarBatch)

                # Update particles.star
                partStar, partStarBatch = _pair('particles.star')
                with StarFile(partStar, 'a') as sf:
                    with StarFile(partStarBatch) as sfBatch:
                        if partTable := sfBatch.getTable('particles'):
                            if firstTime:
                                sf.writeTimeStamp()
                                sf.writeTable('optics', sfBatch.getTable('optics'))
                                sf.writeHeader('particles', partTable)
                            for row in partTable:
                                micName = row.rlnMicrographName
                                i = row.rlnImageName.split('@')[0]
                                sf.writeRow(row._replace(rlnImageName=f"{i}@{partStack[micName]}",
                                                         rlnMicrographName=self.fixOutputPath(micName)))
                    batch.log(f"Removing {partStarBatch}", flush=True)
                    os.remove(partStarBatch)

                batch.info.update({
                    'output_elapsed': str(t.getElapsedTime())
                })
                self.info['outputs'] = [
                    {'label': 'Micrographs',
                     'files': [
                         [micsStar, 'MicrographGroupMetadata.star'],
                         [coordStar, 'MicrographCoordsGroup.star']
                     ]},
                    {'label': 'Particles',
                     'files': [
                         [partStar, 'ParticleGroupMetadata.star']
                     ]},
                ]

                batch.log(f"No call: Updating batchInfo", flush=True)
                # 2025/03/10 JMRT: Calling this function makes the thread lock here
                self.updateBatchInfo(Batch(batch))
                # self.info['batches'][batch.id] = batch.info
                # with open(self.infoFile, 'w') as f:
                #     json.dump(self.info, f, indent=4)

                with StarFile(self.inputStar) as sf:
                    self._totalInput = sf.getTableSize('movies')
                self._totalOutput += len(batch['items'])
                percent = self._totalOutput * 100 / self._totalInput
                self.log(f">>> Processed {Color.green(str(self._totalOutput))} out of "
                         f"{Color.red(str(self._totalInput))} "
                         f"({Color.bold('%0.2f' % percent)} %)", flush=True)

                batch.log(f"Storing outputs. FINISHED", flush=True)
                return batch
        except Exception as e:
            print(Color.red('ERROR: ' + str(e)))
            import traceback
            traceback.print_exc()
            sys.exit(1)


def main():
    PreprocessingPipeline.runFromArgs()


if __name__ == '__main__':
    main()
