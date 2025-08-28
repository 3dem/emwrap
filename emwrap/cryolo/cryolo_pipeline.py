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


class CryoloPipeline(ProcessingPipeline):
    """ Pipeline to run Cryolo in batches. """
    name = 'emw-cryolo'
    input_name = 'in_movies'

    def __init__(self, input_args):
        ProcessingPipeline.__init__(self, input_args)
        args = self._args
        self.gpuList = args['gpu'].split()
        self.outputCoordsDir = None
        self.inputStar = args['in_movies']
        self.batchSize = args.get('batch_size', 32)
        self.inputTimeOut = args.get('timeout', 3600)
        self.acq = self.loadAcquisition()
        self._totalInput = self._totalOutput = 0

    def prerun(self):
        self.log(f"Batch size: {Color.cyan(str(self.batchSize))}")
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}", flush=True)
        self.inputs['Micrographs'] = {
            'key': 'input_movies',
            'label': 'Movies',
            'datatype': 'MicrographMovieGroupMetadata.star.relion',
            'files': [self.inputStar]
        }

        self.outputCoordsDir = self.mkdir('Coordinates')

        # Define the current pipeline with generator and processors
        outputMicStar = self.join('coordinates.star')
        if os.path.exists(outputMicStar):
            with StarFile(outputMicStar) as sf:
                self._totalOutput = sf.getTableSize('coordinate_files')
                self.log(f"Already processed {self._totalOutput} micrographs")

        g = self.addMoviesGenerator(self.inputStar, outputMicStar, self.batchSize,
                                    inputTimeOut=self.inputTimeOut,
                                    queueMaxSize=4)
        outputQueue = None
        self.log(f"Creating {len(self.gpuList)} processing threads.", flush=True)
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_cryolo_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)

    def get_cryolo_proc(self, gpu):

        def _cryolo(batch):
            # Convert items to dict
            batch['items'] = [row._asdict() for row in batch['items']]
            gpuStr = Color.cyan(f"GPU = {gpu}")
            result = None

            batch.mkdir('Coordinates')

            pickingArgs = dict(self.args['picking'])
            if self.particle_size is not None:
                a = round(self.particle_size / acq.pixel_size)
                pickingArgs['anchors'] = [a, a]

            batch.log(f"Running Cryolo, args: {pickingArgs}", flush=True)
            cryolo = CryoloPredict(**pickingArgs)
            cryolo.process_batch(batch, gpu=gpu, cpu=cpu)
            if self.particle_size is None:
                size = cryolo.get_size(batch, 75)

                self.particle_size = round(size * acq.pixel_size)
                print(f">>> Size for percentile 75: {size}, particle_size (A): {self.particle_size}")

            tCoords = RelionStar.coordinates_table()
            extra_cols = ['rlnMicrographCoordinates', 'rlnCoordinatesNumber']

            return batch


        return _cryolo

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
                with StarFile(micsStarBatch) as sfBatch:
                    if micsTable := sfBatch.getTable('micrographs'):
                        with StarFile(micsStar, 'a') as sf:
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
                firstTime = not os.path.exists(coordStar)
                with StarFile(coordStarBatch) as sfBatch:
                    if coordsTable := sfBatch.getTable('coordinate_files'):
                        with StarFile(coordStar, 'a') as sf:
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
                firstTime = not os.path.exists(partStar)
                with StarFile(partStarBatch) as sfBatch:
                    if partTable := sfBatch.getTable('particles'):
                        with StarFile(partStar, 'a') as sf:
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
                self.updateBatchInfo(Batch(batch))

                with StarFile(self.inputStar) as sf:
                    self._totalInput = sf.getTableSize('movies')
                self._totalOutput += len(batch['items'])
                percent = self._totalOutput * 100 / self._totalInput
                batch.log(f">>> Processed {Color.green(str(self._totalOutput))} out of "
                          f"{Color.red(str(self._totalInput))} "
                          f"({Color.bold('%0.2f' % percent)} %)", flush=True)

        except Exception as e:
            batch.log(Color.red('ERROR: ' + str(e)))
            batch.error = str(e)
            import traceback
            traceback.print_exc()

        return batch

    def _only_output(self):
        logs = self.join('Logs')
        stars = ['micrographs.star', 'particles.star', 'coordinates.star']
        batches = []
        for fn in sorted(os.listdir(logs)):
            if fn.endswith('.json'):
                with open(os.path.join(logs, fn)) as f:
                    batches.append(Batch(json.load(f)))

        for batch in sorted(batches, key=lambda b: b.index):
            print(f">>> Batch id: {Color.bold(batch.id)}")
            if all(self.exists(f"{batch.id}_{name}") for name in stars):
                self._output(batch)


def main():
    PreprocessingPipeline.runFromArgs()


if __name__ == '__main__':
    main()
