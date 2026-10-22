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
from emtools.jobs import Batch, Args

from emwrap.base import ProcessingPipeline
from .preprocessing import Preprocessing


class PreprocessingPipeline(ProcessingPipeline):
    """ Pipeline to run Preprocessing in batches.

    Each batch can be processed in a separate process (e.g. submitted to a
    cluster) through the 'launcher_preprocessing' script. It receives the
    project folder and the batch JSON file, and it should setup the
    environment (EMWRAP_CONFIG with the launchers of MOTIONCOR3, CTFFIND,
    CRYOLO and RELION) and run: python -m emwrap.mix.preprocessing "$@"
    """
    name = 'emw-preprocessing'

    def __init__(self, input_args, output):
        ProcessingPipeline.__init__(self, input_args, output)
        args = self._args
        # One processing thread per GPU group (see get_gpu_groups),
        # e.g. "0 0 1 1" (two threads per GPU), with the GPU ids
        # separated by spaces (MotionCor and crYOLO syntax)
        self.gpuList = [' '.join(str(g) for g in group)
                        for group in self.get_gpu_groups(args.get('gpus', '1'))]
        if not self.gpuList:
            raise Exception("Preprocessing requires at least one GPU (gpus param)")
        self.outputDirs = {}
        self.inputStar = args['in_movies']
        self.batchSize = int(args.get('batch_size', 32))
        self.inputTimeOut = int(args.get('input_timeout', 3600))
        # Acquisition (including gain and dose per frame) from the input movies
        self.acq = self.loadAcquisition(self.inputStar)
        self._totalInput = self._totalOutput = 0
        self._pp_args = self._preprocessingArgs()

        # Create a lock to estimate the extraction args only once,
        # for the first batch processed
        self._particle_size_lock = threading.Lock()

    def _preprocessingArgs(self):
        """ Convert the (flat) job params into the args of each
        Preprocessing step (motioncor, ctf, picking and extract). """
        args = self._args

        def _float(key):
            value = args.get(key, '')
            return None if value in (None, '') else float(value)

        mc_args = {'-FtBin': args.get('motioncor.bin', 1),
                   '-Patch': args.get('motioncor.patch', '5 5')}
        if args.get('motioncor.dose_weighting', True) and self.acq.total_dose:
            # rlnMicrographDoseRate from the input movies (dose per frame)
            mc_args['-FmDose'] = self.acq.total_dose
        mc_args.update(Args.fromString(args.get('motioncor.extra_args') or ''))

        ctf_args = {k: v for k, v in args.subset('ctf').items() if v not in (None, '')}

        extract_args = Args.fromString(args.get('extract.extra_args') or '')
        if scale := args.get('extract.scale'):
            extract_args['--scale'] = int(scale)

        return {
            'acquisition': Acquisition(self.acq),
            # Optional launcher to process each batch (e.g. in a cluster),
            # 'launcher_batch' is the old name of this param
            'launcher': (args.get('launcher_preprocessing')
                         or args.get('launcher_batch') or None),
            'motioncor': {'extra_args': mc_args},
            'ctf': ctf_args,
            'picking': {
                'particle_size': _float('picking.particle_size'),
                'threshold': _float('picking.threshold'),
                'model': args.get('picking.model') or None,
                'janni_model': args.get('picking.janni_model') or None
            },
            'extract': {'extra_args': extract_args}
        }

    @property
    def particle_size(self):
        return self._pp_args.get('picking', {}).get('particle_size', None)

    @particle_size.setter
    def particle_size(self, value):
        self._pp_args['picking']['particle_size'] = value

    def prerun(self):
        # Debugging option when there are processing outputs that were processed
        # but not registered in the output. In this case we will load the batch
        # and update output STAR files with missing elements
        if self._pp_args.get('DEBUG_only_output'):
            self._only_output()
            return

        self.log(f"Batch size: {Color.cyan(str(self.batchSize))}")
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}", flush=True)
        self.inputs['Movies'] = {
            'label': 'Movies',
            'datatype': 'MicrographMovieGroupMetadata.star.relion',
            'files': [self.inputStar]
        }

        # Create all required output folders
        for d in ['Micrographs', 'CTFs', 'Coordinates', 'Particles', 'Logs']:
            self.outputDirs[d] = self.mkdir(d)

        # Define the current pipeline with generator and processors
        outputMicStar = self.join('micrographs.star')
        if os.path.exists(outputMicStar):
            with StarFile(outputMicStar) as sf:
                self._totalOutput = sf.getTableSize('micrographs')
                self.log(f"Found {self._totalOutput} existing micrographs")

        g = self.addMoviesGenerator(self.inputStar, outputMicStar, self.batchSize,
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
                                            tmpFolder=self.tmpDir,
                                            scratchDir=self.scratchDir)
            with self._particle_size_lock:
                if self.particle_size is None:
                    batch.log(f"{Color.warn('Estimating the boxSize.')} "
                              f"Running preprocessing {gpuStr}", flush=True)
                    pp, result = _runPP()
                    # This should update particle_size and other args
                    self._pp_args.update(pp.args)

            if result is None:
                batch.log(f"{Color.warn('Using existing boxSize.')} "
                          f"Running preprocessing {gpuStr}", flush=True)

                _, result = _runPP()

            batch.log(f"Preprocessing done.", flush=True)
            return result

        return _preprocessing

    def _removeBatchFile(self, batch, fn):
        """ Remove a batch file merged into the output (unless EMWRAP_CLEAN=0). """
        if self.do_clean():
            batch.log(f"Removing {fn}", flush=True)
            os.remove(fn)

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
                self._removeBatchFile(batch, micsStarBatch)

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
                self._removeBatchFile(batch, coordStarBatch)

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
                self._removeBatchFile(batch, partStarBatch)

                batch.info.update({
                    'output_elapsed': str(t.getElapsedTime())
                })
                self.outputs.update({
                    'Micrographs': {'label': 'Micrographs',
                                    'files': [
                                        [micsStar, 'MicrographGroupMetadata.star.relion.ctf.Micrographs']]
                                    },
                    'Particles': {'label': 'Particles',
                                  'files': [
                                      [partStar, 'ParticleGroupMetadata.star.relion.Particles']]
                                  },
                })
                self.writeRelionOutputNodes(
                    [f for o in self.outputs.values() for f in o['files']])
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


if __name__ == '__main__':
    PreprocessingPipeline.main()
