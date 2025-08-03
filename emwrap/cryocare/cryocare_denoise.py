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
import subprocess
import pathlib
import sys
import json
import argparse
import shutil
from pprint import pprint
from glob import glob

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import MdocBatchManager, Args, Batch
from emtools.metadata import Mdoc, Acquisition

from .cryocare_pipeline import CryoCarePipeline


predict_config = {
  "path": "path/to/your/model/model_name.tar.gz",
  "even": "/path/to/even.rec",
  "odd": "/path/to/odd.rec",
  "n_tiles": [2, 2, 2],
  "output": "./",
  "overwrite": True,
  "gpu_id": 0
}


class CryoCareDenoise(CryoCarePipeline):
    """ Pipeline specific to CryoCare processing. """
    name = 'emw-cryocare-denoise'
    input_name = 'in_movies'

    def get_denoise(self, gpu):
        def denoise(batch, **kwargs):
            # Extract data config
            batch.path = self.join('tmp', batch.id)
            batch.create()

            config = dict(predict_config)
            config['gpu_id'] = gpu
            config['even'] = batch.relpath(batch['even'])
            config['odd'] = batch.relpath(batch['odd'])
            config['path'] = batch.relpath(self.model)

            batch.dump(config, 'predict_config.json')

            program = os.environ['CRYOCARE_DENOISE']
            print(f">>> Running: {program}, GPU = {Color.cyan(gpu)}")
            batch.call(program, [])

            return batch

        return denoise

    def _output(self, batch):
        for fn in batch.listdir():
            if fn.endswith('.mrc'):
                batchFile = batch.join(fn)
                dstFile = self.join('Denoised', fn.replace('EVN', 'denoised'))
                self.log(f"Moving file {batchFile} to {dstFile}", flush=True)
                shutil.move(batchFile, dstFile)
        return batch

    def prerun(self):
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}", flush=True)
        self.model = self._args['model']

        if not os.path.exists(self.model):
            raise Exception(f"Input model '{self.model}' does not exists.")

        self.mkdir('Denoised')

        evenVols, oddVols = self.getInputVols()

        def _generate():
            for i, even in enumerate(evenVols):
                volRoot = os.path.basename(even).split('_EVN')[0]
                yield Batch(id=f'batch_{volRoot}', path='',
                            even=even, odd=oddVols[i])

        g = self.addGenerator(_generate)

        self.log(f"Creating {len(self.gpuList)} processing threads.", flush=True)
        outputQueue = None
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_denoise(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


def main():
    CryoCareDenoise.runFromArgs()


if __name__ == '__main__':
    main()
