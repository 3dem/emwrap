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
from pprint import pprint
from glob import glob

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import MdocBatchManager, Args, Batch
from emtools.metadata import Mdoc, Acquisition

from .cryocare_pipeline import CryoCarePipeline


data_train_config = {
  "even": [
    "/path/to/even.rec"
  ],
  "odd": [
    "/path/to/odd.rec"
  ],
  # "mask": [
  #   "/path/to/mask.mrc"
  # ],
  "patch_shape": [
    72,
    72,
    72
  ],
  "num_slices": 1200,
  "split": 0.9,
  "tilt_axis": "Y",
  "n_normalization_samples": 500,
  "path": "output_data"
}

train_config = {
  "train_data": "output_data",
  "epochs": 100,
  "steps_per_epoch": 200,
  "batch_size": 16,
  "unet_kern_size": 3,
  "unet_n_depth": 3,
  "unet_n_first": 16,
  "learning_rate": 0.0004,
  "model_name": "model",
  "path": "output_model",
  "gpu_id": 0
}


class CryoCareTrain(CryoCarePipeline):
    """ Pipeline specific to CryoCare processing. """
    name = 'emw-cryocare-train'
    input_name = 'in_movies'

    def train(self, batch, **kwargs):
        data_train_config['even'] = [self.relpath(v) for v in batch['even']]
        data_train_config['odd'] = [self.relpath(v) for v in batch['odd']]

        batch.dump(data_train_config, 'train_data_config.json')

        train_config["gpu_id"] = [int(g) for g in self.gpuList]
        batch.dump(train_config, 'train_config.json')

        # Extract data config
        program = os.environ['CRYOCARE_TRAIN']

        print(f">>> Running: {program}")
        batch.call(program, [])

    def prerun(self):
        self.dumpArgs(printMsg="Input args")
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}", flush=True)

        evenVols, oddVols = self.getInputVols()

        batch = Batch(id='cryocare', path=self.path, even=evenVols, odd=oddVols)
        self.train(batch)


def main():
    CryoCareTrain.runFromArgs()


if __name__ == '__main__':
    main()
