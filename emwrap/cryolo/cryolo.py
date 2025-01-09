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

sample_config = {
    "model": {
        "architecture": "PhosaurusNet",
        "input_size": 1024,
        "max_box_per_image": 700,
        "norm": "STANDARD",
        "filter": [
            "/usr/local/em/cryolo/janni_model-20190703/gmodel_janni_20190703.h5",
            24,
            3,
            "cryolo_filtered/"
        ]
    },
    "other": {
        "log_path": "cryolo_logs/"
    }
}

# cryolo_predict.py -c config.json -w model.h5 -i Micrographs/ -g 1 -o boxfiles/ -t 0.95 -nc 8

import os
import subprocess
import shutil
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import Args
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile


class CryoloPredict:
    def __init__(self, *args, **kwargs):
        # Denoise with JANNI model
        self.model = '/usr/local/em/cryolo/cryolo_model-202005_nn_N63_c17/gmodel_phosnet_202005_nn_N63_c17.h5'
        self.janni_model = '/usr/local/em/cryolo/janni_model-20190703/gmodel_janni_20190703.h5'
        self.path = '/usr/local/em/miniconda/envs/cryolo/bin/cryolo_predict.py'

    def process_batch(self, batch, **kwargs):
        gpu = kwargs['gpu']
        cpu = 1 #kwargs.get('cpu', 1)

        t = Timer()
        config = {
            "model": {
                "architecture": "PhosaurusNet",
                "input_size": 1024,
                "max_box_per_image": 700,
                "norm": "STANDARD",
                "filter": [self.janni_model, 24, 3, "cryolo_filtered/"]
            },
            "other": {
                "log_path": "cryolo_logs/"
            }
        }

        with open(batch.join('config.json'), 'w') as f:
            json.dump(config, f, indent=4)

        kwargs = {
            '-c': 'config.json',
            '-w': self.model,
            '-i': 'Micrographs/',
            '-t': 0.2,
            '-nc': cpu,
            '-g': gpu,
            '-o': 'cryolo_boxfiles/'
        }

        batch.call(self.path, kwargs, batch.join('cryolo_log.txt'))

        print(Color.warn("CRYOLO_done"))

        batch.info.update({
            'cryolo_elapsed': str(t.getElapsedTime())
        })



