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
import numpy as np
import shutil
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import Args
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile


class CryoloPredict:
    def __init__(self, **kwargs):
        # Denoise with JANNI model
        self.model = '/usr/local/em/cryolo/cryolo_model-202005_nn_N63_c17/gmodel_phosnet_202005_nn_N63_c17.h5'
        self.janni_model = '/usr/local/em/cryolo/janni_model-20190703/gmodel_janni_20190703.h5'
        self.path = '/usr/local/em/miniconda/envs/cryolo/bin/cryolo_predict.py'
        self.args = kwargs

    def process_batch(self, batch, **kwargs):
        gpu = kwargs['gpu']
        cpu = 1 #kwargs.get('cpu', 1)

        t = Timer()
        model = {
            "architecture": "PhosaurusNet",
            "input_size": 1024,
            "max_box_per_image": 700,
            "norm": "STANDARD",
            "filter": [self.janni_model, 24, 3, "cryolo_filtered/"]
        }
        if 'anchors' in self.args:
            model['anchors'] = self.args['anchors']

        config = {
            "model": model,
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
            '-t': 0.05,
            '-nc': cpu,
            '-g': gpu,
            '-o': 'cryolo_boxfiles/'
        }

        batch.call(self.path, kwargs)

        batch.info.update({
            'cryolo_elapsed': str(t.getElapsedTime())
        })

        return batch

    def __distr_file(self, batch, percentile, prefix):
        distr = batch.join('cryolo_boxfiles', 'DISTR')
        for fn in os.listdir(distr):
            if fn.startswith(prefix):
                filePath = os.path.join(distr, fn)
                for line in TextFile.stripLines(filePath):
                    if line.startswith(f"Q{percentile},"):
                        return line.split(',')[1]
        return None

    def get_size(self, batch, percentile):
        """ Get the estimated particle size (in pixels) value for the given percentile.
        Valid options for percentile are: 25, 50 and 75. """
        return int(self.__distr_file(batch, percentile, 'size_distribution_summary_'))

    def get_confidence(self, batch, percentile):
        """ Get confidence of the detected particle for a given percentile.
        Valid options for percentile are: 25, 50 and 75. """
        return self.__distr_file(batch, percentile, 'confidence_distribution_summary_')


