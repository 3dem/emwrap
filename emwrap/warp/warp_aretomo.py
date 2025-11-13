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
import shutil
import json
import argparse
import time
import sys
from glob import glob
from datetime import datetime

from emtools.utils import Color, FolderManager, Path, Process
from emtools.jobs import Batch, Args

from .warp import WarpBasePipeline


class WarpAreTomo(WarpBasePipeline):
    """ Warp wrapper to run warp_ts_aretomo.
    It will run:
        - ts_import -> mdocs
        - create_settings -> warp_tiltseries.settings
        - ts_aretomo -> ts alignment
    """
    name = 'emw-warp-aretomo'
    input_name = 'in_movies'

    def runBatch(self, batch, importInputs=True, **kwargs):
        # Input run folder from the Motion correction and CTF job
        inputTs=kwargs['inputTs']
        inputFolder = FolderManager(os.path.dirname(inputTs))

        # FIXME: Add validations if the input star exists and required warp folders
        batch.mkdir(self.TS)
        batch.mkdir(self.TM)

        # Link input frameseries folder, settings and gain reference
        if importInputs:
            self._importInputs(inputFolder, keys=['fs', 'fss', 'frames', 'mdocs'])

        # Run ts_import
        args = Args({
            'WarpTools': 'ts_import',
            '--frameseries': self.FS,
            '--tilt_exposure': self.acq['total_dose'],
            '--output': self.TM,
            '--mdocs': 'mdocs'
        })
        subargs = self.get_subargs('ts_import', '--')
        args.update(subargs)
        self.batch_execute('ts_import', batch, args)

        # Run create_settings
        args = Args({
            'WarpTools': 'create_settings',
            '--folder_data': self.TM,
            '--extension': "*.tomostar",
            '--folder_processing': self.TS,
            '--output': self.TSS,
            '--angpix': self.acq.pixel_size,  # * 2,  # FIXME: CHANGE depending on motion bin,
            '--exposure': self.acq['total_dose']
        })
        subargs = self.get_subargs('create_settings', '--')
        args.update(subargs)
        self.batch_execute('create_settings', batch, args)

        # Run fs_motion_and_ctf
        args = Args({
            'WarpTools': 'ts_aretomo',
            '--settings': self.TSS,
            '--exe': os.environ['ARETOMO2']
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = self.get_subargs('ts_aretomo', '--')
        args.update(subargs)
        self.batch_execute('ts_aretomo', batch, args)

        self.updateBatchInfo(batch)

    def prerun(self):
        batch = Batch(id=self.name, path=self.path)
        self.runBatch(batch, inputTs=self._args['input_tiltseries'])


if __name__ == '__main__':
    WarpAreTomo.main()
