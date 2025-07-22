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
        -
    """
    name = 'emw-warp-aretomo'
    input_name = 'in_movies'

    def prerun(self):
        self.dumpArgs(printMsg="Input args")
        # Input run folder from the Motion correction and CTF job
        inputFolder = FolderManager(self._args['in_movies'])
        self.mkdir(self.TS)
        self.mkdir(self.TM)
        batch = Batch(id=self.name, path=self.path)

        # Link input frameseries folder, settings and gain reference
        self._importInputs(inputFolder, keys=['fs', 'fss'])

        # Run ts_import
        args = Args({
            'ts_import': '',
            '--frameseries': self.FS,
            '--tilt_exposure': self.acq['total_dose'],
            '--output': self.TM,
        })
        args.update(self._args['ts_import'])
        args['--mdocs'] = self.link(args['--mdocs'])

        with batch.execute('ts_import'):
            batch.call(self.warptools, args)

        # Run create_settings
        args = Args({
            'create_settings': '',
            '--folder_data': self.TM,
            '--extension': "*.tomostar",
            '--folder_processing': self.TS,
            '--output': self.TSS,
            '--angpix': self.acq.pixel_size,  # FIXME: CHANGE depending on motion bin,
            '--exposure': self.acq['total_dose']
        })
        args.update(self._args['create_settings'])

        with batch.execute('create_settings'):
            batch.call(self.warptools, args)

        # Run fs_motion_and_ctf
        args = Args({
            'ts_aretomo': '',
            '--settings': self.TSS,
            '--device_list': self.gpuList
        })
        args.update(self._args['ts_aretomo']['extra_args'])

        with batch.execute('ts_aretomo'):
            batch.call(self.warptools, args)

        self.updateBatchInfo(batch)


def main():
    WarpAreTomo.runFromArgs()


if __name__ == '__main__':
    main()
