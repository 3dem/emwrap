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
from emtools.metadata import StarFile, Acquisition
from emtools.jobs import Batch, Args
from emtools.image import Image
from emwrap.base import ProcessingPipeline

from .warp import get_warptools


class WarpAreTomo(ProcessingPipeline):
    """ Script to run warp_ts_aretomo. """
    name = 'emw-warp-aretomo'
    input_name = 'in_movies'

    def __init__(self, all_args):
        args = all_args[self.name]
        ProcessingPipeline.__init__(self, args)
        self.gpuList = args['gpu'].split()
        self.inputMotionCtf = args['in_movies']
        self.acq = Acquisition(all_args['acquisition'])
        self.warptools = get_warptools()

    def prerun(self):
        self.dumpArgs(printMsg="Input args")
        # FIXME: Improve the pattern split into root folder and the images suffix
        inputFolder = FolderManager(self.inputMotionCtf)
        fs = 'warp_frameseries'
        ts = 'warp_tiltseries'
        tms = 'warp_tomostar'
        self.mkdir(ts)
        self.mkdir(tms)

        batch = Batch(id=self.name, path=self.path)

        # Run ts_import
        args = Args({
            'ts_import': '',
            '--frameseries': self.link(inputFolder.join(fs)),
            '--tilt_exposure': self.acq['total_dose'],
            '--output': tms,
        })
        args.update(self._args['ts_import'])
        args['--mdocs'] = self.link(args['--mdocs'])

        with batch.execute('ts_import'):
            batch.call(self.warptools, args)

        # Run create_settings
        args = Args({
            'create_settings': '',
            '--folder_data': tms,
            '--extension': "*.tomostar",
            '--folder_processing': ts,
            '--output': f'{ts}.settings',
            '--angpix': self.acq.pixel_size,  # FIXME: CHANGE depending on motion bin,
            '--exposure': self.acq['total_dose']
        })
        args.update(self._args['create_settings'])

        if gain := self.acq.get('gain', None):
            args['--gain_path'] = self.link(gain)

        with batch.execute('create_settings'):
            batch.call(self.warptools, args)

        # Run fs_motion_and_ctf
        args = Args({
            'ts_aretomo': '',
            '--settings': f'{ts}.settings',
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
