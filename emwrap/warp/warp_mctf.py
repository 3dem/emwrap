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


class WarpMotionCtf(ProcessingPipeline):
    """ Pipeline PyTom picking in a set of tomograms. """
    name = 'emw-warp-mctf'
    input_name = 'in_movies'

    def __init__(self, all_args):
        args = all_args[self.name]
        ProcessingPipeline.__init__(self, args)
        self.gpuList = args['gpu'].split()
        self.inputPattern = args['in_movies']
        self.acq = Acquisition(all_args['acquisition'])
        self.warptools = get_warptools()

    def prerun(self):
        self.dumpArgs(printMsg="Input args")
        # FIXME: Improve the pattern split into root folder and the images suffix
        fs = 'warp_frameseries'
        self.mkdir(fs)

        # Run create_settings
        args = Args({
            'create_settings': '',
            '--folder_data': self.link(os.path.dirname(self.inputPattern)),
            '--extension': f"*{Path.getExt(self.inputPattern)}",
            '--folder_processing': fs,
            '--output': f'{fs}.settings',
            '--angpix': self.acq.pixel_size,
            '--exposure': self.acq['total_dose']
        })
        args.update(self._args['create_settings'])

        if gain := self.acq.get('gain', None):
            args['--gain_path'] = self.link(gain)

        batch = Batch(id='mtc', path=self.path)

        with batch.execute('create_settings'):
            batch.call(self.warptools, args)

        if n := self._args['create_settings'].get('--eer_ngroups', 0):
            ngroups = n
        else:
            ngroups = None  # FIXME: Read from movies

        # Run fs_motion_and_ctf
        args = Args({
            'fs_motion_and_ctf': '',
            '--settings': f'{fs}.settings',
            '--m_grid': f'1x1x{ngroups}',
            '--c_grid': '2x2x1',
            '--device_list': self.gpuList
        })
        args.update(self._args['fs_motion_and_ctf']['extra_args'])

        with batch.execute('fs_motion_and_ctf'):
            batch.call(self.warptools, args)

        self.updateBatchInfo(batch)


def main():
    WarpMotionCtf.runFromArgs()


if __name__ == '__main__':
    main()
