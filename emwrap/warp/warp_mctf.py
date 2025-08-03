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


class WarpMotionCtf(WarpBasePipeline):
    """ Warp wrapper to run Motion correction and CTF estimation.
    It will run:
        - create_settings -> frame_series.setting
        - fs_motion_and_ctf
    """
    name = 'emw-warp-mctf'
    input_name = 'in_movies'

    def prerun(self):
        # Input movies pattern for the frame series
        inputPattern = self._args['in_movies']
        inputFolder = os.path.dirname(inputPattern)

        self.mkdir(self.FS)

        # Run create_settings
        args = Args({
            'create_settings': '',
            '--folder_data': self.link(inputFolder),
            '--extension': f"*{Path.getExt(inputPattern)}",
            '--folder_processing': self.FS,
            '--output': self.FSS,
            '--angpix': self.acq.pixel_size,
            '--exposure': self.acq['total_dose']
        })
        if self.gain:
            args['--gain_path'] = self.gain

        args.update(self._args['create_settings'])

        # Just link the gain reference
        self._importInputs(inputFolder, keys=[])

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
            '--settings': self.FSS,
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
