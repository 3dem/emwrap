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
from emtools.metadata import StarFile

from .warp import WarpBasePipeline


class WarpMotionCtf(WarpBasePipeline):
    """ Warp wrapper to run Motion correction and CTF estimation.
    It will run:
        - create_settings -> frame_series.setting
        - fs_motion_and_ctf
    """
    name = 'emw-warp-mctf'
    input_name = 'in_movies'

    def runBatch(self, batch, **kwargs):
        """ This method can be run for only the Mctf pipeline
         or for the preprocessing one, where import inputs is not needed.
        """
        # Input movies pattern for the frame series
        tsAllTable = StarFile.getTableFromFile('global', kwargs['tsStarFile'])

        framesFm = FolderManager(batch.join('frames'))
        framesFm.create()

        mdocsFm = FolderManager(batch.join('mdocs'))
        mdocsFm.create()

        batch.mkdir(self.FS)

        ext = None
        ps = None

        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            ps = tsRow.rlnMicrographOriginalPixelSize
            tsTable = StarFile.getTableFromFile(tsName, tsRow.rlnTomoTiltSeriesStarFile)
            mdocsFm.link(tsRow.rlnMdocFile)
            for frameRow in tsTable:
                frameBase = framesFm.link(frameRow.rlnMicrographMovieName)
                # Calculate extension only once
                if ext is None:
                    ext = Path.getExt(frameBase)

        if gain := self.acq.get('gain', None):
            self.log(f"{self.name}: Linking gain file: {gain}")
            self.link(gain)

        cs = 'create_settings'  # shortcut
        # Run create_settings
        args = Args({
            'WarpTools': cs,
            '--folder_data': 'frames',
            '--extension': f"*{ext}",
            '--folder_processing': self.FS,
            '--output': self.FSS,
            '--angpix': ps,
            '--exposure': self.acq['total_dose']
        })
        if self.gain:
            args['--gain_path'] = self.gain

        args.update({k.replace(f'{cs}.', '--'): v
                     for k, v in self._args.items() if k.startswith(cs)})

        with batch.execute('create_settings'):
            batch.call(self.loader, args, logfile=self.join('run.out'))

        #parts = self._args['']
        n = int(self._args.get(f'{cs}.eer_ngroups', 0))

        if n:
            ngroups = n
        else:
            ngroups = None  # FIXME: Read the number of frames from movies
            raise Exception("Only working with .eer for now")

        # Run fs_motion_and_ctf
        args = Args({
            'WarpTools': 'fs_motion_and_ctf',
            '--settings': self.FSS,
            '--m_grid': f'1x1x{ngroups}',  # FIXME: Read m_grid from params
            '--c_grid': '2x2x1',
            #'--device_list': self.gpuList  FIXME: Allow selection of gpus
        })

        # args.update(self._args['fs_motion_and_ctf']['extra_args'])

        with batch.execute('fs_motion_and_ctf'):
            batch.call(self.loader, args, logfile=self.join('run.out'))

        self.updateBatchInfo(batch)

    def prerun(self):
        batch = Batch(id='mtc', path=self.path)
        self.runBatch(batch, tsStarFile=self._args['input_tiltseries'])


if __name__ == '__main__':
    WarpMotionCtf.main()
