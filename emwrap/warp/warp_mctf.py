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
from emtools.metadata import StarFile, Table

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
        framesFm = FolderManager(batch.join('frames'))
        framesFm.create()

        mdocsFm = FolderManager(batch.join('mdocs'))
        mdocsFm.create()

        batch.mkdir(self.FS)

        ext = None
        ps = None

        # Input movies pattern for the frame series
        tsAllTable = StarFile.getTableFromFile('global', kwargs['tsStarFile'])

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

        subargs = self.get_subargs(cs, '--')
        args.update(subargs)

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

        subargs = self.get_subargs('fs_motion_and_ctf')
        if gpus := self._args['gpus']:
            args['--device_list'] = self.get_gpu_list(gpus)
        if pd := subargs['perdevice']:
            args['--perdevice'] = int(pd)
        """ 
        fs_motion_and_ctf.c_use_sum            Yes                               
fs_motion_and_ctf.out_averages         Yes                               
fs_motion_and_ctf.out_average_halves   Yes 
        """
        for a in ['c_use_sum', 'out_averages', 'out_average_halves']:
            if subargs[a]:
                args[f"--{a}"] = ""

        with batch.execute('fs_motion_and_ctf'):
            batch.call(self.loader, args, logfile=self.join('run.out'))

        self.updateBatchInfo(batch)

    def _output(self, batch):
        """ Register output STAR files. """
        batch.mkdir('tilt_series')
        self.log("Registering output STAR files.")
        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)

        newTsStarFile = batch.join('corrected_tilt_series.star')

        newPsLabel = 'rlnTomoTiltSeriesPixelSize'
        newTsAllTable = Table(tsAllTable.getColumnNames() + [newPsLabel])
        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            tsStarFile = self.join('tilt_series', f"{tsName}.star")
            ps = tsRow.rlnMicrographOriginalPixelSize
            newPs = ps  # FIXME: Take into account if there is binning at Mc level
            tsDict = tsRow._asdict()
            tsDict[newPsLabel] = newPs
            tsDict['rlnTomoTiltSeriesStarFile'] = tsStarFile
            newTsAllTable.addRowValues(**tsDict)

            tsTable = StarFile.getTableFromFile(tsName, tsRow.rlnTomoTiltSeriesStarFile)

            """
            _rlnCtfPowerSpectrum #7 
            _rlnMicrographNameEven #8 
            _rlnMicrographNameOdd #9 
            _rlnMicrographName #10 
            _rlnMicrographMetadata #11 
            _rlnAccumMotionTotal #12 
            _rlnAccumMotionEarly #13 
            _rlnAccumMotionLate #14 
            
            """
            # FIXME: Do not add even/odd when this option is not selected
            extra_cols = [
                'rlnCtfPowerSpectrum', 'rlnMicrographName', 'rlnMicrographMetadata',
                'rlnAccumMotionTotal', 'rlnAccumMotionEarly', 'rlnAccumMotionLate',
                'rlnMicrographNameEven', 'rlnMicrographNameOdd'
            ]
            filesMap = {
                'rlnMicrographName': 'average',
                'rlnCtfPowerSpectrum': 'powerspectrum',
                'rlnMicrographNameEven': 'average/even',
                'rlnMicrographNameOdd': 'average/odd'
            }
            newTsTable = Table(tsTable.getColumnNames() + extra_cols)
            for frameRow in tsTable:
                moviePrefix = Path.removeBaseExt(frameRow.rlnMicrographMovieName)
                movieMrc = moviePrefix + '.mrc'
                frameDict = frameRow._asdict()
                for k, v in filesMap.items():
                    frameDict[k] = batch.join(self.FS, v, movieMrc)
                frameDict['rlnMicrographMetadata'] = "None"
                # FIXME: Parse the movie values
                for k in extra_cols:
                    if k.startswith('rlnAccumMotion'):
                        frameDict[k] = 0
                newTsTable.addRowValues(**frameDict)
            # Write the new ts.star file
            with StarFile(tsStarFile, 'w') as sfOut:
                sfOut.writeTable(tsName, newTsTable,
                                 computeFormat='left',
                                 timeStamp=True)

        # Write the corrected_tilt_series.star
        with StarFile(newTsStarFile, 'w') as sfOut:
            sfOut.writeTable('global', newTsAllTable,
                             computeFormat='left',
                             timeStamp=True)

        self.updateBatchInfo(batch)

    def prerun(self):
        self.inputTs = self._args['input_tiltseries']
        batch = Batch(id='mtc', path=self.path)
        # self.runBatch(batch, tsStarFile=self.inputTs)
        self._output(batch)


if __name__ == '__main__':


    WarpMotionCtf.main()
