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
from collections import defaultdict

from emtools.utils import Color, FolderManager, Path, Process
from emtools.jobs import Batch, Args
from emtools.metadata import StarFile, Table, WarpXml
from emtools.image import Image

from .warp import WarpBasePipeline


class WarpMotionCtf(WarpBasePipeline):
    """ Warp wrapper to run Motion correction and CTF estimation.
    It will run:
        - create_settings -> frame_series.setting
        - fs_motion_and_ctf
    """
    name = 'emw-warp-mctf'

    def targetPs(self, inputPs):
        return float(self._args.get('create_settings.bin_angpix', 0)) or inputPs

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
        dims = None
        N = None

        # Input movies pattern for the frame series
        inputTsStar = kwargs['inputTs']
        tsAllTable = StarFile.getTableFromFile('global', inputTsStar)

        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            ps = tsRow.rlnMicrographOriginalPixelSize
            tsTable = StarFile.getTableFromFile(tsName, tsRow.rlnTomoTiltSeriesStarFile)
            mdocsFm.link(tsRow.rlnMdocFile)
            N = len(tsTable)
            for frameRow in tsTable:
                frameBase = framesFm.link(frameRow.rlnMicrographMovieName)
                # Calculate extension only once
                if ext is None:
                    ext = Path.getExt(frameBase)
                    dims = Image.get_dimensions(frameRow.rlnMicrographMovieName)

        x, y, n = dims
        self.inputs = {
            'FrameSeries': {
                'label': 'Frame Series',
                'type': 'FrameSeries',
                'info': f"{len(tsAllTable)} items, {x} x {y} x {n} x {N}, {ps:0.3f} Å/px",
                'files': [
                    [inputTsStar, 'TomogramGroupMetadata.star.relion.tomo.import']
                ]
            }
        }

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
        tPs = self.targetPs(ps)
        if tPs > ps:
            args['--bin_angpix'] = tPs

        if self.gain:
            args['--gain_path'] = self.gain

        subargs = self.get_subargs(cs, '--')
        subargs.pop('--bin', None)  # Remove bin if it exists
        args.update(subargs)

        self.batch_execute('create_settings', batch, args)

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
            '--c_grid': '2x2x1',  # FIXME: Read c_grid option
            '--c_voltage': int(self.acq.voltage),
            '--c_cs': self.acq.cs,
            '--c_amplitude': self.acq.amplitude_contrast
        })

        subargs = self.get_subargs('fs_motion_and_ctf')
        if self.gpuList:
            args['--device_list'] = self.gpuList
        if pd := subargs['perdevice']:
            args['--perdevice'] = int(pd)

        for a in ['c_use_sum', 'out_averages', 'out_average_halves']:
            if subargs[a]:
                args[f"--{a}"] = ""

        self.batch_execute('fs_motion_and_ctf', batch, args)
        self.updateBatchInfo(batch)

    def _output(self, batch):
        """ Register output STAR files. """

        def _float(v):
            return round(float(v), 2)

        batch.mkdir('tilt_series')
        self.log("Registering output STAR files.")
        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)

        newTsStarFile = batch.join('tilt_series_ctf.star')
        newPs = None
        n = None
        dims = None

        newPsLabel = 'rlnTomoTiltSeriesPixelSize'
        newTsAllTable = Table(tsAllTable.getColumnNames() + [newPsLabel])
        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            tsStarFile = self.join('tilt_series', tsName + '.star')
            ps = tsRow.rlnMicrographOriginalPixelSize
            if newPs is None:
                newPs = self.targetPs(ps)
            tsDict = tsRow._asdict()
            tsDict.update({
                newPsLabel: newPs,
                'rlnTomoTiltSeriesStarFile': tsStarFile
            })
            newTsAllTable.addRowValues(**tsDict)

            tsTable = StarFile.getTableFromFile(tsName, tsRow.rlnTomoTiltSeriesStarFile)
            n = len(tsTable)
            # FIXME: Do not add even/odd when this option is not selected
            extra_cols = [
                'rlnCtfPowerSpectrum', 'rlnMicrographName', 'rlnMicrographMetadata',
                'rlnAccumMotionTotal', 'rlnAccumMotionEarly', 'rlnAccumMotionLate',
                'rlnMicrographNameEven', 'rlnMicrographNameOdd', 'rlnCtfImage',
                'rlnDefocusU', 'rlnDefocusV', 'rlnCtfAstigmatism', 'rlnDefocusAngle',
                'rlnCtfFigureOfMerit', 'rlnCtfMaxResolution', 'rlnCtfIceRingDensity',
            ]

            filesMap = {
                'rlnMicrographName': 'average',
                'rlnCtfPowerSpectrum': 'powerspectrum',
                'rlnCtfImage': 'powerspectrum',
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

                if dims is None:  # Compute image dims once
                    dims = Image.get_dimensions(frameDict['rlnMicrographName'])

                movieXml = batch.join(self.FS, moviePrefix + '.xml')
                defocusDict = defaultdict(lambda: 0)

                if os.path.exists(movieXml):
                    # self.log(f"Reading {movieXml}")
                    ctf = WarpXml(movieXml).getDict('Movie', 'CTF', 'Param')
                    defocusDict['rlnDefocusU'] = _float(ctf['Defocus'])
                    defocusDict['rlnCtfAstigmatism'] = _float(ctf['DefocusDelta'])
                    defocusDict['rlnDefocusV'] = _float(defocusDict['rlnDefocusU'] + defocusDict['rlnCtfAstigmatism'])
                    defocusDict['rlnDefocusAngle'] = _float(ctf['DefocusAngle'])
                else:
                    pass  # FIXME Do something when xml is missing

                for k in extra_cols:
                    if k.startswith('rlnAccumMotion'):
                        # FIXME: Parse the movie values
                        frameDict[k] = 0
                    elif k.startswith('rlnDefocus') or k.startswith('rlnCtf') and k not in frameDict:
                        frameDict[k] = defocusDict[k]

                newTsTable.addRowValues(**frameDict)
            # Write the new ts.star file
            self.write_ts_table(tsName, newTsTable, tsStarFile)

        # Write the corrected_tilt_series.star
        self.write_ts_table('global', newTsAllTable, newTsStarFile)
        x, y = dims
        self.outputs = {
            'TiltSeries': {
                'label': 'Tilt Series',
                'type': 'TiltSeries',
                'info': f"{len(newTsAllTable)} items, {x} x {y} x {n}, {newPs:0.3f} Å/px",
                'files': [
                    [newTsStarFile, 'TomogramGroupMetadata.star.relion.tomo.import']
                ]
            }
        }

        self.updateBatchInfo(batch)

    def prerun(self):
        self.prerunTs()


if __name__ == '__main__':
    WarpMotionCtf.main()
