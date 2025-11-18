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
from emtools.metadata import StarFile, Table, WarpXml

from .warp import WarpBasePipeline


class WarpCtfReconstruct(WarpBasePipeline):

    """ Script to run warp_ts_aretomo. """
    name = 'emw-warp-ctfrec'

    def runBatch(self, batch, **kwargs):
        inputTs = kwargs['inputTs']
        inputFolder = FolderManager(os.path.dirname(inputTs))

        if kwargs.get('importInputs', True):
            self._importInputs(inputFolder)

        # Run ts_ctf
        args = Args({
            'WarpTools': 'ts_ctf',
            '--settings': self.TSS,
            '--voltage': int(self.acq.voltage),
            '--cs': self.acq.cs,
            '--amplitude': self.acq.amplitude_contrast
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = self.get_subargs('ts_ctf', '--')
        args.update(subargs)
        self.batch_execute('ts_ctf', batch, args)

        # # Run filter_quality
        # args = Args({
        #     'WarpTools': 'filter_quality',
        #     '--settings': self.TSS,
        #     "--resolution": [1, 6],
        #     "--output": "warp_tiltseries_filtered.txt"
        # })
        # with batch.execute('filter_quality'):
        #     batch.call(self.loader, args)

        # Run ts_reconstruct
        args = Args({
            'WarpTools': 'ts_reconstruct',
            '--settings': self.TSS
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList
        subargs = self.get_subargs('ts_reconstruct', '--')
        args.update(subargs)
        self.batch_execute('ts_reconstruct', batch, args)

        self.updateBatchInfo(batch)

    def _output(self, batch):
        """ Register output STAR files. """

        def _float(v):
            return round(float(v), 3)

        self.log("Registering output STAR files.")
        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)

        newTsStarFile = batch.join('tomograms.star')

        extraLabels = [
            'rlnTomogram',
            'rlnTomogramPixelSize',
            'rlnTomoTomogramBinning',
            'rlnDefocus',
            'rlnTomoReconstructedTomogramHalf1',
            'rlnTomoReconstructedTomogramHalf2',
            'wrpTomostar'
        ]
        recpath = self.join(self.TS, 'reconstruction')
        newPs = _float(self._args["ts_reconstruct.angpix"])

        def _rec(*p):
            return os.path.join(recpath, *p)

        tomoDict = {}
        for tfn in glob(_rec('*.mrc')):
            base = os.path.basename(tfn)
            suffix = '_' + base.split('_')[-1]
            tsName = base.replace(suffix, '')
            tomoDict[tsName] = base

        newTsAllTable = Table(tsAllTable.getColumnNames() + extraLabels)
        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            tsDict = tsRow._asdict()

            # FIXME: validate for missing tomograms
            if tomoFile := tomoDict.get(tsName, ''):
                t, te, to = _rec(tomoFile), _rec('even', tomoFile), _rec('odd', tomoFile)
            else:
                t, te, to = '', '', ''
            xmlFile = self.join(self.TS, tsName + '.xml')
            if os.path.exists(xmlFile):
                # self.log(f"Reading {movieXml}")
                ctf = WarpXml(xmlFile).getDict('TiltSeries', 'CTF', 'Param')
                defocus = _float(ctf['Defocus'])
            else:
                defocus = 999

            # FIXME: validate for missing tomostar files
            tomostar = self.join(self.TM, tsName + '.tomostar')

            tsDict.update({
                'rlnTomogram': t,
                'rlnTomogramPixelSize': newPs,
                'rlnTomoTomogramBinning': _float(newPs / tsDict['rlnTomoTiltSeriesPixelSize']),
                'rlnDefocus': defocus,
                'rlnTomoReconstructedTomogramHalf1': te,
                'rlnTomoReconstructedTomogramHalf2': to,
                'wrpTomostar': tomostar
            })
            newTsAllTable.addRowValues(**tsDict)

        # Write the corrected_tilt_series.star
        self.write_ts_table('global', newTsAllTable, newTsStarFile)

        self.updateBatchInfo(batch)

    def prerun(self):
        self.prerunTs()


if __name__ == '__main__':
    WarpCtfReconstruct.main()
