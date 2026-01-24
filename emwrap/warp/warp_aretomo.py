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
from emtools.image import Image

from .warp import WarpBasePipeline


class WarpAreTomo(WarpBasePipeline):
    """ Warp wrapper to run warp_ts_aretomo.
    It will run:
        - ts_import -> mdocs
        - create_settings -> warp_tiltseries.settings
        - ts_aretomo -> ts alignment
    """
    name = 'emw-warp-aretomo'

    def _getInfo(self, tsAllTable):
        """ Load input or output information. """
        first = tsAllTable[0]
        ps = first.rlnTomoTiltSeriesPixelSize
        tsTable = StarFile.getTableFromFile(first.rlnTomoName, first.rlnTomoTiltSeriesStarFile)
        N = len(tsAllTable)
        n = len(tsTable)
        movieFn = tsTable[0].rlnMicrographMovieName
        dim = Image.get_dimensions(movieFn)
        self.log(f"get_dimensions: {dim}")
        x = dim[0]
        y = dim[1]
        return N, x, y, n, ps

    def runBatch(self, batch, importInputs=True, **kwargs):
        # Input run folder from the Motion correction and CTF job
        inputTs = kwargs['inputTs']
        tsAllTable = StarFile.getTableFromFile('global', inputTs)
        N, x, y, n, ps = self._getInfo(tsAllTable)

        self.inputs = {
            'TiltSeries': {
                'label': 'Tilt Series',
                'type': 'TiltSeries',
                'info': f"{N} items, {x} x {y} x {n}, {ps:0.3f} Å/px",
                'files': [
                    [inputTs, 'TomogramGroupMetadata.star.relion.tomo.motioncorr']
                ]
            }
        }
        self.writeInfo()

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
            '--angpix': ps,
            '--exposure': self.acq['total_dose']
        })
        subargs = self.get_subargs('create_settings', '--')
        args.update(subargs)
        self.batch_execute('create_settings', batch, args)

        # Run ts_aretomo wrapper
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

    def _output(self, batch):
        """ Register output STAR files. """
        def _float(v):
            return round(float(v), 2)

        batch.mkdir('tilt_series')
        self.log("Registering output STAR files.")
        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)

        newTsStarFile = batch.join('tilt_series_aln.star')

        newTsAllTable = Table(tsAllTable.getColumnNames() + ['rlnTiltSeriesAligned'])
        dims = 0, 0, 0
        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            # FIXME: The proper star files for each aligned TS needs to be generated
            tsStarFile = self.join('tilt_series', tsName + '.star')
            tsAligned = self.join(self.TS, 'tiltstack', tsName, f"{tsName}_aligned.mrc")
            if not os.path.exists(tsAligned):
                self.log(f"ERROR: Missing expected aligned TS: {tsAligned}")
                tsAligned = "None"  # FIXME Handle missing aligned TS
            else:
                newDims = Image.get_dimensions(tsAligned)
                if newDims[2] > dims[2]:
                    dims = newDims
            tsDict = tsRow._asdict()
            tsDict.update({
                'rlnTomoTiltSeriesStarFile': tsStarFile,
                'rlnTiltSeriesAligned': tsAligned
            })
            newTsAllTable.addRowValues(**tsDict)

        self.write_ts_table('global', newTsAllTable, newTsStarFile)
        N = len(newTsAllTable)
        # ps = newTsAllTable[0].rlnTomoTiltSeriesPixelSize
        newPs = float(self._args['ts_aretomo.angpix'])
        x, y, n = dims
        self.outputs = {
            'TiltSeriesAligned': {
                'label': 'Tilt Series Aligned',
                'type': 'TiltSeriesAligned',
                'info': f"{N} items, {x} x {y} x {n}, {newPs:0.3f} Å/px",
                'files': [
                    [newTsStarFile, 'TomogramGroupMetadata.star.relion.tomo.aligntiltseries']
                ]
            }
        }
        self.updateBatchInfo(batch)

    def prerun(self):
        self.prerunTs()


if __name__ == '__main__':
    WarpAreTomo.main()
