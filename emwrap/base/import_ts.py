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
import argparse
import shutil
import time
from glob import glob
import threading
from datetime import datetime

from emtools.utils import Color, Pretty, Path, FolderManager
from emtools.metadata import Acquisition, StarFile, RelionStar, Table
from emtools.jobs import MdocBatchManager

from emwrap.base import ProcessingPipeline


class ImportTsPipeline(ProcessingPipeline):
    """
    {
        "tilt_images": "data/*.eer",
        "mdoc_files": "data/Position*[1-9].mdoc",
        "acq.gain": "",
        "tilt_axis_angle": "85",
        "acq.pixel_size": "1.19",
        "acq.voltage": "300",
        "acq.spherical_aberration": "2.7",
        "acq.amplitude_contrast": "2.7",
        "acq.total_dose": "4.01"
    }
    """
    name = 'emw-import-ts'
    input_name = 'in_movies'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.acq = Acquisition({k.replace('acq.', ''): v
                                for k, v in args.items() if k.startswith('acq.')})

        self.wait = {
            'timeout': int(args.get('wait.timeout', 60)),
            'file_change': int(args.get('wait.file_change', 30)),
            'sleep': int(args.get('wait.sleep', 30)),
        }
        self.outputStar = self.join('tilt_series.star')
        self.tsFolder = args["tilt_images"]
        self.mdocPattern = args['mdoc_files']
        self.tiltAxisAngle = args['tilt_axis_angle']

    def _output(self, batch):
        """
data_Position_19

loop_
_rlnMicrographMovieName #1
_rlnTomoTiltMovieFrameCount #2
_rlnTomoNominalStageTiltAngle #3
_rlnTomoNominalTiltAxisAngle #4
_rlnMicrographPreExposure #5
_rlnTomoNominalDefocus #6
data/Position_19_001_0.00_20251010_172146_EER.eer	1	-0.020000	85.000000	0.000000	-2.000000
data/Position_19_002_3.00_20251010_172203_EER.eer	1	2.990000	85.000000	4.000000	-2.000000
data/Position_19_003_-3.00_20251010_172230_EER.eer	1	-3.010000	85.000000	8.000000	-2.000000
data/Position_19_004_-6.00_20251010_172250_EER.eer	1	-6.010000	85.000000	12.000000	-2.000000
data/Position_19_005_6.00_20251010_172316_EER.eer	1	5.990000	85.000000	16.000000	-2.000000
data/Position_19_006_9.00_20251010_172333_EER.eer	1	8.990000	85.000000	20.000000	-2.000000
data/Position_19_007_-9.00_20251010_172400_EER.eer	1	-9.010000	85.000000	24.000000	-2.000000
data/Position_19_008_-12.00_20251010_172419_EER.eer	1	-12.010000	85.000000	28.000000	-2.000000
        """
        tsName = batch['tsName']
        mdoc = batch['mdoc']
        mdocFile = self.join('mdocs', f'{tsName}.mdoc')
        # Write a copy of the mdoc file
        mdoc.write(mdocFile)
        # Write the TS star file
        tsStarFile = self.join('tilt_series', f"{tsName}.star")
        
        tsTable = Table([
            'rlnMicrographMovieName',
            'rlnTomoTiltMovieFrameCount',
            'rlnTomoNominalStageTiltAngle',
            'rlnTomoNominalTiltAxisAngle',
            'rlnMicrographPreExposure',
            'rlnTomoNominalDefocus'
        ])

        preExposure = 0
        for z, s in mdoc.zsections():
            movieName = mdoc.getSubFrameBase(s)
            a = float()
            tsTable.addRowValues(
                rlnMicrographMovieName=os.path.join(self.tsFolder, movieName),
                rlnTomoTiltMovieFrameCount=1,
                rlnTomoNominalStageTiltAngle=s['TiltAngle'],
                rlnTomoNominalTiltAxisAngle=self.tiltAxisAngle,
                rlnMicrographPreExposure='%0.3f' % preExposure,
                rlnTomoNominalDefocus=s['TargetDefocus'])
            preExposure += self.acq.total_dose

        with StarFile(tsStarFile, 'w') as sfOut:
            sfOut.writeTable(tsName, tsTable,
                             computeFormat="left",
                             timeStamp=True)

        if self.allTsTable is None:
            self.allTsTable = Table([
                'rlnTomoName',
                'rlnTomoTiltSeriesStarFile',
                'rlnVoltage',
                'rlnSphericalAberration',
                'rlnAmplitudeContrast',
                'rlnMicrographOriginalPixelSize',
                'rlnTomoHand',
                'rlnOpticsGroupName',
                'rlnMdocFile'
            ])

        self.allTsTable.addRowValues(
            rlnTomoName=tsName,
            rlnTomoTiltSeriesStarFile=tsStarFile,
            rlnVoltage=self.acq.voltage,
            rlnSphericalAberration=self.acq.cs,
            rlnAmplitudeContrast=self.acq.amplitude_contrast,
            rlnMicrographOriginalPixelSize=self.acq.pixel_size,
            rlnTomoHand=-1,
            rlnOpticsGroupName='optics_group1',
            rlnMdocFile=mdocFile
        )

        with StarFile(self.outputStar, 'w') as sfOut:
            sfOut.writeTable('global', self.allTsTable,
                             computeFormat="left",
                             timeStamp=True)

    def prerun(self):
        previousTs = set()
        self.allTsTable = None

        # Load already seen movies if we are continuing the job
        if os.path.exists(self.outputStar):
            self.allTsTable = StarFile.getTableFromFile('global', self.outputStar)
            previousTs.update(row.rlnTomoName for row in self.allTsTable)
        else:
            self.mkdir('tilt_series')
            self.mkdir('mdocs')

        self.log(f">>>> STARTING RUN: ")
        self.log(f"  - Mdocs pattern: {Color.cyan(self.mdocPattern)}")
        self.log(f"  - Input TS pattern:  {Color.cyan(self.mdocPattern)}")
        self.log(f"  - TS from previous run: {Color.cyan(len(previousTs))}")

        batchMgr = MdocBatchManager(self.mdocPattern, self.tmpDir,
                                    moviesPath=self.tsFolder,
                                    blacklist=previousTs,
                                    createBatch=False,
                                    timeout=self.wait['timeout'])
        g = self.addGenerator(batchMgr.generate)
        self.addProcessor(g.outputQueue, self._output)


if __name__ == '__main__':
    ImportTsPipeline.main()



