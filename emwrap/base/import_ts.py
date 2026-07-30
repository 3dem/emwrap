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
from emtools.image import Image

from emwrap.base import ProcessingPipeline


class ImportTsPipeline(ProcessingPipeline):
    name = 'emw-import-ts'

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
        tsName = batch['tsName']
        mdoc = batch['mdoc']
        mdocFile = self.join('mdocs', f'{tsName}.mdoc')
        # Write a copy of the mdoc file
        mdoc.write(mdocFile)
        # Write the TS star file
        tsStarFile = self.join('tilt_series', f"{tsName}.star")
        rows = []
        preExposure = 0
        dims = None
        N = 0
        minAngle = 999
        maxAngle = -999
        # There are mdocs where the ZValue index does not match the acquisition order
        # We will sort the zvalues by date and assign the index accordingly
        for z, s in mdoc.zsections(sort='date'):
            n = 0
            N += 1
            movieName = mdoc.getSubFrameBase(s)
            framesPath = os.path.join(self.tsFolder, movieName)
            if dims is None:
                x, y, n = Image.get_dimensions(framesPath)
            angle = float(s['TiltAngle'])
            minAngle = min(minAngle, angle)
            maxAngle = max(maxAngle, angle)
            rows.append(dict(
                rlnTomoTiltMovieIndex=N,
                rlnMicrographMovieName=framesPath,
                rlnTomoTiltMovieFrameCount=n,
                rlnTomoNominalStageTiltAngle=s['TiltAngle'],
                rlnTomoNominalTiltAxisAngle=self.tiltAxisAngle,
                rlnMicrographPreExposure='%0.3f' % preExposure,                
                rlnTomoNominalDefocus=s['TargetDefocus'],
                #emwMdocPriorRecordDose=s['PriorRecordDose'],
                #emwMdocDateTime=s['DateTime']
            ))

            preExposure += self.acq.total_dose

        with StarFile(tsStarFile, 'w') as sfOut:
            tsTable = Table.fromDict(rows)
            sfOut.writeTable(tsName, tsTable,
                             computeFormat="left",
                             timeStamp=True)

        ps = self.acq.pixel_size
        rowValues = dict(
            rlnTomoName=tsName,
            rlnTomoTiltSeriesStarFile=tsStarFile,
            rlnVoltage=self.acq.voltage,
            rlnSphericalAberration=self.acq.cs,
            rlnAmplitudeContrast=self.acq.amplitude_contrast,
            rlnMicrographOriginalPixelSize=ps,
            rlnTomoHand=-1,
            rlnOpticsGroupName='optics_group1',
            rlnTomoMdocFile=mdocFile
        )
        if gain := self.acq.get('gain'):
            rowValues['rlnMicrographGainName'] = gain
        if total_dose := self.acq.get('total_dose'):
            rowValues['rlnMicrographDoseRate'] = total_dose

        if self.allTsTable is None:
            self.allTsTable = Table.fromDict([rowValues])
        else:
            self.allTsTable.addRowValues(**rowValues)
        
        with StarFile(self.outputStar, 'w') as sfOut:
            sfOut.writeTable('global', self.allTsTable,
                             computeFormat="left",
                             timeStamp=True)  
        self.writeRelionOutputNodes([[self.outputStar, 'TomogramGroupMetadata.star.emwrap.frameseries']])          
        self.writeInfo()

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
        self.log(f"  - Input TS folder:  {Color.cyan(self.tsFolder)}")
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



