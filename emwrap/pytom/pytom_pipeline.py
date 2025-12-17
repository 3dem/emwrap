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
from datetime import datetime, timedelta

from emtools.utils import Color, FolderManager, Path, Process
from emtools.metadata import StarFile, Acquisition, StarMonitor, Table
from emtools.jobs import Batch
from emtools.image import Image
from emwrap.base import ProcessingPipeline

from .pytom import PyTom


class PyTomPipeline(ProcessingPipeline):
    """ Pipeline PyTom picking in a set of tomograms. """
    name = 'emw-pytom'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        # FIXME add support to comma separated values for parallels in batches
        self.gpuList = [self.get_gpu_list(args['gpus'], as_string=True)]

        self.acq = self.loadAcquisition()

        # FIXME: Read this from the input arguments
        self.wait = {
            'timeout': int(args.get('wait.timeout', 60)),
            'file_change': int(args.get('wait.file_change', 30)),
            'sleep': int(args.get('wait.sleep', 30)),
        }

        self.inTomoStar = self._args['input_tomograms']
        self.outTomoStar = self.join('tomograms_coords.star')

        self._pytom_args = {
            'pytom': self.get_subargs('pytom'),
            'pytom_extract': self.get_subargs('pytom_extract')
        }

    def get_pytom_proc(self, gpu):

        def _pytom(batch):
            args = dict(self._pytom_args)
            args['pytom']['g'] = gpu
            pytom = PyTom(self.acq, args)
            pytom.process_batch(batch)
            return batch

        return _pytom

    def _output(self, batch):
        tsName = batch['tsName']

        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
        else:
            Process.system(f"mv {batch.join('output', '*')} {self.join('Coordinates')}")
            rowDict = batch['rowDict']
            coordsStar = Path.replaceBaseExt(batch['tomogram'], '_default_particles.star')
            coordsStarPath = self.join('Coordinates', coordsStar)
            nCoords = 0
            if os.path.exists(coordsStarPath):
                t = StarFile.getTableFromFile('particles', coordsStarPath)
                nCoords = len(t)

            rowDict.update({
                'rlnCoordinatesMetadata': coordsStarPath,
                'rlnCoordinatesCount': nCoords
            })
            self.outTable.addRowValues(**rowDict)
            with StarFile(self.outTomoStar, 'w') as sfOut:
                sfOut.writeTable('global', self.outTable,
                                 timeStamp=True, computeFormat='left')

            self._updateInput()
            self._updateOutput()
            self.updateBatchInfo(batch)

        return batch

    def _loadAcquisitionFromRow(self, row):
        return Acquisition(
            voltage=row.rlnVoltage,
            cs=row.rlnSphericalAberration,
            amplitude_contrast=row.rlnAmplitudeContrast,
            pixel_size=row.rlnTomogramPixelSize
        )

    def _getInputTomograms(self):
        """ Create a generator for input tomograms. """
        # Let's create a STAR file monitor to check for incoming tomograms
        # Get the tomograms IDs to avoid processing again that ones
        counter = 0
        blacklist = []
        self.outTable = None
        inTable = StarFile.getTableFromFile('global', self.inTomoStar)
        n = len(inTable)
        if os.path.exists(self.outTomoStar):
            self.outTable = StarFile.getTableFromFile('global', self.outTomoStar)
            counter = len(self.outTable)
            self.log(f"Previously processed tomograms: {Color.cyan(counter)}")
            blacklist = self.outTable
        else:
            extraLabels = ['rlnCoordinatesMetadata', 'rlnCoordinatesCount']
            self.outTable = Table(inTable.getColumnNames() + extraLabels)


        self.acq.update(self._loadAcquisitionFromRow(inTable[0]))
        self.log(f"Input star file: {Color.bold(self.inTomoStar)}")
        self.log(f"Total input tomograms: {Color.bold(n)}")
        self.log(f"Tomograms to process: {Color.green(n - counter)}")

        monitor = StarMonitor(self.inTomoStar, 'global',
                              lambda row: row.rlnTomoName,
                              timeout=self.wait['timeout'],
                              blacklist=blacklist)

        # This will keep monitor the star files for new tomograms until timed out.
        for row in monitor.newItems():
            tsName = row.rlnTomoName
            counter += 1
            nowPrefix = datetime.now().strftime('%y%m%d-%H%M%S')
            batchId = f"{nowPrefix}_{counter:03}_{tsName}"
            # FIXME: Now reading these values from Warp tomostar, but
            # it should be from Relion's star files
            t = StarFile.getTableFromFile('', row.wrpTomostar)
            yield Batch(id=batchId, index=counter,
                        rowDict=row._asdict(),
                        path=os.path.join(self.tmpDir, batchId),
                        tsName=tsName, tomogram=row.rlnTomogram,
                        defocus=float(row.rlnDefocus),
                        tilt_angles=[float(r.wrpAngleTilt) for r in t],
                        dose_accumulation=[float(r.wrpDose) for r in t])

    def _updateInput(self):
        inputTomoTable = StarFile.getTableFromFile('global', self.inTomoStar)
        first = inputTomoTable[0]
        N = len(inputTomoTable)
        if self._dims is None:
            self._dims = Image.get_dimensions(first.rlnTomogram)
        x, y, n = self._dims
        ps = first.rlnTomogramPixelSize
        bin = first.rlnTomoTomogramBinning
        self.inputs = {
            'Tomograms': {
                'label': 'Tomograms',
                'type': 'Tomograms',
                'info': f"{N} items, {x} x {y} x {n}, {ps:0.3f} Å/px, bin {bin:0.1f}",
                'files': [
                    [self.inTomoStar, 'TomogramGroupMetadata.star.relion.tomo.tomograms']
                ]
            }
        }

    def _updateOutput(self):
        N = len(self.outTable)
        n = sum(row.rlnCoordinatesCount for row in self.outTable)
        self.outputs = {
            'TomogramsCoordinates': {
                'label': 'Tomograms Coordiantes',
                'type': 'TomogramsCoordinates',
                'info': f"{n} particles from {N} tomograms",
                'files': [
                    [self.outTomoStar, 'TomogramGroupMetadata.star.relion.tomo.tomocoordinates']
                ]
            }
        }

    def prerun(self):
        self._dims = None
        self._updateInput()
        self.writeInfo()

        g = self.addGenerator(self._getInputTomograms)
        outputQueue = None
        self.mkdir('Coordinates')
        self.log(f"Creating {len(self.gpuList)} processing threads.", flush=True)

        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_pytom_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


if __name__ == '__main__':
    PyTomPipeline.main()
