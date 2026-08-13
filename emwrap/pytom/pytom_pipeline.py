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
from emtools.metadata import StarFile, Acquisition, StarMonitor, Table, RelionStar
from emtools.jobs import Batch
from emtools.image import Image
from emwrap.base import ProcessingPipeline

from .pytom import PyTom


class PyTomPipeline(ProcessingPipeline):
    """ Pipeline PyTom picking in a set of tomograms. """
    name = 'emw-pytom'

    # Expected suffices after running PyTom in one batch
    OUTPUT_SUFFICES = ['angles.mrc',
                       'extraction_graph.svg',
                       'job.json',
                       'particles_default.star',
                       'particles_relion5.star',
                       'scores.mrc']

    PARTICLES_COLUMNS = [
        'rlnTomoName',
        'rlnCenteredCoordinateXAngst',
        'rlnCenteredCoordinateYAngst',
        'rlnCenteredCoordinateZAngst',
        'rlnAngleRot',
        'rlnAngleTilt',
        'rlnAnglePsi',
        'rlnLCCmax'
    ]

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        # FIXME add support to comma separated values for parallels in batches
        self.gpuList = self.get_gpu_list(args['gpus'])
        self.launcher = args.get('launcher', '') or ProcessingPipeline.get_launcher('PYTOM')

        self.inTomoStar = self._args['input_tomograms']
        self.acq = self.loadAcquisition(self.inTomoStar)
        self.outTomoStar = self.join('tomograms.star')
        self.outParticlesStar = self.join('particles.star')
        self.outTomoOptimisationSet = self.join('optimisation_set.star')

        # FIXME: Read this from the input arguments
        self.wait = {
            'timeout': int(args.get('wait.timeout', 60)),
            'file_change': int(args.get('wait.file_change', 30)),
            'sleep': int(args.get('wait.sleep', 30)),
        }

        self._pytom_args = {
            'pytom': self.get_subargs('pytom'),
            'pytom_extract': self.get_subargs('pytom_extract')
        }

    def get_pytom_proc(self, gpu):

        def _pytom(batch):
            args = dict(self._pytom_args)
            args['pytom']['g'] = gpu
            pytom = PyTom(self.acq, args)
            pytom.process_batch(batch, launcher=self.launcher)
            return batch

        return _pytom

    def _moveBatchFiles(self, batch):
        tsName = batch['tsName']
        missing = []
        outFiles = {}
        def _out(s):
            return self.join('Coordinates', f'{tsName}_{s}')

        for suffix in self.OUTPUT_SUFFICES:
            if files := batch.glob(f'output/*_{suffix}'):
                f = files[0]
                dst = _out(suffix)
                shutil.copy(f, dst)
                outFiles[suffix] = dst
            else:
                missing.append(suffix)
        if missing:
            with open(_out('missing.json'), 'w') as f:
                json.dump(missing, f)

        return outFiles

        Process.system(f"mv {batch.join('output', '*')} {self.join('Coordinates')}")

    def _processedTomoNames(self):
        if self.outTable is None:
            return set()
        return {row.rlnTomoName for row in self.outTable}

    def _ensureOptimisationSet(self):
        if os.path.exists(self.outTomoOptimisationSet):
            return

        with StarFile(self.outTomoOptimisationSet, 'w') as sf:
            values = {
                'rlnTomoParticlesFile': self.outParticlesStar,
                'rlnTomoTomogramsFile': self.outTomoStar,
            }
            sf.writeTable('optimisation_set', Table.fromDict(values), timeStamp=True)

        outputNodes = [[self.outTomoOptimisationSet,
                        'TomogramGroupMetadata.star.emwrap.TomoCoordinates']]
        self.writeRelionOutputNodes(outputNodes)

    def _appendParticles(self, tsName, coordsStar, batch):
        if os.path.exists(self.outParticlesStar):
            ptsTable = StarFile.getTableFromFile('particles', self.outParticlesStar)
        else:
            ptsTable = Table(columns=self.PARTICLES_COLUMNS)

        if any(row.rlnTomoName == tsName for row in ptsTable):
            batch.log(
                f"WARNING: particles for tomogram '{tsName}' already in "
                f"{self.outParticlesStar}, skipping")
            return

        if coordsStar and os.path.exists(coordsStar):
            coordsTable = StarFile.getTableFromFile('particles', coordsStar)
            for coord in coordsTable:
                values = {c: getattr(coord, c) for c in self.PARTICLES_COLUMNS}
                values['rlnTomoName'] = tsName
                ptsTable.addRowValues(**values)
        elif coordsStar:
            batch.log(f'ERROR: coordinate file {coordsStar} does not exist, skipping')

        with StarFile(self.outParticlesStar, 'w') as sf:
            sf.writeTable('particles', ptsTable, timeStamp=True)

    def _output(self, batch):
        tsName = batch['tsName']

        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
        else:
            outFiles = self._moveBatchFiles(batch)
            if tsName in self._processedTomoNames():
                batch.log(
                    f"WARNING: tomogram '{tsName}' already in "
                    f"{self.outTomoStar}, skipping output")
                return batch

            rowDict = batch['rowDict']
            rowDict['rlnParticleNumber'] = 0
            rowDict['rlnCoordinatesMetadata'] = 'None'
            if coordsStar := outFiles.get('particles_default.star'):
                t = StarFile.getTableFromFile('particles', coordsStar)
                rowDict.update({
                    'rlnCoordinatesMetadata': coordsStar,
                    'rlnParticleNumber': len(t)
                })

            self.outTable.addRowValues(**rowDict)
            with StarFile(self.outTomoStar, 'w') as sfOut:
                sfOut.writeTable('global', self.outTable,
                                 timeStamp=True, computeFormat='left')

            self._ensureOptimisationSet()
            self._appendParticles(
                tsName, outFiles.get('particles_relion5.star'), batch)

            self._updateInput()
            self.updateBatchInfo(batch)

        return batch

    def _getInputTomograms(self):
        """ Create a generator for input tomograms. """
        # Let's create a STAR file monitor to check for incoming tomograms
        # Get the tomograms IDs to avoid processing again that ones
        counter = 0
        blacklist = []
        self.outTable = None
        inTable = StarFile.getTableFromFile('global', self.inTomoStar,
                                            guessType=False)
        n = len(inTable)
        if os.path.exists(self.outTomoStar):
            self.outTable = StarFile.getTableFromFile('global', self.outTomoStar,
                                            guessType=False)
            counter = len(self.outTable)
            self.log(f"Previously processed tomograms: {Color.cyan(counter)}")
            blacklist = self.outTable
        else:
            extraLabels = ['rlnCoordinatesMetadata', 'rlnParticleNumber']
            self.outTable = Table(inTable.getColumnNames() + extraLabels)

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
            t = StarFile.getTableFromFile(tsName, row.rlnTomoTiltSeriesStarFile,
                                          guessType=False)

            batch = Batch(id=batchId, index=counter,
                        rowDict=row._asdict(),
                        path=os.path.join(self.tmpDir, batchId),
                        tsName=tsName, tomogram=RelionStar.getTomogram(row),
                        tilt_angles=[float(r.rlnTomoNominalStageTiltAngle) for r in t],
                        dose_accumulation=[float(r.rlnMicrographPreExposure) for r in t])
            if hasattr(row, 'rlnDefocus'):
                batch['defocus'] = float(row.rlnDefocus)

            yield batch

    def _updateInput(self):
        inputTomoTable = StarFile.getTableFromFile('global', self.inTomoStar)
        first = inputTomoTable[0]
        N = len(inputTomoTable)
        if self._dims is None:
            self._dims = Image.get_dimensions(RelionStar.getTomogram(first))
        x, y, n = self._dims
        ps = RelionStar.getTomoPixelSize(first)
        bin = first.rlnTomoTomogramBinning

    def prerun(self):
        self.log("Setting up PyTom picking pipeline.")

        self._dims = None
        self._updateInput()
        self.writeInfo()

        g = self.addGenerator(self._getInputTomograms)
        outputQueue = None
        self.mkdir('Coordinates')
        n = len(self.gpuList)
        self.log(f"Total GPUs: {n}.", flush=True)

        if n % 2 == 0:
            gpu_groups = [self.gpuList[i:i+2] for i in range(0, n, 2)]
        else:
            gpu_groups = [[g] for g in self.gpuList]

        self.log(f"Creating {len(gpu_groups)} processing threads.")

        for gpus in gpu_groups:
            gpuStr = ' '.join(str(g) for g in gpus)
            p = self.addProcessor(g.outputQueue,
                                  self.get_pytom_proc(gpuStr),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


if __name__ == '__main__':
    PyTomPipeline.main()
