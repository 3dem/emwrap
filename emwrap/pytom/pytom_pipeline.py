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
from emwrap.base import ProcessingPipeline, getTomoPixelSize, getTomogram

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

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        # FIXME add support to comma separated values for parallels in batches
        self.gpuList = [self.get_gpu_list(args['gpus'], as_string=True)]
        self.launcher = args.get('launcher', '') or ProcessingPipeline.get_launcher('PYTOM')

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

    def _output(self, batch):
        tsName = batch['tsName']

        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
        else:
            outFiles = self._moveBatchFiles(batch)
            rowDict = batch['rowDict']
            rowDict['rlnParticleNumber'] = 0
            rowDict['rlnCoordinatesMetadata'] = 'None'
            if coordsStar := outFiles.get('default_particles.star'):
                t = StarFile.getTableFromFile('particles', coordsStar)
                rowDict.update({
                    'rlnCoordinatesMetadata': coordsStar,
                    'rlnParticleNumber': len(t)
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
            pixel_size=getTomoPixelSize(row)
        )

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
            t = StarFile.getTableFromFile(tsName, row.rlnTomoTiltSeriesStarFile,
                                          guessType=False)

            batch = Batch(id=batchId, index=counter,
                        rowDict=row._asdict(),
                        path=os.path.join(self.tmpDir, batchId),
                        tsName=tsName, tomogram=getTomogram(row),
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
            self._dims = Image.get_dimensions(getTomogram(first))
        x, y, n = self._dims
        ps = getTomoPixelSize(first)
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
        n = sum(row.rlnParticleNumber for row in self.outTable)
        self.outputs = {
            'TomogramCoordinates': {
                'label': 'Tomogram Coordinates',
                'type': 'TomogramCoordinates',
                'info': f"{n} particles from {N} tomograms",
                'files': [
                    [self.outTomoStar, 'TomogramGroupMetadata.star.relion.tomo.tomocoordinates']
                ]
            }
        }

    def prerun(self):
        self.log("Testing output generation, nothing else....exiting.")

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

    def postrun(self):
        self.log("Generating Relion 5 compatible outputs: optimisation_set.star and related files.")
        tomoCoordsTable = StarFile.getTableFromFile('global', self.join('tomograms_coords.star'))

        def _output(key):
            return self.join(f'pytom_{key}.star')

        optsetFn = _output('optimisation_set')
        tomogramsFn = _output('tomograms')
        particlesFn = _output('particles')

        # First create the optimisation_set.star file and then the associated tomograms and particles
        with StarFile(optsetFn, 'w') as sf:
            t = Table(columns=['rlnTomoParticlesFile', 'rlnTomoTomogramsFile'])
            t.addRowValues(particlesFn, tomogramsFn)
            sf.writeTable('optimisation_set', t, timeStamp=True)

        with StarFile(tomogramsFn, 'w') as sf:
            newTomoTable = tomoCoordsTable.cloneColumns(['rlnCoordinatesMetadata'])
            for row in tomoCoordsTable:
                rowDict = row._asdict()
                del rowDict['rlnCoordinatesMetadata']
                newTomoTable.addRowValues(**rowDict)
            sf.writeTable('global', newTomoTable, timeStamp=True)

        ptsColumns = [
            'rlnTomoName',
            'rlnCenteredCoordinateXAngst',
            'rlnCenteredCoordinateYAngst',
            'rlnCenteredCoordinateZAngst',
            'rlnAngleRot',
            'rlnAngleTilt',
            'rlnAnglePsi',
            'rlnLCCmax'
        ]

        with StarFile(particlesFn, 'w') as sf:
            ptsTable = Table(columns=ptsColumns)
            for row in tomoCoordsTable:
                tsName = row.rlnTomoName
                # FIMXE Now it is hardcoded how to match tomoName with its particle
                # we might handle this at the batch output, instead of using simply mv
                # we can prefix it with the proper tomoName
                coordsFn = self.join('Coordinates', f'{tsName}_particles_relion5.star')
                self.log(f'Mapped tomo {tsName} to coordinates {coordsFn}')
                if os.path.exists(coordsFn):
                    coordsTable = StarFile.getTableFromFile('particles', coordsFn)
                    for coord in coordsTable:
                        values = {c: getattr(coord, c) for c in ptsColumns}
                        values['rlnTomoName'] = tsName
                        ptsTable.addRowValues(**values)

                else:
                    self.log(f'ERROR: coordinate file {coordsFn} does not exist, skipping')

            sf.writeTable('particles', ptsTable, timeStamp=True)


if __name__ == '__main__':
    PyTomPipeline.main()
