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
import sys
import json
import shutil
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process, FolderManager
from emtools.metadata import Acquisition, StarFile, RelionStar, Mdoc
from emtools.jobs import MdocBatchManager

from emwrap.base import ProcessingPipeline
from .preprocessing import Preprocessing


class TomoPreprocessingPipeline(ProcessingPipeline):
    """ Pipeline to run Preprocessing in batches. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)
        self._args = args

        self.gpuList = args['gpu'].split()
        self.outputDirs = {}
        self.inputMovies = args['in_movies']
        self.inputMdocs = args['mdoc']
        self.inputTimeOut = args.get('input_timeout', 3600)
        self.moviesImport = None
        self.outputTsDir = 'TS'
        self._totalInput = self._totalOutput = 0
        self._pp_args = args['preprocessing']
        self.acq = Acquisition(self._args['acquisition'])

    def dumpArgs(self, printMsg=''):
        argStr = json.dumps(self._args, indent=4) + '\n'
        with open(self.join('args.json'), 'w') as f:
            f.write(argStr)
        if printMsg:
            self.log(f"{Color.cyan(printMsg)}: \n\t{Color.bold(argStr)}")

    def get_preprocessing(self, gpu):
        def _preprocessing(batch):
            # Replace items temporarily for preprocessing execution
            items = batch['items']
            batch['items'] = [{
                'rlnMicrographMovieName': Mdoc.getSubFrameBase(item[1]),
                'rlnOpticsGroup': 1
            } for item in items]
            with batch.execute():
                args = dict(self._args['preprocessing'])
                args['acquisition'] = dict(self.acq)
                ppBatch = Preprocessing(args).process_batch(batch, gpu=gpu)
            batch['items'] = items
            batch['results'] = ppBatch['results']
            return batch

        return _preprocessing

    def _output(self, batch):
        if batch.error:
            batch.log(f"ERROR: {batch.error}")
        else:
            # Move files to TS folder
            tsName = batch['tsName']
            tsFolder = FolderManager(self.join(self.outputTsDir, tsName))
            tsFolder.create()

            def _move_file(fn):
                newBase = os.path.basename(fn)[8:]  # remove aligned_ prefix
                dstFn = tsFolder.join(newBase)
                # Move to TS folder
                shutil.move(batch.join(fn), dstFn)
                return dstFn

            tsTable = RelionStar.tiltseries_table()

            for r, item in zip(batch['results'], batch['items']):
                key, section = item
                movieName = Mdoc.getSubFrameBase(section)
                srcMicName = r.pop('rlnMicrographName')
                srcMicStar = srcMicName.replace('.mrc', '.star')
                micName = _move_file(srcMicName)
                ctfName = _move_file(r.pop('rlnCtfImage'))
                del r['rlnOpticsGroup']
                tsTable.addRowValues(
                    rlnMicrographMovieName=os.path.join(self.inputMovies, movieName),
                    rlnTomoTiltMovieFrameCount=8, #FIXME
                    rlnTomoNominalStageTiltAngle=0.001, #FIXME
                    rlnTomoNominalTiltAxisAngle=85, #fixme
                    rlnMicrographPreExposure=section['PriorRecordDose'],
                    rlnTomoNominalDefocus=section['TargetDefocus'],
                    rlnMicrographNameEven="",
                    rlnMicrographNameOdd="",
                    rlnMicrographName=micName,
                    rlnCtfImage=ctfName,
                    rlnCtfIceRingDensity=0,  # FIXME
                    rlnMicrographMetadata=_move_file(srcMicStar),
                    rlnAccumMotionTotal=0,  # FIXME
                    rlnAccumMotionEarly=0,  # FIXME
                    rlnAccumMotionLate=0,   # FIXME
                    **r
                )
            tsStar = tsFolder.join(tsName + '.star')
            batch.log(f"Writing TS star file: {tsStar}")
            with StarFile(tsStar, 'w') as sf:
                sf.writeTimeStamp()
                sf.writeTable(tsName, tsTable)

            with self.outputLock:
                allStar = self.join('tilt_series.star')
                t = RelionStar.global_tiltseries_table()
                firstTime = not os.path.exists(allStar)
                with StarFile(allStar, 'a') as sf:
                    if firstTime:
                        sf.writeTimeStamp()
                        sf.writeHeader('global', t)
                    sf.writeRow(t.Row(
                        rlnTomoName=tsName,
                        rlnTomoTiltSeriesStarFile=tsStar,
                        rlnVoltage=self.acq.voltage,
                        rlnSphericalAberration=self.acq.cs,
                        rlnAmplitudeContrast=self.acq.amplitude_contrast,
                        rlnMicrographOriginalPixelSize=self.acq.pixel_size,
                        rlnTomoHand=1,  # FIXME
                        rlnOpticsGroupName='OpticsGroup1',  # FIXME ???
                        rlnTomoTiltSeriesPixelSize=self.acq.pixel_size,  # FIXME If binning
                    ))

        self.updateBatchInfo(batch)
        return batch

    def prerun(self):
        self.dumpArgs(printMsg="Input args")
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}")
        inputs = self.info['inputs']
        inputs.append({
            'label': 'Movies',
            'files': []  #FIXME
        })
        # TODO: support for streaming when new Mdocs are written
        batchMgr = MdocBatchManager(Mdoc.glob(self.inputMdocs), self.tmpDir,
                                    suffix=self._args.get('mdoc_suffix', None),
                                    movies=self.inputMovies)
        g = self.addGenerator(batchMgr.generate, queueMaxSize=4)
        outputQueue = None
        self.log(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_preprocessing(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


def main():
    input_args = ProcessingPipeline.getInputArgs('emw-tomo-preprocessing',
                                                 'in_movies')
    TomoPreprocessingPipeline(input_args).run()


if __name__ == '__main__':
    main()
