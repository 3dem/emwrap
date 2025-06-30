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

from emtools.utils import Color, FolderManager, Path
from emtools.metadata import Table, Column, StarFile, RelionStar
from emtools.jobs import TsStarBatchManager
from emtools.image import Image
from emwrap.base import ProcessingPipeline

from .motioncor import Motioncor


class McPipelineTomo(ProcessingPipeline):
    """ Pipeline specific to Motioncor tilt-series processing. """
    name = 'emw-mc-tomo'
    input_name = 'in_movies'

    def __init__(self, all_args):
        args = all_args[self.name]
        ProcessingPipeline.__init__(self, args)
        self.gpuList = args['gpu'].split()
        self.outputMicDir = self.join('Micrographs')
        self.inputStar = args['in_movies']
        self.inputLen = 0
        self.acq = None
        self.outputTsDir = 'TS'
        self.acqInfo = all_args['acquisition']
        self._DEBUG_only_output = 'DEBUG_only_output' in args
        extra = self._args['motioncor']['extra_args']
        self.bin = float(extra.get('-FtBin', 1.0))

    def get_motioncor_proc(self, gpu):
        def _motioncor(batch):
            # In this pipeline, batch are not created until now, when we are
            # processing each one.
            # We also need to create the links to movie files
            items = batch['items']
            batch.create()

            # Link all input movies in the batch folder
            def _absfn(item):
                return os.path.abspath(item['rlnMicrographMovieName'])

            framesFolder = os.path.dirname(_absfn(items[0]))
            os.symlink(framesFolder, batch.join('frames'))

            for item in items:
                baseName = os.path.basename(_absfn(item))
                os.symlink(os.path.join('frames', baseName), batch.join('movie-' + baseName))

            # Link the gain reference
            if input_gain := self.acqInfo.get('gain', None):
                base_gain = os.path.basename(input_gain)
                os.symlink(os.path.abspath(input_gain), batch.join(base_gain))
                self.acq['gain'] = base_gain

            mc = Motioncor(self.acq, **self._args['motioncor'])
            mc.process_batch(batch, gpu=gpu)
            return batch

        return _motioncor

    def _output(self, batch):
        tsName = batch['tsName']
        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
        else:
            batchFolder = self._getOutputTsFolder(tsName)
            batchFolder.create()
            tsStar = f"{batchFolder.path}.star"

            with StarFile(tsStar, 'w') as sfOut:
                tsTable = Table(columns=["rlnMicrographMovieName",
                                         "rlnTomoTiltMovieFrameCount",
                                         "rlnTomoNominalStageTiltAngle",
                                         "rlnTomoNominalTiltAxisAngle",
                                         "rlnMicrographPreExposure",
                                         "rlnTomoNominalDefocus",
                                         "rlnMicrographNameEven",
                                         "rlnMicrographNameOdd",
                                         "rlnMicrographName",
                                         "rlnMicrographMetadata",
                                         "rlnAccumMotionTotal",
                                         "rlnAccumMotionEarly",
                                         "rlnAccumMotionLate"])
                sfOut.writeTimeStamp()
                sfOut.writeHeader(tsName, tsTable)
                movieDimensions = None

                for item in batch['items']:
                    values = dict(item)  # take initial values from input row
                    movName = item['rlnMicrographMovieName']

                    # Read image dimensions only once
                    if movieDimensions is None:
                        movieDimensions = Image.get_dimensions(movName)

                    baseName = Path.removeBaseExt(movName)
                    files = {}
                    for suffix in ['', '_ODD', '_EVN']:
                        name = f'{baseName}{suffix}.mrc'
                        src = batch.join('output', f'micrograph-{name}')
                        dst = batchFolder.join(name)
                        #self.log(f"Moving {src} -> {dst}")
                        shutil.move(src, dst)
                        files[suffix] = dst

                    micFile = files['']
                    micStar = Path.replaceExt(micFile, '.star')
                    values['rlnMicrographNameEven'] = files['_EVN']
                    values['rlnMicrographNameOdd'] = files['_ODD']
                    values['rlnMicrographName'] = micFile
                    values['rlnMicrographMetadata'] = micStar
                    values['rlnAccumMotionTotal'] = 0
                    values['rlnAccumMotionEarly'] = 0
                    values['rlnAccumMotionLate'] = 0

                    # Read shifts from the input star file and write it
                    # to the proper destination, updating some values and
                    # column names
                    inMovStar = batch.join('output', f'micrograph-{baseName}.star')

                    with StarFile(inMovStar) as sf:
                        with StarFile(micStar, 'w') as sfOut2:
                            sfOut2.writeTimeStamp()
                            t = sf.getTable('general')
                            row = t[0]._replace(rlnImageSizeX=movieDimensions[0],
                                                rlnImageSizeY=movieDimensions[1],
                                                rlnImageSizeZ=movieDimensions[2])
                            # Update some values of the first (only) row of general
                            sfOut2.writeSingleRow('general', row)
                            sfOut2.writeTable('global_shift', sf.getTable('global_shift'))

                    sfOut.writeRowValues(values)

            self._writeCorrectedTS()

        batch.info['tsName'] = batch['tsName']  # Store tsName in the info.json
        self.updateBatchInfo(batch)
        if self.inputLen:
            totalOutput = len(self.info['batches'])
            percent = totalOutput * 100 / self.inputLen
            batch.log(f">>> Processed {Color.green(totalOutput)} out of "
                      f"{Color.red(self.inputLen)} "
                      f"({Color.bold('%0.2f' % percent)} %)", flush=True)
        return batch

    def _getInputTsTable(self):
        """ Read input star file and return the 'global' table. """
        with StarFile(self.inputStar) as sf:
            t = sf.getTable('global')
            self.inputLen = len(t)  # Let's update the inputLen property
            return t
        return None

    def _getOutputTsFolder(self, tsName):
        return FolderManager(self.join(self.outputTsDir, tsName))

    def _writeCorrectedTS(self):
        inputTs = self._getInputTsTable()
        cols = inputTs.getColumnNames()
        outTs = Table(cols + ['rlnTomoTiltSeriesPixelSize'])
        newPixelSize = self.acq.pixel_size * self.bin

        self.log(f"DEBUG >>> corrected TS>>> ps: {self.acq.pixel_size} "
                 f"bin: {self.bin} new_ps: {newPixelSize}")

        with StarFile(self.join('corrected_tilt_series.star'), 'w') as sfOut:
            sfOut.writeTimeStamp()
            sfOut.writeHeader('global', outTs)
            for row in inputTs:
                tsName = row.rlnTomoName
                tsFolder = self._getOutputTsFolder(tsName)
                tsStarName = f"{tsFolder.path}.star"
                if os.path.exists(tsStarName):
                    values = row._asdict()
                    values.update(rlnTomoTiltSeriesStarFile=tsStarName,
                                  rlnTomoTiltSeriesPixelSize=newPixelSize)
                    sfOut.writeRowValues(values)

    def prerun(self):
        if self._DEBUG_only_output:
            print("DEBUG: Only generating output...")
            self._writeCorrectedTS()
            return

        self.dumpArgs(printMsg="Input args")

        inputTs = self._getInputTsTable()
        self.acq = RelionStar.get_acquisition(inputTs)
        batchMgr = TsStarBatchManager(inputTs, self.tmpDir)
        g = self.addGenerator(batchMgr.generate)
        outputQueue = None
        self.mkdir(self.outputTsDir)
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_motioncor_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


def main():
    McPipelineTomo.runFromArgs()


if __name__ == '__main__':
    main()
