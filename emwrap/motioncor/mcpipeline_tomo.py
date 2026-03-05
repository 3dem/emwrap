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
import time
import sys
import subprocess
import re
import shlex

from emtools.utils import Color, FolderManager, Path
from emtools.metadata import Table, Column, StarFile, RelionStar, Acquisition
from emtools.jobs import TsStarBatchManager
from emtools.image import Image
from emwrap.base import ProcessingPipeline

from .motioncor import Motioncor


class McPipelineTomo(ProcessingPipeline):
    """ Pipeline specific to Motioncor tilt-series processing. """
    name = 'emw-mc-tomo'

    def __init__(self, input_args, output):
        super().__init__(input_args, output)
        args = self._args
        self.get_gpu_list(args['gpu_ids'])
        self.outputMicDir = self.join('Micrographs')
        self.inputLen = 0
        self.micsPerTs = 0
        self.movieDims = ()
        self.acq = self.loadAcquisition()
        self.inputGain = self.acq.get('gain', None)
        self.outputTsDir = 'TS'
        self._DEBUG_only_output = 'DEBUG_only_output' in args
        self.get_extras()
        ###print(f"\n{os.path.basename(__file__)}:50: self._args='{self._args}'")

    def get_gpu_list(self, gpu_field):
        """
        If GPU list not provided, then uses all.
        If GPU list provided, then parses into list.
        """

        # Trap for double double quotes
        if gpu_field.startswith('"') and gpu_field.endswith('"'):
            gpu_field = gpu_field[1:-1]

        gpu_field = gpu_field.strip()

        # Use all GPUs
        if not gpu_field:
            gpuResult = subprocess.run(
                ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader"],
                capture_output=True, text=True, check=True)
            self.gpuList = [int(line.strip()) for line in gpuResult.stdout.splitlines()]

        else:
            parts = re.split(r"[,\s]+", gpu_field)
            self.gpuList = [int(p) for p in parts if p]

    def get_extras(self):
        """
        Split other_motioncor2_args.
        Get FtBin.
        """

        extra = self._args.get('other_motioncor2_args', '')

        # Turn other_motioncor2_args into a dictionary
        if extra:
            tokens = shlex.split(extra)
            extra_dict = dict(zip(tokens[::2], tokens[1::2]))
        else:
            extra_dict = {}

        self.bin = float(extra_dict.get('-FtBin', 1.0))
        self._args['extra_args'] = extra_dict
        del self._args['other_motioncor2_args']

    def get_motioncor_proc(self, gpu):
        def _motioncor(batch):
            # In this pipeline, batches are not created until now,
            # when we are processing each one.
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
            acq = Acquisition(self.acq)
            if self.inputGain:
                acq['gain'] = batch.link(self.inputGain)

            mc = Motioncor(acq, **self._args)
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
                        if os.path.exists(src):
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
        """
        Read input star file and return the 'global' table.
        Also stores:
            inputLen : number of tilt series
            micsPerTs : number of micrographs per tilt series
            movieDims : movie dimensions (x, y, num_frames)

        Adapted from warp.WarpBaseTsAlign._getInfo()
        """

        inputStar = self._args['input_star_mics']
        with StarFile(inputStar) as sf:
            tsAllTable = sf.getTable('global')
            ###sf.printTable(tsAllTable)
            self.inputLen = len(tsAllTable)  # Let's update the inputLen property

            # Get number of frame from first movie in first tilt series
            first = tsAllTable[0]
            tsTable = StarFile.getTableFromFile(first.rlnTomoName, first.rlnTomoTiltSeriesStarFile)
            self.micsPerTs = len(tsTable)
            movieFn = tsTable[0].rlnMicrographMovieName
            self._args['movieDims'] = self.movieDims = Image.get_dimensions(movieFn)
            # (What happens if it isn't a movie? (only 2 dimensions will be returned))

            return tsAllTable
        return None

    def _getOutputTsFolder(self, tsName):
        return FolderManager(self.join(self.outputTsDir, tsName))

    def _writeCorrectedTS(self):
        inputTs = self._getInputTsTable()
        cols = inputTs.getColumnNames()
        outTs = Table(cols + ['rlnTomoTiltSeriesPixelSize'])
        newPixelSize = self.acq.pixel_size * self.bin
        newTsStarFile = self.join('corrected_tilt_series.star')

        with StarFile(newTsStarFile, 'w') as sfOut:
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

        self.outputs = {
            'TiltSeries': {
                'label': 'Tilt Series',
                'type': 'TiltSeries',
                'info': f"{len(inputTs)} items, {self.movieDims[0]} x {self.movieDims[1]} x {self.micsPerTs}, {newPixelSize:0.3f} Å/px",
                'files': [
                    [newTsStarFile, 'TomogramGroupMetadata.star.relion.tomo.import']
                ]
            }
        }

    def prerun(self):
        if self._DEBUG_only_output:
            print("DEBUG: Only generating output...")
            self._writeCorrectedTS()
            return

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

if __name__ == '__main__':
    McPipelineTomo.main()
