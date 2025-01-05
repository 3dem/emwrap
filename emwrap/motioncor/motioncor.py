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

"""

"""

import os
import subprocess
import shutil
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path
from emtools.jobs import ProcessingPipeline, BatchManager, Args
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile


class Motioncor:
    """ Motioncor wrapper to run in a batch folder. """
    def __init__(self, args, **kwargs):
        if path := kwargs.get('path', None):
            self.path = path, self.version = int(kwargs['version'])
        else:
            self.path, self.version = Motioncor.__get_environ()
        self.args = Args(args)
        self.outputPrefix = "output/aligned_"

    def process_batch(self, gpu, batch):
        batch.mkdir('output')
        batch.mkdir('log')
        args = [self.path]
        ext = Path.getExt(batch.items[0].rlnMicrographMovieName)
        extLower = ext.lower()

        if extLower.startswith('.tif'):
            inArg = '-InTiff'
        elif extLower.startswith('.mrc'):
            inArg = '-InMrc'
        elif extLower.startswith('.eer'):
            inArg = '-InEer'
        else:
            raise Exception(f"Unsupported movie format: {ext}")

        kwargs = Args({
            inArg: './', '-OutMrc': self.outputPrefix, '-InSuffix': ext,
            '-Serial': 1, '-Gpu': gpu, '-LogDir': 'log/'
        })
        kwargs.update(self.args)
        args.extend(kwargs.toList())

        t = Timer()

        with open(batch.join('log.txt'), 'w') as logFile:
            print(">>>", Color.green(args[0]), Color.bold(' '.join(args[1:])))
            subprocess.call(args, cwd=batch.path, stderr=logFile, stdout=logFile)

        batch.info.update({
            'mc_input': len(batch.items),
            'mc_elapsed': str(t.getElapsedTime())
        })

        return batch

    def parse_batch(self, batch, outputDir):
        batch['results'] = []
        total = 0
        t = Timer()

        for row in batch.items:
            result = {}
            try:
                movieName = row.rlnMicrographMovieName
                baseName = Path.removeBaseExt(movieName)
                micName = batch.join('output', f"aligned_{baseName}.mrc")

                # Check that the expected output micrograph file exists
                # and move it to the final output directory
                self.__expect(micName)
                shutil.move(micName, outputDir)
                result['rlnMicrographName'] = micName

                if '-Patch' in self.args:
                    logsFull = batch.join('log', f"{baseName}-Patch-Full.log")
                    logsPatch = batch.join('log', f"{baseName}-Patch-Patch.log")
                else:
                    logsFull = batch.join('log', f"{baseName}-Full.log")
                    logsPatch = None

                shiftsStar = os.path.join(outputDir, f'aligned_{baseName}.star')
                self.__write_shift_star(batch, logsFull, logsPatch, movieName, shiftsStar)
                result['rlnMicrographMetadata'] = shiftsStar
                total += 1

            except Exception as e:
                result['error'] = str(e)
                print(Color.red(f"ERROR: {result['error']}"))

            batch['results'].append(result)

        batch.info.update({
            'output_total': total,
            'output_elapsed': str(t.getElapsedTime())
        })

    def __expect(self, fileName):
        if not os.path.exists(fileName):
            raise Exception(f"Missing expected output: {fileName}")

    def __write_shift_star(self, batch, logsFull, logsPatch, movieName, shiftsStar):
        # Parse global motion movements
        self.__expect(logsFull)
        tGeneral = Table(
            ['rlnImageSizeX', 'rlnImageSizeY', 'rlnImageSizeZ',
             'rlnMicrographMovieName', 'rlnMicrographBinning',
             'rlnMicrographOriginalPixelSize', 'rlnMicrographDoseRate',
             'rlnMicrographPreExposure', 'rlnVoltage',
             'rlnMicrographStartFrame', 'rlnMotionModelVersion'
             ])
        x, y, z, _ = self.__parse_dimensions(batch.join('log.txt'))
        tGeneral.addRowValues(x, y, z, movieName,
                              self.args.get('-FtBin', 1), self.args['-PixSize'], 1.0, 0.0,
                              self.args['-kV'], 1, 0)

        t = Table(['rlnMicrographFrameNumber',
                   'rlnMicrographShiftX',
                   'rlnMicrographShiftY'])

        for line in TextFile.stripLines(logsFull):
            t.addRowValues(*line.split())

        with StarFile(shiftsStar, 'w') as sf:
            sf.writeTimeStamp()
            sf.writeTable('general', tGeneral, singleRow=True)
            sf.writeTable('global_shift', t)

            # Parse local motions
            if logsPatch:
                self.__expect(logsPatch)
                t = Table(['rlnMicrographFrameNumber',
                           'rlnCoordinateX', 'rlnCoordinateY',
                           'rlnMicrographShiftX', 'rlnMicrographShiftY'])
                for line in TextFile.stripLines(logsPatch):
                    parts = line.split()
                    t.addRowValues(*parts[:5])
                sf.writeTable('local_shift', t)

    @staticmethod
    def __get_environ():
        varPath = 'MOTIONCOR_PATH'
        varVersion = 'MOTIONCOR_VERSION'

        if program := os.environ.get(varPath, None):
            if not os.path.exists(program):
                raise Exception(f"Motioncor path ({varPath}={program}) does not exists.")
        else:
            raise Exception(f"Motioncor path variable {varPath} is not defined.")

        if version := int(os.getenv(varVersion, 3)):
            pass
        else:
            raise Exception(f"Motioncor version variable {varVersion} is not defined.")

        return program, version

    @staticmethod
    def __parse_dimensions(logFile):
        """ Parse output dimensions from the log file. """
        with open(logFile) as f:
            for line in f:
                if 'size mode:' in line:
                    return line.split(':')[-1].split()
        return None
