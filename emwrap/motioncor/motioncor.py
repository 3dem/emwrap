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
from emtools.jobs import ProcessingPipeline, BatchManager
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile


class Motioncor:
    """ Motioncor wrapper to run in a batch folder. """
    def __init__(self, args, **kwargs):
        if path := kwargs.get('path', None):
            self.path = path, self.version = int(kwargs['version'])
        else:
            self.path, self.version = Motioncor.__get_environ()
        self.args = args
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

        opts = f"{inArg} ./ -OutMrc {self.outputPrefix} -InSuffix {ext} "
        opts += f"-Serial 1  -Gpu {gpu} -LogDir log/ {self.args}"
        args.extend(opts.split())
        t = Timer()

        with open(batch.join('log.txt'), 'w') as logFile:
            print(">>>", Color.green(args[0]), Color.bold(' '.join(args[1:])))
            subprocess.call(args, cwd=batch.path, stderr=logFile, stdout=logFile)

        batch['info'] = info = {
            'items': len(batch.items),
            'elapsed': str(t.getElapsedTime())
        }

        print(json.dumps(info, indent=4))

        with open(batch.join('info.json'), 'w') as batch_info:
            json.dump(info, batch_info, indent=4)

        return batch

    def parse_batch(self, batch):
        batch['results'] = []

        def _expect(fileName):
            if not os.path.exists(fileName):
                raise Exception(f"Missing expected output: {fileName}")

        for row in batch.items:
            result = {}
            try:
                movieName = row.rlnMicrographMovieName
                print(f"- {movieName}")
                baseName = Path.removeBaseExt(movieName)
                micName = batch.join('output', f"aligned_{baseName}.mrc")
                _expect(micName)
                result['rlnMicrographName'] = micName

                logs = {}
                if 'Patch' in self.args:
                    logsFull = batch.join('log', f"{baseName}-Patch-Full.log")
                    logsPatch = batch.join('log', f"{baseName}-Patch-Patch.log")
                else:
                    logsFull = batch.join('log', f"{baseName}-Full.log")
                    logsPatch = None

                # Parse global motion movements
                _expect(logsFull)
                t = Table(['rlnMicrographFrameNumber',
                           'rlnMicrographShiftX',
                           'rlnMicrographShiftY'])
                with open(logsFull) as f:
                    for line in f:
                        if line := line.strip():
                            if not line.startswith('#'):
                                parts = line.split()
                                t.addRowValues(*parts)
                StarFile.printTable(t, 'global_shift')

                # Parse local motions
                if logsPatch:
                    _expect(logsPatch)

            except Exception as e:
                result['error'] = str(e)
            batch['results'].append(result)


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
