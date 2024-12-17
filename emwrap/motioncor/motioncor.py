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
    def __init__(self, path, version, acquisition, args):
        self.path = path
        self.version = version
        self.acquisition = acquisition
        self.args = args
        self.outputPrefix = "output/aligned_"

    def process_batch(self, gpu, batch):
        batch_dir = batch['path']

        def _path(p):
            return os.path.join(batch_dir, p)

        os.mkdir(_path('output'))
        os.mkdir(_path('log'))

        logFn = _path('motioncor_log.txt')
        args = [self.path]

        ext = Path.getExt(batch['items'][0].rlnMicrographMovieName)
        extLower = ext.lower()

        if extLower.startswith('.tif'):
            inArg = '-InTiff'
        elif extLower.startswith('.mrc'):
            inArg = '-InMrc'
        elif extLower.startswith('.eer'):
            inArg = '-InEer'
        else:
            raise Exception(f"Unsupported movie format: {ext}")

        # Load acquisition parameters from the optics table
        acq = self.acquisition
        ps = acq['pixel_size']
        voltage = acq['voltage']
        cs = acq['cs']

        opts = f"{inArg} ./ -OutMrc {self.outputPrefix} -InSuffix {ext} "
        opts += f"-Serial 1  -Gpu {gpu} -LogDir log/ "
        opts += f"-PixSize {ps} -kV {voltage} -Cs {cs} "
        opts += self.args
        args.extend(opts.split())
        # batchStr = Color.cyan("BATCH_%02d" % batch['index'])

        t = Timer()

        with open(logFn, 'w') as logFile:
            subprocess.call(args, cwd=batch_dir, stderr=logFile, stdout=logFile)

        batch['elapsed'] = t.getToc()

        return batch

