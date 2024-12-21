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


class Ctffind:
    """ Ctffind wrapper to run in a batch folder. 
    $CTFFIND << eof
    aligned_20170629_00037_frameImage.mrc
    aligned_20170629_00037_frameImage_ctffind.mrc
    0.648500
    200.000000
    2.700000
    0.100000
    512
    30.000000
    5.000000
    5000.000000
    50000.000000
    100.000000
    no
    no
    no
    no
    no
    no
    no
    eof


    """
    def __init__(self, args, **kwargs):
        if path := kwargs.get('path', None):
            self.path = path, self.version = kwargs['version']
        else:
            self.path, self.version = Motioncor.__get_environ()
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

        opts = f"{inArg} ./ -OutMrc {self.outputPrefix} -InSuffix {ext} "
        opts += f"-Serial 1  -Gpu {gpu} -LogDir log/ {self.args}"
        args.extend(opts.split())
        t = Timer()

        with open(logFn, 'w') as logFile:
            subprocess.call(args, cwd=batch_dir, stderr=logFile, stdout=logFile)

        batch['elapsed'] = t.getToc()

        return batch


