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
from emtools.jobs import Batch
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile

from emwrap.motioncor import Motioncor
from emwrap.ctffind import Ctffind


class Preprocessing:
    """
    Class that combines motion correction, CTF estimation,
     picking and particle extraction. The goal is to reuse
     the scratch and minimize the transfer of temporary files.
     """
    def __init__(self, **kwargs):
        mc = kwargs['motioncor']
        self.motioncor = Motioncor(*mc['args'], **mc['kwargs'])
        if 'ctf' in kwargs:
            ctf = kwargs['ctf']
            self.ctf = Ctffind(*ctf['args'], **ctf['kwargs'])
        else:
            self.ctf = None

        self.outputMics = kwargs['outputMics']
        self.outputCtfs = kwargs['outputCtfs']

        # TODO
        self.picking = kwargs.get('picking', None)
        self.extract = kwargs.get('extract', None)

    def process_batch(self, batch, **kwargs):
        v = kwargs.get('verbose', False)
        gpu = kwargs['gpu']
        mc = self.motioncor
        mc.process_batch(batch, gpu=gpu)
        mc.output_batch(batch, self.outputMics)
        ctf_batch = Batch(batch)
        ctf_batch['items'] = [r['rlnMicrographName'] for r in batch['results'] if 'error' not in r]
        self.ctf.process_batch(ctf_batch, verbose=v)
        batch.info.update(ctf_batch.info)

        def _move(outputs, d):
            for o in outputs:
                if v:
                    print(f"Moving {o} -> {d}")
                shutil.move(o, d)

        _move(batch['outputs'], self.outputMics)
        _move(ctf_batch['outputs'], self.outputCtfs)
        # TODO: update with ctf values
        return batch

