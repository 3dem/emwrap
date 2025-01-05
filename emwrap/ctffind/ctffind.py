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

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import ProcessingPipeline, BatchManager
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile


class Ctffind:
    def __init__(self, *args, **kwargs):
        pixel_size, voltage, spherical_aberration, amplitude_contrast = args
        self.path = kwargs.get('path', '/usr/local/em/ctffind-5.0/bin/ctffind')
        self.version = 5
        _get = kwargs.get  # shortcut
        self.args = [pixel_size, voltage, spherical_aberration, amplitude_contrast,
                     _get('window', 512), _get('min_res', 30.0), _get('max_res', 5.0),
                     _get('min_def', 5000.0), _get('max_def', 50000.0), _get('step_def', 100.0),
                     'no', 'no', 'no', 'no', 'no', 'no', 'no']

    def process(self, micrograph, **kwargs):
        verbose = kwargs.get('verbose', False)
        ctf_files = [Path.replaceExt(micrograph, '_ctf.mrc'),
                     Path.replaceExt(micrograph, '_ctf_avrot.txt')]
        args = [micrograph, ctf_files[0]] + self.args
        if verbose:
            print(">>>", Color.green(self.path), Color.bold(' '.join(str(a) for a in args)))
        p = Process(self.path, input='\n'.join(str(a) for a in args))
        for f in ctf_files:
            if not os.path.exists(f):
                raise Exception(f"Missing expected CTF file: {f}")
        ctf_values = self.__parse_output(p.lines())
        if verbose:
            print(ctf_values)
        return ctf_values, ctf_files

    def process_batch(self, batch, **kwargs):
        t = Timer()
        batch['results'] = []
        batch['outputs'] = []

        for mic in batch['items']:
            try:
                ctf_values, ctf_files = self.process(mic, verbose=kwargs.get('verbose', False))
                result = {'ctf_values': ctf_values}
            except Exception as e:
                result = {'error': str(e)}
                ctf_files = []
                print(Color.red(f"ERROR: {result['error']}"))

            batch['results'].append(result)
            batch['outputs'].extend(ctf_files)

        batch.info.update({
            'ctf_elapsed': str(t.getElapsedTime())
        })

        return batch

    def __parse_output(self, lines):
        """ Parsing CTF values from an output with the form:
        Estimated defocus values        : 5784.86 , 5614.60 Angstroms
        Estimated azimuth of astigmatism: 70.84 degrees
        Score                           : 0.22323
        Pixel size for fitting          : 1.400 Angstroms
        Thon rings with good fit up to  : 4.8 Angstroms
        """
        for line in lines:
            def _parts():
                return line.split(':')[-1].split()

            if line.startswith('Estimated defocus values'):
                defocus = _parts()
            elif line.startswith('Estimated azimuth of astigmatism'):
                defocusAngle = _parts()[0]
            elif line.startswith('Score       '):
                ctfScore = _parts()[0]
            elif line.startswith('Thon rings with good fit up to'):
                ctfRes = _parts()[0]

        return defocus[0], defocus[2], defocusAngle, ctfScore, ctfRes

