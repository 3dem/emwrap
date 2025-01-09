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
import unittest
import tempfile
import glob
import numpy as np
import time
import inspect
from pprint import pprint

from emtools.utils import Color, Process, System, Path
from emtools.jobs import BatchManager, Args
from emtools.metadata import Table, StarFile

from emwrap.mix import Preprocessing


def _filename(row):
    """ Helper to get unique name from a particle row. """
    pts, stack = row.rlnImageName.split('@')
    return stack.replace('.mrcs', f'_p{pts}.mrcs')


def _movies_table():
    dataset = '/jude/facility/jmrt/SCIPION/TESTDATA/relion30_tutorial/Movies'
    movies = glob.glob(os.path.join(dataset, '*.tiff'))
    t = Table(['rlnMicrographMovieName', 'rlnOpticsGroup'])
    for m in movies:
        t.addRowValues(m, 1)
    return t


def _optics_table(ps, voltage, cs, ac=0.1, opticsGroup=1, opticsGroupName="opticsGroup1", mtf=None):
    cols = ['rlnOpticsGroupName', 'rlnOpticsGroup', 'rlnMicrographOriginalPixelSize',
            'rlnVoltage', 'rlnSphericalAberration', 'rlnAmplitudeContrast']
    values = [opticsGroupName, opticsGroup, ps, voltage, cs, ac]
    if mtf:
        cols.append('rlnMtfFileName')
        values.append(mtf)
    t = Table(cols)
    t.addRowValues(*values)
    return t


def _make_batch(path, n):
    """ Create a batch with that number of movies. """
    table = _movies_table()
    batchMgr = BatchManager(n, iter(table[:n]), path,
                            itemFileNameFunc=lambda i: i.rlnMicrographMovieName)
    return next(batchMgr.generate())


class TestPreprocessing(unittest.TestCase):
    def _get_args(self):
        return {
            'acquisition': {
                'pixel_size': 0.885,
                'voltage': 200,
                'cs': 1.4,
                'amplitude_contrast': 0.1
            },
            'motioncor': {
                'extra_args': {'-FtBin': 2, '-Patch': '5 5', '-FmDose': 1.277}
            },
            'ctf': {},
            'picking': {},
            'extract': {
                'extra_args': {'--extract_size': 150, '--scale': 100, '--bg_radius': 64}
            }
        }

    def _run_batch(self, N, args):
        callerName = inspect.currentframe().f_back.f_code.co_name
        testName = f"{self.__class__.__name__}.{callerName}"
        print(Color.warn(f"\n============= {testName} ============="))
        with Path.tmpDir(prefix=f"{testName}__") as tmp:
            batch = _make_batch(tmp, N)
            preproc = Preprocessing(args)

            preproc.process_batch(batch, gpu=0, verbose=True)
            batch.dump_info()
            info = batch.info
            pprint(info)
            # Check that there is no failed micrograph
            self.assertEqual(info['mc_input'], info['mc_output'])
            self.assertFalse(any('error' in r for r in batch['results']))

    def test_batch(self):
        self._run_batch(8, self._get_args())

    def test_batch_full(self):
        self._run_batch(24, self._get_args())

