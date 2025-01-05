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
    def test_batch(self):
        def _run(name, **kwargs):

            with Path.tmpDir(prefix=f'TestPreprocessing.test_batch_{name}__') as tmp:
                def _mkdir(d):
                    tmpd = os.path.join(tmp, d)
                    Process.system(f'mkdir {tmpd}', color=Color.bold)
                    return tmpd

                outMics = _mkdir('Micrographs')
                outCtfs = _mkdir('CTFs')
                batch = _make_batch(tmp, 8)
                kwargs['outputMics'] = outMics
                kwargs['outputCtfs'] = outCtfs
                preproc = Preprocessing(**kwargs)

                preproc.process_batch(batch, gpu=0, verbose=True)
                batch.dump_info()
                info = batch.info
                pprint(info)
                # Check that there is no failed micrograph
                self.assertEqual(info['mc_input'], info['mc_output'])
                self.assertFalse(any('error' in r for r in batch['results']))

        mc_args = {'-PixSize': 0.64, '-kV': 200, '-Cs': 2.7, '-FtBin': 2}
        mc = {'args': [mc_args], 'kwargs': {}}

        ctf = {'args': [0.64, 200, 2.7, 0.1], 'kwargs': {}}

        _run('global', motioncor=mc, ctf=ctf)

        # mc_args.update({'-Patch': "5 5", '-FtBin': 1})
        # _run('local', motioncor=mc, ctf=ctf)

