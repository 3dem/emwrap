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

from emtools.utils import Color, Process, System, Path
from emtools.jobs import BatchManager, Args
from emtools.metadata import Table, StarFile

from emwrap.motioncor import Motioncor, McPipeline


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


class TestMotioncor(unittest.TestCase):
    def test_batch(self):
        def _run(name, mc_args):
            mc = Motioncor(mc_args)
            with Path.tmpDir(prefix=f'TestMotioncor.test_batch_{name}__') as tmp:
                outMics = os.path.join(tmp, 'Micrographs')
                Process.system(f'mkdir {outMics}', color=Color.bold)
                batch = _make_batch(tmp, 8)
                mc.process_batch(0, batch)
                mc.parse_batch(batch, outMics)
                batch.dump_info()
                info = batch.info
                # Check that there is no failed micrograph
                self.assertEquals(info['mc_input'], info['output_total'])
                self.assertFalse(any('error' in r for r in batch['results']))

        mc_args = {'-PixSize': 0.64, '-kV': 200, '-Cs': 2.7, '-FtBin': 2}
        _run('global', mc_args)

        mc_args.update({'-Patch': "5 5", '-FtBin': 1})
        _run('local', mc_args)

    def test_pipeline(self):
        print(Color.bold(">>> Running test: "), Color.warn("test_pipeline"))
        gpus = System.gpus()

        if not gpus:
            raise Exception("No GPU detected, required for Motioncor tests. ")

        with Path.tmpDir(prefix='TestMotioncor.test_pipeline__', chdir=True) as tmp:
            args = {
                'output_dir': 'output',
                'gpu_list': str(gpus[0]['index']),
                'input_star': 'movies.star',
                'batch_size': 6,
                'motioncor_args': {'-FtBin': 2}
            }

            with StarFile(args['input_star'], 'w') as sf:
                sf.writeTable('optics', _optics_table(0.64, 200, 2.7, 0.1))
                sf.writeTable('movies', _movies_table())

            # Run the pipeline with 1 gpu
            Process.system('mkdir output', color=Color.bold)
            McPipeline(args).run()

            # Run with 2 GPUs if available
            if len(gpus) > 1:
                args.update(gpu_list=' '.join(g['index'] for g in gpus[:2]),
                            output_dir='output2')
                Process.system('mkdir output2', color=Color.bold)
                McPipeline(args).run()
