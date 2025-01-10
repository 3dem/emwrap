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

from emtools.utils import Color, Process, System, Path
from emtools.jobs import BatchManager, Args
from emtools.metadata import Table, StarFile

from emwrap.motioncor import Motioncor, McPipeline
from emwrap.tests import RelionTutorial


class TestMotioncor(unittest.TestCase):
    def _run_batch(self, extra_args, n=8):
        callerName = inspect.currentframe().f_back.f_code.co_name
        testName = f"{self.__class__.__name__}.{callerName}"
        print(Color.warn(f"\n============= {testName} ============="))

        with Path.tmpDir(prefix=f"{testName}__") as tmp:
            mc = Motioncor(RelionTutorial.acquisition, extra_args=extra_args)
            batch = RelionTutorial.make_batch(tmp, n)
            mc.process_batch(batch, gpu=0)
            batch.dump_info()
            info = batch.info
            # Check that there is no failed micrograph
            self.assertEquals(info['mc_input'], n)
            self.assertEquals(info['mc_output'], n)
            self.assertFalse(any('error' in r for r in batch['results']))

    def test_batch_global(self):
        self._run_batch({'-FtBin': 2})

    def test_batch_local(self):
        self._run_batch({'-Patch': "5 5", '-FtBin': 1})

    def _run_pipeline(self):
        callerName = inspect.currentframe().f_back.f_code.co_name
        testName = f"{self.__class__.__name__}.{callerName}"
        print(Color.warn(f"\n============= {testName} ============="))

        gpus = System.gpus()

        if not gpus:
            raise Exception("No GPU detected, required for Motioncor tests. ")

        with Path.tmpDir(prefix=f"{testName}__") as tmp:
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
