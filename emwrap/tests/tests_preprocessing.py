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

from emwrap.mix import Preprocessing, PreprocessingPipeline
from emwrap.tests import RelionTutorial


class TestPreprocessing(unittest.TestCase):
    def _get_args(self):
        return {
            'acquisition': RelionTutorial.acquisition,
            'motioncor': {
                'extra_args': {'-FtBin': 2, '-Patch': '5 5', '-FmDose': 1.277}
            },
            'ctf': {},
            'picking': {},
            'extract': {
                'extra_args': {'--extract_size': 150, '--scale': 100, '--bg_radius': 40}
            }
        }

    def _get_gpus(self):
        gpus = System.gpus()

        if not gpus:
            raise Exception("No GPU detected, required for Motioncor tests. ")

        return [str(g['index']) for g in gpus]


    def _run_batch(self, N, args):
        callerName = inspect.currentframe().f_back.f_code.co_name
        testName = f"{self.__class__.__name__}.{callerName}"
        print(Color.warn(f"\n============= {testName} ============="))
        with Path.tmpDir(prefix=f"{testName}__") as tmp:
            batch = RelionTutorial.make_batch(tmp, N)
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

    def _run_pipeline(self, preprocessing_args, gpus, n=8):
        callerName = inspect.currentframe().f_back.f_code.co_name
        testName = f"{self.__class__.__name__}.{callerName}"
        print(Color.warn(f"\n============= {testName} ============="))

        gpus = self._get_gpus()

        if not gpus:
            raise Exception("No GPU detected, required for Motioncor tests. ")

        with Path.tmpDir(prefix=f"{testName}__", chdir=True) as tmp:
            args = {
                'output_dir': 'output',
                'gpu_list': ' '.join(gpus),
                'input_star': 'movies.star',
                'batch_size': n,
                'preprocessing_args': preprocessing_args
            }

            RelionTutorial.write_movies_star('movies.star')

            # Run the pipeline with 1 gpu
            Process.system('mkdir output', color=Color.bold)
            PreprocessingPipeline(args).run()

            # Run with 2 GPUs if available
            # if len(gpus) > 1:
            #     args.update(gpu_list=' '.join(g['index'] for g in gpus[:2]),
            #                 output_dir='output2')
            #     Process.system('mkdir output2', color=Color.bold)
            #     McPipeline(args).run()

    def test_pipeline(self):
        prep_args = self._get_args()
        gpus = self._get_gpus()
        self._run_pipeline(prep_args, gpus[:1], n=12)

    def test_pipeline_multigpu(self):
        prep_args = self._get_args()
        gpus = self._get_gpus()
        if len(gpus) > 1:
            self._run_pipeline(prep_args, gpus[:2], n=12)
        else:
            raise Exception("This test requires more than one GPU")
