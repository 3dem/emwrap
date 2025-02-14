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
import argparse
import shutil
import sys
import re
from collections import defaultdict

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import Args, Batch
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile


class RelionClassify2D:
    def __init__(self, **kwargs):
        self.path = '/usr/local/em/scripts/relion_refine.sh'
        self.args = Args(kwargs.get('extra_args', {}))

    def process_batch(self, batch, **kwargs):
        t = Timer()

        clean = kwargs.get('clean', False)
        gpu = kwargs.get('gpu', '')

        # COMMAND:
        # From Relion GUI: EM
        # `which relion_refine_mpi`
        # --o Class2D/job005/run --iter 25 --i Extract/job002/particles.star
        # --dont_combine_weights_via_disc --preread_images  --pool 50 --pad 2
        # --ctf  --tau2_fudge 2 --particle_diameter 180 --K 50
        # --flatten_solvent  --zero_mask  --center_classes  --oversampling 1 --psi_step 12
        # --offset_range 5 --offset_step 2 --norm --scale  --j 16 --gpu ""  --pipeline_control Class2D/job005/

        # `which relion_refine` --o Class2D/job005/run --grad --class_inactivity_threshold 0.1 --grad_write_iter 10
        # --iter 200 --i Extract/job002/particles.star --dont_combine_weights_via_disc --preread_images
        # --pool 50 --pad 2  --ctf  --tau2_fudge 2 --particle_diameter 180 --K 50
        # --flatten_solvent  --zero_mask  --center_classes  --oversampling 1 --psi_step 12 --offset_range 5
        # --offset_step 2 --norm --scale  --j 32 --gpu ""  --pipeline_control Class2D/job005/

        args = Args({
            '--i': batch.join('particles.star'),
            '--o': batch.join('run'),
            '--particle_diameter': 209,
            '--ctf': '',
            '--zero_mask': '',
            '--K': 50,
            '--grad': '',
            '--grad_write_iter': 10,
            '--class_inactivity_threshold': 0.1,
            '--center_classes': '',
            '--norm': '',
            '--scale': '',
            '--oversampling': 1,
            '--flatten_solvent': '',
            '--tau2_fudge': 2.0,
            '--iter': 200,
            '--offset_range': 5.0,
            '--offset_step': 2.0,
            '--psi_step': 12.0,
            '--dont_combine_weights_via_disc': '',
            '--preread_images': '',
            '--pool': 50,
            '--gpu': gpu,
            '--maxsig': 50,
            '--j': 32
        })
        args.update(self.args)
        batch.call(self.path, args, cwd=False)

        batch.info.update({
            '2d_elapsed': str(t.getElapsedTime())
        })

        if clean:
            self.clean_iter_files(batch)

    @classmethod
    def get_iter_files(cls, batch):
        """ Return results files grouped by iteration from a given folder. """
        r = re.compile("\w+_it(?P<iter>\d{3})_")
        iterFiles = defaultdict(lambda: {})

        for fn in os.listdir(batch.path):
            m = r.match(fn)
            if m is not None:
                it = m.groupdict()['iter']
                # Store specific files with a key, the others with their filename
                key = fn
                for k in ['optimiser', 'classes', 'sampling', 'model', 'data']:
                    if k in fn:
                        key = k
                        break
                iterFiles[it][key] = fn

        return iterFiles

    @classmethod
    def clean_iter_files(cls, batch):
        """ Clean all iteration files except the last one. """
        iterFiles = cls.get_iter_files(batch)
        iterations = list(sorted(iterFiles.keys(), reverse=True))
        for it in iterations[1:]:
            for fn in iterFiles[it].values():
                os.remove(batch.join(fn))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('batch_folder',
                   help="Batch folder to run the preprocessing")
    args = p.parse_args()
    batch = Batch(path=args.batch_folder)
    r2d = RelionClassify2D()
    r2d.process_batch(batch, clean=True)

