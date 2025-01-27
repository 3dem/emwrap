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
import subprocess
import shutil
import sys
import json
import numpy as np

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import Args
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile


class RelionExtract:
    def __init__(self, acq, **kwargs):
        self.acq = acq
        self.path = '/usr/local/em/scripts/relion_extract.sh'
        self.args = Args(kwargs.get('extra_args', {}))

    def process_batch(self, batch, **kwargs):
        t = Timer()

        # COMMAND:
        # `which relion_preprocess_mpi`
        # --i CtfFind/job005/micrographs_ctf.star
        # --coord_list AutoPick/job007/autopick.star
        # --part_star Extract/job008/particles.star --part_dir Extract/job008/
        # --extract --extract_size 512 --float16  --scale 128 --norm --bg_radius 27
        # --white_dust -1 --black_dust -1 --invert_contrast   --pipeline_control Extract/job008/

        args = Args({
            self.path: '',
            '--i': 'micrographs.star',
            '--coord_list': 'coordinates.star',
            '--part_star': 'particles.star',
            '--extract': '',
            '--float16': '',
            '--norm': '',
            '--white_dust': -1,
            '--black_dust': -1,
            '--invert_contrast': ''
        })
        args.update(self.args)
        batch.call(self.path, args)

        batch.info.update({
            'extract_elapsed': str(t.getElapsedTime())
        })

    def update_args(self, particle_size):
        """ Estimate extraction parameters based on pixel size
        and particle_size in A.
        """

        particle_size_pix = particle_size / self.acq.pixel_size
        boxsize = RelionExtract.estimate_box_size(particle_size_pix)
        bg_radius = np.round(particle_size_pix * 0.7)

        if scale := self.args.get('--scale', None):
            bg_radius *= scale / boxsize

        self.args.update({
            '--extract_size': boxsize,
            '--bg_radius': round(bg_radius)
        })

    @staticmethod
    def estimate_box_size(particle_size_pix, scale=2.2):
        """ Calculate the box size based on the input particle size
        and recommended boxsize from Eman's wiki page.
        """
        EMAN_BOXSIZES = np.array(
            [24, 32, 36, 40, 44, 48, 52, 56, 60, 64, 72, 84, 96, 100,
             104, 112, 120, 128, 132, 140, 168, 180, 192, 196, 208,
             216, 220, 224, 240, 256, 260, 288, 300, 320, 352, 360,
             384, 416, 440, 448, 480, 512, 540, 560, 576, 588, 600,
             630, 640, 648, 672, 686, 700, 720, 750, 756, 768, 784,
             800, 810, 840, 864, 882, 896, 900, 960, 972, 980, 1000,
             1008, 1024, 2048])

        return int(EMAN_BOXSIZES[np.argmax(EMAN_BOXSIZES >= particle_size_pix * scale)])





