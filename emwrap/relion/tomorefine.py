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
import pathlib
import sys
import time
import json
import argparse
from datetime import timedelta, datetime
from glob import glob

from emtools.utils import Color, Timer, Path, Process, FolderManager, Pretty
from emtools.jobs import Batch, Args
from emtools.metadata import Mdoc, StarFile

from emwrap.base import ProcessingPipeline
from .classify2d import RelionClassify2D


class RelionTomoRefine(ProcessingPipeline):
    """ Wrapper around relion_refine for subtomograms 3D refinement. """
    name = 'emw-relion-tomorefine'
    input_name = 'in_particles'

    def __init__(self, input_args):
        ProcessingPipeline.__init__(self, input_args)
        #self.gpuList = args['gpu'].split()

    def prerun(self):
        """
        mpirun -n 9 `which relion_refine_mpi` \
        --o refine_output/run \
        --auto_refine \
        --split_random_halves \
        --ios warp_particles_optimisation_set.star \
        --ref reconstruct.mrc \
        --firstiter_cc \
        --ini_high 60 \
        --dont_combine_weights_via_disc \
        --pool 50 \
        --pad 2  \
        --ctf \
        --particle_diameter 140 \
        --flatten_solvent \
        --zero_mask \
        --oversampling 1 \
        --healpix_order 2 \
        --auto_local_healpix_order 4 \
        --offset_range 5 \
        --offset_step 2 \
        --sym O \
        --low_resol_join_halves 40 \
        --norm --scale  \
        --j 8 --gpu ""
        """
        inputParticles = self._args['in_particles']
        if not os.path.exists(inputParticles):
            raise Exception(f"Input particles '{inputParticles}' do not exist.")
        inputFm = FolderManager(os.path.dirname(inputParticles))
        inputPts = inputFm.join('Particles')
        if not os.path.exists(inputPts):
            raise Exception(f"Expected folder '{inputPts}' does not exist.")
        inputBase = Path.removeBaseExt(inputParticles)
        inputs = inputFm.glob(f"{inputBase}*.star") + [inputPts]

        if inputFm.exists('dummy_tiltseries.mrc'):
            inputs.append(inputFm.join('dummy_tiltseries.mrc'))

        batch = Batch(id=self.name, path=self.path)
        for fn in inputs:
            batch.link(fn)
        refVol = batch.link('data/reference_vol.mrc')

        batch.mkdir('output')

        # Run ts_import
        args = Args({
            "--o": "output/run",
            "--auto_refine": "",
            "--split_random_halves": "",
            "--ios": f"{inputBase}_optimisation_set.star",
            "--ref": refVol,
            "--firstiter_cc": "",
            "--ini_high": 60,
            "--dont_combine_weights_via_disc": "",
            "--pool": 50,
            "--pad": 2,
            "--ctf": "",
            "--flatten_solvent": "",
            "--zero_mask": "",
            "--oversampling": 1,
            "--healpix_order": 2,
            "--auto_local_healpix_order": 4,
            "--offset_range": 5,
            "--offset_step": 2,
            "--low_resol_join_halves": 40,
            "--norm": "",
            "--scale": "",
            "--j": 8,
            "--gpu": ""
        })

        # FIXME: Handle better MPIs and threads
        args.update(self._args['relion_refine']['extra_args'])
        with batch.execute('relion_refine_mpi'):
            batch.call(os.environ['RELION_TOMOREFINE'], args)


def main():
    RelionTomoRefine.runFromArgs()


if __name__ == '__main__':
    main()
