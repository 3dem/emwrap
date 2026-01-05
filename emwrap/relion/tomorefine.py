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
from emtools.metadata import Mdoc, StarFile, RelionStar

from . relion_base import RelionBasePipeline
from .classify2d import RelionClassify2D


class RelionTomoRefine(RelionBasePipeline):
    """ Wrapper around relion_refine for subtomograms 3D refinement. """
    name = 'emw-relion-tomorefine'

    def prerun(self):
        """
        {
    "gpus": "",
    "relion_refine.ios": "External/job069/warp_particles_optimisation_set.star",
    "relion_refine.ref": "External/job070/reconstructed_volume.mrc",
    "relion_refine.solvent_mask": "",
    "relion_refine.firstiter_cc": "false",
    "relion_refine.trust_ref_size": "true",
    "relion_refine.ini_high": "40",
    "relion_refine.sym": "C1",
    "relion_refine.ctf": "true",
    "relion_refine.ctf_intact_first_peak": "false",
    "relion_refine.particle_diameter": "150",
    "relion_refine.zero_mask": "true",
    "relion_refine.solvent_correct_fsc": "false",
    "relion_refine.blush": "false",
    "relion_refine.healpix_order": "2",
    "relion_refine.offset_range": "5",
    "relion_refine.offset_step": "1",
    "relion_refine.auto_local_healpix_order": "4",
    "relion_refine.relax_sym": ""
}
        """
        inputOS = self._args['relion_refine.ios']
        if not os.path.exists(inputOS):
            raise Exception(f"Input optimization set '{inputOS}' do not exist.")

        refVol = self._args['relion_refine.ref']
        if not os.path.exists(refVol):
            raise Exception(f"Reference volume '{refVol}' do not exist.")

        batch = Batch(id=self.name, path=self.workingDir)
        self.mkdir('output')

        subargs = self.get_subargs('relion_refine')

        threads = 10
        gpus = int(self._args['gpus'])
        cpus = gpus * threads
        mpis = gpus + 1
        # Run ts_import
        args = Args({
            'relion_refine': mpis,
            "--o": self.join("output/run"),
            "--auto_refine": "",
            "--split_random_halves": "",
            "--flatten_solvent": "",  # TODO: Check the param for this
            "--dont_combine_weights_via_disc": "",
            "--pool": 50,
            "--pad": 2,
            "--oversampling": 1,
            "--low_resol_join_halves": 40,
            "--norm": "",
            "--scale": "",
            "--gpu": "",  # Use all submitted in the job
            "--j": threads
            # TODO allow extra_args
        })
        args.update(subargs)
        self.batch_execute('relion_refine', batch, args)

        self.updateBatchInfo(batch)


if __name__ == '__main__':
    RelionTomoRefine.main()
