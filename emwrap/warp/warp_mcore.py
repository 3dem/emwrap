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
import json
import argparse
import time
import sys
from glob import glob
from datetime import datetime

from emtools.utils import Color, FolderManager, Path, Process
from emtools.jobs import Batch, Args

from .warp import WarpBasePipeline


class WarpMcore(WarpBasePipeline):
    """ Warp wrapper to run MCore refinements.
    Input is assumed from a Relion tomorefine job from warp particles.
    """
    name = 'emw-warp-mcore'
    input_name = 'in_particles'

    def prerun(self):
        # Input movies pattern for the frame series
        inputJob = self._args['in_particles']
        inputFm = FolderManager(inputJob)

        for fn in ['Particles', 'warp_particles.star']:
            inputPath = inputFm.join(fn)
            if not os.path.exists(inputPath):
                raise Exception(f"Missing expected path: {inputPath}")
            if not os.path.islink(inputPath):
                raise Exception(f"Expecting {inputPath} to be a link "
                                f"to a Warp export particles job")

        ptsLink = os.readlink(inputFm.join('Particles'))
        inputWarp = inputFm.join(os.path.dirname(ptsLink))
        self.log(f"Input Warp folder: {inputWarp}")

        # Link input folders
        self._importInputs(inputWarp)
        self.link(inputFm.join('output'))

        batch = Batch(id='mcore', path=self.path)
        batch.mkdir('m')

        def _run(label, *commands):
            with batch.execute(label):
                for cmd in commands:
                    batch.call(self.loader, cmd)
            self.updateBatchInfo(batch)

        # FIXME: Take all HARD-CODED values from input
        name = "15854"
        population = f"--population m/{name}.population"
        relion = "output/run"
        diameter = 140
        sym = "O"
        perdevice = 2  # FIXME
        perdevice_refine = f"--perdevice_refine {perdevice}"
        mask = self.link("data/mask_4pt76apx.mrc")  # FIXME
        resample_angpix = 1.19

        mcore_basic = f"MCore {population} {perdevice_refine}"
        mcore_imagewarp = f"{mcore_basic} --refine_imagewarp 3x3 --refine_particles"

        _run("iter0",
             f"MTools create_population --directory m --name {name}",
             f"MTools create_source --name {name} {population} "
             f"  --processing_settings warp_tiltseries.settings ",
             f"MTools create_species {population} "
             f"  --name apoF --diameter {diameter} --sym {sym} "
             f"  --temporal_samples 1 "
             f"  --half1 {relion}_half1_class001_unfil.mrc "
             f"  --half2 {relion}_half2_class001_unfil.mrc "
             f"  --mask {mask} "
             f"  --particles_relion {relion}_data.star "
             f"  --angpix_resample {resample_angpix} --lowpass 10 ",
             f"{mcore_basic} --iter 0"
             )

        _run("iter1",
             f"{mcore_imagewarp} --ctf_defocus --ctf_defocusexhaustive")

        _run('iter2',
             f"{mcore_imagewarp} --ctf_defocus")

        _run('iter3',
             f"{mcore_imagewarp} --refine_stageangles")

        _run('iter4',
             f"{mcore_imagewarp} --refine_mag --ctf_cs --ctf_defocus --ctf_zernike3")


def main():
    WarpMcore.runFromArgs()


if __name__ == '__main__':
    main()
