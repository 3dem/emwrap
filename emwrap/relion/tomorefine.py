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
from datetime import timedelta, datetime
from glob import glob

from emtools.image import Image
from emtools.jobs import Batch, Args
from emtools.metadata import StarFile

from . relion_base import RelionBasePipeline
from .classify2d import RelionClassify2D


class RelionTomoRefine(RelionBasePipeline):
    """ Wrapper around relion_refine for subtomograms 3D refinement. """
    name = 'emw-relion-tomorefine'

    def prerun(self):
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
        args.update(self.get_subargs('relion_refine'))
        self.batch_execute('relion_refine', batch, args)

        # Register output Volume and Particle STAR file
        outStar = self.join('output', 'run_data.star')
        with StarFile(outStar) as sf:
            o = sf.getTable('optics')
            box = o[0].rlnImageSize
            ps = o[0].rlnImagePixelSize
            N = sf.getTableSize('particles')
            
        outVol = self.join('output', 'run_class001.mrc')

        self.outputs = {
            'TomogramParticles': {
                'label': 'Refined Particles',
                'type': 'TomogramParticles',
                'info': f"{N} pts (size: {box} px, {ps} Å/px)",
                'files': [
                    [outStar, 'TomogramGroupMetadata.star.relion.tomo.particles']
                ]
            },
            'Volume': {
                'label': 'Refined Volume',
                'type': 'Volume',
                'info': f"box size: {box} px, {ps} Å/px",
                'files': [
                    [outVol, 'TomogramGroupMetadata.star.relion.volume']
                ]
            }
        }
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    RelionTomoRefine.main()
