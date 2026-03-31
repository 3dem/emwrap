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

"""
RELION 3D classification (Class3D) for subtomogram averaging.

Uses relion_refine (same as 3D auto-refine) but without --auto_refine, with --K
for number of classes, and optionally --skip_align for classification without
alignment. See RELION STA tutorial:
https://relion.readthedocs.io/en/release-5.0/STA_tutorial/Class3D.html
https://tomoguide.github.io/03-tutorial/05-sta-in-relion5/

Example command:
  relion_refine_mpi --o Class3D/job154/run --i particles.star --ref ref.mrc
  --K 10 --iter 25 --skip_align --flatten_solvent --zero_mask --sym C1 --ctf ...
"""

import os
import re
from glob import glob

from emtools.image import Image
from emtools.jobs import Batch, Args
from emtools.metadata import StarFile

from .relion_base import RelionBasePipeline


class RelionTomoClassify(RelionBasePipeline):
    """Wrapper around relion_refine for subtomogram 3D classification (Class3D).

    Produces multiple class volumes and particles assigned to each class.
    """
    name = 'emw-relion-tomoclassify'

    def prerun(self):
        input_ios = self._args.get('relion_classify3d.ios')
        if not input_ios or not str(input_ios).strip():
            raise Exception("Input particles/optimization set (--ios) is required.")
        if not os.path.exists(input_ios):
            raise Exception(f"Input optimization set '{input_ios}' does not exist.")

        ref_vol = self._args.get('relion_classify3d.ref')
        if not ref_vol or not str(ref_vol).strip():
            raise Exception("Reference map (--ref) is required.")
        if not os.path.exists(ref_vol):
            raise Exception(f"Reference volume '{ref_vol}' does not exist.")

        batch = Batch(id=self.name, path=self.workingDir)
        self.mkdir('output')

        subargs = self.get_subargs('relion_classify3d')
        subargs['--ios'] = input_ios
        subargs['--ref'] = ref_vol

        
        gpus = int(self._args.get('gpus', 1))
        cpus = max(gpus * 10, int(self._args.get('cpus', 0)))
        mpis = gpus + 1 if gpus > 1 else 5  # FIXME
        threads = cpus // mpis

        args = Args({
            'relion_refine': mpis,
            '--o': self.join('output/run'),
            '--dont_combine_weights_via_disc': '',
            '--pool': 30,
            '--pad': 2,
            '--norm': '',
            '--scale': '',
            '--flatten_solvent': '',
            '--zero_mask': '',
            '--gpu': '',
            '--j': threads,
        })

        """
        /research/rgs01/applications/hpcf/authorized_apps/cryo_apps/emstack/scripts/relion_launcher.sh relion_refine 8 \
--ios External/job052/optimisation_set.star \
--o External/job052/output/run \
--ref External/job011/output/run_class001.mrc \
--firstiter_cc \
--ini_high 15 \
--dont_combine_weights_via_disc \
--scratch_dir /lustre_scratch/user_scratch/jdela80/relion_tomorefine \
--pool 30 \
--pad 2 \
--ctf \
--iter 25 \
--tau2_fudge 3 \
--particle_diameter 180 \
--K 5 \
--flatten_solvent \
--zero_mask \
--solvent_mask External/job013/mask.mrc \
--skip_align \
--sym D2 \
--norm \
--scale \
--j 8 
        """
        args.update(subargs)
        if extra_args := self._args.get('extra_args', None):
            args.update(Args.fromString(extra_args))


        self.batch_execute('relion_refine', batch, args)

        # Register output: particles with class assignments
        out_star = self.join('output', 'run_data.star')
        if not os.path.exists(out_star):
            raise Exception(
                f"relion_refine did not produce run_data.star. Check {self.join('run.out')}."
            )

        with StarFile(out_star) as sf:
            o = sf.getTable('optics')
            box = o[0].rlnImageSize
            ps = o[0].rlnImagePixelSize
            N = sf.getTableSize('particles')

        # Find latest iteration class maps (run_it{N}_class001.mrc, ...)
        output_dir = self.join('output')
        class_pattern = os.path.join(output_dir, 'run_it*_class*.mrc')
        class_files = glob(class_pattern)
        last_iter = None
        last_iter_files = []
        iter_re = re.compile(r'run_it(\d+)_class(\d+)\.mrc')
        for p in class_files:
            m = iter_re.search(os.path.basename(p))
            if m:
                it = int(m.group(1))
                if last_iter is None or it > last_iter:
                    last_iter = it
                    last_iter_files = [p]
                elif it == last_iter:
                    last_iter_files.append(p)

        if last_iter_files:
            last_iter_files.sort(
                key=lambda p: (int(iter_re.search(os.path.basename(p)).group(2)), p)
            )

        n_classes = len(last_iter_files)
        self.outputs = {
            'TomogramParticles': {
                'label': 'Classified Particles',
                'type': 'TomogramParticles',
                'info': f"{N} pts, {n_classes} classes (box: {box} px, {ps} Å/px)",
                'files': [
                    [out_star, 'TomogramGroupMetadata.star.relion.tomo.particles']
                ]
            },
        }

        # Register each class volume
        for i, vol_path in enumerate(last_iter_files, start=1):
            self.outputs[f'Volume_class{i:02d}'] = {
                'label': f'Class {i}',
                'type': 'Volume',
                'info': f"box size: {box} px, {ps} Å/px",
                'files': [
                    [vol_path, 'TomogramGroupMetadata.star.relion.volume']
                ]
            }

        self.updateBatchInfo(batch)


if __name__ == '__main__':
    RelionTomoClassify.main()
