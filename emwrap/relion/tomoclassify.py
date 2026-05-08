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

Mirrors ``RelionJob::initialiseClass3DJob`` / ``RelionJob::getCommandsClass3DJob``
in ``relion-source/src/pipeline_jobs.cpp`` for **tomography** and **not** a
continuation run: ``relion_refine`` without ``--auto_refine``, with ``--K``,
``--flatten_solvent``, optional ``--skip_align``, etc.

See RELION STA tutorial (Class3D) and TomoGuide.
"""

import os
import re
from glob import glob

from emtools.jobs import Batch, Args
from emtools.metadata import StarFile

from .relion_base import RelionBasePipeline


class RelionTomoClassify(RelionBasePipeline):
    """Wrapper around relion_refine for subtomogram 3D classification (Class3D).

    Produces multiple class volumes and particles assigned to each class.
    """

    name = "emw-relion-tomoclassify"

    def prerun(self):
        batch = Batch(id=self.name, path=self.workingDir)
        only_output = 'emwrap.only_output' in self._args['extra_args']
        if not only_output:
            self._classify(batch)
        self._output(batch)

    def _classify(self, batch):
        self._check_input("relion_refine.ios", "Input optimisation set")
        self._check_input("relion_refine.ref", "Reference volume")
        self._check_input("relion_refine.solvent_mask", "Solvent mask", allow_empty=True)

        gpus = int(self._args.get("gpus", 1))
        # This is one of the inverted booleans in Relion's GUI
        perform_align = self._args['perform_image_alignment']

        if not perform_align and gpus > 0:
            raise Exception(
                "RELION does not use GPUs when skipping alignment (--skip_align). "
                "Set GPUs to 0 or enable alignment (Perform image alignment?)."
            )

        inverted_booleans = [
            "firstiter_cc",
            "dont_combine_weights_via_disc",
            "no_parallel_disc_io"
        ]
        subargs = self.get_subargs(
            "relion_refine",
            inverted_booleans=inverted_booleans,
            possitive=["ini_high", "sigma_tilt"],
        )

        args = Args(
            {
                "relion_refine": gpus + 1,
                "--o": self.join("output/run"),
                "--flatten_solvent": "",
                "--norm": "",
                "--scale": "",
                "--j": 10,
            }
        )

        if perform_align:
            subargs["--gpu"] = ""
            subargs["--oversampling"] = 1
            subargs["--offset_step"] = float(subargs["--offset_step"]) * 2
            if self._args['do_local_ang_searches']:
                subargs["--sigma_ang"] = float(subargs["--sigma_ang"]) / 3.0
            else:
                for k in ["--sigma_ang", "--relax_sym"]:
                    subargs.pop(k, None)
            
        else:
            subargs['--skip_align'] = ''
            # Remove some parameters not used when skipping alignment
            for k in ["--healpix_order", "--offset_range", "--offset_step", "--sigma_ang", "--relax_sym", "--allow_coarser_sampling"]:
                subargs.pop(k, None)
            

        args.update(subargs)

        if extra := self._args.get("extra_args"):
            args.update(Args.fromString(extra))

        self.mkdir("output")
        self.batch_execute("relion_refine", batch, args)

    def _output(self, batch):

        out_star = self.join("output", "run_data.star")
        if not os.path.exists(out_star):
            raise Exception(
                f"relion_refine did not produce run_data.star. Check {self.join('run.out')}."
            )

        with StarFile(out_star) as sf:
            o = sf.getTable("optics")
            box = o[0].rlnImageSize
            ps = o[0].rlnImagePixelSize
            N = sf.getTableSize("particles")

        output_dir = self.join("output")
        class_pattern = os.path.join(output_dir, "run_it*_class*.mrc")
        class_files = glob(class_pattern)
        last_iter = None
        last_iter_files = []
        iter_re = re.compile(r"run_it(\d+)_class(\d+)\.mrc")
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
            "TomogramParticles": {
                "label": "Classified Particles",
                "type": "TomogramParticles",
                "info": f"{N} pts, {n_classes} classes (box: {box} px, {ps} Å/px)",
                "files": [
                    [out_star, "TomogramGroupMetadata.star.relion.tomo.particles"]
                ],
            },
        }

        for i, vol_path in enumerate(last_iter_files, start=1):
            self.outputs[f"Volume_class{i:02d}"] = {
                "label": f"Class {i}",
                "type": "Volume",
                "info": f"box size: {box} px, {ps} Å/px",
                "files": [
                    [vol_path, "TomogramGroupMetadata.star.relion.volume"]
                ],
            }

        self.updateBatchInfo(batch)


if __name__ == "__main__":
    RelionTomoClassify.main()
