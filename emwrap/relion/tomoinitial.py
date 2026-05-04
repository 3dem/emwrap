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
RELION STA tomography initial model (VDAM) from pseudo-subtomograms.

Mirrors ``RelionJob::initialiseInimodelJob`` / ``RelionJob::getCommandsInimodelJob``
in ``src/pipeline_jobs.cpp`` for **tomography** and **not** a continuation run:
``relion_refine`` with ``--grad --denovo_3dref``, ``--ios``, fixed sampling
(``--oversampling 1 --healpix_order 1 --offset_range 6 --offset_step 2
--auto_sampling``), ``--zero_mask``, ``--pad 1``, then optionally
``relion_align_symmetry`` as in the RELION GUI pipeline.

See RELION 5 STA tutorial:
https://relion.readthedocs.io/en/release-5.0/STA_tutorial/InitialModel.html
"""

import os
import re
from glob import glob

from emtools.image import Image
from emtools.jobs import Batch, Args
from emtools.metadata import StarFile

from .relion_base import RelionBasePipeline


_MODEL_STAR_RE = re.compile(r"run_it(\d+)_model\.star$")

# Always appended for STA inimodel (non-continue); RELION GUI does not expose these.
_REFINE_FIXED_TOMO = {
    
}


class RelionTomoinitial(RelionBasePipeline):
    """Wrapper around relion_refine inimodel (VDAM) for STA."""

    name = "emw-relion-tomoinitial"

    @staticmethod
    def _latest_model_star(output_dir):
        """Return path to run_itNNN_model.star with largest N under output_dir."""
        pattern = os.path.join(output_dir, "run_it*_model.star")
        best_path, best_it = None, -1
        for path in glob(pattern):
            m = _MODEL_STAR_RE.search(os.path.basename(path))
            if not m:
                continue
            it = int(m.group(1))
            if it > best_it:
                best_it = it
                best_path = path
        return best_path

    def prerun(self):
        ios = self._args.get("relion_refine.ios")
        if not ios or not str(ios).strip():
            raise Exception("Input optimisation set (--ios) is required.")
        if not os.path.exists(ios):
            raise Exception(f"Input optimisation set '{ios}' does not exist.")

        gpus = int(self._args.get("gpus", 1))
        mpis = gpus + 1

        inverted_booleans = [
            "dont_combine_weights_via_disc",
            "no_parallel_disc_io",
        ]
        subargs = self.get_subargs(
            "relion_refine",
            inverted_booleans=inverted_booleans,
            possitive=['sigma_tilt'],
        )

        args = Args(
            {
                "relion_refine": 1,
                "--o": self.join("output/run"),
                "--grad": "",
                "--denovo_3dref": "",
                "--pad": 1,
                "--oversampling": 1,
                "--healpix_order": 1,
                "--offset_range": 6,
                "--offset_step": 2,
                "--auto_sampling": "",
                "--zero_mask": "",
                "--j": mpis,
            }
        )
        args.update(_REFINE_FIXED_TOMO)
        args.update(subargs)

        # Orchestration (not relion_refine CLI keys)
        run_in_c1 = bool(self._args.pop("run_in_c1", True))
        sym_target = str(self._args.get("relion_refine.sym", "C1")).strip()

        args["--sym"] = "C1" if run_in_c1 else sym_target

        # if "--preread_images" in args:
        #     args.pop("--scratch_dir", None)

        if extra := self._args.get("extra_args"):
            args.update(Args.fromString(extra))

        batch = Batch(id=self.name, path=self.workingDir)
        self.mkdir("output")
        self.batch_execute("relion_refine", batch, args)

        iterations = int(args["--iter"])
        out_dir = self.join("output")
        fn_model = self._latest_model_star(out_dir)
        if not fn_model:
            raise Exception(
                f"relion_refine did not produce run_it*_model.star under '{out_dir}'. "
                f"Check run.out and --iter ({iterations})."
            )

        out_initial = self.join("output", "initial_model.mrc")
        out_class = fn_model.replace("_model.star", "_class001.mrc")

        sym_align = sym_target if (run_in_c1 and sym_target.upper() != "C1") else "C1"
        args_sym = Args(
            {
                "relion_align_symmetry": 1,
                "--i": fn_model,
                "--o": out_initial,
                "--sym": sym_align,
                "--apply_sym": "",
                "--select_largest_class": "",
            }
        )
        self.batch_execute("relion_align_symmetry", batch, args_sym)
        primary_vol = out_initial

        if not os.path.exists(primary_vol):
            raise Exception(
                f"Expected output volume '{primary_vol}' was not produced."
            )

        box, ps = None, None
        out_star = self.join("output", "run_data.star")
        if os.path.exists(out_star):
            with StarFile(out_star) as sf:
                o = sf.getTable("optics")
                box = o[0].rlnImageSize
                ps = o[0].rlnImagePixelSize
                n_pts = sf.getTableSize("particles")
        else:
            n_pts = None

        if box is None:
            dims = Image.get_dimensions(primary_vol)
            box = dims[0] if isinstance(dims, (list, tuple)) else dims
            ps = "?"

        self.outputs = {
            "Volume": {
                "label": "Tomoinitial map",
                "type": "Volume",
                "info": f"box size: {box} px, {ps} Å/px",
                "files": [
                    [primary_vol, "TomogramGroupMetadata.star.relion.volume"]
                ],
            }
        }

        if os.path.exists(out_star):
            self.outputs["TomogramParticles"] = {
                "label": "Tomoinitial particles",
                "type": "TomogramParticles",
                "info": f"{n_pts} pts (size: {box} px, {ps} Å/px)",
                "files": [
                    [out_star, "TomogramGroupMetadata.star.relion.tomo.particles"]
                ],
            }

        self.updateBatchInfo(batch)


if __name__ == "__main__":
    RelionTomoinitial.main()
