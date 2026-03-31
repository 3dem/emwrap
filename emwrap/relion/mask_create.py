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
RELION mask creation job. Two modes:

1) From volume: use --i (input map) and --ini_threshold to binarize and create mask.
   Optional: --extend_inimask, --width_soft_edge, --invert, --lowpass, --angpix, etc.

2) De novo: use --denovo with --box_size and optional --inner_radius, --outer_radius,
   --center_x/y/z to create a spherical/circular mask.

Help from relion_mask_create:
  --i () : Input map to use for thresholding to generate initial binary mask
  --o (mask.mrc) : Output mask
  --and/--or/--and_not/--or_not : Optional second map for combined masking
  --ini_threshold (0.01) : Initial threshold for binarization
  --extend_inimask (0) : Extend initial binary mask this number of pixels
  --width_soft_edge (0) : Width (in pixels) of the additional soft edge
  --invert (false) : Invert the final mask
  --helix (false) : Generate a mask for 3D helix
  --lowpass (-1) : Lowpass filter (Å) prior to binarization
  --angpix (-1) : Pixel size (Å) for the lowpass filter
  --denovo (false) : Create a mask de novo
  --box_size (-1) : The box size of the mask in pixels (de novo)
  --inner_radius (0) : Inner radius of the masked region in pixels
  --outer_radius (99999) : Outer radius of the mask region in pixels
  --center_x/y/z (0) : Center of the mask in pixels
"""

import os

from emtools.utils import Color
from emtools.jobs import Batch, Args
from emtools.image import Image

from .relion_base import RelionBasePipeline


class RelionMaskCreate(RelionBasePipeline):
    """Wrapper around relion_mask_create for creating 3D masks.

    Two modes: (1) From input volume using threshold; (2) De novo with radius/box.
    """
    name = 'emw-relion-mask_create'

    def prerun(self):
        denovo = self._args.get('relion_mask_create.denovo', False)
        batch = Batch(id=self.name, path=self.outputDir)
        out_mask_local = 'mask.mrc'

        if denovo:
            box_size = self._args.get('relion_mask_create.denovo.box_size')
            if box_size is None or str(box_size).strip() == '':
                raise Exception("De novo mode requires --box_size (in pixels).")
            self.log(f"Mode: {Color.green('De novo')} (box_size={box_size})")
            subargs = self.get_subargs('relion_mask_create.denovo')
            subargs['--denovo'] = ''
        else:
            in_vol = self._args.get('relion_mask_create.volume.i')
            if not in_vol or not str(in_vol).strip():
                raise Exception("From-volume mode requires an input volume (--i).")
            # Resolve path relative to project (workingDir)
            in_vol_abs = os.path.join(self.workingDir, in_vol) if not os.path.isabs(in_vol) else in_vol
            if not os.path.exists(in_vol_abs):
                raise Exception(f"Input volume '{in_vol}' does not exist (resolved: {in_vol_abs}).")
            in_vol_local = batch.link(in_vol_abs)
            self.log(f"Mode: {Color.green('From volume')} — input: {Color.bold(in_vol)}")
            subargs = self.get_subargs('relion_mask_create.volume')
            subargs['--i'] = in_vol_local

        self.log(f"Output mask: {Color.bold(out_mask_local)}")
        subargs['--o'] = out_mask_local

        args = Args({'relion_mask_create': 1})
        args.update(subargs)

        self.batch_execute('relion_mask_create', batch, args)

        out_mask = batch.join(out_mask_local)
        if not os.path.exists(out_mask):
            raise Exception(
                f"relion_mask_create did not produce output. Check {self.join('run.out')}."
            )

        try:
            dims = Image.get_dimensions(out_mask)
            if isinstance(dims, (list, tuple)) and len(dims) >= 3:
                info = f"box size: {dims[0]} x {dims[1]} x {dims[2]} px"
            elif isinstance(dims, (list, tuple)) and len(dims) > 0:
                info = f"box size: {dims[0]} px"
            else:
                info = "3D mask"
        except Exception:
            info = "3D mask"

        self.outputs = {
            'VolumeMask': {
                'label': 'Output Mask',
                'type': 'VolumeMask',
                'info': info,
                'files': [
                    [out_mask, 'TomogramGroupMetadata.star.relion.mask3d']
                ]
            }
        }
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    RelionMaskCreate.main()
