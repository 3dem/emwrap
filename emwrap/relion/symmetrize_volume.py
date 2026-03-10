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

from emtools.utils import Color
from emtools.jobs import Batch, Args
from emtools.image import Image

from .relion_base import RelionBasePipeline


class RelionSymmetrizeVolume(RelionBasePipeline):
    """ Wrapper around relion_align_symmetry to align a volume for a given symmetry. """
    name = 'emw-relion-symmetrize_volume'

    def prerun(self):
        inVol = self._args['relion_align_symmetry.i']
        if not os.path.exists(inVol):
            raise Exception(f"Input volume '{inVol}' does not exist.")

        sym = self._args.get('relion_align_symmetry.sym', 'C1')
        if not sym:
            sym = 'C1'

        batch = Batch(id=self.name, path=self.outputDir)
        inVolLocal = batch.link(inVol)
        outVolLocal = 'aligned_volume.mrc'
        self.log(f"Input volume: {Color.bold(inVol)}")
        self.log(f"Symmetry: {Color.green(sym)}")
        self.log(f"Output volume: {Color.bold(outVolLocal)}")

        # relion_align_symmetry is single-process (no MPI)
        args = Args({
            'relion_align_symmetry': 1,  # No MPI
            '--i': inVolLocal,
            '--o': outVolLocal,
            '--sym': sym,
        })

        self.batch_execute('relion_align_symmetry', batch, args)

        # Make the path relative to the project folder
        outVol = batch.join(outVolLocal)

        if not os.path.exists(outVol):
            raise Exception(f"relion_align_symmetry did not produce output. Check {self.join('run.out')}.")

        # Get volume dimensions for output info
        try:
            dims = Image.get_dimensions(outVol)
            if isinstance(dims, (list, tuple)) and len(dims) >= 3:
                info = f"box size: {dims[0]} x {dims[1]} x {dims[2]} px"
            elif isinstance(dims, (list, tuple)) and len(dims) > 0:
                info = f"box size: {dims[0]} px"
            else:
                info = "Aligned volume"
        except Exception:
            info = "Aligned volume"

        self.outputs = {
            'Volume': {
                'label': 'Aligned Volume',
                'type': 'Volume',
                'info': info,
                'files': [
                    [outVol, 'TomogramGroupMetadata.star.relion.volume']
                ]
            }
        }
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    RelionSymmetrizeVolume.main()
