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

from emtools.jobs import Args
from emtools.metadata import TextFile, RelionStar

from .warp import WarpBaseTsAlign


class WarpAreTomo(WarpBaseTsAlign):
    """ Warp wrapper to run warp_ts_aretomo.
    It will run:
        - ts_import -> mdocs
        - create_settings -> warp_tiltseries.settings
        - ts_aretomo -> ts alignment
    """
    name = 'emw-warp-aretomo'
    output_angpix = "ts_aretomo.angpix"

    def runAlignment(self, batch):
        aretomo_launcher = self.get_launcher_arg('launcher_aretomo', 'ARETOMO2')

        # Run ts_aretomo wrapper
        args = Args({
            'WarpTools': 'ts_aretomo',
            '--settings': self.TSS,
            '--exe': aretomo_launcher
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = self._args.subset('ts_aretomo', '--', filters=['remove_false', 'remove_empty'])
        args.update(subargs)
        self.batch_execute('ts_aretomo', batch, args) 
                           #launcher=self.get_launcher_arg('launcher_warp', 'WARP'))

    def parseAlignmentParams(self, batch, tsName, ps):
        self.log(f"Parsing alignments for tomo: {tsName}")
        alnFile = batch.join(self.TS, 'tiltstack', tsName, f'{tsName}.st.aln')
        alignments = []
        # Despite Warp's Aretomo wrapper writes the angles from positive to negative
        # Aretomo always write the alignment back from negative to positive order,
        # So we don't need to reverse it when parsing to the STAR file
        for line in TextFile.stripLines(alnFile):
            parts = line.split()
            values = {
                'rlnTomoXTilt': 0,
                'rlnTomoYTilt': float(parts[-1]),
                "rlnTomoZRot": float(parts[1]),
                'rlnTomoXShiftAngst': float(parts[3]) * ps,
                'rlnTomoYShiftAngst': float(parts[4]) * ps,
            }
            alignments.append({k: float(values[k]) for k in values})

        return alignments

if __name__ == '__main__':
    WarpAreTomo.main()
