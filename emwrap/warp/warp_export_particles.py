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
from emtools.metadata import StarFile, Acquisition
from emtools.jobs import Batch, Args
from emtools.image import Image


from .warp import WarpBasePipeline


class WarpExportParticles(WarpBasePipeline):
    """ Script to run warp_ts_aretomo. """
    name = 'emw-warp-export'
    input_name = 'in_particles'

    def prerun(self):
        inputFm = FolderManager(self._args['in_particles'])
        tomostarFolder = self._getTomostarFolder(inputFm)
        tomostarFm = FolderManager(tomostarFolder)
        self._joinStarFiles(inputFm, tomostarFm)
        # Assume that the Warp folder is one level up from the tomostar
        self._importInputs(FolderManager(os.path.dirname(tomostarFolder)))
        self.mkdir('Particles')

        batch = Batch(id=self.name, path=self.path)

        # Run ts_ctf
        args = Args({
            "ts_export_particles": "",
            "--settings": self.TSS,
            "--input_star": "all_coordinates.star",
            "--box": 64,
            "--diameter": 140,
            "--coords_angpix": 9.52,  # FIXME
            "--output_angpix": 4.76,  # FIXME
            "--output_star": "warp_particles.star",
            "--output_processing": "Particles",
            "--device_list": self.gpuList
        })
        args.update(self._args['ts_export_particles']['extra_args'])
        with batch.execute('ts_export_particles'):
            batch.call(self.warptools, args)

        self.updateBatchInfo(batch)

    def _getTomostarFolder(self, inputFm):
        """ Find the warp_tomostar folder from the input picking. """
        with open(inputFm.join('..', 'info.json')) as f:
            info = json.load(f)
        for i in info['inputs']:
            if 'tomograms' in i:
                return i['tomograms']
        return None

    def _joinStarFiles(self, inputFm, tomostarFm):
        """ Join all input coordinates star files into a single one,
        and correct the rlnMicrographName to use the .tomostar suffix
        """
        suffix = None
        starFiles = inputFm.glob('*default_particles.star')

        with StarFile(self.join('all_coordinates.star'), 'w') as sfOut:
            for starFn in starFiles:
                self.log(f"Parsing file: {starFn}")
                # Update micrographs.star
                with StarFile(starFn) as sf:
                    if t := sf.getTable('particles'):
                        if suffix is None:  # First time
                            sfOut.writeTimeStamp()
                            sfOut.writeHeader('particles', t)

                        micName = t[0].rlnMicrographName
                        suffix = '_' + micName.split('_')[-1]
                        newMicName = micName.replace(suffix, '.tomostar')
                        if not tomostarFm.exists(newMicName):
                            print(Color.red("     Missing: " + newMicName))
                        for row in t:
                            sfOut.writeRow(row._replace(rlnMicrographName=newMicName))


def main():
    WarpExportParticles.runFromArgs()


if __name__ == '__main__':
    main()
