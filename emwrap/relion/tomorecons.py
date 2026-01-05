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
from emtools.metadata import StarFile, Acquisition, Table
from emtools.jobs import Batch, Args
from emtools.image import Image

from . relion_base import RelionBasePipeline


class RelionTomoRecons(RelionBasePipeline):
    """ Script to run warp_ts_aretomo. """
    name = 'emw-relion-tomorecons'

    def prerun(self):
        cpus = self._args['cpus']
        inStar = self._args['relion_reconstruct.i']
        doCtf = self._args['relion_reconstruct.ctf']

        with StarFile(inStar) as sf:
            tableNames = sf.getTableNames()
            if 'general' in tableNames:
                tableGeneral = sf.getTable('general')
                if tableGeneral[0].rlnTomoSubTomosAre2DStacks == 1:
                    raise Exception("Current implementation for relion_reconstruct "
                                    "only support particles extracted as 3d. ")
            ptsTableName = 'particles' if 'particles' in tableNames else ''
            table = sf.getTable(ptsTableName, guessType=False)
            n = len(table)
            ps = '%0.3f' % float(table[0].rlnPixelSize)
            box = Image.get_dimensions(table[0].rlnImageName)[0]

        self.log(f"Input star file: {Color.bold(inStar)}")
        self.log(f"Total input particles: {Color.green(n)}")
        self.inputs = {
            'TomogramParticles': {
                'label': 'Tomogram Particles',
                'type': 'TomogramParticles',
                'info': f"{n} items (box size: {box} px, {ps} Å/px)",
                'files': [
                    [inStar, 'TomogramGroupMetadata.star.relion.tomo.particles']
                ]
            }
        }
        self.writeInfo()

        batch = Batch(id=self.name, path=self.workingDir)
        outVol = self.join('reconstructed_volume.mrc')
        # Run ts_ctf
        args = Args({
            'relion_reconstruct': cpus,
            "--i": inStar,
            "--o": outVol
        })
        if doCtf:
            args['--ctf'] = ''

        self.batch_execute('relion_reconstruct', batch, args)
        self.outputs = {
            'Volume': {
                'label': 'Reconstructed Volume',
                'type': 'Volume',
                'info': f"box size: {box} px, {ps} Å/px",
                'files': [
                    [outVol, 'TomogramGroupMetadata.star.relion.volume']
                ]
            }
        }
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    RelionTomoRecons.main()
