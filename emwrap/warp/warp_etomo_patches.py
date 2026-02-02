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
from emtools.metadata import StarFile, Table, WarpXml
from emtools.image import Image


from .warp import WarpBaseTsAlign


class WarpEtomoPatches(WarpBaseTsAlign):
    """ Warp wrapper to run warp_ts_aretomo.
    It will run:
        - ts_import -> mdocs
        - create_settings -> warp_tiltseries.settings
        - ts_aretomo -> ts alignment
    """
    name = 'emw-warp-etomo_patches'

    def runAlignment(self, batch):
        # Run ts_aretomo wrapper
        args = Args({
            'WarpTools': 'ts_etomo_patches',
            '--settings': self.TSS
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = self.get_subargs('ts_etomo_patches', '--')
        args.update(subargs)
        self.batch_execute('ts_etomo_patches', batch, args)


if __name__ == '__main__':
    WarpEtomoPatches.main()
