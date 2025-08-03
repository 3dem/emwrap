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

from .warp import WarpBasePipeline


class WarpCtfReconstruct(WarpBasePipeline):

    """ Script to run warp_ts_aretomo. """
    name = 'emw-warp-ctfrec'
    input_name = 'in_movies'

    def prerun(self):
        inputFolder = FolderManager(self._args['in_movies'])
        self._importInputs(inputFolder)

        batch = Batch(id=self.name, path=self.path)

        # Run ts_ctf
        args = Args({
            'ts_ctf': '',
            '--settings': self.TSS,
            '--device_list': self.gpuList
        })
        args.update(self._args['ts_ctf']['extra_args'])
        with batch.execute('ts_ctf'):
            batch.call(self.warptools, args)

        # Run filter_quality
        args = Args({
            'filter_quality': '',
            '--settings': self.TSS,
            "--resolution": [1, 6],
            "--output": "warp_tiltseries_filtered.txt"
        })
        with batch.execute('filter_quality'):
            batch.call(self.warptools, args)

        # Run ts_reconstruct
        args = Args({
            'ts_reconstruct': '',
            '--settings': self.TSS,
            '--device_list': self.gpuList
        })
        args.update(self._args['ts_reconstruct']['extra_args'])

        with batch.execute('ts_reconstruct'):
            batch.call(self.warptools, args)

        self.updateBatchInfo(batch)


def main():
    WarpCtfReconstruct.runFromArgs()


if __name__ == '__main__':
    main()
