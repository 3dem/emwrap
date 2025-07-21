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
from emwrap.base import ProcessingPipeline

from .warp import get_warptools


class WarpCtfReconstruct(ProcessingPipeline):
    """ Script to run warp_ts_aretomo. """
    name = 'emw-warp-ctfrec'
    input_name = 'in_movies'

    def __init__(self, all_args):
        args = all_args[self.name]
        ProcessingPipeline.__init__(self, args)
        self.gpuList = args['gpu'].split()
        self.inputTsAlign = args['in_movies']
        self.acq = Acquisition(all_args['acquisition'])
        self.warptools = get_warptools()

    def prerun(self):
        self.dumpArgs(printMsg="Input args")
        # FIXME: Improve the pattern split into root folder and the images suffix
        inputFolder = FolderManager(self.inputTsAlign)
        ts = self.link(inputFolder.join('warp_tiltseries'))
        tss = self.link(inputFolder.join(f"{ts}.settings"))
        tms = self.link(inputFolder.join('warp_tomostar'))
        fs = self.link(inputFolder.join('warp_frameseries'))
        if gain := self.acq.get('gain', None):
            self.link(gain)

        batch = Batch(id=self.name, path=self.path)

        # Run ts_ctf
        args = Args({
            'ts_ctf': '',
            '--settings': tss,
            '--device_list': self.gpuList
        })
        args.update(self._args['ts_ctf']['extra_args'])
        with batch.execute('ts_ctf'):
            batch.call(self.warptools, args)

        # Run filter_quality
        args = Args({
            'filter_quality': '',
            '--settings': tss,
            "--resolution": [1, 6],
            "--output": "warp_tiltseries_filtered.txt"
        })
        with batch.execute('filter_quality'):
            batch.call(self.warptools, args)

        # Run ts_reconstruct
        args = Args({
            'ts_reconstruct': '',
            '--settings': tss,
            '--device_list': self.gpuList
        })
        from pprint import pprint
        print("ts_reconstruct: ARGS:")
        pprint(args)
        args.update(self._args['ts_reconstruct']['extra_args'])

        with batch.execute('ts_reconstruct'):
            batch.call(self.warptools, args)

        self.updateBatchInfo(batch)


def main():
    WarpCtfReconstruct.runFromArgs()


if __name__ == '__main__':
    main()
