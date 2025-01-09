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

"""

import os
import subprocess
import pathlib
import sys
import json
import argparse
from pprint import pprint
from glob import glob

from emtools.utils import Color, Timer, Path
from emtools.jobs import ProcessingPipeline, BatchManager
from emtools.metadata import Mdoc

from emr

from .aretomo_pipeline import AreTomoPipeline


def main():
    p = argparse.ArgumentParser(prog='emw-aretomo')
    p.add_argument('--json',
                   help="Input all arguments through this JSON file. "
                        "The other arguments will be ignored. ")
    p.add_argument('--in_movies', '-i')
    p.add_argument('--output', '-o')
    p.add_argument('--aretomo_path', '-p')
    p.add_argument('--aretomo_args', '-a', default='')
    p.add_argument('--scratch', '-s', default='',
                   help="Scratch directory where to store intermediate "
                        "results of the processing. ")
    p.add_argument('--batch_size', '-b', type=int, default=8)
    p.add_argument('--j', help="Just to ignore the threads option from Relion")
    p.add_argument('--gpu', default='0')
    p.add_argument('--mdoc_suffix', '-m',
                   help="Suffix to be removed from the mdoc file names to "
                        "assign each tilt series' name. ")

    args = p.parse_args()

    if len(sys.argv) == 1:
        p.print_help()
        sys.exit(0)

    if args.json:
        raise Exception("JSON input not yet implemented.")
    else:
        argsDict = {
            'input_mdocs': args.in_movies,
            'output_dir': args.output,
            'aretomo_args': args.aretomo_args,
            'gpu_list': args.gpu,
            'batch_size': args.batch_size,
            'mdoc_suffix': args.mdoc_suffix
        }
        aretomo = AreTomoPipeline(argsDict)
        aretomo.run()


if __name__ == '__main__':
    main()
