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
import argparse
import glob
import numpy as np
import time
import json

from emtools.utils import Color, Process, System, Path, GpuMonitor
from emtools.jobs import BatchManager, Args
from emtools.metadata import Table, StarFile


def main():
    parser = argparse.ArgumentParser(prog='emwrap.warp')
    subparsers = parser.add_subparsers(
        help='Subcommand to execute',
        dest='command')

    remap_parser = subparsers.add_parser("remap")

    remap_parser.add_argument('old_path', help="Old path to be re-mapped")
    remap_parser.add_argument('new_path', help="New path to replace OLD")
    remap_parser.add_argument('pattern', help="Pattern of files for remapping.")

    args = parser.parse_args()

    for fn in glob.glob(args.pattern):
        print("Parsing file: ", fn)
        with open(fn) as f:
            lines = f.readlines()
        with open(fn, 'w') as f:
            for line in lines:
                if args.old_path in line:
                    line = line.replace(args.old_path, args.new_path)
                f.write(line)


if __name__ == '__main__':
    main()
