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
    p = argparse.ArgumentParser()
    p.add_argument('xml_dir', help="Old path to be re-mapped")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--replace_root", nargs=2, metavar=('OLD_PATH', 'NEW_PATH'),
                   help="Replace old path prefix with new one")
    g.add_argument("--strip_tmp", action="store_true",
                   help="Remove tmp batch folder from data directory")
    p.add_argument("--output", "-o", metavar="OUTPUT DIR",
                   help="Output directory")

    args = p.parse_args()

    def _replace(line):
        return line.replace(args.replace_root[0], args.replace_root[1])

    def _strip(line):
        parts = line.split('"')
        fn = parts[1]
        root, base = os.path.split(fn)
        beforeTmp = os.path.dirname(os.path.dirname(root))
        newFn = os.path.join(beforeTmp, base)
        return line.replace(fn, newFn)

    for fn in glob.glob(args.xml_dir + "/*.xml"):
        print(Color.bold(f">>> Parsing file: {fn}"))
        with open(fn) as f:
            lines = f.readlines()

        if args.replace_root:
            remapFunc = _replace
        elif args.strip_tmp:
            remapFunc = _strip
        else:
            remapFunc = None

        newlines = []
        for line in lines:
            if 'DataDirectory' in line:
                print(f" OLD_LINE: {Color.red(line)}")
                line = remapFunc(line)
                print(f" NEW_LINE: {Color.green(line)}")
            newlines.append(line)

        if outputDir := args.output:
            outFn = os.path.join(outputDir, os.path.basename(fn))
            with open(outFn, 'w') as f:
                for line in newlines:
                    f.write(line)


if __name__ == '__main__':
    main()
