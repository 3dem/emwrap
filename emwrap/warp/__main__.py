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
import re

from emtools.utils import Color, Process, System, Path, GpuMonitor
from emtools.jobs import BatchManager, Args
from emtools.metadata import Table, StarFile

from .utils import load_tomograms_table
from .warp import WarpBasePipeline


# Regex to match absolute path inside quotes
regex_pattern = r'''
    (['"])          # Capture Group 1: Match and capture the opening quote (single or double)
    (               # Capture Group 2: Start capturing the path
        /           #   An absolute path starts with a forward slash
        (?:         #   Start a non-capturing group for path segments
            [^/\0]+ #     Match one or more characters that are not / or null
            /?      #     Optionally match a trailing slash for a directory
        )*          #   End non-capturing group, allow zero or more segments
    )               # End capturing the path
    \1              # Match the closing quote, which must be the same as the opening quote (Group 1)
'''


def remap(args):
    for fn in glob.glob(args.pattern):
        print("Parsing file: ", fn)
        with open(fn) as f:
            lines = f.readlines()
        with open(fn, 'w') as f:
            for line in lines:
                if args.old_path in line:
                    print("   ", Color.bold("Line: "), line)
                    if args.split:
                        if m := regex_pattern.match(line):
                            print("   Match: ", Color.green(m.groups()[0]))
                    else:
                        line = line.replace(args.old_path, args.new_path)
                f.write(line)


def star(args):
    tomo_session = {
        'path': os.getcwd()
    }
    for k in ['tomograms', 'reconstruction', 'picking', 'thickness']:
        if v := getattr(args, k, None):
            tomo_session[k] = v

    from pprint import pprint
    pprint(tomo_session)

    table = load_tomograms_table(tomo_session)

    with StarFile('tomograms.star', 'w') as sfOut:
        sfOut.writeTable('tomograms', table)


def copy(args):
    if os.path.exists(args.output) and not args.force:
        raise Exception("Output folder already exists.")
    WarpBasePipeline.importInputs(os.getcwd(), args.output)


def ctf_views(args):
    """ Redo the CTF fit views (<movie>_ps.jpg and _ctf.json) of a motion
    correction job that are missing or out of date, as the job would. """
    from emtools.metadata import WarpXml

    class JobThumbnails(WarpBasePipeline):
        def __init__(self, jobDir):     # only the thumbnail methods are used
            self._jobDir = jobDir

        def join(self, *p):
            return os.path.join(self._jobDir, *p)

        def log(self, msg):
            print(msg, flush=True)

    for jobDir in args.job:
        job = JobThumbnails(os.path.abspath(jobDir))
        xmls = sorted(glob.glob(job.join(job.FS, '*.xml')))
        done = 0
        for i, xml in enumerate(xmls, 1):
            ps = job.join(job.FS, 'powerspectrum', Path.removeBaseExt(xml) + '.mrc')
            if os.path.exists(ps):
                jsonPath = job.join(job.FS, job.THUMBNAILS, Path.removeBaseExt(xml) + '_ctf.json')
                before = os.path.getmtime(jsonPath) if os.path.exists(jsonPath) else None
                job.writePsThumbnail(ps, WarpXml(xml))
                done += os.path.exists(jsonPath) and os.path.getmtime(jsonPath) != before
            if i % 1000 == 0:
                print(f'  {i}/{len(xmls)} movies, {done} views redone', flush=True)
        print(f'{jobDir}: {done} of {len(xmls)} CTF views redone')
    # emhub keeps the correlations until a tilt series star file changes
    print('A running emhub server shows the new CTF profile correlations after a restart.')


def main():
    parser = argparse.ArgumentParser(prog='emwrap.warp')
    subparsers = parser.add_subparsers(
        help='Subcommand to execute',
        dest='command')

    remap_parser = subparsers.add_parser("remap")

    remap_parser.add_argument('old_path', help="Old path to be re-mapped")
    remap_parser.add_argument('new_path', help="New path to replace OLD")
    remap_parser.add_argument('pattern', help="Pattern of files for remapping.")
    remap_parser.add_argument('--split', action="store_true",
                              help="Split path using old_path as token, "
                                   "not just replacing the path. ")

    star_parser = subparsers.add_parser("star")

    star_parser.add_argument('--tomograms', '-t', help="Tomostar folder")
    star_parser.add_argument('--reconstruction', '-r', help="Reconstruction folder")
    star_parser.add_argument('--picking', '-p', help="Picking folder")
    star_parser.add_argument('--thickness', '-k', help="Star file with thicknes values. ")

    copy_parser = subparsers.add_parser("copy")

    copy_parser.add_argument('--output', '-o', help="Output folder")
    copy_parser.add_argument('--force', '-f', action="store_true",
                              help="Clean output directory if it exist. ")

    ctf_parser = subparsers.add_parser(
        "ctf-views", help="Redo missing or out of date CTF fit views "
                          "(thumbnails/<movie>_ps.jpg, _ctf.json) of a job")
    ctf_parser.add_argument('job', nargs='+', help="Motion correction job folders")

    args = parser.parse_args()

    if args.command == 'remap':
        remap(args)
    elif args.command == 'star':
        star(args)
    elif args.command == 'copy':
        copy(args)
    elif args.command == 'ctf-views':
        ctf_views(args)
    else:
        raise Exception(f"Unknown command '{args.command}")


if __name__ == '__main__':
    main()
