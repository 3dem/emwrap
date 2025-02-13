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

import argparse
from .project import RelionProject


def main():
    p = argparse.ArgumentParser()
    p.add_argument('path', metavar="PROJECT_PATH",
                   help="Project path", default='.', nargs='?')
    g = p.add_mutually_exclusive_group()
    g.add_argument('--clean', '-c', action='store_true',
                   help="Clean project files")
    g.add_argument('--update', '-u', action='store_true',
                   help="Update job status and pipeline star file.")
    g.add_argument('--run', '-r', nargs=2, metavar=('JOB_TYPE', 'COMMAND'))

    args = p.parse_args()

    rlnProject = RelionProject(args.path)

    if args.clean:
        rlnProject.clean()
    elif args.update:
        rlnProject.update()
    elif args.run:
        folder, cmd = args.run
        rlnProject.run(folder, cmd)


if __name__ == '__main__':
    main()
