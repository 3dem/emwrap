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

from __future__ import annotations

import argparse
from errno import EMULTIHOP
import os
import sys
from pprint import pp, pprint

from emtools.metadata import StarFile, Table
from emtools.utils import Color


def subset_tomograms_star(args):
    cwd = os.getcwd()

    input_tomograms_table = StarFile.getTableFromFile('global', args.input_tomograms_star)
    input_particles_table = StarFile.getTableFromFile('particles', args.input_particles_star)
    tomograms_names = set(row.rlnTomoName.replace('.tomostar', '') for row in input_particles_table)

    count = 0
    with StarFile(args.output_tomograms_star, 'w') as sf:
        sf.writeTimeStamp()
        sf.writeHeader('global', input_tomograms_table)
        for row in input_tomograms_table:
            if row.rlnTomoName in tomograms_names:
                sf.writeRow(row)
                count += 1
        print(f'>>> Created subset with {Color.green(count)} tomograms')
        print(f'>>> Total tomograms: {len(input_tomograms_table)}')
        print(f'>>> Total particles: {len(input_particles_table)}')        


def main(argv=None):
    p = argparse.ArgumentParser(
        description='Subset a tomograms STAR file base on the tomograms present in the particles STAR file.')
    p.add_argument(
        'input_tomograms_star',
        help='Input tomograms STAR file')
    p.add_argument(
        'input_particles_star',
        help='Input particles STAR file')
    p.add_argument(
        'output_tomograms_star',
        help='Output tomograms STAR file')

    subset_tomograms_star(p.parse_args(argv))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
