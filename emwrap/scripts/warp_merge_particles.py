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


TOMO_STAR = 'warp_particles_tomograms.star'
PARTICLES_STAR = 'warp_particles.star' 
SET_OUTPUT_STAR = 'warp_particles_optimisation_set.star'

DUMMY_TILT = 'dummy_tiltseries.mrc'


def _output_star(fn):
    return fn.replace('warp_particles', 'warp_merged_particles')


def _write_tables(sf, tablesDict, blacklist=None):
    for name, table in tablesDict.items():
        if blacklist and name in blacklist:
            continue
        sf.writeTable(name, table)


def merge_export_particles_outputs(args):
    alias = args.alias
    cwd = os.getcwd()

    def _exists(file_path):
        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            raise FileNotFoundError(f'Missing {file_path}')
        return file_path
    
    def _primary(fn):
        return _exists(os.path.join(cwd, fn))

    def _secondary(fn):
        return _exists(os.path.join(args.other_folder, fn))

    primary_tomo = _primary(TOMO_STAR)
    secondary_tomo = _secondary(TOMO_STAR)
    primary_particles = _primary(PARTICLES_STAR)
    secondary_particles = _secondary(PARTICLES_STAR)

    ogNameMap = {}
    ogIdMap = {}

    # Merge tomograms star files
    primary_tomo_tables = StarFile.getTablesDict(primary_tomo)
    secondary_tomo_tables = StarFile.getTablesDict(secondary_tomo)
    # Merge global tables first 
    global_table = primary_tomo_tables['global']
    dummy = global_table[0].rlnTomoTiltSeriesName
    for row in secondary_tomo_tables['global']:
        rowValues = row._asdict()
        og = rowValues['rlnOpticsGroupName']
        rowValues['rlnTomoTiltSeriesName'] = dummy
        ogNew = f'{alias}_{og}'
        rowValues['rlnOpticsGroupName'] = ogNew
        ogNameMap[og] = ogNew
        global_table.addRowValues(**rowValues)

    output_tomo_star = _output_star(TOMO_STAR)
    with StarFile(output_tomo_star, 'w') as sf:
        sf.writeTimeStamp()
        _write_tables(sf, primary_tomo_tables)
        _write_tables(sf, secondary_tomo_tables, blacklist=['global'])

    # Merge particles star files
    primary_particles_tables = StarFile.getTablesDict(primary_particles)
    secondary_particles_tables = StarFile.getTablesDict(secondary_particles)
    
    optics_table = primary_particles_tables['optics']
    maxOgId = max(int(row.rlnOpticsGroup) for row in optics_table)

    for row in secondary_particles_tables['optics']:
        rowValues = row._asdict()
        ogName = rowValues['rlnOpticsGroupName']
        ogId = rowValues['rlnOpticsGroup']
        maxOgId += 1
        ogNewName = ogNameMap[ogName]
        ogIdMap[ogId] = maxOgId
        rowValues['rlnOpticsGroup'] = maxOgId
        rowValues['rlnOpticsGroupName'] = ogNewName
        optics_table.addRowValues(**rowValues)

    particles_table = primary_particles_tables['particles']
    jobId = particles_table[0].rlnImageName.split('/Particles/')[0]
    newPrefix = os.path.join(jobId, alias)

    for row in secondary_particles_tables['particles']:
        rowValues = row._asdict()
        ogId = rowValues['rlnOpticsGroup']
        ogNewId = ogIdMap[ogId]
        rowValues['rlnOpticsGroup'] = ogNewId
        imageName = rowValues['rlnImageName']
        imageNewName = os.path.join(newPrefix, 'Particles', imageName.split('/Particles/')[1])
        rowValues['rlnImageName'] = imageNewName
        particles_table.addRowValues(**rowValues)

    output_particles_star = _output_star(PARTICLES_STAR)
    with StarFile(output_particles_star, 'w') as sf:
        sf.writeTimeStamp()
        _write_tables(sf, primary_particles_tables)

    # Finally, write the optimisation set star file
    optimisation_table = StarFile.getTableFromFile('', SET_OUTPUT_STAR)
    pprint(optimisation_table[0]._asdict())
    optimisation_table[0] = optimisation_table[0]._replace(rlnTomoParticlesFile=os.path.join(jobId, output_particles_star), 
                                                           rlnTomoTomogramsFile=os.path.join(jobId, output_tomo_star))

    with StarFile(_output_star(SET_OUTPUT_STAR), 'w') as sf:
        sf.writeTimeStamp()
        sf.writeTable('', optimisation_table, singleRow=True)


def main(argv=None):
    p = argparse.ArgumentParser(
        description=(
            f'Merge {TOMO_STAR} -> {_output_star(TOMO_STAR)} and '
            f'{PARTICLES_STAR} -> {_output_star(PARTICLES_STAR)} from another '
            'export-particles folder into this one (symlink + STAR merges).'))
    p.add_argument(
        'other_folder',
        help='Path to the other emw-warp-export_particles output directory')
    p.add_argument(
        'alias',
        help='Name of the symlink to create in the current folder pointing at other_folder')

    merge_export_particles_outputs(p.parse_args(argv))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
