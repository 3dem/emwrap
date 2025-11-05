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

from emtools.utils import Color, Process, System, Path, FolderManager
from emtools.jobs import BatchManager, Args
from emtools.metadata import Table, StarFile, WarpXml


def load_tomograms_table(tomo_session):
    session_path = tomo_session['path']
    s = FolderManager(session_path)

    if not s.exists():
        raise Exception("Session path does not exist")
    else:
        for k in ['tomograms', 'reconstruction', 'picking']:
            if k in tomo_session and not s.exists(tomo_session[k]):
                raise Exception(f"*{k}* folder does not exist.")

    picking = tomo_session.get('picking', '')
    t = FolderManager(s.join(tomo_session['tomograms']))
    c = FolderManager(s.join(picking, 'Coordinates')) if picking else None
    r = FolderManager(s.join(tomo_session['reconstruction']))
    ts = FolderManager(r.path.replace('reconstruction', 'tiltstack'))

    def _glob_file(fm, pattern):
        if files := fm.glob(pattern):
            return files[0]
        else:
            return ''

    coordsDict = {}
    if c:
        if coords := c.glob('*_default_particles.star'):
            # Get the splitting token (e.g 9.52Apx)
            token = coords[0].split('_')[-3]
            coordsDict = {os.path.basename(c.split(token)[0]): c for c in coords}

    tomoDict = {}
    if tomos := r.glob('*.mrc'):
        # Get the splitting token (e.g 9.52Apx)
        suffix = tomos[0].split('_')[-1]
        tomoDict = {os.path.basename(t).replace(suffix, ''): t for t in tomos}

    thickDict = {}
    if tomoStar := tomo_session.get('thickness', None):
        with StarFile(s.join(tomoStar)) as sf:
            for row in sf.iterTable('tomograms'):
                if 'Apx' in row.rlnTomoName:
                    suffix = "_" + row.rlnTomoName.split('_')[-1]
                else:
                    suffix = ""
                thickDict[row.rlnTomoName.replace(suffix, '')] = row.slabThickness

    table = Table([
        'rlnTomoName',
        'rlnTomoMetadata',
        'rlnCoordinatesMetadata',
        'rlnCoordinatesCount',
        'rlnTomogram',
        'rlnAlignedTiltSeries',
        'wrpTomoMetadataXml',
        'rlnDefocus',
        'rlnThickness'
    ])

    for tstar in t.glob("*.tomostar"):
        tsName = Path.removeBaseExt(tstar)

        # Load coordinates file
        # coordMd = _glob_file(c, tsName + '*default_particles.star')
        tsKey = f'{tsName}_'
        coordMd = coordsDict.get(tsKey, '')

        if coordMd:
            with StarFile(coordMd) as sf:
                coordN = sf.getTableSize('particles')
        else:
            coordN = ''

        # Load xml, tomogram, and aligned TS files
        # tomoFn = _glob_file(r, tsName + '*.mrc')
        tomoFn = tomoDict.get(tsKey, '')
        alignedTs = _glob_file(ts, f"{tsName}/{tsName}_aligned.mrc")
        tomoXml = _glob_file(r, f"../{tsName}.xml")

        # Load defocus
        if tomoXml:
            ctf = WarpXml(tomoXml).getDict('TiltSeries', 'CTF', 'Param')
            defocus = round(float(ctf['Defocus']), 2)
        else:
            defocus = ''

        # Relative path if non-empty value
        def _rel(p):
            return s.relpath(p) if p else p

        table.addRow(table.Row(
            rlnTomoName=tsName,
            rlnTomoMetadata=_rel(tstar),
            rlnCoordinatesMetadata=_rel(coordMd),
            rlnCoordinatesCount=coordN,
            rlnTomogram=_rel(tomoFn),
            rlnAlignedTiltSeries=_rel(alignedTs),
            wrpTomoMetadataXml=_rel(tomoXml),
            rlnDefocus=defocus,
            rlnThickness=thickDict.get(tsName, 0)
        ))

    return table

