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


import sys
import json
import os.path
import argparse
import ast
import time
from collections import OrderedDict
import datetime as dt
import re
from pprint import pprint

from emtools.utils import Process, Color, System, Path
from emtools.metadata import EPU, SqliteFile, StarFile, Table
from emtools.jobs import Pipeline


def cryosparc_prepare(epuFolder, movStar, micStar):
    if os.path.exists('CS'):
        raise Exception("CS folder already exists. Remove it before running this command.")

    logger = Process.Logger(format="%(message)s", only_log=False)#True)

    for folder in ['Micrographs', 'Movies', 'XML']:
        logger.mkdir(f'CS/{folder}')

    movTable = StarFile.getTableFromFile(movStar, 'movies')
    micTable = StarFile.getTableFromFile(micStar, 'micrographs')

    def _micKey(micFn):
        micBase = Path.removeBaseExt(micFn)
        return micBase.replace('aligned_', '').replace('_EER_DW', '')

    micDict = {_micKey(row.rlnMicrographName): row for row in micTable}

    xmlDict = {}

    for root, dirs, files in os.walk(epuFolder):
        for fn in files:
            if fn.endswith('.xml'):
                xmlKey = Path.removeBaseExt(fn)
                xmlDict[xmlKey] = os.path.join(root, fn)

    with StarFile('CS/particles.star', 'w') as sfOut:
        ctfCols = ['rlnDefocusU', 'rlnDefocusV', 'rlnDefocusAngle', 'rlnCtfFigureOfMerit', 'rlnCtfMaxResolution']
        ctfCols = []  # CS is giving an error when using CTF
        t = Table(['rlnMicrographName', 'rlnCoordinateX', 'rlnCoordinateY'] + ctfCols)
        print("cols", len(t.getColumnNames()), t.getColumnNames())

        sfOut.writeHeader('particles', t)

        mId = 0
        for rowMov in sorted(movTable, key=lambda r: r.TimeStamp):

            movFn = rowMov.rlnMicrographMovieName
            movBase = Path.removeBaseExt(movFn).replace('_EER', '')

            micRow = micDict.get(movBase, None)
            if micRow is None:
                print(Color.red(f'No micrograph found for movie: {movFn}'))
                continue

            xmlFn = xmlDict.get(movBase, None)
            if xmlFn is None:
                print(Color.red(f'No XML found for movie: {movFn}'))
                continue

            micFn = micRow.rlnMicrographName

            print(f">>> {movFn}: \n"
                  f"\tMicrograph: {Color.bold(micFn)}\n"
                  f"\t   EPU XML: {Color.bold(xmlFn)}")

            mId += 1
            ext = Path.getExt(movFn)
            if ext == '.tiff':
                ext = '.tif'  # Make it always 4 chars

            micName = f"micrograph_{mId:06}.mrc"
            movRoot = f"movie_{mId:06}"
            movName = f"{movRoot}{ext}"
            xmlName = f"{movRoot}.xml"

            logger.system(f'ln -s ../../{micFn} CS/Micrographs/{micName}')
            logger.system(f'ln -s ../../{movFn} CS/Movies/{movName}')
            logger.system(f'ln -s ../../{xmlFn} CS/XML/{xmlName}')

            micPath = os.path.dirname(micFn)
            coordPath = micPath.replace('Micrographs', 'Coordinates')
            coordBase = Path.replaceBaseExt(micFn, "_coords.star")
            coordsFn = os.path.join(coordPath, coordBase)
            ctfValues = [getattr(row, k) for k in ctfCols]
            print(len(ctfValues))
            if os.path.exists(coordsFn):
                with StarFile(coordsFn) as sfCoords:
                    for rowCoord in sfCoords.iterTable(''):
                        sfOut.writeRow(t.Row(f'{micName}',
                                             rowCoord.rlnCoordinateX,
                                             rowCoord.rlnCoordinateY,
                                             *ctfValues))


def cryosparc_import(projId, dataRoot):

    acq = {
        "psize_A": 0.724,
        "accel_kv": 300,
        "cs_mm": 0.1,
    }

    cs = CryoSparc(projId)
    csRoot = os.path.join(dataRoot, 'CS')

    print(f">>> Importing data from: {Color.green(dataRoot)}")

    args = {
        "blob_paths": f"{csRoot}/Micrographs/mic_*.mrc",
        "total_dose_e_per_A2": 40,
        "parse_xml_files": True,
        "xml_paths": f"{csRoot}/XML/mov_*.xml",
        "mov_cut_prefix_xml": 4,
        "mov_cut_suffix_xml": 4,
        "xml_cut_prefix_xml": 4,
        "xml_cut_suffix_xml": 4
    }
    args.update(acq)
    micsImport = cs.job_run("W1", "import_micrographs", args)
    time.sleep(5)  # FIXME: wait for job completion
    args = {
        "ignore_blob": True,
        "particle_meta_path": f"{csRoot}/particles.star",
        "query_cut_suff": 4,
        "remove_leading_uid": True,
        "source_cut_suff": 4,
        "enable_validation": True,
        "location_exists": True,
        "amp_contrast": 2.7,
    }
    args.update(acq)
    ptsImport = cs.job_run("W1", "import_particles", args,
                          {'micrographs': f'{micsImport}.imported_micrographs'})


if __name__ == '__main__':
    cryosparc_prepare(sys.argv[1], sys.argv[2], sys.argv[3])
