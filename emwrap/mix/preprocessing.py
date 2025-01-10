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
import shutil
import sys
import json
import argparse
from pprint import pprint
import threading

from emtools.utils import Pretty, Timer, Path
from emtools.jobs import Batch
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile

from emwrap.base import Acquisition
from emwrap.motioncor import Motioncor
from emwrap.ctffind import Ctffind
from emwrap.cryolo import CryoloPredict
from emwrap.relion import RelionStar, RelionExtract


class Preprocessing:
    """
    Class that combines motion correction, CTF estimation,
     picking and particle extraction. The goal is to reuse
     the scratch and minimize the transfer of temporary files.
     """
    def __init__(self, args):
        self.acq = Acquisition(args['acquisition'])
        mc_args = args['motioncor']
        self.motioncor = Motioncor(self.acq, **mc_args)
        if 'ctf' in args:
            self.ctf = Ctffind(self.acq, **args['ctf'])
        else:
            self.ctf = None

        # FIXME
        self.picking = args.get('picking', None)
        self.extract = args.get('extract', None)
        if 'extract' in args:
            self.extract = RelionExtract(**args['extract'])
        else:
            self.extract = None

    def process_batch(self, batch, **kwargs):
        t = Timer()
        start = Pretty.now()
        v = kwargs.get('verbose', False)
        gpu = kwargs['gpu']
        cpu = kwargs.get('cpu', 4)

        # Motion correction
        mc = self.motioncor
        mc.process_batch(batch, gpu=gpu)

        # Picking in a separate thread
        batch.mkdir('Coordinates')

        old_batch = batch
        batch = Batch(old_batch)

        def _item(r):
            return None if 'error' in r else r['rlnMicrographName']

        batch['items'] = [_item(r) for r in old_batch['results']]
        self.ctf.process_batch(batch, verbose=v)
        # batch.info.update(batch.info)

        def _move(outputs, outName):
            outDir = batch.mkdir(outName)
            for o in outputs:
                shutil.move(o, outDir)

        _move(old_batch['outputs'], 'Micrographs')
        _move(batch['outputs'], 'CTFs')

        cryolo = CryoloPredict()
        cryolo.process_batch(batch, gpu=gpu, cpu=cpu)

        # Write output micrographs star file
        acq = Acquisition(self.acq)
        origPs = self.acq.pixel_size
        acq.pixel_size = origPs * mc.args['-FtBin']
        tOptics = RelionStar.optics_table(acq, originalPixelSize=origPs)
        tMics = RelionStar.micrograph_table(extra_cols=['rlnMicrographCoordinates'])
        tCoords = RelionStar.coordinates_table()
        def _move_cryolo(micName, folder, ext):
            """ Move result box files from cryolo. """
            srcCoords = batch.join('cryolo_boxfiles', folder, Path.replaceExt(micName, ext))
            dstCoords = os.path.join('Coordinates', Path.replaceExt(micName, f'_coords{ext}'))
            shutil.move(srcCoords, batch.join(dstCoords))
            return dstCoords

        for row, r in zip(old_batch['items'], batch['results']):
            if 'error' not in r:
                values = r['values']
                micName = os.path.basename(values[0])
                values[0] = os.path.join('Micrographs', micName)
                values[1] = row.rlnOpticsGroup  # Fix optics group
                values[2] = os.path.join('CTFs', os.path.basename(values[2]))
                dstCoords = _move_cryolo(micName, 'STAR', '.star')
                _move_cryolo(micName, 'CBOX', '.cbox')
                values.append(dstCoords)
                tMics.addRowValues(*values)
                tCoords.addRowValues(values[0], dstCoords)

        # Write output STAR files, as outputs and needed by extraction
        with StarFile(batch.join('micrographs.star'), 'w') as sf:
            sf.writeTimeStamp()
            sf.writeTable('optics', tOptics)
            sf.writeTable('micrographs', tMics)

        with StarFile(batch.join('coordinates.star'), 'w') as sf:
            sf.writeTimeStamp()
            sf.writeTable('coordinate_files', tCoords)

        self.extract.process_batch(batch)

        batch.info.update({
            'preprocessing_start': start,
            'preprocessing_end': Pretty.now(),
            'preprocessing_elapsed': str(t.getElapsedTime())
        })
        return batch

