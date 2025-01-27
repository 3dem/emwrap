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
        self.args = args

    @property
    def particle_size(self):
        return self.args.get('picking', {}).get('particle_size', None)

    @particle_size.setter
    def particle_size(self, value):
        self.args['picking']['particle_size'] = value

    def process_batch(self, batch, **kwargs):
        batch['Preprocessing.args'] = self.args
        batch['Preprocessing.process_batch.kwargs'] = kwargs
        # Let's replace the items to make the rows more json-ready
        batch['items'] = [row._asdict() for row in batch['items']]
        batch.dump_all()  # Write batch json to be used in sub-process

        # Launcher can be used in the case that we want to launch
        # processing of a batch to the cluster
        # the launcher should load the proper environment
        # and launch this main in the batch folder
        if launcher := self.args.get('launcher', None):
            batch.call(launcher, [os.path.abspath(batch.path)],
                       logfile=None, #batch.join('pp.log'),
                       verbose=True)
            batch.load_all()
        else:
            self._process_batch(batch, kwargs)
            return batch

    def _process_batch(self, batch, kwargs):
        t = Timer()
        start = Pretty.now()
        v = kwargs.get('verbose', False)
        gpu = kwargs['gpu']
        cpu = kwargs.get('cpu', 4)

        # Motion correction
        mc = Motioncor(self.acq, **self.args['motioncor'])
        mc.process_batch(batch, gpu=gpu)

        # Picking in a separate thread
        batch.mkdir('Coordinates')

        old_batch = batch
        batch = Batch(old_batch)

        def _item(r):
            return None if 'error' in r else r['rlnMicrographName']

        # Change input items as expected by CTF job
        batch['items'] = [_item(r) for r in old_batch['results']]
        ctf = Ctffind(self.acq, **self.args['ctf'])
        ctf.process_batch(batch, verbose=v)
        # Restore items
        batch['items'] = old_batch['items']

        def _move(outputs, outName):
            outDir = batch.mkdir(outName)
            for o in outputs:
                shutil.move(o, outDir)

        _move(old_batch['outputs'], 'Micrographs')
        _move(batch['outputs'], 'CTFs')
        del old_batch['outputs']
        del batch['outputs']

        # Calculate new pixel size based on the motioncor binning option
        acq = Acquisition(self.acq)
        origPs = self.acq.pixel_size
        acq.pixel_size = origPs * mc.args['-FtBin']

        cryolo = CryoloPredict(**self.args['picking'])
        cryolo.process_batch(batch, gpu=gpu, cpu=cpu)
        if self.particle_size is None:
            size = cryolo.get_size(batch, 75)

            self.particle_size = round(size * acq.pixel_size)
            print(f">>> Size for percentile 25: {size}, particle_size (A): {self.particle_size}")

        tOptics = RelionStar.optics_table(acq, originalPixelSize=origPs)
        tMics = RelionStar.micrograph_table(extra_cols=['rlnMicrographCoordinates',
                                                        'rlnCoordinatesNumber'])
        tCoords = RelionStar.coordinates_table()

        def _move_cryolo(micName, folder, ext):
            """ Move result box files from cryolo. """
            srcCoords = batch.join('cryolo_boxfiles', folder, Path.replaceExt(micName, ext))
            dstCoords = os.path.join('Coordinates', Path.replaceExt(micName, f'_coords{ext}'))
            shutil.move(srcCoords, batch.join(dstCoords))
            return dstCoords

        for row, r in zip(batch['items'], batch['results']):
            if 'error' not in r:
                values = r['values']
                micName = os.path.basename(values[0])
                values[0] = os.path.join('Micrographs', micName)
                values[1] = row['rlnOpticsGroup']
                values[2] = os.path.join('CTFs', os.path.basename(values[2]))
                dstCoords = _move_cryolo(micName, 'STAR', '.star')
                _move_cryolo(micName, 'CBOX', '.cbox')
                values.append(dstCoords)
                with StarFile(batch.join(dstCoords)) as sf:
                    values.append(sf.getTableSize(''))
                tMics.addRowValues(*values)
                tCoords.addRowValues(values[0], dstCoords)

        # Write output STAR files, as outputs needed by extraction
        with StarFile(batch.join('micrographs.star'), 'w') as sf:
            sf.writeTimeStamp()
            sf.writeTable('optics', tOptics)
            sf.writeTable('micrographs', tMics)

        with StarFile(batch.join('coordinates.star'), 'w') as sf:
            sf.writeTimeStamp()
            sf.writeTable('coordinate_files', tCoords)

        extract = RelionExtract(acq, **self.args['extract'])
        if '--extract_size' not in extract.args:
            extract.update_args(self.particle_size)
            self.args['extract']['extra_args'].update(extract.args)

        extract.process_batch(batch)

        batch.info.update({
            'preprocessing_start': start,
            'preprocessing_end': Pretty.now(),
            'preprocessing_elapsed': str(t.getElapsedTime())
        })

        batch.dump_all()

        return batch


def main():
    p = argparse.ArgumentParser()
    p.add_argument('batch_folder',
                   help="Batch folder to run the preprocessing")
    args = p.parse_args()
    os.chdir(args.batch_folder)
    batch = Batch(path=args.batch_folder)
    batch.load_all()
    # Restore path value that might be corrupted after load_all
    batch['path'] = args.batch_folder
    pp = Preprocessing(batch['Preprocessing.args'])
    pp._process_batch(batch, batch['Preprocessing.process_batch.kwargs'])


if __name__ == '__main__':
    # The purpose of this main is to be used from launcher scripts
    # For example, to launch the processing of a given batch to
    # as a job in a queueing system, the only argument should
    # be the batch folder, which should contain the batch.json file
    # and args.json file
    main()

