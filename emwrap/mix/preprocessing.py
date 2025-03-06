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
from emtools.metadata import Acquisition, StarFile, RelionStar

from emwrap.motioncor import Motioncor
from emwrap.ctffind import Ctffind
from emwrap.cryolo import CryoloPredict
from emwrap.relion.extract import RelionExtract


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

    @property
    def picking(self):
        return 'picking' in self.args

    def process_batch(self, batch, **kwargs):
        batch['Preprocessing.args'] = self.args
        batch['Preprocessing.process_batch.kwargs'] = kwargs
        # batch['items'] are expected to be a Python dict, where
        # the keys are the relion labels from the row
        batch.dump_all()  # Write batch json to be used in sub-process

        # Launcher can be used in the case that we want to launch
        # processing of a batch to the cluster
        # the launcher should load the proper environment
        # and launch this main in the batch folder
        if launcher := self.args.get('launcher', None):
            batch.call(launcher, [os.getcwd(), batch.path],
                       logfile=None, #batch.join('pp.log'),
                       verbose=True)
            batch.load_all()
            # Reload any args that was set by the subprocesses
            self.args = batch['Preprocessing.args']
        else:
            batch = self._process_batch(batch, kwargs)

        return batch

    def _process_batch(self, batch, kwargs):
        t = Timer()
        start = Pretty.now()
        v = kwargs.get('verbose', False)
        gpu = kwargs['gpu']
        cpu = kwargs.get('cpu', 4)

        # Motion correction
        batch.log("Running Motioncor", flush=True)
        mc = Motioncor(self.acq, **self.args['motioncor'])
        mc.process_batch(batch, gpu=gpu)

        old_batch = batch
        batch = Batch(old_batch)

        def _item(r):
            return None if 'error' in r else r['rlnMicrographName']

        # Change input items as expected by CTF job
        batch['items'] = [_item(r) for r in old_batch['results']]

        # Calculate new pixel size based on the motioncor binning option
        acq = Acquisition(self.acq)
        origPs = self.acq.pixel_size
        acq.pixel_size = origPs * mc.args['-FtBin']

        batch.log("Running Ctffind", flush=True)
        ctf = Ctffind(acq, **self.args['ctf'])
        ctf.process_batch(batch, verbose=True)
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

        extra_cols = []
        if self.picking:
            batch.mkdir('Coordinates')

            pickingArgs = dict(self.args['picking'])
            if self.particle_size is not None:
                a = round(self.particle_size / acq.pixel_size)
                pickingArgs['anchors'] = [a, a]

            batch.log(f"Running Cryolo, args: {pickingArgs}", flush=True)
            cryolo = CryoloPredict(**pickingArgs)
            cryolo.process_batch(batch, gpu=gpu, cpu=cpu)
            if self.particle_size is None:
                size = cryolo.get_size(batch, 75)

                self.particle_size = round(size * acq.pixel_size)
                print(f">>> Size for percentile 75: {size}, particle_size (A): {self.particle_size}")

            tCoords = RelionStar.coordinates_table()
            extra_cols = ['rlnMicrographCoordinates', 'rlnCoordinatesNumber']
        tOptics = RelionStar.optics_table(acq, originalPixelSize=origPs)
        tMics = RelionStar.micrograph_table(extra_cols=extra_cols)

        def _move_cryolo(micName, folder, ext):
            """ Move result box files from cryolo. """
            srcCoords = batch.join('cryolo_boxfiles', folder, Path.replaceExt(micName, ext))
            dstCoords = os.path.join('Coordinates', Path.replaceExt(micName, f'_coords{ext}'))
            shutil.move(srcCoords, batch.join(dstCoords))
            return dstCoords

        for i, row in enumerate(batch['items']):
            r = batch['results'][i]
            if 'error' not in r:
                values = r['values']
                micName = os.path.basename(values[0])
                micPath = os.path.join('Micrographs', micName)
                kvalues = {
                    'rlnMicrographName': micPath,
                    'rlnOpticsGroup': row['rlnOpticsGroup'],
                    'rlnCtfImage': os.path.join('CTFs', os.path.basename(values[2])),
                    'rlnDefocusU': values[3],
                    'rlnDefocusV': values[4],
                    'rlnCtfAstigmatism': values[5],
                    'rlnDefocusAngle': values[6],
                    'rlnCtfFigureOfMerit': values[7],
                    'rlnCtfMaxResolution': values[8]
                }
                if self.picking:
                    dstCoords = _move_cryolo(micName, 'STAR', '.star')
                    _move_cryolo(micName, 'CBOX', '.cbox')
                    with StarFile(batch.join(dstCoords)) as sf:
                        kvalues.update({
                            'rlnMicrographCoordinates': dstCoords,
                            'rlnCoordinatesNumber': sf.getTableSize('')
                        })
                tMics.addRowValues(**kvalues)
                if self.picking:
                    tCoords.addRowValues(micPath, dstCoords)

            else:
                batch.log(f"ERROR: For micrograph {micName}, {r['error']}")
            # Update results for each item
            batch['results'][i] = kvalues

        # Write output STAR files, as outputs needed by extraction
        with StarFile(batch.join('micrographs.star'), 'w') as sf:
            sf.writeTimeStamp()
            sf.writeTable('optics', tOptics)
            sf.writeTable('micrographs', tMics)

        if self.picking:
            with StarFile(batch.join('coordinates.star'), 'w') as sf:
                sf.writeTimeStamp()
                sf.writeTable('coordinate_files', tCoords)

            batch.log("Running Particle Extraction", flush=True)
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

        batch['Preprocessing.args'] = self.args
        batch.dump_all()

        return batch


def main():
    p = argparse.ArgumentParser()
    p.add_argument('project_folder',
                   help="Project folder where to run the preprocessing")
    p.add_argument('batch_folder',
                   help="Batch folder relative to project folder")
    args = p.parse_args()
    os.chdir(args.project_folder)
    with open(os.path.join(args.batch_folder, 'batch.json')) as f:
        batch = Batch(json.load(f))

    # Restore path value that might be corrupted after load_all
    pp = Preprocessing(batch['Preprocessing.args'])
    pp._process_batch(batch, batch['Preprocessing.process_batch.kwargs'])


if __name__ == '__main__':
    # The purpose of this main is to be used from launcher scripts
    # For example, to launch the processing of a given batch to
    # as a job in a queueing system, the only argument should
    # be the batch folder, which should contain the batch.json file
    # and args.json file
    main()

