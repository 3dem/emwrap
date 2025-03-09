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
import subprocess
import shutil
import sys
import json
import argparse
from pprint import pprint
import tempfile

from emtools.utils import Pretty, Timer, Path, FolderManager, Color, Process
from emtools.jobs import Batch
from emtools.metadata import Acquisition, StarFile, RelionStar

from emwrap.base import ProcessingPipeline
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
        # Launcher can be used in the case that we want to launch
        # processing of a batch to the cluster
        # the launcher should load the proper environment
        # and launch this main in the project folder
        if launcher := self.args.get('launcher', None):
            outputFolder = FolderManager(kwargs['outputFolder'])
            logsPrefix = outputFolder.join('Logs', batch.id)
            batchJson = os.path.abspath(logsPrefix + '.json')
            batch['Preprocessing.args'] = self.args
            batch['Preprocessing.process_batch.kwargs'] = kwargs
            # batch['items'] are expected to be a Python dict, where
            # the keys are the relion labels from the row
            batch.dump_all(batchJson)  # Write batch json to be used in sub-process

            batch.call(launcher, [os.getcwd(), batchJson], cwd=False,
                       logfile=logsPrefix + '.log', verbose=True)
            batch.load_all(batchJson)
            # Reload any args that was set by the subprocesses
            self.args = batch['Preprocessing.args']
        else:
            batch = self._process_batch(batch, kwargs)
            # Also clean the batch folder if not running with a launcher
            if ProcessingPipeline.do_clean():
                shutil.rmtree(batch.path)

        return batch

    def _process_batch(self, batch, kwargs):
        """ Real processing work is done here. This function is called either
        directly from the Pipeline process or through an external 'launcher'
        script. The launcher script is the way to submit this job to a cluster.
        """
        # Folder where intermediate result from the batch will be copied
        # the outputFolder should be a location that is visible by the
        # main process running the pipeline and the worker process running
        # the batch
        outputFolder = FolderManager(kwargs['outputFolder'])

        # Where the temporary batch folder will be created
        # This is a local, fast storage in the worker process
        tmpFolder = '/scr/'  # FIXME
        tmpPrefix = os.path.join(tmpFolder, f'emwap_{batch.id}')

        # The batch will be created in the temporary local storage for
        # intermediate results and only the relevant ones will be copied
        # to the outputFolder
        batch.path = tempfile.mkdtemp(prefix=tmpPrefix)
        batch.log(f"batch.path (from mkdtemp): {batch.path}", flush=True)

        # Let's create symbolic links to the input movies
        for item in batch['items']:
            movFn = item['rlnMicrographMovieName']
            baseName = os.path.basename(movFn)
            os.symlink(os.path.abspath(movFn), batch.join(baseName))

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
        batch.path = old_batch.path  # FIXME bath.path is not copied as dict key

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
            values = r['values']
            micName = os.path.basename(values[0])
            if 'error' not in r:
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
        batchMicStar = batch.join('micrographs.star')
        with StarFile(batchMicStar, 'w') as sf:
            sf.writeTimeStamp()
            sf.writeTable('optics', tOptics)
            sf.writeTable('micrographs', tMics)
        # Copy batch's micrograph star file to the outputFolder
        shutil.copy(batchMicStar, outputFolder.join(f"{batch.id}_micrographs.star"))

        if self.picking:
            batchCoordStar = batch.join('coordinates.star')
            with StarFile(batchCoordStar, 'w') as sf:
                sf.writeTimeStamp()
                sf.writeTable('coordinate_files', tCoords)
            # Copy batch's coordinates star file to the outputFolder
            shutil.copy(batchCoordStar, outputFolder.join(f"{batch.id}_coordinates.star"))

            batch.log("Running Particle Extraction", flush=True)
            extract = RelionExtract(acq, **self.args['extract'])
            if '--extract_size' not in extract.args:
                extract.update_args(self.particle_size)
                self.args['extract']['extra_args'].update(extract.args)

            extract.process_batch(batch)
            # Copy batch's coordinates star file to the outputFolder
            shutil.copy(batch.join('particles.star'),
                        outputFolder.join(f"{batch.id}_particles.star"))

        batch.info.update({
            'preprocessing_start': start,
            'preprocessing_end': Pretty.now(),
            'preprocessing_elapsed': str(t.getElapsedTime())
        })

        self._move(batch, outputFolder)
        batch['Preprocessing.args'] = self.args
        batch.log(f"Batch path is: {batch.path}", flush=True)
        batch.dump_all()


        return batch

    def _move(self, batch, outputFolder):
        """ Move processing results to output folder. """
        try:
            """ Move output files from the batch to the final destination. """
            batch.log("Moving results.")
            t = Timer()
            # Move output files
            for d in ['Micrographs', 'CTFs', 'Coordinates']:
                if batch.exists(d):
                    Process.system(f"mv {batch.join(d, '*')} {outputFolder.join(d)}",
                                   print=batch.log, color=Color.bold)

            if batch.exists('Particles'):
                for root, dirs, files in os.walk(batch.join('Particles')):
                    for name in files:
                        if name.endswith('.mrcs'):
                            shutil.move(os.path.join(root, name),
                                        outputFolder.join('Particles'))

            batch.info.update({
                'move_elapsed': str(t.getElapsedTime())
            })
            return batch
        except Exception as e:
            print(Color.red('ERROR: ' + str(e)))
            import traceback
            traceback.print_exc()
            sys.exit(1)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('project_folder',
                   help="Project folder where to run the preprocessing")
    p.add_argument('batch_json',
                   help="Json file with batch info")
    args = p.parse_args()
    os.chdir(args.project_folder)
    with open(args.batch_json) as f:
        batch = Batch(json.load(f))

    # Restore path value that might be corrupted after load_all
    pp = Preprocessing(batch['Preprocessing.args'])
    pp._process_batch(batch, batch['Preprocessing.process_batch.kwargs'])

    # Copy back the resulting .json file to the input one
    shutil.copy(batch.join('batch.json'), args.batch_json)

    if ProcessingPipeline.do_clean():
        shutil.rmtree(batch.path)


if __name__ == '__main__':
    # The purpose of this main is to be used from launcher scripts
    # For example, to launch the processing of a given batch to
    # as a job in a queueing system, the only argument should
    # be the batch folder, which should contain the batch.json file
    # and args.json file
    main()

