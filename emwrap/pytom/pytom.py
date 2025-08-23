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

from emtools.utils import Color, Timer, Path, FolderManager, Pretty
from emtools.jobs import Args
from emtools.metadata import Table, StarFile, TextFile, Acquisition
from emtools.image import Image


class PyTom:
    """ PyTom wrapper to run in a batch folder. """
    def __init__(self, acq, **kwargs):
        self.acq = Acquisition(acq)
        self.args = self.argsFromAcq(acq)
        self.args.update(kwargs.get('extra_args', {}))

    def process_batch(self, batch, **kwargs):
        def _write_list(key, ext):
            fn = f"{batch['tsName']}_{key}.{ext}"
            with open(batch.join(fn), 'w') as f:
                for v in batch[key]:
                    f.write(f"{v:>0.2f}\n")
            return fn

        batch.create()
        outputDir = batch.mkdir('output')
        fm = FolderManager(outputDir)

        # pytom_match arguments
        args = Args(self.args)
        args.update({
            '--destination': 'output',
            '--voxel-size-angstrom': 9.52,  # FIXME: This should be taken from input
            '--tomogram': batch.link(batch['tomogram']),
            '--tilt-angles': _write_list('tilt_angles', 'rawtlt'),
            '--dose-accumulation': _write_list('dose_accumulation', 'txt'),
            '-g': kwargs['gpu']
        })
        #args.update(self.args)
        # Let's create some relative symbolic links and update arguments
        for a in ['--template', '--mask']:
            args[a] = batch.link(args[a])

        # Let's fix some arguments that need to be a list
        for a in ['-s', '-g']:
            if a in args:
                args[a] = args[a].split()

        with batch.execute('pytom_match'):
            batch.call(PyTom.get_program("PYTOM_MATCH"), args)

        def _rename_star(newSuffix):
            """ Rename output star files to avoid overwrite. """
            e = '_particles.star'
            for fn in fm.glob(f'*Apx{e}'):
                newFn = fn.replace(e, f'_{newSuffix}{e}')
                os.rename(fn, newFn)

        jsonFile = os.path.basename(fm.glob('*.json')[0])

        # pytom_extract arguments
        args = {
            '-j': f'output/{jsonFile}',
            '-n': 2500,
            "--particle-diameter": args['--particle-diameter']
        }

        with batch.execute('pytom_extract'):
            pytom_extract = PyTom.get_program("PYTOM_EXTRACT")
            batch.call(pytom_extract, args)
            _rename_star('default')
            args.update({
                '--tophat-filter': "",
                '--tophat-connectivity': 1
            })
            batch.call(pytom_extract, args)
            _rename_star('tophat')

    @staticmethod
    def get_program(var):
        return os.environ.get(var, None)

    def argsFromAcq(self, acq):
        """ Define arguments from a given acquisition """
        return Args({
            '--voltage': acq.voltage,
            '--spherical-aberration': acq.cs,
            '--amplitude-contrast': acq.amplitude_contrast,
        })
