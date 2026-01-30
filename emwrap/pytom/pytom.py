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

from emwrap.base import ProcessingPipeline


class PyTom:
    """ PyTom wrapper to run in a batch folder. """
    def __init__(self, acq, args):
        #self.args = self.argsFromAcq(acq)
        self.acq = acq
        self.args = args
        print(f"pytom args: {str(args)}")

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
        launcher = PyTom.get_launcher()

        # Initialize with the launcher and load parameters from acquisition
        args = {'pytom_match_template.py': ''}
        args.update(self.argsFromAcq(self.acq))
        args.update({
            '--destination': 'output',
            '--tomogram': batch.link(batch['tomogram']),
            '--tilt-angles': _write_list('tilt_angles', 'rawtlt'),
            '--dose-accumulation': _write_list('dose_accumulation', 'txt'),
            '--defocus': batch['defocus']
        })

        for k, v in self.args['pytom'].items():
            # Let's create some relative symbolic links and update arguments
            if k in ['template', 'mask']:
                args[f'--{k}'] = batch.link(v)
            elif k in ['s', 'g']:
                args[f'--{k}'] = v.split()
            elif isinstance(v, bool):
                args[f'--{k}'] = ""  # For booleans just add the argument
            else:
                args[f'--{k}'] = v

        with batch.execute('pytom_match'):
            batch.call(launcher, args)

        def _rename_star(newSuffix):
            """ Rename output star files to avoid overwrite. """
            e = '_particles.star'
            for fn in fm.glob(f'*Apx{e}'):
                newFn = fn.replace(e, f'_{newSuffix}{e}')
                os.rename(fn, newFn)

        if files := fm.glob('*.json'):
            jsonFile = os.path.basename(files[0])

            subargs = self.args['pytom_extract']
            # pytom_extract arguments
            args = {
                'pytom_extract_candidates.py': '',
                '-j': f'output/{jsonFile}',
                '-n': subargs['n'],
                "--particle-diameter": subargs['particle-diameter']
            }

            with batch.execute('pytom_extract'):
                batch.call(launcher, args)
                _rename_star('default')

                if subargs['tophat-filter']:
                    args.update({
                        '--tophat-filter': "",
                        '--tophat-connectivity': subargs['tophat-connectivity']
                    })
                    batch.call(launcher, args)
                    _rename_star('tophat')
        else:
            batch.log("No output json files, not running pytom_extract")

    def argsFromAcq(self, acq):
        """ Define arguments from a given acquisition """
        return Args({
            '--voltage': acq.voltage,
            '--spherical-aberration': acq.cs,
            '--amplitude-contrast': acq.amplitude_contrast,
            '--voxel-size-angstrom': acq.pixel_size
        })

    @staticmethod
    def get_launcher():
        return ProcessingPipeline.get_launcher('PyTOM', 'PYTOM_LAUNCHER')
