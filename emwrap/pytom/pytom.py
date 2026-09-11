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

    # Aliases for the '--g'/'--s' keys this class builds internally
    # (from the 'g'/'s' pytom_match_template.py options) to their real,
    # documented long-form flag names ('--gpu-ids'/'--volume-split').
    # Used only to detect collisions with 'extra_args', so that e.g.
    # '--gpu-ids ...' typed in extra_args is still recognized as already
    # set via the 'GPUs' form field. NOTE: pytom_match_template.py's own
    # true single-letter short flags (-t, -v, -d, -m, -a, -r, -g, -s) are
    # not usable from extra_args at all, since emtools' Args.fromString
    # only recognizes options with 2+ characters after the dash(es).
    _EXTRA_ARGS_ALIASES = {
        '--g': '--gpu-ids',
        '--s': '--volume-split',
    }

    @classmethod
    def _canonical_arg(cls, key):
        return cls._EXTRA_ARGS_ALIASES.get(key, key)

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
        launcher = kwargs.get('launcher', '') or ProcessingPipeline.get_launcher('PYTOM')

        # Initialize with the launcher and load parameters from acquisition
        args = {'pytom_match_template.py': ''}
        args.update(self.argsFromAcq(self.acq))
        extraArgs = {
            '--destination': 'output',
            '--tomogram': batch.link(batch['tomogram']),
            '--tilt-angles': _write_list('tilt_angles', 'rawtlt'),
            '--dose-accumulation': _write_list('dose_accumulation', 'txt')
        }
        if defocus := batch.get('defocus', None):
            extraArgs['--defocus'] = defocus

        args.update(extraArgs)

        extra_args_str = self.args['pytom'].get('extra_args', '')

        for k, v in self.args['pytom'].items():
            if k == 'extra_args':
                continue  # handled separately below, once all other args are set
            # Let's create some relative symbolic links and update arguments
            if k in ['template', 'mask']:
                args[f'--{k}'] = batch.link(v)
            elif k in ['s', 'g']:
                args[f'--{k}'] = v.split()
            elif isinstance(v, bool):
                args[f'--{k}'] = ""  # For booleans just add the argument
            else:
                args[f'--{k}'] = v

        if extra_args_str:
            # Free-form extra arguments for pytom_match_template.py
            # (e.g. '--rng-seed 42'). Any option already set above --
            # either directly through its own form field, or
            # automatically from the acquisition/batch data -- is
            # rejected: the user already has a dedicated way to set it,
            # so silently overriding it here would be confusing.
            extra = Args.fromString(extra_args_str)
            canonical_set = {self._canonical_arg(k) for k in args}
            clashes = sorted(
                k for k in extra if self._canonical_arg(k) in canonical_set
            )
            if clashes:
                verb = 'is' if len(clashes) == 1 else 'are'
                raise ValueError(
                    f"pytom.extra_args: {', '.join(clashes)} {verb} already "
                    "set (by its own form field, or automatically from the "
                    "acquisition/batch data). Remove it from extra_args and "
                    "use the dedicated field instead."
                )
            args.update(extra)

        with batch.execute('pytom_match'):
            batch.call(launcher, args)

        def _rename_star(newSuffix):
            """ Rename output star files to avoid overwrite. """
            e = '_particles.star'
            if files := fm.glob(f'*{e}'):
                for fn in files:
                    newFn = fn.replace(e, f'_particles_{newSuffix}.star')
                    print(f'>>> Renaming {fn} to {newFn}')
                    os.rename(fn, newFn)
            else:
                print(f'>>> ERROR: Not files found for prefix "{newSuffix}')

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
                    argsT = dict(args)
                    argsT.update({
                        '--tophat-filter': "",
                        '--tophat-connectivity': subargs['tophat-connectivity']
                    })
                    batch.call(launcher, argsT)
                    _rename_star('tophat')

                # Run with relion5 compatibility mode
                args.update({
                    '--relion5-comp': ""
                })
                batch.call(launcher, args)
                _rename_star('relion5')
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
