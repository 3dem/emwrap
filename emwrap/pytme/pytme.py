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
import math

from emtools.utils import FolderManager
from emtools.jobs import Args
from emtools.metadata import StarFile, Table

from emwrap.base import ProcessingPipeline


class PyTME:
    """ pyTME wrapper to pick one tomogram in a batch folder.

    It runs (pyTME >= 0.3.6, the 'pytme' command):
        pytme match: template matching, writes the scores (pickle)
        pytme postprocess: peak calling, once per output format:
            relion5 -> <tsName>_particles_relion5.star (centered Angstroms)
            relion4 -> <tsName>_particles_default.star (voxel coordinates)
            orientations -> <tsName>_orientations.tsv

    Units of pyTME arguments: --defocus and --spherical-aberration are in
    Angstrom, --acceleration-voltage in kV, --min-distance and
    --min-boundary-distance of 'pytme postprocess' in voxels of the tomogram.
    """

    # CTF models: no CTF, a single defocus (mean of the tilts) or per-tilt
    # defocus (from the tilt-series STAR file, mean of defocus U and V)
    CTF_NONE = 'none'
    CTF_TOMOGRAM = 'tomogram'
    CTF_PER_TILT = 'per-tilt'

    # Options computed here (from the acquisition, batch data or other form
    # fields), they cannot be given in extra_args.
    # Aliases are the short/long forms of the same pyTME option.
    _EXTRA_ARGS_ALIASES = {
        '--target': '-m', '--template': '-i', '--template-mask': '-I',
        '--output': '-o', '--angular-sampling': '-a', '--score': '-s',
        '--cores': '-n'
    }

    @classmethod
    def _canonical_arg(cls, key):
        return cls._EXTRA_ARGS_ALIASES.get(key, key)

    def __init__(self, acq, args):
        self.acq = acq
        self.args = args
        print(f"pytme args: {str(args)}")

    def process_batch(self, batch, **kwargs):
        tsName = batch['tsName']
        batch.create()
        outputDir = batch.mkdir('output')
        fm = FolderManager(outputDir)
        launcher = kwargs.get('launcher', '') or ProcessingPipeline.get_launcher('PYTME')
        matchArgs = self.args['pytme']
        ppArgs = self.args['pytme_postprocess']

        # Absolute links: the batch folder may live in a scratch tmp/ (symlink)
        # where relative links would not resolve
        tomogram = batch.link(batch['tomogram'], absolute=True)
        pickleFn = f'output/{tsName}_match.pickle'

        template = batch.link(matchArgs['template'], absolute=True)
        mask = matchArgs.get('template-mask', None)
        mask = batch.link(mask, absolute=True) if mask else None

        args = Args({
            'pytme': 'match',
            '-m': tomogram,
            '-i': template,
            '-o': pickleFn,
            '-s': matchArgs.get('score', 'FLCSphericalMask') or 'FLCSphericalMask',
            # The command runs inside the batch folder
            '--temp-directory': os.path.abspath(batch.path),
        })
        if mask:
            args['-I'] = mask

        if angular := matchArgs.get('angular-sampling', None):
            args['-a'] = angular
        elif diameter := ppArgs.get('particle-diameter', None):
            # pyTME derives the angular sampling from the particle diameter
            # (in units of the sampling rate, Angstrom)
            args['--particle-diameter'] = diameter
        else:
            raise ValueError("Provide the angular sampling or the "
                             "particle diameter.")

        if gpus := matchArgs.get('gpu-indices', ''):
            # With GPUs pyTME uses one process per GPU (-n is ignored)
            args['--backend'] = 'cupy'
            args['--gpu-indices'] = gpus
        else:
            args['--backend'] = 'numpyfftw'
            args['-n'] = matchArgs.get('cores', 4) or 4

        for k in ['lowpass', 'highpass']:
            if v := matchArgs.get(k, None):
                args[f'--{k}'] = v

        for k in ['invert-target-contrast', 'whiten-spectrum']:
            if matchArgs.get(k, False):
                args[f'--{k}'] = ''

        if matchArgs.get('background-correction', False):
            args['--background-correction'] = 'phase-scrambling'

        # Tilt angles (and the accumulated dose in the weights column, used
        # by the 'relion' and 'grigorieff' tilt weighting)
        args['--tilt-angles'] = self._writeTilts(batch)
        weighting = matchArgs.get('tilt-weighting', 'relion') or 'none'
        if weighting != 'none':
            args['--tilt-weighting'] = weighting

        args.update(self.argsFromAcq(self.acq))
        ctfModel = matchArgs.get('ctf', self.CTF_TOMOGRAM) or self.CTF_NONE
        if ctfModel != self.CTF_NONE:
            ctf = self._readCtf(batch)
            if ctfModel == self.CTF_PER_TILT:
                args['--ctf-file'] = self._writeCtf(batch, ctf)
            else:
                defocus = sum((u + v) / 2 for u, v, _ in ctf) / len(ctf)
                args['--defocus'] = round(defocus, 2)
            if not matchArgs.get('flip-phase', True):
                args['--no-flip-phase'] = ''

        if extra_args_str := matchArgs.get('extra_args', ''):
            # Free-form extra arguments for 'pytme match'. Any option
            # already set above (by its own form field or computed from the
            # acquisition/batch data) is rejected, to avoid confusion.
            extra = Args.fromString(extra_args_str)
            canonical_set = {self._canonical_arg(k) for k in args}
            clashes = sorted(
                k for k in extra if self._canonical_arg(k) in canonical_set
            )
            if clashes:
                verb = 'is' if len(clashes) == 1 else 'are'
                raise ValueError(
                    f"pytme.extra_args: {', '.join(clashes)} {verb} already "
                    "set (by its own form field, or automatically from the "
                    "acquisition/batch data). Remove it from extra_args and "
                    "use the dedicated field instead."
                )
            args.update(extra)

        def _call(label, callArgs):
            """ Run a pyTME command and return its exit code, raising if it
            fails (batch.execute stores exceptions in batch.error). """
            batch.info.pop('error', None)
            returncode = None
            with batch.execute(label):
                returncode = batch.call(launcher, callArgs)
            return returncode

        def _check(label, returncode, outputs=None):
            ProcessingPipeline.check_execution(
                label, batch, returncode, outputs=outputs,
                logfile=batch.join('batch.log'))

        _check('pytme_match', _call('pytme_match', args), [pickleFn])

        # Peak calling, once for each output format
        pixelSize = float(self.acq.pixel_size)
        ppBase = Args({
            'pytme': 'postprocess',
            '--input-file': pickleFn,
            '--peak-caller': ppArgs.get('peak-caller', 'PeakCallerMaximumFilter')
                             or 'PeakCallerMaximumFilter',
            '--num-peaks': ppArgs['num-peaks'],
            '--min-distance': self._toVoxels(self._minDistance(ppArgs), pixelSize)
        })
        if v := ppArgs.get('min-boundary-distance', None):
            ppBase['--min-boundary-distance'] = self._toVoxels(v, pixelSize)
        if ppArgs.get('mask-edges', False):
            ppBase['--mask-edges'] = ''
        for k in ['min-score', 'n-false-positives']:
            if (v := ppArgs.get(k, None)) not in (None, ''):
                ppBase[f'--{k}'] = v

        formats = [('relion5', f'{tsName}_particles_relion5', 'star'),
                   ('relion4', f'{tsName}_particles_default', 'star'),
                   ('orientations', f'{tsName}_orientations', 'tsv')]

        for fmt, prefix, ext in formats:
            label = f'pytme_postprocess_{fmt}'
            cmdArgs = Args(ppBase)
            cmdArgs.update({
                '--output-prefix': f'output/{prefix}',
                '--output-format': fmt
            })
            outFn = f'output/{prefix}.{ext}'
            returncode = _call(label, cmdArgs)
            if returncode and not batch.error and self._noPeaks(batch):
                # 'pytme postprocess' exits with an error when no peaks are found,
                # this tomogram is picked with 0 particles
                print(f">>> {label}: no peaks found, writing empty {outFn}")
                self._writeEmpty(batch.join(outFn), fmt)
                continue
            _check(label, returncode, [outFn])

    def _minDistance(self, ppArgs):
        """ Minimum distance between peaks (A), by default the particle
        radius, as PyTom does during extraction. """
        if v := ppArgs.get('min-distance', None):
            return float(v)
        if d := ppArgs.get('particle-diameter', None):
            return float(d) / 2
        raise ValueError("Provide the minimum distance or the particle diameter.")

    @staticmethod
    def _toVoxels(value, pixelSize):
        return max(1, int(math.ceil(float(value) / pixelSize)))

    def _noPeaks(self, batch):
        logFn = batch.join('batch.log')
        if not os.path.exists(logFn):
            return False
        with open(logFn) as f:
            lines = f.readlines()[-50:]
        return any('Found no peaks' in line for line in lines)

    def _writeEmpty(self, fn, fmt):
        """ Write the output of 'pytme postprocess' with no particles. """
        columns = {
            'relion5': ['rlnCenteredCoordinateXAngst', 'rlnCenteredCoordinateYAngst',
                        'rlnCenteredCoordinateZAngst'],
            'relion4': ['rlnCoordinateX', 'rlnCoordinateY', 'rlnCoordinateZ']
        }
        if fmt == 'orientations':
            with open(fn, 'w') as f:
                f.write("\t".join(['x', 'y', 'z', 'euler_x', 'euler_y',
                                   'euler_z', 'score', 'detail']) + '\n')
            return
        cols = columns[fmt] + ['rlnAngleRot', 'rlnAngleTilt', 'rlnAnglePsi',
                               'rlnClassNumber',
                               'rlnTomoName' if fmt == 'relion5' else 'rlnMicrographName',
                               'pytmeScore']
        with StarFile(fn, 'w') as sf:
            sf.writeTable('particles', Table(cols))

    def _tiltSeriesTable(self, batch):
        tsStar = batch['rowDict']['rlnTomoTiltSeriesStarFile']
        return StarFile.getTableFromFile(batch['tsName'], tsStar, guessType=False)

    def _writeTilts(self, batch):
        """ Tab-separated file with tilt angles and accumulated dose
        (pyTME 'angles' and 'weights' columns). """
        fn = f"{batch['tsName']}_tilts.tsv"
        with open(batch.join(fn), 'w') as f:
            f.write("angles\tweights\n")
            for a, d in zip(batch['tilt_angles'], batch['dose_accumulation']):
                f.write(f"{a:0.2f}\t{d:0.2f}\n")
        return fn

    def _readCtf(self, batch):
        """ Per-tilt (defocusU, defocusV, defocusAngle) in A and degrees, in
        the same order as the tilt angles (tilt-series STAR file). """
        def _angstrom(value):
            # RELION stores defocus in A, but older emwrap Warp tilt series
            # STAR files have it in um (no real defocus is below 100 A)
            value = float(value)
            return value * 1e4 if 0 < abs(value) < 100 else value

        ctf = []
        for row in self._tiltSeriesTable(batch):
            if row is None:
                continue
            u = _angstrom(row.rlnDefocusU)
            v = _angstrom(getattr(row, 'rlnDefocusV', row.rlnDefocusU))
            ctf.append((u, v, float(getattr(row, 'rlnDefocusAngle', 0))))
        if not ctf:
            raise Exception("No defocus values found in the tilt-series STAR file.")
        if len(ctf) != len(batch['tilt_angles']):
            raise Exception(f"Expected {len(batch['tilt_angles'])} defocus "
                            f"values, found {len(ctf)}.")
        return ctf

    def _writeCtf(self, batch, ctf):
        """ Per-tilt CTF file for --ctf-file, as a STOPGAP wedge list (the
        STAR layout with the tilt angles read by pyTME). The units are
        the STOPGAP ones: defocus in um, spherical aberration in mm and
        voltage in kV. It has one defocus per tilt, so the astigmatism is
        not used (mean of defocus U and V). """
        acq = self.acq
        fn = f"{batch['tsName']}_ctf.star"
        cols = ['tomo_num', 'pixelsize', 'tilt_angle', 'defocus', 'voltage',
                'cs', 'amp_contrast']
        with open(batch.join(fn), 'w') as f:
            f.write("data_stopgap_wedgelist\n\nloop_\n")
            for i, c in enumerate(cols):
                f.write(f"_{c} #{i + 1}\n")
            for angle, (u, v, _) in zip(batch['tilt_angles'], ctf):
                values = [1, acq.pixel_size, f"{angle:0.2f}",
                          f"{(u + v) / 2e4:0.4f}", acq.voltage, acq.cs,
                          acq.amplitude_contrast]
                f.write(" ".join(str(x) for x in values) + "\n")
        return fn

    def argsFromAcq(self, acq):
        """ Define arguments from a given acquisition (pyTME units). """
        return Args({
            '--acceleration-voltage': acq.voltage,  # kV
            '--spherical-aberration': float(acq.cs) * 1e7,  # mm -> A
            '--amplitude-contrast': acq.amplitude_contrast
        })
