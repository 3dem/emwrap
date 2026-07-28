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

from emtools.utils import Timer
from emwrap.base import ProcessingPipeline


class ImodTomoReconstruct:
    """Run IMOD aligned-stack generation and tomogram reconstruction for one TS."""

    ALIGNED_PATTERNS = ('{name}_ali.st', '{name}_ali.mrc',
                        '{name}_aligned.st', '{name}_aligned.mrc')
    TOMO_PATTERNS = ('{name}.rec',)

    def __init__(self, **kwargs):
        self.height = int(kwargs['tomogram_height'])
        self.start_step = float(kwargs.get('starting_step', 8))
        self.end_step = float(kwargs.get('ending_step', 14))
        self.launcher_batchtomo = (
            kwargs.get('launcher_batchtomo')
            or ProcessingPipeline.get_launcher('BATCHTOMO')
        )

    @staticmethod
    def _resolve_path(path, working_dir):
        if os.path.isabs(path):
            return path

        for candidate in (os.path.join(working_dir, path), path):
            if os.path.exists(candidate):
                return os.path.abspath(candidate)

        return os.path.abspath(os.path.join(working_dir, path))

    @classmethod
    def _find_file(cls, folder, ts_name, patterns):
        for pattern in patterns:
            candidate = os.path.join(folder, pattern.format(name=ts_name))
            if os.path.exists(candidate):
                return candidate
        return None

    def _write_reconstruct_directive(self, batch):
        directive_file = batch.join('reconstruct.edf')
        with open(directive_file, 'w') as f:
            f.write(f'comparam.tilt.tilt.THICKNESS = {self.height}\n')
            f.write(f'runtime.Reconstruction.any.fallbackThickness = {self.height}\n')
        return directive_file

    def process_batch(self, batch, working_dir):
        ts_name = batch['tsName']
        row = batch['rowDict']
        edf_key = 'rlnEtomoDirectiveFile'

        if edf_key not in row or not row[edf_key]:
            raise Exception(
                f"Missing {edf_key} for tilt series {ts_name}. "
                "Input must come from an IMOD alignment job."
            )

        edf_file = self._resolve_path(row[edf_key], working_dir)
        if not os.path.exists(edf_file):
            raise Exception(f"Etomo directive file not found: {edf_file}")

        imod_dir = os.path.dirname(edf_file)
        batch.create()

        extra_edf = self._write_reconstruct_directive(batch)
        logfile = batch.join('batchruntomo.log')

        args = [
            '-DirectiveFile', edf_file,
            '-CurrentLocation', imod_dir,
            '-RootName', ts_name,
            '-StartingStep', str(self.start_step),
            '-EndingStep', str(self.end_step),
            extra_edf,
        ]

        batch.call(self.launcher_batchtomo, args, logfile=logfile, cwd=False)

        aligned_stack = self._find_file(imod_dir, ts_name, self.ALIGNED_PATTERNS)
        tomogram = self._find_file(imod_dir, ts_name, self.TOMO_PATTERNS)

        if aligned_stack is None:
            raise Exception(
                f"Aligned stack not found for {ts_name} under {imod_dir}"
            )
        if tomogram is None:
            raise Exception(
                f"Reconstructed tomogram not found for {ts_name} under {imod_dir}"
            )

        t = Timer()
        result = {
            'rlnTiltSeriesAligned': aligned_stack,
            'rlnTomoReconstructedTomogram': tomogram,
        }
        batch.info.update({
            'imod_input': len(batch['items']),
            'imod_elapsed': str(t.getElapsedTime()),
            'imod_dir': imod_dir,
            'edf_file': edf_file,
        })
        batch['results'] = [result]
        return batch
