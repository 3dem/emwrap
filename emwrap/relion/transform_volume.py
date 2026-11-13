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
Transform a volume with relion_image_handler: rescale (pixel size, binning
or box size), crop/pad the box, flip/invert hand, shift, invert contrast
and filter (low/high-pass, B-factor).

relion_image_handler applies the operations in this fixed order:
    remove NaN -> invert contrast -> B-factor -> low-pass -> high-pass
    -> flip / invert hand -> shift -> rescale -> crop/pad

Two relion_image_handler details are handled here:

  * The flip/invert-hand branches read from the *input* map, so any previous
    operation (contrast inversion, filters, NaN removal) is lost when combined
    with a flip in the same call, and the input header (pixel size) is not
    copied to the output. When a flip is combined with other operations it
    runs in its own first pass.
  * The output header pixel size is only updated when rescaling, and it keeps
    the (possibly wrong) input header value otherwise, even if --angpix is
    given. The expected output pixel size is computed here and always written
    with --force_header_angpix, then checked on the output file.
"""

import os
import math

import mrcfile

from emtools.utils import Color
from emtools.jobs import Batch, Args

from .relion_base import RelionBasePipeline


P = 'relion_image_handler'

# Rescale modes (EnumParam relion_image_handler.rescale_mode)
RESCALE_NONE, RESCALE_ANGPIX, RESCALE_BINNING, RESCALE_BOX = 0, 1, 2, 3

# Flip modes (EnumParam relion_image_handler.flip) -> relion option
FLIP_OPTIONS = {1: '--flipX', 2: '--flipY', 3: '--flipZ', 4: '--invert_hand'}


def _relion_round(x):
    """ RELION's ROUND (half away from zero), not Python's banker's rounding. """
    return int(math.floor(x + 0.5)) if x >= 0 else int(math.ceil(x - 0.5))


def rescaled_size(size, angpix, requested_angpix):
    """ Box size and real pixel size after relion_image_handler --rescale_angpix
    (the new size is rounded to an even number). """
    newsize = _relion_round(size * angpix / requested_angpix)
    newsize -= newsize % 2
    if newsize < 2:
        raise Exception(f"Rescaling a {size} px box from {angpix:.4f} to "
                        f"{requested_angpix:.4f} Å/px gives an invalid box "
                        f"size ({newsize} px).")
    return newsize, size * angpix / newsize


def read_header(volPath):
    """ Return the dimensions (x, y, z) and the header pixel size (x). """
    with mrcfile.open(volPath, permissive=True, header_only=True) as mrc:
        h = mrc.header
        dims = (int(h.nx), int(h.ny), int(h.nz))
        return dims, round(float(mrc.voxel_size.x), 6)


class RelionTransformVolume(RelionBasePipeline):
    """ Wrapper around relion_image_handler to transform a volume. """
    name = 'emw-relion-transform_volume'

    def _get(self, key, default=None):
        value = self._args.get(f'{P}.{key}', default)
        return default if value is None or str(value).strip() == '' else value

    def _float(self, key, default=-1.0):
        return float(self._get(key, default))

    def _int(self, key, default=-1):
        return int(float(self._get(key, default)))

    def _bool(self, key):
        value = self._get(key, False)
        return value is True or str(value).lower() in ('true', '1', 'yes')

    def plan(self, dims, header_angpix):
        """ Compute the relion_image_handler passes and the expected output
        properties from the job arguments and the input volume header.

        Returns a dict with: 'in_angpix', 'out_angpix', 'out_dims',
        'flip' (relion option or None) and 'args' (the Args of the main pass,
        without --i/--o, or None if the flip is the only operation).
        """
        xdim, ydim, zdim = dims
        if zdim < 2:
            raise Exception(f"Input is not a volume (dimensions {xdim} x {ydim} x {zdim}).")

        # Input pixel size: from the form if given, otherwise from the header
        in_angpix = self._float('angpix')
        if in_angpix <= 0:
            in_angpix = header_angpix
            if in_angpix <= 0:
                raise Exception("The input volume header has no pixel size, "
                                "please provide the input pixel size.")
            if abs(in_angpix - 1.0) < 1e-6:
                self.log(Color.warn("WARNING: the input header pixel size is "
                                    "1.0 Å/px, it might not be set. Provide the "
                                    "input pixel size if it is not correct."))

        args = Args()
        out_dims = [xdim, ydim, zdim]
        out_angpix = in_angpix

        # ---- Intensity and filters (applied before flipping by RELION) ----
        if self._bool('remove_nan'):
            args['--remove_nan'] = ''
        if self._bool('invert_contrast'):
            args['--multiply_constant'] = -1
        bfactor = self._float('bfactor', 0)
        if abs(bfactor) > 0:
            args['--bfactor'] = bfactor
        lowpass = self._float('lowpass')
        if lowpass > 0:
            if lowpass < 2 * in_angpix:
                raise Exception(f"Low-pass {lowpass} Å is beyond Nyquist "
                                f"({2 * in_angpix:.3f} Å).")
            args['--lowpass'] = lowpass
        highpass = self._float('highpass')
        if highpass > 0:
            args['--highpass'] = highpass
        if lowpass > 0 or highpass > 0:
            args['--filter_edge_width'] = self._int('filter_edge_width', 2)

        # ---- Geometry ----
        flip = FLIP_OPTIONS.get(self._int('flip', 0))

        if self._bool('shift_com'):
            args['--shift_com'] = ''
        else:
            for axis in 'xyz':
                shift = self._float(f'shift_{axis}', 0)
                if abs(shift) > 0:
                    args[f'--shift_{axis}'] = shift

        # ---- Rescale ----
        mode = self._int('rescale_mode', RESCALE_NONE)
        requested_angpix = None
        if mode == RESCALE_ANGPIX:
            requested_angpix = self._float('rescale_angpix')
            if requested_angpix <= 0:
                raise Exception("Provide a valid target pixel size to rescale.")
        elif mode == RESCALE_BINNING:
            binning = self._float('binning', 1)
            if binning <= 0:
                raise Exception("Provide a valid binning factor.")
            requested_angpix = in_angpix * binning
        elif mode == RESCALE_BOX:
            box = self._int('rescale_box')
            if box < 2 or box % 2:
                raise Exception("The rescaled box size must be an even number of pixels.")
            requested_angpix = xdim * in_angpix / box

        if requested_angpix is not None and abs(requested_angpix - in_angpix) > 1e-6:
            if not (xdim == ydim == zdim):
                raise Exception(f"Rescaling requires a cubic volume "
                                f"(input is {xdim} x {ydim} x {zdim}).")
            newsize, out_angpix = rescaled_size(xdim, in_angpix, requested_angpix)
            if abs(out_angpix - requested_angpix) / requested_angpix > 0.001:
                self.log(Color.warn(
                    f"WARNING: requested pixel size {requested_angpix:.4f} Å/px, "
                    f"the real one will be {out_angpix:.4f} Å/px (box rounded "
                    f"to an even size, {newsize} px)."))
            args['--rescale_angpix'] = requested_angpix
            out_dims = [newsize] * 3

        # ---- Crop / pad (after rescaling) ----
        new_box = self._int('new_box')
        if new_box > 0 and new_box != out_dims[0]:
            args['--new_box'] = new_box
            out_dims = [new_box] * 3

        # ---- Output ----
        force_angpix = self._float('force_angpix')
        if force_angpix > 0:
            out_angpix = force_angpix

        if not args and flip is None and abs(out_angpix - header_angpix) < 1e-6:
            raise Exception("No operation selected.")

        mainArgs = None
        if args or flip is None:
            mainArgs = args
            # Always pass the pixel size used for filters/rescale and the
            # one to write in the output header (see module docs)
            mainArgs['--angpix'] = in_angpix
            mainArgs['--force_header_angpix'] = round(out_angpix, 6)

        return {
            'in_angpix': in_angpix,
            'out_angpix': out_angpix,
            'out_dims': out_dims,
            'flip': flip,
            'args': mainArgs
        }

    def _run_handler(self, batch, label, inFn, outFn, extraArgs):
        args = Args({'relion_image_handler': '', '--i': inFn, '--o': outFn})
        args.update(extraArgs)
        if self._bool('float16'):
            args['--float16'] = ''
        self.batch_execute(label, batch, args, outputs=[outFn])

    def prerun(self):
        inVol = self._get('i')
        if not inVol:
            raise Exception("Input volume is required.")
        inVolAbs = inVol if os.path.isabs(inVol) else os.path.join(self.workingDir, inVol)
        if not os.path.exists(inVolAbs):
            raise Exception(f"Input volume '{inVol}' does not exist.")

        dims, header_angpix = read_header(inVolAbs)
        plan = self.plan(dims, header_angpix)

        batch = Batch(id=self.name, path=self.outputDir)
        outLocal = 'transformed_volume.mrc'
        outVol = batch.join(outLocal)
        # Input link named apart from the outputs, removing the files of a
        # previous run (so they are not taken as generated by this one)
        inLocal = 'input_' + os.path.basename(inVolAbs)
        for fn in [inLocal, outLocal, 'flipped_volume.mrc']:
            if os.path.lexists(batch.join(fn)):
                os.remove(batch.join(fn))
        batch.link(inVolAbs, name=inLocal)

        self.log(f"Input volume: {Color.bold(inVol)} "
                 f"({' x '.join(map(str, dims))} px, header {header_angpix:.4f} Å/px, "
                 f"using {plan['in_angpix']:.4f} Å/px)")

        current = inLocal
        if plan['flip']:
            # Flip on its own pass (relion flips from the input map, see docs)
            flipOut = outLocal if plan['args'] is None else 'flipped_volume.mrc'
            flipArgs = Args({plan['flip']: '',
                             '--angpix': plan['in_angpix'],
                             '--force_header_angpix': round(
                                 plan['out_angpix'] if plan['args'] is None
                                 else plan['in_angpix'], 6)})
            self._run_handler(batch, 'relion_image_handler (flip)',
                              current, flipOut, flipArgs)
            current = flipOut

        if plan['args'] is not None:
            self._run_handler(batch, 'relion_image_handler',
                              current, outLocal, plan['args'])

        # Remove the intermediate flipped volume
        if current not in (inLocal, outLocal) and os.path.exists(batch.join(current)):
            os.remove(batch.join(current))

        self._check_output(outVol, plan)

        outDims, outAngpix = read_header(outVol)
        info = (f"{' x '.join(map(str, outDims))} px, {outAngpix:.4f} Å/px")
        self.log(f"Output volume: {Color.bold(outLocal)} ({info})")
        batch.info['output_volume'] = {
            'path': self.toProjectPath(outVol),
            'dims': list(outDims),
            'angpix': outAngpix,
            'info': info
        }

        self.writeRelionOutputNodes(
            [[self.toProjectPath(outVol), 'TomogramGroupMetadata.star.relion.volume']])
        self.updateBatchInfo(batch)

    def _check_output(self, outVol, plan):
        """ Check that the output has the expected box and pixel size,
        fixing the header pixel size if it was not written correctly. """
        outDims, outAngpix = read_header(outVol)
        if list(outDims) != list(plan['out_dims']):
            raise Exception(f"Output volume dimensions {outDims} differ from "
                            f"the expected ones {plan['out_dims']}.")
        expected = plan['out_angpix']
        if abs(outAngpix - expected) > 1e-3:
            self.log(Color.warn(f"WARNING: output header pixel size is "
                                f"{outAngpix:.4f} Å/px, setting it to {expected:.4f} Å/px."))
            with mrcfile.open(outVol, mode='r+', permissive=True) as mrc:
                mrc.voxel_size = expected


if __name__ == '__main__':
    RelionTransformVolume.main()
