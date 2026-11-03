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

""" A tentative 3D mask computed with numpy only, as relion_mask_create does
it: lowpass, threshold, extend (or shrink) and a raised cosine soft edge,
optionally inverted.

It is a guide to choose the parameters before running relion_mask_create,
not a replacement: the map is downscaled (block mean) to about 100 px and
the distances are approximated by dilations with balls (FFT convolutions),
so the mask is coarser and its edges are not exact.
"""

import base64
import math
import os
import zlib
from collections import OrderedDict

import numpy as np

PREVIEW_SIZE = 100  # Longest side of the downscaled map (px)
_CACHE_SIZE = 4
_cache = OrderedDict()


class BinnedMap:
    """ A map downscaled by an integer factor (block mean). """
    def __init__(self, data, binning, angpix, dimensions):
        self.data = data  # float32 (z, y, x)
        self.binning = binning  # full-size voxels per voxel
        self.angpix = angpix  # A/px of the full-size map (None if unknown)
        self.dimensions = dimensions  # [X, Y, Z] of the full-size map

    @property
    def binnedAngpix(self):
        return self.angpix * self.binning if self.angpix else None


def load_binned_map(path, max_size=PREVIEW_SIZE):
    """ Read an MRC map downscaled to at most max_size px per side with an
    integer block mean, slab by slab (the map is not loaded in memory).
    The last maps are cached (by path, modification time and size). """
    import mrcfile

    key = (os.path.abspath(path), os.path.getmtime(path), max_size)
    if key in _cache:
        _cache.move_to_end(key)
        return _cache[key]

    with mrcfile.mmap(path, mode='r', permissive=True) as mrc:
        vol = mrc.data
        if vol.ndim != 3:
            raise ValueError(f"{path} is not a volume (shape {vol.shape}).")
        nz, ny, nx = vol.shape
        angpix = float(mrc.voxel_size.x) or None
        f = max(1, math.ceil(max(nz, ny, nx) / max_size))
        bz, by, bx = (math.ceil(n / f) for n in (nz, ny, nx))
        out = np.zeros((bz, by, bx), dtype=np.float32)
        counts = np.zeros((by, bx), dtype=np.float32)
        # Voxels per block in y, x (the last blocks may be partial)
        cy = np.bincount(np.arange(ny) // f, minlength=by).astype(np.float32)
        cx = np.bincount(np.arange(nx) // f, minlength=bx).astype(np.float32)
        counts[:] = np.outer(cy, cx)
        for z in range(bz):
            slab = np.asarray(vol[z * f:(z + 1) * f], dtype=np.float32).sum(axis=0)
            slab = np.add.reduceat(slab, np.arange(0, ny, f), axis=0)
            slab = np.add.reduceat(slab, np.arange(0, nx, f), axis=1)
            out[z] = slab / (counts * min(f, nz - z * f))

    binned = BinnedMap(out, f, angpix, [nx, ny, nz])
    _cache[key] = binned
    while len(_cache) > _CACHE_SIZE:
        _cache.popitem(last=False)
    return binned


def _lowpass(data, angpix, resolution):
    """ Lowpass filter to resolution (A) with a raised cosine edge of two
    Fourier shells, as Relion does. """
    cutoff = angpix / resolution  # cycles per px
    if cutoff >= 0.5:
        return data
    shape = data.shape
    freqs = np.meshgrid(*[np.fft.fftfreq(n) for n in shape[:-1]],
                        np.fft.rfftfreq(shape[-1]), indexing='ij')
    r = np.sqrt(sum(q ** 2 for q in freqs))
    edge = 2.0 / max(shape)
    filt = np.clip((cutoff + edge - r) / edge, 0, 1)
    filt = 0.5 - 0.5 * np.cos(np.pi * filt)
    return np.fft.irfftn(np.fft.rfftn(data) * filt, s=shape, axes=(0, 1, 2)).astype(np.float32)


def _ball_kernel_ft(shape, radius):
    """ FFT of a ball of the radius (px) centred at the origin. """
    grids = np.meshgrid(*[np.fft.fftfreq(n) * n for n in shape], indexing='ij')
    ball = (sum(g ** 2 for g in grids) <= radius ** 2).astype(np.float32)
    return np.fft.rfftn(ball)


def _dilate(binaryFt, shape, radius):
    """ Binary dilation by a ball, from the FFT of the binary map. """
    conv = np.fft.irfftn(binaryFt * _ball_kernel_ft(shape, radius), s=shape, axes=(0, 1, 2))
    return conv > 0.5


def compute_mask(binned, threshold, lowpass=None, extend=0, soft_edge=0,
                 invert=False, angpix=None):
    """ Tentative mask (float32 0-1, on the grid of binned) of a BinnedMap.

    threshold: initial binarization threshold (map units), after the lowpass
    lowpass: resolution (A) of the lowpass applied first (None or <= 0: none)
    extend: pixels (of the full-size map) to extend the binary mask by
        (negative: to shrink it)
    soft_edge: width (full-size px) of the raised cosine soft edge
    invert: invert the final mask
    angpix: A/px of the full-size map, overrides the one of the header
    """
    f = binned.binning
    data = binned.data
    pixelSize = angpix if angpix and angpix > 0 else binned.angpix
    if lowpass and lowpass > 0 and pixelSize:
        data = _lowpass(data, pixelSize * f, lowpass)

    # Pad, so the FFT convolutions do not wrap around the box
    ext = (extend or 0) / f
    edge = (soft_edge or 0) / f
    pad = int(math.ceil(abs(ext) + edge)) + 1
    binary = np.pad(data >= threshold, pad)
    shape = binary.shape

    if ext > 0:
        binary = _dilate(np.fft.rfftn(binary.astype(np.float32)), shape, ext)
    elif ext < 0:
        outside = np.fft.rfftn((~binary).astype(np.float32))
        binary = ~_dilate(outside, shape, -ext)

    mask = binary.astype(np.float32)
    if edge > 0:
        # Raised cosine over the distance to the binary mask, the distance
        # taken from dilations by balls of growing radius (half px steps)
        binaryFt = np.fft.rfftn(mask)
        dist = np.full(shape, np.inf, dtype=np.float32)
        dist[binary] = 0
        for r in np.arange(0.5, edge + 0.5, 0.5):
            inside = _dilate(binaryFt, shape, r) & np.isinf(dist)
            dist[inside] = r
        soft = dist < edge
        mask[soft] = 0.5 + 0.5 * np.cos(np.pi * dist[soft] / edge)

    mask = mask[pad:-pad, pad:-pad, pad:-pad]
    return 1 - mask if invert else mask


def density_from_array(array, binning, valueRange=(0.0, 1.0)):
    """ The array (z, y, x) as emflow's VolumeDensity: uint8 over valueRange,
    zlib compressed, base64. """
    v0, v1 = valueRange
    u8 = np.clip(np.round((array - v0) * 255.0 / ((v1 - v0) or 1)), 0, 255).astype(np.uint8)
    return {
        'shape': list(array.shape),
        'binning': binning,
        'data': base64.b64encode(zlib.compress(u8.tobytes(), 6)).decode(),
        'encoding': 'zlib',
        'valueRange': [float(v0), float(v1)]
    }
