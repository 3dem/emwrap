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


def _crop_pad(data, shape):
    """ The centre of data (z, y, x) cropped or zero padded to shape. """
    out = np.zeros(shape, dtype=data.dtype)
    src, dst = [], []
    for n, m in zip(data.shape, shape):
        k = min(n, m)
        # Aligned at n // 2, also the zero frequency of a shifted FFT
        src.append(slice(n // 2 - k // 2, n // 2 - k // 2 + k))
        dst.append(slice(m // 2 - k // 2, m // 2 - k // 2 + k))
    out[tuple(dst)] = data[tuple(src)]
    return out


def compute_template(binned, in_angpix, out_angpix, box, center=False,
                     invert=False, mirror=False, max_size=PREVIEW_SIZE):
    """ Tentative template (float32 (z, y, x)) of a BinnedMap, as
    pytom_create_template.py: centred by its centre of mass (of the positive
    density), resampled (Fourier crop/pad) to the output pixel size, cropped
    or padded to the output box, inverted and mirrored.

    The template is computed on the output box downscaled by an integer
    factor to at most max_size px; return (template, binning of the box).
    """
    f = max(1, math.ceil(box / max_size))
    previewAngpix = out_angpix * f
    data = binned.data
    ft = np.fft.fftshift(np.fft.fftn(data))
    if center:
        weights = np.clip(data, 0, None)
        total = weights.sum()
        if total > 0:
            grids = np.indices(data.shape, dtype=np.float32)
            shift = [(n - 1) / 2 - (g * weights).sum() / total
                     for g, n in zip(grids, data.shape)]
            freqs = np.meshgrid(*[np.fft.fftshift(np.fft.fftfreq(n))
                                  for n in data.shape], indexing='ij')
            ft *= np.exp(-2j * np.pi * sum(q * s for q, s in zip(freqs, shift)))
    # Fourier crop (or pad) to the preview pixel size
    scale = in_angpix * binned.binning / previewAngpix
    shape = tuple(max(1, int(round(n * scale))) for n in data.shape)
    ft = _crop_pad(ft, shape)
    template = np.fft.ifftn(np.fft.ifftshift(ft)).real * (np.prod(shape) / data.size)
    n = math.ceil(box / f)
    template = _crop_pad(template.astype(np.float32), (n, n, n))
    if invert:
        template = -template
    if mirror:  # PyTom flips its first axis, x
        template = template[:, :, ::-1]
    return np.ascontiguousarray(template), f


def spherical_mask(box, radius, sigma=0, binning=1, cutoff_sd=3):
    """ Spherical mask (float32 0-1) as pytom_create_mask.py, on the box
    (px) downscaled by binning: 1 inside the radius (px) and a Gaussian
    fall-off of sigma (px) outside, cut at cutoff_sd sigmas. """
    n = math.ceil(box / binning)
    coords = np.arange(n) * binning + (binning - 1) / 2 - (box - 1) / 2
    z, y, x = np.meshgrid(coords, coords, coords, indexing='ij')
    dist = np.sqrt(x ** 2 + y ** 2 + z ** 2) - radius
    mask = (dist <= 0).astype(np.float32)
    if sigma and sigma > 0:
        edge = (dist > 0) & (dist <= cutoff_sd * sigma)
        mask[edge] = np.exp(-0.5 * (dist[edge] / sigma) ** 2)
    return mask


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
