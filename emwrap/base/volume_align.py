# **************************************************************************
# *
# * Authors:     Daniel Marchan (danielmarchan3@gmail.com)
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

""" Rigid alignment of a volume to a reference map, with numpy only.

Used to put the references of a 3D classification in the frame of the
particle poses: a classification that does not align the particles (or that
only searches locally around their poses) needs references whose orientation
and centre match that frame, usually the map of the refinement that gave the
poses. De novo initial models come out in an arbitrary orientation.

The search maximises the normalised cross-correlation (CC) of the low-pass
filtered maps inside a soft spherical mask:

  - Rotations: a uniform random set of the whole SO(3) (about the requested
    angular step apart, the identity included), then the best distinct
    candidates refined locally on finer and finer grids.
  - Shifts: for each rotation, the best shift (up to a maximum) from the FFT
    cross-correlation, refined below the pixel with a parabola.

The search runs on a grid binned to the low-pass resolution (a 32-64 px box
is enough at 20-30 Å), the result is applied to the full-size volume with a
trilinear interpolation. Conventions: arrays are (z, y, x), the centre of a
box of n px is n // 2 (as Relion), rotations act on (x, y, z) positions:
aligned(r) = volume(R^-1 (r - shift)) around the centre.

similarity_matrix gives the CC of every pair of volumes of the same grid,
low-passed and masked as the alignment compares them.
"""

import math
import multiprocessing

import numpy as np

from .volume_mask import _lowpass


# ---------------------------- Rotations ------------------------------------

def random_rotations(n, seed=0):
    """ n uniform random rotation matrices (n, 3, 3), Shoemake's method. """
    rng = np.random.default_rng(seed)
    u1, u2, u3 = rng.random((3, n))
    a, b = np.sqrt(1 - u1), np.sqrt(u1)
    q = np.stack([a * np.sin(2 * np.pi * u2), a * np.cos(2 * np.pi * u2),
                  b * np.sin(2 * np.pi * u3), b * np.cos(2 * np.pi * u3)], axis=1)
    return quaternions_to_matrices(q)


def quaternions_to_matrices(q):
    """ Rotation matrices (n, 3, 3) of unit quaternions (n, 4) as (x, y, z, w). """
    x, y, z, w = q.T
    return np.stack([
        np.stack([1 - 2 * (y * y + z * z), 2 * (x * y - z * w), 2 * (x * z + y * w)], -1),
        np.stack([2 * (x * y + z * w), 1 - 2 * (x * x + z * z), 2 * (y * z - x * w)], -1),
        np.stack([2 * (x * z - y * w), 2 * (y * z + x * w), 1 - 2 * (x * x + y * y)], -1),
    ], axis=1)


def rotvec_to_matrices(v):
    """ Rotation matrices (n, 3, 3) of rotation vectors (n, 3) (Rodrigues). """
    v = np.atleast_2d(v)
    theta = np.linalg.norm(v, axis=1)
    k = np.divide(v, theta[:, None], out=np.zeros_like(v), where=theta[:, None] > 0)
    K = np.zeros((len(v), 3, 3))
    K[:, 0, 1], K[:, 0, 2], K[:, 1, 2] = -k[:, 2], k[:, 1], -k[:, 0]
    K[:, 1, 0], K[:, 2, 0], K[:, 2, 1] = k[:, 2], -k[:, 1], k[:, 0]
    s, c = np.sin(theta)[:, None, None], np.cos(theta)[:, None, None]
    return np.eye(3)[None] + s * K + (1 - c) * (K @ K)


def rotation_angle(R):
    """ Angle (degrees) of a rotation matrix. """
    return math.degrees(math.acos(max(-1.0, min(1.0, (np.trace(R) - 1) / 2))))


def rotation_axis(R):
    """ Unit axis (x, y, z) of a rotation matrix ((0, 0, 1) for the identity). """
    axis = np.array([R[2, 1] - R[1, 2], R[0, 2] - R[2, 0], R[1, 0] - R[0, 1]])
    norm = np.linalg.norm(axis)
    if norm < 1e-8:
        if rotation_angle(R) < 1:
            return np.array([0.0, 0.0, 1.0])
        # 180 degrees: the eigenvector of the eigenvalue 1
        w, v = np.linalg.eigh((R + R.T) / 2)
        return v[:, np.argmax(w)]
    return axis / norm


def angular_distance(Ra, Rb):
    return rotation_angle(Ra @ Rb.T)


def rotations_for_step(step):
    """ Number of uniform random rotations about step degrees apart. """
    return int(math.ceil(8 * math.pi ** 2 / math.radians(step) ** 3))


def local_rotations(R0, step, nstep=2):
    """ The rotations of a cubic grid of rotation vectors (step degrees,
    nstep steps each side) around R0. """
    a = math.radians(step) * np.arange(-nstep, nstep + 1)
    v = np.array(np.meshgrid(a, a, a, indexing='ij')).reshape(3, -1).T
    return rotvec_to_matrices(v) @ R0


# ---------------------------- Resampling -----------------------------------

def _trilinear(vol, coords):
    """ Values of vol (z, y, x) at the points coords (3, N) in (z, y, x)
    index units, 0 outside the box. """
    # A border of zeros: the points outside sample it, no bounds per corner
    padded = np.pad(np.asarray(vol, dtype=np.float32), 1).ravel()
    dims = np.array(vol.shape) + 2
    c = coords + 1
    c0 = np.floor(c)
    f = (c - c0).astype(np.float32)
    outside = np.any((c0 < 0) | (c0 > dims[:, None] - 2), axis=0)
    c0 = np.clip(c0, 0, dims[:, None] - 2).astype(np.int64)
    sz, sy = dims[1] * dims[2], dims[2]
    base = c0[0] * sz + c0[1] * sy + c0[2]
    fz, fy, fx = f
    gz, gy, gx = 1 - fz, 1 - fy, 1 - fx
    out = (gz * (gy * (gx * padded[base] + fx * padded[base + 1])
                 + fy * (gx * padded[base + sy] + fx * padded[base + sy + 1]))
           + fz * (gy * (gx * padded[base + sz] + fx * padded[base + sz + 1])
                   + fy * (gx * padded[base + sz + sy] + fx * padded[base + sz + sy + 1])))
    out[outside] = 0
    return out


def _centred_grid(n):
    """ Positions (x, y, z) of the voxels of an n^3 box, from its centre (3, n^3). """
    r = np.arange(n) - n // 2
    z, y, x = np.meshgrid(r, r, r, indexing='ij')
    return np.stack([x.ravel(), y.ravel(), z.ravel()]).astype(np.float64)


def transform_volume(vol, R=None, shift=None, out_size=None, scale=1.0,
                     grid=None):
    """ The volume rotated by R about its centre, shifted by shift (x, y, z,
    output px) and sampled on an out_size^3 box whose pixel is scale times
    the one of vol (trilinear interpolation, 0 outside). """
    n_out = out_size or vol.shape[0]
    X = _centred_grid(n_out) if grid is None else grid
    if shift is not None:
        X = X - np.asarray(shift, dtype=np.float64)[:, None]
    if R is not None:
        X = R.T @ X
    centre = np.array([s // 2 for s in vol.shape[::-1]], dtype=np.float64)[:, None]
    P = X * scale + centre  # (x, y, z) index units of vol
    return _trilinear(vol, P[::-1]).reshape(n_out, n_out, n_out)


def fourier_resize(vol, n):
    """ The volume (cubic) cropped or padded in Fourier space to n^3
    (binning or up-sampling with the same field of view). """
    m = vol.shape[0]
    if n == m:
        return vol.astype(np.float32)
    F = np.fft.fftshift(np.fft.fftn(vol))
    out = np.zeros((n, n, n), dtype=complex)
    k = min(n, m)
    a, b = m // 2 - k // 2, n // 2 - k // 2
    out[b:b + k, b:b + k, b:b + k] = F[a:a + k, a:a + k, a:a + k]
    return (np.real(np.fft.ifftn(np.fft.ifftshift(out))) * (n / m) ** 3).astype(np.float32)


def match_grid(vol, angpix, n, target_angpix):
    """ The volume (angpix Å/px) on an n^3 box of target_angpix Å/px, with
    the same centre: low-pass filtered first if the pixel gets bigger. """
    m = vol.shape[0]
    if m == n and abs(angpix - target_angpix) < 1e-3 * target_angpix:
        return vol.astype(np.float32)
    if target_angpix > angpix:
        vol = _lowpass(vol.astype(np.float32), angpix, 2 * target_angpix)
    return transform_volume(vol, out_size=n, scale=target_angpix / angpix)


def soft_sphere(n, radius, edge=2.0):
    """ Soft spherical mask (raised cosine edge of edge px) of an n^3 box. """
    r = np.sqrt((_centred_grid(n) ** 2).sum(0)).reshape(n, n, n)
    m = np.clip((radius + edge - r) / edge, 0, 1)
    return (0.5 - 0.5 * np.cos(np.pi * m)).astype(np.float32)


# ---------------------------- Search ---------------------------------------

class SearchGrid:
    """ The binned grid of the search: target map, mask and limits. """
    MIN_SIZE = 24

    def __init__(self, target, angpix, lowpass, diameter=None, max_shift=None):
        n = target.shape[0]
        self.n = n
        self.angpix = angpix
        self.lowpass = max(lowpass or 0, 2 * angpix)
        # Three pixels per resolution element are enough
        ns = int(round(n * angpix * 3 / self.lowpass))
        ns = max(self.MIN_SIZE, ns + ns % 2)
        self.size = min(n, ns)
        self.search_angpix = angpix * n / self.size
        diameter = diameter if diameter and diameter > 0 else 0.9 * n * angpix
        self.diameter = min(diameter, n * angpix)
        self.radius = self.diameter / 2 / self.search_angpix
        self.mask = soft_sphere(self.size, self.radius - 2)
        self.inside = self.mask > 0.5
        max_shift = (max_shift if max_shift is not None and max_shift >= 0
                     else self.diameter / 4)
        self.max_shift = max_shift
        self.max_shift_px = max_shift / self.search_angpix
        s = np.fft.fftfreq(self.size, 1 / self.size)
        sz, sy, sx = np.meshgrid(s, s, s, indexing='ij')
        self.shift_ok = (sz ** 2 + sy ** 2 + sx ** 2) <= self.max_shift_px ** 2 + 1e-6
        self.grid = _centred_grid(self.size)
        t = self.prepare(target)
        self.target_ft = np.fft.fftn(t)
        self.target_norm = np.linalg.norm(t)

    def prepare(self, vol):
        """ A volume of the target grid binned, low-pass filtered, masked and
        with zero mean inside the mask. """
        v = _lowpass(fourier_resize(vol, self.size), self.search_angpix, self.lowpass)
        return self._masked(v)

    def _masked(self, v):
        v = v * self.mask
        return v - v[self.inside].mean() * self.mask

    def score(self, prepared_moving, R):
        """ (CC, shift (x, y, z) in search px) of the moving volume (prepared,
        but not masked: use prepare_moving) rotated by R. """
        m = transform_volume(prepared_moving, R=R, grid=self.grid)
        m = self._masked(m)
        cc = np.real(np.fft.ifftn(self.target_ft * np.conj(np.fft.fftn(m))))
        cc /= (self.target_norm * np.linalg.norm(m) + 1e-12)
        cc = np.where(self.shift_ok, cc, -np.inf)
        i = np.unravel_index(np.argmax(cc), cc.shape)
        shift = np.array([_subpixel(cc, i, axis) for axis in (2, 1, 0)])
        return float(cc[i]), shift

    def prepare_moving(self, vol):
        """ Binned and low-pass filtered (rotated by score, then masked). """
        return _lowpass(fourier_resize(vol, self.size), self.search_angpix, self.lowpass)

    def cc_as_is(self, vol):
        """ CC of the volume (target grid) without moving it. """
        m = self.prepare(vol)
        return float((m * np.real(np.fft.ifftn(self.target_ft))).sum()
                     / (self.target_norm * np.linalg.norm(m) + 1e-12))


def _subpixel(cc, i, axis):
    """ Shift along axis (index of cc) of the peak at i, refined with a
    parabola through its neighbours (wrapped FFT indexes, signed). """
    n = cc.shape[axis]
    j = list(i)
    values = []
    for d in (-1, 0, 1):
        j[axis] = (i[axis] + d) % n
        values.append(cc[tuple(j)])
    c = i[axis] if i[axis] <= n // 2 else i[axis] - n
    a, b, e = values
    denom = a - 2 * b + e
    if not np.all(np.isfinite(values)) or denom >= 0:
        return float(c)
    return float(c + 0.5 * (a - e) / denom)


# Moves of the hill climbing at each step size of the local refinement
MAX_CLIMB = 6

_WORKER = {}


def _init_worker(grid, moving):
    _WORKER['grid'], _WORKER['moving'] = grid, moving


def _score_chunk(Rs):
    grid, moving = _WORKER['grid'], _WORKER['moving']
    return [grid.score(moving, R) for R in Rs]


class _Scorer:
    """ Scores of many rotations, in parallel processes if threads > 1. """
    def __init__(self, grid, moving, threads=1):
        self.grid, self.moving, self.threads = grid, moving, max(1, threads)
        self.pool = None
        if self.threads > 1:
            ctx = multiprocessing.get_context('fork')
            self.pool = ctx.Pool(self.threads, _init_worker, (grid, moving))

    def __call__(self, Rs):
        if self.pool is None:
            return [self.grid.score(self.moving, R) for R in Rs]
        chunks = np.array_split(Rs, min(len(Rs), self.threads * 8))
        return [r for part in self.pool.map(_score_chunk, chunks) for r in part]

    def close(self):
        if self.pool is not None:
            self.pool.close()
            self.pool.join()


def similarity_matrix(grid, volumes):
    """ Matrix (n x n) of the normalised CC of every pair of volumes (on the
    target grid of `grid`), low-passed and masked as in the alignment (see
    SearchGrid.cc_as_is): how similar the references of the classes are. """
    prepared = [grid.prepare(v).ravel() for v in volumes]
    prepared = [p / (np.linalg.norm(p) + 1e-12) for p in prepared]
    m = np.array([[float(a @ b) for b in prepared] for a in prepared])
    np.fill_diagonal(m, 1.0)
    return m


class AlignmentResult:
    def __init__(self, R, shift, cc, cc_before, alternatives, angpix):
        self.R = R  # rotation matrix applied to the volume
        self.shift = shift  # (x, y, z) in px of the target grid
        self.cc = cc
        self.cc_before = cc_before
        # [(cc, angular distance to the best (degrees))] of the other refined
        # candidates, best first: close CCs far away mean an ambiguous fit
        self.alternatives = alternatives
        self.angpix = angpix

    @property
    def angle(self):
        return rotation_angle(self.R)

    @property
    def axis(self):
        return rotation_axis(self.R)

    @property
    def shift_angst(self):
        return self.shift * self.angpix


def align_volume(target, vol, angpix, lowpass=25.0, diameter=None,
                 max_shift=None, step=10.0, candidates=5, threads=1,
                 seed=0, log=None, grid=None):
    """ Align vol to target (both n^3 boxes of angpix Å/px, see match_grid).

    lowpass: resolution (Å) of the maps compared
    diameter: of the spherical mask (Å), 90% of the box if None
    max_shift: of the search (Å), a quarter of the diameter if None
    step: angular step (degrees) of the global search
    candidates: number of distinct global solutions refined locally
    """
    grid = grid or SearchGrid(target, angpix, lowpass, diameter, max_shift)
    moving = grid.prepare_moving(vol)
    cc_before = grid.cc_as_is(vol)
    Rs = np.concatenate([np.eye(3)[None], random_rotations(rotations_for_step(step), seed)])
    scorer = _Scorer(grid, moving, threads)
    try:
        if log:
            log(f"global search: {len(Rs)} rotations, {grid.size} px box at "
                f"{grid.search_angpix:0.2f} Å/px, mask {grid.diameter:0.0f} Å, "
                f"shifts up to {grid.max_shift:0.0f} Å")
        scores = scorer(Rs)
        order = np.argsort([-s[0] for s in scores])
        # Best distinct candidates (two steps apart)
        picked = []
        for k in order:
            if all(angular_distance(Rs[k], Rs[j]) > 2 * step for j in picked):
                picked.append(k)
            if len(picked) == candidates:
                break
        refined = []
        for k in picked:
            R = Rs[k]
            cc, sh = scores[k]
            # Hill climbing on finer and finer grids: at each step size,
            # move to the best rotation of the grid around the current one
            # until the current one is the best
            for fraction in (0.4, 0.15, 0.05):
                for _ in range(MAX_CLIMB):
                    local = local_rotations(R, step * fraction)
                    local_scores = scorer(local)
                    j = int(np.argmax([s[0] for s in local_scores]))
                    if local_scores[j][0] <= cc:
                        break
                    R, (cc, sh) = local[j], local_scores[j]
            refined.append((cc, R, sh))
    finally:
        scorer.close()

    refined.sort(key=lambda t: -t[0])
    cc, R, sh = refined[0]
    # Distinct solutions only: candidates that converged to (or near) a
    # better one are the same solution, its broad optimum. As the global
    # candidates, distinct solutions are more than two steps apart
    distinct = [R]
    alternatives = []
    for c, r, _ in refined[1:]:
        if all(angular_distance(r, d) > 2 * step for d in distinct):
            distinct.append(r)
            alternatives.append((c, angular_distance(r, R)))
    shift = sh * grid.search_angpix / angpix
    return AlignmentResult(R, shift, cc, cc_before, alternatives, angpix)
