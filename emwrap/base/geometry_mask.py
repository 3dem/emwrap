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

""" 3D masks made of geometric shapes (sphere, cylinder and box),
used by the emw-geometry-mask job and its wizard (GeometryMaskWizard).

Each shape has a center (px, from the first voxel of the box, x y z), the
Euler angles rot, tilt, psi (degrees, ZYZ as Relion) and its sizes (px):

    sphere:   radius
    cylinder: radius and height, its axis along the (rotated) z
    box:      size_x, size_y, size_z, its edges along the (rotated) axes

A voxel r is inside a shape if the local coordinates A · (r - center) are,
where A is Relion's Euler matrix of (rot, tilt, psi). The mask is the union
of the shapes: 1 inside any of them, 0 outside. With a soft edge of width W
(px), as relion_mask_create --width_soft_edge, the voxels outside the shapes
at a distance d < W of the nearest one get a raised cosine
0.5 + 0.5 cos(pi d / W); the distances to the shapes are exact.

The job emw-geometry-mask (GeometryMaskPipeline) writes the mask on the box
(and pixel size) of the optional input volume, or of the box size param.

Params:
    input_volume: optional map, gives the box and pixel size
    box_x, box_y, box_z: box size (px), without input volume
    angpix: pixel size (A) of the mask, empty: the one of the volume (or 1)
    soft_edge: width (px) of the raised cosine soft edge, 0: binary mask
    shapes: TableParam, one row per shape (COLUMNS)

Output: mask.mrc (VolumeMask)
"""

import math
import os

import numpy as np

from emtools.utils import Color

from emwrap.base import ProcessingPipeline

SHAPES = ('sphere', 'cylinder', 'box')
# The sizes of each shape (px)
SHAPE_SIZES = {
    'sphere': ('radius',),
    'cylinder': ('radius', 'height'),
    'box': ('size_x', 'size_y', 'size_z'),
}
CENTER = ('center_x', 'center_y', 'center_z')
ANGLES = ('rot', 'tilt', 'psi')
# The columns of the shapes table (TableParam of the job form)
COLUMNS = ('shape',) + CENTER + ANGLES + ('radius', 'height', 'size_x', 'size_y', 'size_z')


def _number(value):
    """ value as float, None if empty; ValueError if not a number. """
    if value is None or str(value).strip() == '':
        return None
    return float(value)


def parse_shapes(rows, dimensions):
    """ Validate the rows of the shapes table (dicts with COLUMNS) for a box
    of dimensions [X, Y, Z] (px). An empty center is the center of the box
    (N // 2) and empty angles are 0. Return the list of shapes, each a dict
    with all COLUMNS (the sizes not used by the shape are ''); raise
    ValueError with the list of errors. """
    shapes, errors = [], []
    for i, row in enumerate(rows or []):
        if not isinstance(row, dict) or not any(str(v).strip() for v in row.values()):
            continue  # An empty row of the table
        label = f"Shape {i + 1}"
        shape = str(row.get('shape') or '').strip().lower()
        if shape not in SHAPES:
            errors.append(f"{label}: unknown shape '{row.get('shape', '')}' "
                          f"(expected one of: {', '.join(SHAPES)}).")
            continue
        out = {k: '' for k in COLUMNS}
        out['shape'] = shape
        try:
            for k, n in zip(CENTER, dimensions):
                v = _number(row.get(k))
                out[k] = float(n // 2) if v is None else v
            for k in ANGLES:
                out[k] = _number(row.get(k)) or 0.0
            for k in SHAPE_SIZES[shape]:
                v = _number(row.get(k))
                if v is None or v <= 0:
                    errors.append(f"{label} ({shape}): '{k}' should be a positive number.")
                out[k] = v
        except (TypeError, ValueError):
            errors.append(f"{label} ({shape}): the center, angles and sizes "
                          f"should be numbers.")
            continue
        shapes.append(out)
    if errors:
        raise ValueError(errors)
    return shapes


def parse_soft_edge(value):
    """ The soft edge width (px, >= 0; empty: 0); ValueError if not valid. """
    try:
        width = _number(value) or 0.0
    except (TypeError, ValueError):
        width = -1
    if width < 0:
        raise ValueError(["The soft edge should be a number of pixels (0 or more)."])
    return width


def euler_matrix(rot, tilt, psi):
    """ Relion's Euler matrix (Euler_angles2matrix) of the angles in degrees. """
    a, b, g = (math.radians(v) for v in (rot, tilt, psi))
    ca, sa, cb, sb, cg, sg = (math.cos(a), math.sin(a), math.cos(b),
                              math.sin(b), math.cos(g), math.sin(g))
    cc, cs, sc, ss = cb * ca, cb * sa, sb * ca, sb * sa
    return np.array([[cg * cc - sg * sa, cg * cs + sg * ca, -cg * sb],
                     [-sg * cc - cg * sa, -sg * cs + cg * ca, sg * sb],
                     [sc, ss, cb]])


def _distance(shape, u, v, w):
    """ Signed distance (px) of the local coordinates (u, v, w) to the
    surface of the shape: negative inside, exact outside. """
    kind = shape['shape']
    if kind == 'sphere':
        return np.sqrt(u ** 2 + v ** 2 + w ** 2) - shape['radius']
    if kind == 'cylinder':
        q = [np.sqrt(u ** 2 + v ** 2) - shape['radius'],
             np.abs(w) - shape['height'] / 2]
    else:
        q = [np.abs(c) - shape[k] / 2
             for c, k in zip((u, v, w), SHAPE_SIZES['box'])]
    outside = np.sqrt(sum(np.maximum(c, 0) ** 2 for c in q))
    return outside + np.minimum(np.maximum.reduce(q), 0)


def geometry_mask(shapes, dimensions, binning=1, soft_edge=0):
    """ Mask (float32 0-1, (z, y, x)) of the union of the shapes (see
    parse_shapes) on a box of dimensions [X, Y, Z] px, downscaled by an
    integer binning (the voxels of the binned grid are sampled at their
    center, in px of the full box), with a raised cosine soft edge of
    soft_edge px outside the shapes (0: binary). It is computed slice by
    slice. """
    f = max(1, int(binning))
    width = float(soft_edge or 0)
    axes = [np.arange(math.ceil(n / f)) * f + (f - 1) / 2 for n in dimensions]
    xs, ys, zs = axes
    mask = np.zeros((len(zs), len(ys), len(xs)), dtype=np.float32)
    y2, x2 = np.meshgrid(ys, xs, indexing='ij')
    prepared = [(s, euler_matrix(*(s[k] for k in ANGLES)),
                 np.array([s[k] for k in CENTER])) for s in shapes]
    for iz, z in enumerate(zs):
        dist = np.full(y2.shape, np.inf)
        for s, A, c in prepared:
            dx, dy, dz = x2 - c[0], y2 - c[1], z - c[2]
            u, v, w = (A[i, 0] * dx + A[i, 1] * dy + A[i, 2] * dz for i in range(3))
            dist = np.minimum(dist, _distance(s, u, v, w))
        if width > 0:
            soft = np.clip(dist / width, 0, 1)
            mask[iz] = 0.5 + 0.5 * np.cos(np.pi * soft)
        else:
            mask[iz] = dist <= 0
    return mask


def default_shape(shape, dimensions):
    """ A new shape at the center of the box, sized from its smallest side. """
    n = min(dimensions)
    s = {k: '' for k in COLUMNS}
    s.update({'shape': shape, 'rot': 0.0, 'tilt': 0.0, 'psi': 0.0})
    s.update({k: float(d // 2) for k, d in zip(CENTER, dimensions)})
    if shape == 'sphere':
        s['radius'] = round(n / 4, 1)
    elif shape == 'cylinder':
        s.update({'radius': round(n / 6, 1), 'height': round(n / 2, 1)})
    else:
        s.update({k: round(n / 3, 1) for k in SHAPE_SIZES['box']})
    return s


def mask_box(project, inputs):
    """ Dimensions [X, Y, Z] (px) and pixel size (A or None) of the mask:
    of the input volume or, without it, from the box size. inputs has
    volume (path relative to the project), box (3 values) and angpix.
    Raise ValueError if they can not be resolved. """
    from emwrap.base.wizards import read_volume_header, to_float, to_int

    volume = str(inputs.get('volume') or '').strip()
    angpix = to_float(inputs.get('angpix'))
    angpix = angpix if angpix and angpix > 0 else None
    if volume:
        path = volume if os.path.isabs(volume) else project.join(volume)
        if not os.path.exists(path):
            raise ValueError(f"The input volume was not found: {volume}")
        dims, headerAngpix = read_volume_header(path)
        return dims, angpix or headerAngpix
    box = [to_int(v) for v in inputs.get('box') or []]
    if len(box) != 3 or not all(b and b > 0 for b in box):
        raise ValueError("Select an input volume or set the box size "
                         "(X, Y and Z, positive integers).")
    return box, angpix


class GeometryMaskPipeline(ProcessingPipeline):
    name = 'emw-geometry-mask'
    OUTPUT_MASK = 'mask.mrc'
    OUTPUT_TYPE = 'TomogramGroupMetadata.star.relion.mask3d'

    def prerun(self):
        import mrcfile
        from emwrap.base.job_form import _parse_table_param_value

        args = self._args
        try:
            dims, angpix = mask_box(self.project, {
                'volume': args.get('input_volume'),
                'box': [args.get(f'box_{a}') for a in 'xyz'],
                'angpix': args.get('angpix')
            })
            rows = args.get('shapes') or []
            if isinstance(rows, str):
                rows = _parse_table_param_value(rows)
            shapes = parse_shapes(rows, dims)
            softEdge = parse_soft_edge(args.get('soft_edge'))
        except ValueError as e:
            errors = e.args[0] if isinstance(e.args[0], list) else [str(e)]
            raise Exception('\n'.join(errors))
        if not shapes:
            raise Exception("Add at least one shape to the mask.")

        self.log(f"Box: {Color.bold(' x '.join(map(str, dims)))} px, "
                 f"pixel size: {Color.bold(angpix or 1.0)} A, "
                 f"soft edge: {Color.bold(f'{softEdge:g}')} px")
        for i, s in enumerate(shapes):
            self.log(f"Shape {i + 1}: {Color.green(s['shape'])} "
                     + ', '.join(f"{k}={s[k]:g}" for k in s if s[k] != '' and k != 'shape'))

        mask = geometry_mask(shapes, dims, soft_edge=softEdge)
        outMask = self.join(self.OUTPUT_MASK)
        with mrcfile.new(outMask, overwrite=True) as mrc:
            mrc.set_data(mask)
            mrc.voxel_size = angpix or 1.0

        fraction = float(mask.mean())
        self.log(f"Mask: {Color.bold(outMask)}, "
                 f"{Color.green(f'{fraction * 100:.2f}%')} of the box")
        if fraction == 0:
            self.log(Color.warn("The mask is empty: the shapes are outside the box."))

        self.info['summary'] = {'shapes': len(shapes), 'dimensions': dims,
                                'angpix': angpix or 1.0, 'soft_edge': softEdge,
                                'mask_fraction': round(fraction, 4)}
        self.writeRelionOutputNodes([[self.toProjectPath(outMask), self.OUTPUT_TYPE]])
        self.writeInfo()


if __name__ == '__main__':
    GeometryMaskPipeline.main()
