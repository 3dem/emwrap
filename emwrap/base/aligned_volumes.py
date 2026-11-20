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

""" References for a multi-reference 3D classification (Relion 5).

The input volumes, in their order (class 1 first), are copied as the
references of the classes, optionally aligned to a map in the frame of the
particle poses (see volume_align): a 3D classification that does not align
the particles, or only searches around their poses, needs references with
that orientation and centre, and de novo initial models have their own. The
map is usually the one of the refinement (or 3D classification) that gave
the input poses of the classification, of the same iteration.

Outputs, in the job folder:
  ref_class<NNN>_<name>.mrc  the references (in the grid of the alignment
                             map, if any: same box and pixel size)
  input_references.star      their list (a Volumes output), for the
                             'Reference map' of the 3D classification (the
                             class number is the row)
  alignment.star             how each volume was aligned (CC before and
                             after, rotation, shift and the next best
                             solution) and the CC of every pair of
                             references (how similar the classes are),
                             shown by the job's Analyze Results
"""

import os
import re

import mrcfile
import numpy as np

from emtools.utils import Color
from emtools.metadata import StarFile, Table, RelionStar

from emwrap.base import ProcessingPipeline
from emwrap.base.job_form import _parse_table_param_value
from emwrap.base.wizards import to_bool, to_float, to_int
from emwrap.base import volume_align as va

REFERENCES_STAR = 'input_references.star'
ALIGNMENT_STAR = 'alignment.star'
VOLUME_NODE = 'TomogramGroupMetadata.star.relion.volume'
REFERENCES_NODE = 'TomogramGroupMetadata.star.emwrap.Volumes'


def default_volume_name(path):
    """ Name of a volume from its path: its job and file name, e.g.
    'InitialModel/job466/run_it120_class001.mrc' -> 'job466_run_it120_class001'. """
    base = os.path.splitext(os.path.basename(path))[0]
    job = next((p for p in reversed(os.path.normpath(path).split(os.sep)[:-1])
                if re.fullmatch(r'job\d+', p)), None)
    return f'{job}_{base}' if job else base


def clean_name(name):
    """ A name usable in file names (letters, digits, '.', '_' and '-'). """
    return re.sub(r'[^A-Za-z0-9._-]+', '_', str(name).strip()).strip('_')


def parse_volumes(rawValue):
    """ [(volume path, name)] of the rows of the input_volumes table param
    (columns 'volume' and 'name', the name may be empty), without the empty
    rows. """
    try:
        rows = _parse_table_param_value(rawValue)
    except ValueError:
        return []
    volumes = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        path = str(row.get('volume') or '').strip()
        name = str(row.get('name') or '').strip()
        if path:
            volumes.append((path, name))
        elif name:
            raise Exception(f"No volume for the name {name!r} in 'input_volumes'.")
    return volumes


def parse_numbers(text):
    """ The volume numbers (1-based) of a parameter like '1 3' or '1,3'. """
    numbers = set()
    for p in re.split(r'[\s,]+', str(text or '').strip()):
        if p:
            if not p.isdigit():
                raise Exception(f"Invalid volume number: {p!r}")
            numbers.add(int(p))
    return numbers


def read_volume(path):
    """ (data (z, y, x) float32, pixel size) of an MRC volume. """
    with mrcfile.open(path, permissive=True) as mrc:
        data = np.asarray(mrc.data, dtype=np.float32)
        angpix = float(mrc.voxel_size.x)
    if data.ndim != 3:
        raise Exception(f"{path} is not a volume (shape {data.shape}).")
    if len(set(data.shape)) != 1:
        raise Exception(f"{path} is not a cubic volume (shape {data.shape}).")
    return data, angpix


def write_volume(path, data, angpix):
    with mrcfile.new(path, overwrite=True) as mrc:
        mrc.set_data(np.asarray(data, dtype=np.float32))
        mrc.voxel_size = angpix


class AlignedVolumesPipeline(ProcessingPipeline):
    """ Copy (and optionally align) volumes as the references of a
    multi-reference 3D classification. """

    name = 'emw-aligned-volumes'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        volumes = parse_volumes(args.get('input_volumes'))
        self.inputVolumes = [path for path, _ in volumes]
        self.names = [name for _, name in volumes]
        self.doAlign = to_bool(args.get('do_align', True))
        self.alignReference = str(args.get('align_reference') or '').strip()
        self.skip = parse_numbers(args.get('align_skip'))
        self.lowpass = to_float(args.get('align_lowpass')) or 25.0
        self.diameter = to_float(args.get('align_diameter'))
        self.maxShift = to_float(args.get('align_max_shift'))
        self.step = to_float(args.get('align_angular_step')) or 10.0
        self.threads = max(1, to_int(args.get('threads')) or 1)

    def _path(self, path):
        return path if os.path.isabs(path) else os.path.join(self.workingDir, path)

    def _volumeNames(self):
        names = []
        for i, (path, given) in enumerate(zip(self.inputVolumes, self.names)):
            name = clean_name(given) or clean_name(default_volume_name(path))
            if name in names:
                name = f'{name}_{i + 1}'
            names.append(name)
        return names

    def _validate(self):
        if not self.inputVolumes:
            raise Exception("Parameter 'input_volumes' must contain at least one volume.")
        for path in self.inputVolumes:
            if not os.path.exists(self._path(path)):
                raise Exception(f"Input volume not found: {path}")
        n = len(self.inputVolumes)
        if bad := sorted(k for k in self.skip if not 1 <= k <= n):
            raise Exception(f"Volumes to keep as they are out of range (1-{n}): {bad}")
        if self.doAlign:
            if not self.alignReference:
                raise Exception("Parameter 'align_reference' (the map in the frame "
                                "of the particle poses) is required to align.")
            if not os.path.exists(self._path(self.alignReference)):
                raise Exception(f"Alignment map not found: {self.alignReference}")
            if self.step <= 0 or self.step > 45:
                raise Exception(f"Invalid angular step: {self.step} (0-45 degrees).")

    def prerun(self):
        self._validate()
        names = self._volumeNames()
        volumes = [read_volume(self._path(p)) for p in self.inputVolumes]

        if self.doAlign:
            target, angpix = read_volume(self._path(self.alignReference))
            if angpix <= 0:
                raise Exception(f"The alignment map {self.alignReference} has no "
                                "pixel size in its header.")
            size = target.shape[0]
            self.log(f"Alignment map: {Color.cyan(self.alignReference)} "
                     f"({size} px, {angpix:0.3f} Å/px)")
            grid = va.SearchGrid(target, angpix, self.lowpass,
                                 self.diameter, self.maxShift)
        else:
            target = grid = None
            _, angpix = volumes[0]
            size = volumes[0][0].shape[0]
            for path, (data, ps) in zip(self.inputVolumes, volumes):
                if data.shape[0] != size or abs(ps - angpix) > 1e-3 * angpix:
                    raise Exception(
                        f"{path} ({data.shape[0]} px, {ps:0.3f} Å/px) does not match "
                        f"the first volume ({size} px, {angpix:0.3f} Å/px). The "
                        "references need the same box and pixel size: align them "
                        "to a map (they are resampled to its grid).")

        rows = []
        refPaths = []
        outputs = []
        for k, (path, name, (data, ps)) in enumerate(
                zip(self.inputVolumes, names, volumes), 1):
            self.log(f"Class {k}: {Color.cyan(path)} as {Color.green(name)}")
            sourceSize = data.shape[0]
            if ps <= 0:
                self.log(f"  WARNING: no pixel size in its header, "
                         f"{angpix:0.3f} Å/px assumed")
                ps = angpix
            resampled = data.shape[0] != size or abs(ps - angpix) > 1e-3 * angpix
            if resampled:
                self.log(f"  resampled from {data.shape[0]} px, {ps:0.3f} Å/px "
                         f"to {size} px, {angpix:0.3f} Å/px")
                data = va.match_grid(data, ps, size, angpix)

            aligned = self.doAlign and k not in self.skip
            result = None
            if aligned:
                result = va.align_volume(
                    target, data, angpix, step=self.step, threads=self.threads,
                    grid=grid, log=lambda m: self.log('  ' + m, flush=True))
                out = va.transform_volume(data, R=result.R, shift=result.shift)
                ccAfter = grid.cc_as_is(out)
                nxt = self._nextSolution(result, ccAfter)
                self.log(f"  CC {result.cc_before:0.3f} -> {Color.green(f'{ccAfter:0.3f}')}, "
                         f"rotation {result.angle:0.1f}°, shift "
                         f"{np.round(result.shift_angst, 1).tolist()} Å"
                         + (f", next solution CC {nxt[0]:0.3f} at {nxt[1]:0.0f}°" if nxt else ''),
                         flush=True)
            else:
                out = data
                if self.doAlign:
                    self.log("  kept as it is (not aligned)")

            fileName = f'ref_class{k:03d}_{name}.mrc'
            write_volume(self.join(fileName), out, angpix)
            outputs.append(out)
            refPath = self.fixOutputPath(fileName)
            refPaths.append(refPath)
            rows.append(self._alignmentRow(k, name, path, refPath, sourceSize, data,
                                           ps, resampled, aligned, result, grid, out))

        # The CC of the references with each other, as the CC with the map
        # (the same low-pass and mask; without a map, those of the grid of
        # the first reference)
        simGrid = grid or va.SearchGrid(outputs[0], angpix, self.lowpass,
                                        self.diameter, self.maxShift)
        similarity = va.similarity_matrix(simGrid, outputs)
        if len(outputs) > 1:
            self.log("CC of the references with each other:")
            for k, ccs in enumerate(similarity, 1):
                self.log(f"  {k:3d}: " + ' '.join(f'{cc:6.3f}' for cc in ccs))
        self._writeReferences(refPaths)
        self._writeAlignment(rows, size, angpix, grid, simGrid, similarity)
        self.writeRelionOutputNodes(
            [[self.fixOutputPath(REFERENCES_STAR), REFERENCES_NODE]]
            + [[p, VOLUME_NODE] for p in refPaths])
        self.info['summary'] = {
            'references': len(refPaths),
            'aligned': sum(r['emwAligned'] for r in rows),
            'boxSize': size,
            'pixelSize': angpix,
        }
        self.writeInfo()
        self.log(f"Wrote {Color.green(len(refPaths))} references to "
                 f"{Color.cyan(self.fixOutputPath(REFERENCES_STAR))}")

    def _alignmentRow(self, k, name, path, refPath, sourceSize, data, ps,
                      resampled, aligned, result, grid, out):
        """ The alignment.star row of a reference. The CC columns are only
        written with an alignment map; emwNextCc and emwNextAngle (the next
        best solution) are -1 if there is none. """
        row = {
            'rlnClassNumber': k,
            'rlnReferenceImage': refPath,
            'emwReferenceName': name,
            'emwSourceImage': path,
            'emwSourceBoxSize': sourceSize,
            'emwSourcePixelSize': round(ps, 4),
            'emwResampled': int(resampled),
            'emwAligned': int(aligned),
            'emwRotationAngle': 0.0,
            'emwRotationAxisX': 0.0, 'emwRotationAxisY': 0.0, 'emwRotationAxisZ': 1.0,
            'emwShiftXAngst': 0.0, 'emwShiftYAngst': 0.0, 'emwShiftZAngst': 0.0,
            'emwNextCc': -1.0, 'emwNextAngle': -1.0,
        }
        if grid is not None:
            row['emwCcBefore'] = round(grid.cc_as_is(data), 4)
            row['emwCcAfter'] = round(grid.cc_as_is(out), 4)
        nxt = self._nextSolution(result, row.get('emwCcAfter'))
        if result is not None:
            axis = result.axis
            shift = result.shift_angst
            row.update({
                'emwRotationAngle': round(result.angle, 2),
                'emwRotationAxisX': round(float(axis[0]), 4),
                'emwRotationAxisY': round(float(axis[1]), 4),
                'emwRotationAxisZ': round(float(axis[2]), 4),
                'emwShiftXAngst': round(float(shift[0]), 2),
                'emwShiftYAngst': round(float(shift[1]), 2),
                'emwShiftZAngst': round(float(shift[2]), 2),
            })
            if nxt:
                row['emwNextCc'] = round(nxt[0], 4)
                row['emwNextAngle'] = round(nxt[1], 1)
        return row

    @staticmethod
    def _nextSolution(result, ccAfter):
        """ (CC, angle to the best) of the next best solution, or None. Its
        CC is on the scale of emwCcAfter, measured on the written reference
        (after its interpolation): the search CC plus their difference for
        the best solution, so both can be compared. """
        if result is None or not result.alternatives:
            return None
        cc, angle = result.alternatives[0]
        if ccAfter is not None:
            cc += ccAfter - result.cc
        return cc, angle

    def _writeReferences(self, refPaths):
        table = Table(['rlnReferenceImage'])
        for p in refPaths:
            table.addRowValues(p)
        with StarFile(self.join(REFERENCES_STAR), 'w') as sf:
            sf.writeTable('references', table, computeFormat='left',
                          version=RelionStar.PIPELINE_VERSION)

    def _writeAlignment(self, rows, size, angpix, grid, simGrid, similarity):
        """ The 'general' table (the low-pass and mask diameter of the CC,
        with or without alignment, and the alignment params, only with an
        alignment map: no emwAlignReference for copied volumes), the
        'references' table and the 'similarity' table: the CC of every pair
        of references (emwClassA, emwClassB, emwCc; every ordered pair, the
        diagonal too). """
        general = {
            'emwBoxSize': size,
            'emwPixelSize': round(angpix, 4),
            'emwLowpass': round(simGrid.lowpass, 2),
            'emwMaskDiameter': round(simGrid.diameter, 1),
        }
        if grid is not None:
            general.update({
                'emwAlignReference': self.alignReference,
                'emwMaxShift': round(grid.max_shift, 1),
                'emwAngularStep': self.step,
                'emwSearchBoxSize': grid.size,
            })
        table = Table(list(rows[0].keys()))
        for r in rows:
            table.addRowValues(**r)
        with StarFile(self.join(ALIGNMENT_STAR), 'w') as sf:
            sf.writeTable('general', Table.fromDict(general), singleRow=True,
                          timeStamp=True)
            sf.writeTable('references', table, computeFormat='left')
            sim = Table(['emwClassA', 'emwClassB', 'emwCc'])
            for a, b in np.ndindex(similarity.shape):
                sim.addRowValues(a + 1, b + 1, round(float(similarity[a, b]), 4))
            sf.writeTable('similarity', sim, computeFormat='left')


if __name__ == '__main__':
    AlignedVolumesPipeline.main()
