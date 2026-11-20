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

""" The references of a multi-reference 3D classification (emw-aligned-volumes)
and the volume alignment, with synthetic volumes (no datasets needed):

    python -m unittest emwrap.tests.test_aligned_volumes
"""

import os
import shutil
import tempfile
import unittest

import mrcfile
import numpy as np

from emtools.utils import FolderManager
from emtools.metadata import StarFile

from emwrap.base import volume_align as va
from emwrap.base.aligned_volumes import (AlignedVolumesPipeline, default_volume_name,
                                         parse_volumes, parse_numbers, REFERENCES_STAR,
                                         ALIGNMENT_STAR)
from emwrap.base.project_data import ProjectData
from emwrap.tests.test_output_types import _Job, _Workflow

N = 32
ANGPIX = 8.0


def blobs(n=N, angpix=ANGPIX):
    """ An asymmetric volume: Gaussian blobs of different sizes and heights
    (positions in Å, so the same object at any pixel size). No dominant
    central blob, which would hardly constrain the orientation. """
    r = (np.arange(n) - n // 2) * angpix
    z, y, x = np.meshgrid(r, r, r, indexing='ij')
    vol = np.zeros((n, n, n), dtype=np.float32)
    for (bx, by, bz), sigma, h in [((0, 0, 0), 14, 0.5), ((56, 0, 0), 12, 1.0),
                                   ((0, 40, 8), 10, 0.8), ((-16, -24, 48), 10, 0.9),
                                   ((-40, 16, -32), 12, 0.7)]:
        vol += h * np.exp(-((x - bx) ** 2 + (y - by) ** 2 + (z - bz) ** 2) / (2 * sigma ** 2))
    return vol


def real_cc(a, b, radius=N * 0.4):
    m = va.soft_sphere(a.shape[0], radius) > 0.5
    a, b = a[m] - a[m].mean(), b[m] - b[m].mean()
    return float((a * b).sum() / np.sqrt((a * a).sum() * (b * b).sum()))


def table(volumes, names=()):
    """ The input_volumes table param as job.star stores it. """
    names = list(names) + [''] * (len(volumes) - len(names))
    return str([{'volume': v, 'name': n} for v, n in zip(volumes, names)])


def write(path, data, angpix):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with mrcfile.new(path, overwrite=True) as mrc:
        mrc.set_data(np.asarray(data, dtype=np.float32))
        mrc.voxel_size = angpix


class TestVolumeAlign(unittest.TestCase):

    def test_rotations(self):
        Rs = va.random_rotations(50)
        for R in Rs:
            np.testing.assert_allclose(R @ R.T, np.eye(3), atol=1e-10)
            self.assertAlmostEqual(np.linalg.det(R), 1.0, places=10)
        R = va.rotvec_to_matrices(np.radians([[0, 0, 30]]))[0]
        self.assertAlmostEqual(va.rotation_angle(R), 30, places=6)
        np.testing.assert_allclose(va.rotation_axis(R), [0, 0, 1], atol=1e-10)
        self.assertAlmostEqual(va.angular_distance(R, np.eye(3)), 30, places=6)
        # A rotation of 90 degrees about z takes x to y
        R = va.rotvec_to_matrices(np.radians([[0, 0, 90]]))[0]
        np.testing.assert_allclose(R @ [1, 0, 0], [0, 1, 0], atol=1e-10)

    def test_transform(self):
        vol = blobs()
        np.testing.assert_allclose(va.transform_volume(vol), vol, atol=1e-6)
        # An integer shift (x, y, z) moves the content by it:
        # out[z, y, x] = vol[z - 3, y + 1, x - 2]
        out = va.transform_volume(vol, shift=[2, -1, 3])
        np.testing.assert_allclose(out[10:-10, 10:-10, 10:-10],
                                   vol[7:-13, 11:-9, 8:-12], atol=1e-6)
        # The same object at another pixel size and box
        other = va.match_grid(blobs(40, 6.4), 6.4, N, ANGPIX)
        self.assertEqual(other.shape, (N, N, N))
        self.assertGreater(real_cc(other, vol), 0.99)

    def test_align(self):
        vol = blobs()
        R = va.rotvec_to_matrices(np.radians([[40, -60, 25]]))[0]
        moved = va.transform_volume(vol, R=R, shift=[2, -1, 1.5])
        res = va.align_volume(vol, moved, ANGPIX, lowpass=20, step=30, threads=2)
        self.assertLess(res.cc_before, 0.8)
        self.assertGreater(res.cc, 0.94)
        self.assertLess(va.angular_distance(res.R, R.T), 5)
        # The other solutions are distinct ones (more than two steps away)
        self.assertTrue(all(angle > 60 for _, angle in res.alternatives), res.alternatives)
        aligned = va.transform_volume(moved, R=res.R, shift=res.shift)
        self.assertGreater(real_cc(aligned, vol), 0.95)


class TestAlignedVolumes(unittest.TestCase):

    def setUp(self):
        self.path = tempfile.mkdtemp(prefix='emwrap_aligned_volumes_')
        self.target = blobs()
        write(self._p('Refine3D/job010/run_class001.mrc'), self.target, ANGPIX)
        self.R = va.rotvec_to_matrices(np.radians([[0, 70, -30]]))[0]
        write(self._p('InitialModel/job020/run_it100_class001.mrc'),
              va.transform_volume(self.target, R=self.R, shift=[1, 2, -1]), ANGPIX)
        write(self._p('Reconstruct/job030/merged.mrc'), self.target, ANGPIX)
        write(self._p('Other/job040/big.mrc'), blobs(40, 6.4), 6.4)

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def _p(self, *parts):
        return os.path.join(self.path, *parts)

    def _pipeline(self, **params):
        output = self._p('EMwrap', 'job050')
        os.makedirs(output, exist_ok=True)
        args = {'working_dir': self.path, 'threads': 2, 'align_angular_step': 30,
                'align_lowpass': 20}
        args.update(params)
        return AlignedVolumesPipeline(args, output)

    def test_helpers(self):
        self.assertEqual(default_volume_name('InitialModel/job466/run_it120_class001.mrc'),
                         'job466_run_it120_class001')
        self.assertEqual(default_volume_name('maps/emd_1234.mrc'), 'emd_1234')
        self.assertEqual(parse_volumes(table(['a.mrc', 'b.mrc'], ['nucleosome'])),
                         [('a.mrc', 'nucleosome'), ('b.mrc', '')])
        # Empty rows (the one left to add a volume) are skipped
        self.assertEqual(parse_volumes("[{'volume': 'a.mrc', 'name': ''}, "
                                       "{'volume': '', 'name': ''}]"), [('a.mrc', '')])
        self.assertEqual(parse_volumes(''), [])
        with self.assertRaises(Exception):
            parse_volumes(table([''], ['orphan']))
        self.assertEqual(parse_numbers('1, 3 4'), {1, 3, 4})
        with self.assertRaises(Exception):
            parse_numbers('one')

    def test_align_references(self):
        volumes = ['Reconstruct/job030/merged.mrc',
                   'InitialModel/job020/run_it100_class001.mrc',
                   'Other/job040/big.mrc']
        pp = self._pipeline(input_volumes=table(volumes, ['nucleosome', 'flat membrane']),
                            do_align='Yes', align_reference='Refine3D/job010/run_class001.mrc',
                            align_skip='1')
        pp.prerun()

        refs = StarFile.getTableFromFile('references', pp.join(REFERENCES_STAR))
        expected = ['EMwrap/job050/ref_class001_nucleosome.mrc',
                    'EMwrap/job050/ref_class002_flat_membrane.mrc',
                    'EMwrap/job050/ref_class003_job040_big.mrc']
        self.assertEqual([r.rlnReferenceImage for r in refs], expected)
        with open(pp.join(REFERENCES_STAR)) as f:
            self.assertIn('# version 50001', f.read())

        out = [mrcfile.read(self._p(p)) for p in expected]
        for data in out:
            self.assertEqual(data.shape, (N, N, N))
        with mrcfile.open(self._p(expected[2])) as mrc:
            self.assertAlmostEqual(float(mrc.voxel_size.x), ANGPIX, places=3)
        # Kept as it is, aligned, resampled (and aligned)
        np.testing.assert_allclose(out[0], self.target, atol=1e-6)
        self.assertGreater(real_cc(out[1], self.target), 0.95)
        self.assertGreater(real_cc(out[2], self.target), 0.95)

        alignment = StarFile.getTableFromFile('references', pp.join(ALIGNMENT_STAR))
        rows = list(alignment)
        self.assertEqual([int(r.emwAligned) for r in rows], [0, 1, 1])
        self.assertEqual([int(r.emwResampled) for r in rows], [0, 0, 1])
        self.assertEqual(int(rows[2].emwSourceBoxSize), 40)
        self.assertEqual(rows[1].emwReferenceName, 'flat_membrane')
        self.assertLess(float(rows[1].emwCcBefore), 0.8)
        self.assertGreater(float(rows[1].emwCcAfter), 0.94)
        self.assertAlmostEqual(float(rows[1].emwRotationAngle),
                               va.rotation_angle(self.R), delta=5)
        self.assertEqual(float(rows[0].emwRotationAngle), 0)
        general = StarFile.getTableFromFile('general', pp.join(ALIGNMENT_STAR))[0]
        self.assertEqual(general.emwAlignReference, 'Refine3D/job010/run_class001.mrc')

        nodes = StarFile.getTableFromFile('pipeline_nodes',
                                          pp.join('RELION_OUTPUT_NODES.star'))
        self.assertEqual([n.rlnPipeLineNodeName for n in nodes],
                         ['EMwrap/job050/' + REFERENCES_STAR] + expected)

        # The list of references is Volumes (for the 'Reference map' of Class3D)
        data = ProjectData.__new__(ProjectData)
        FolderManager.__init__(data, self.path)
        data._debug = lambda *args: None
        output = 'EMwrap/job050/' + REFERENCES_STAR
        data._wf = _Workflow({output: _Job('EMwrap/job050', 'emw-aligned-volumes')})
        info = data._computeOutputTypeInfo(output, [])
        self.assertEqual(info['type'], 'Volumes')
        self.assertTrue(info['info'].startswith('3 references, '), info['info'])
        self.assertEqual(nodes[0].rlnPipeLineNodeTypeLabel,
                         'TomogramGroupMetadata.star.emwrap.Volumes')

        # The CC of every pair of references: 1 on the diagonal, symmetric,
        # high for the aligned copies of the same map
        sim = StarFile.getTableFromFile('similarity', pp.join(ALIGNMENT_STAR))
        self.assertEqual(len(sim), 9)
        cc = {(int(r.emwClassA), int(r.emwClassB)): float(r.emwCc) for r in sim}
        for k in (1, 2, 3):
            self.assertEqual(cc[(k, k)], 1.0)
        self.assertEqual(cc[(1, 2)], cc[(2, 1)])
        self.assertGreater(cc[(1, 2)], 0.94)
        self.assertGreater(cc[(1, 3)], 0.9)
        # As the CC with the map (class 1 is the map, kept as it is)
        self.assertAlmostEqual(cc[(1, 2)], float(rows[1].emwCcAfter), delta=0.002)

    def test_copy_only(self):
        volumes = ['Reconstruct/job030/merged.mrc', 'InitialModel/job020/run_it100_class001.mrc']
        pp = self._pipeline(input_volumes=table(volumes), do_align='No')
        pp.prerun()
        refs = StarFile.getTableFromFile('references', pp.join(REFERENCES_STAR))
        self.assertEqual([r.rlnReferenceImage for r in refs],
                         ['EMwrap/job050/ref_class001_job030_merged.mrc',
                          'EMwrap/job050/ref_class002_job020_run_it100_class001.mrc'])
        np.testing.assert_allclose(mrcfile.read(self._p(refs[1].rlnReferenceImage)),
                                   mrcfile.read(self._p(volumes[1])))
        rows = list(StarFile.getTableFromFile('references', pp.join(ALIGNMENT_STAR)))
        self.assertFalse(StarFile.getTableFromFile(
            'references', pp.join(ALIGNMENT_STAR)).hasColumn('emwCcBefore'))
        self.assertEqual([int(r.emwAligned) for r in rows], [0, 0])
        # No alignment map
        general = StarFile.getTableFromFile('general', pp.join(ALIGNMENT_STAR))
        self.assertFalse(general.hasColumn('emwAlignReference'))
        self.assertEqual(int(general[0].emwBoxSize), N)
        # The similarity, without a map: the low-pass of the params
        self.assertEqual(float(general[0].emwLowpass), 20)
        sim = StarFile.getTableFromFile('similarity', pp.join(ALIGNMENT_STAR))
        cc = {(int(r.emwClassA), int(r.emwClassB)): float(r.emwCc) for r in sim}
        self.assertEqual(len(cc), 4)
        self.assertLess(cc[(1, 2)], 0.8)  # not aligned

    def test_form(self):
        from emwrap.base.config import ProcessingConfig
        from emwrap.base.job_form import JobForm
        form = ProcessingConfig.get_job_form('emw-aligned-volumes')
        params = {'input_volumes': table(['Reconstruct/job030/merged.mrc'], ['nucleosome']),
                  'do_align': False}
        self.assertEqual(JobForm.validate_params(form, params, project_path=self.path), [])
        # The table needs one volume (min), the empty row is not one
        for value in ('[]', table([''])):
            errors = JobForm.validate_params(form, dict(params, input_volumes=value),
                                             project_path=self.path)
            self.assertEqual(errors, ['Input volumes:: at least 1 row(s) are required.'])
        # The map of the poses is only required to align
        errors = JobForm.validate_params(form, dict(params, do_align=True),
                                         project_path=self.path)
        self.assertTrue(any('Map of the poses' in e for e in errors), errors)

    def test_errors(self):
        merged = 'Reconstruct/job030/merged.mrc'
        cases = [
            dict(input_volumes='[]'),
            dict(input_volumes=table([merged, 'Missing/job1/x.mrc'])),
            # Different grids without an alignment map
            dict(input_volumes=table([merged, 'Other/job040/big.mrc']), do_align='No'),
            dict(input_volumes=table([merged]), do_align='Yes'),
            dict(input_volumes=table([merged]), do_align='Yes',
                 align_reference='Refine3D/job010/run_class001.mrc', align_skip='2'),
        ]
        for params in cases:
            with self.subTest(params=params), self.assertRaises(Exception):
                self._pipeline(**params).prerun()


if __name__ == '__main__':
    unittest.main()
