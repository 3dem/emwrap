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

""" Unit tests for the job form wizards (no datasets needed):

    python -m unittest emwrap.tests.test_wizards
"""

import os
import shutil
import tempfile
import unittest

from emtools.utils import FolderManager

from emwrap.base.config import ProcessingConfig
from emwrap.base.wizards import Wizards, WizardError

TOMOGRAMS_STAR = """
data_global

loop_
_rlnTomoName #1
_rlnTomoTiltSeriesPixelSize #2
TS_01 1.5
TS_02 1.5
"""

OPTIMISATION_SET_STAR = """
data_optimisation_set

_rlnTomoParticlesFile Extract/job002/particles.star
_rlnTomoTomogramsFile Tomograms/job001/tomograms.star
"""

WARP = 'emw-warp-export_particles'
RELION = 'relion.pseudosubtomo'
CLASS3D = 'relion.class3d.tomo'
REFINE3D = 'relion.refine3d.tomo'
MASK_CREATE = 'emw-relion-mask_create'
MTOOLS_CREATE = 'emw-warp-mtools_create'
PYTOM_TEMPLATE = 'emw-pytom-create_template'
GEOMETRY_MASK = 'emw-geometry-mask'


class TestWizards(unittest.TestCase):

    def setUp(self):
        self.path = tempfile.mkdtemp(prefix='emwrap_wizards_')
        self.project = FolderManager(self.path)
        self._write('Tomograms/job001/tomograms.star', TOMOGRAMS_STAR)
        self._write('Extract/job002/optimisation_set.star', OPTIMISATION_SET_STAR)

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def _write(self, rel, content):
        fn = self.project.join(rel)
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        with open(fn, 'w') as f:
            f.write(content)

    def test_tag_form(self):
        for jobType, params in [(WARP, ['ts_export_particles.output_angpix',
                                        'ts_export_particles.box']),
                                (RELION, ['box_size_angst', 'binned_size_pix'])]:
            jobForm = ProcessingConfig.get_job_form(jobType)
            formDef = {'sections': jobForm['sections']}
            Wizards.tag_form(formDef, jobForm)
            self.assertEqual(formDef['wizards'][0]['id'], 'export_box')
            self.assertNotIn('adaptor', formDef['wizards'][0])
            tagged = []

            def _collect(ps):
                for p in ps:
                    if p.get('wizard'):
                        tagged.append(p['name'])
                    _collect(p.get('params', []))

            for s in formDef['sections']:
                _collect(s['params'])
            self.assertEqual(sorted(tagged), sorted(params))

    def test_warp(self):
        params = {'input_tomograms': 'Tomograms/job001/tomograms.star',
                  'ts_export_particles.output_angpix': '6',
                  'ts_export_particles.box': '48',
                  'ts_export_particles.diameter': '150'}
        result = Wizards.open(self.project, WARP, 'export_box', params)
        self.assertEqual(result['ui'], 'ExportTomoParticlesWizard')
        self.assertEqual(result['data']['in_angpix'], 1.5)
        self.assertEqual(result['data']['tomograms_count'], 2)
        self.assertEqual(result['inputs'],
                         {'in_box': 192, 'out_angpix': 6, 'out_box': 48,
                          'binning': 4, 'diameter': 150})

        result = Wizards.apply(self.project, WARP, 'export_box', params,
                               {'in_box': 200, 'out_angpix': 3, 'out_box': 64,
                                'diameter': 160})
        self.assertEqual(result['params'],
                         {'ts_export_particles.output_angpix': 3,
                          'ts_export_particles.box': 64,
                          'ts_export_particles.diameter': 160})
        self.assertEqual(result['warnings'], [])

        # Without diameter in the form: half of the input box (192 px * 1.5 / 2)
        params['ts_export_particles.diameter'] = ''
        result = Wizards.open(self.project, WARP, 'export_box', params)
        self.assertEqual(result['inputs']['diameter'], 144)

        # Tomograms from the optimisation set of the coordinates
        params = {'input_coordinates': 'Extract/job002/optimisation_set.star'}
        result = Wizards.open(self.project, WARP, 'export_box', params)
        self.assertEqual(result['data']['in_angpix'], 1.5)
        self.assertEqual(result['inputs']['binning'], 1)

    def test_relion(self):
        params = {'in_optimisation': 'Extract/job002/optimisation_set.star',
                  'use_direct_entries': 'No',
                  'box_size_angst': '240',
                  'binned_size_pix': '40'}
        result = Wizards.open(self.project, RELION, 'export_box', params)
        self.assertEqual(result['inputs'],
                         {'in_box': 160, 'out_angpix': 6, 'out_box': 40,
                          'binning': 4, 'diameter': 120})

        outputs = {'in_box': 160, 'out_angpix': 6, 'out_box': 40}
        result = Wizards.apply(self.project, RELION, 'export_box', params, outputs)
        self.assertEqual(result['params'],
                         {'box_size_angst': 240, 'binned_size_pix': 40})
        self.assertEqual(result['warnings'], [])

        # Boxes not related by the binning: Relion uses another pixel size
        outputs = {'in_box': 160, 'out_angpix': 6, 'out_box': 48}
        result = Wizards.apply(self.project, RELION, 'export_box', params, outputs)
        self.assertEqual(result['params']['binned_size_pix'], 48)
        self.assertEqual(len(result['warnings']), 1)

        # Direct entries
        params = {'use_direct_entries': True,
                  'in_tomograms': 'Tomograms/job001/tomograms.star'}
        result = Wizards.open(self.project, RELION, 'export_box', params)
        self.assertEqual(result['inputs']['out_angpix'], 1.5)
        self.assertIsNone(result['inputs']['diameter'])  # No box, no diameter

    def _write_mrc(self, rel, shape, angpix=2.0, data=None):
        import mrcfile
        import numpy as np
        fn = self.project.join(rel)
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        with mrcfile.new(fn, overwrite=True) as mrc:
            mrc.set_data(np.zeros(shape, dtype=np.float32) if data is None else data)
            mrc.voxel_size = angpix

    def test_mask_create(self):
        import numpy as np
        from emwrap.base.volume_mask import load_binned_map, compute_mask

        # A ball of radius 40 px in a 200 px box: downscaled by 2 to 100 px
        n = 200
        z, y, x = np.mgrid[:n, :n, :n]
        r = np.sqrt((x - 100) ** 2 + (y - 100) ** 2 + (z - 100) ** 2)
        self._write_mrc('Refine3D/job020/run_class001.mrc', (n, n, n),
                        angpix=1.5, data=(r < 40).astype(np.float32))
        binned = load_binned_map(self.project.join('Refine3D/job020/run_class001.mrc'))
        self.assertEqual(binned.binning, 2)
        self.assertEqual(binned.data.shape, (100, 100, 100))

        def radius(mask):  # Of the mask > 0.5, in full-size px
            return ((mask > 0.5).sum() * 8 * 3 / (4 * np.pi)) ** (1 / 3)

        self.assertAlmostEqual(radius(compute_mask(binned, 0.5)), 40, delta=1)
        self.assertAlmostEqual(radius(compute_mask(binned, 0.5, extend=10)), 50, delta=2)
        self.assertAlmostEqual(radius(compute_mask(binned, 0.5, extend=-10)), 30, delta=2)
        soft = compute_mask(binned, 0.5, soft_edge=10)
        self.assertTrue(((soft > 0.05) & (soft < 0.95)).any())
        self.assertAlmostEqual(compute_mask(binned, 0.5, invert=True).mean(),
                               1 - compute_mask(binned, 0.5).mean())

        prefix = 'relion_mask_create.volume.'
        params = {'relion_mask_create.denovo': False,
                  prefix + 'i': 'Refine3D/job020/run_class001.mrc',
                  prefix + 'ini_threshold': '0.5', prefix + 'lowpass': '-1',
                  prefix + 'extend_inimask': '4', prefix + 'width_soft_edge': '6',
                  prefix + 'invert': False, prefix + 'angpix': '-1'}
        result = Wizards.open(self.project, MASK_CREATE, 'mask_create', params)
        self.assertEqual(result['ui'], 'MaskCreateWizard')
        self.assertEqual(result['inputs'], {'threshold': 0.5, 'lowpass': None,
                                            'extend': 4, 'soft_edge': 6,
                                            'invert': False})
        self.assertEqual(result['data']['angpix'], 1.5)
        self.assertEqual(result['data']['mask']['shape'], [100, 100, 100])
        self.assertEqual(result['data']['mask']['binning'], 2)

        # A threshold above the map: an empty mask, with a warning
        result = Wizards.preview(self.project, MASK_CREATE, 'mask_create', params,
                                 {**result['inputs'], 'threshold': 2})
        self.assertEqual(result['data']['mask_fraction'], 0)
        self.assertEqual(len(result['warnings']), 1)

        result = Wizards.apply(self.project, MASK_CREATE, 'mask_create', params,
                               {'threshold': 0.3, 'lowpass': 15, 'extend': 2,
                                'soft_edge': 8, 'invert': True})
        self.assertEqual(result['params'], {
            prefix + 'ini_threshold': 0.3, prefix + 'lowpass': 15,
            prefix + 'extend_inimask': 2, prefix + 'width_soft_edge': 8,
            prefix + 'invert': True})

        # De novo masks have no map; a negative soft edge is an error
        with self.assertRaises(WizardError):
            Wizards.open(self.project, MASK_CREATE, 'mask_create',
                         {**params, 'relion_mask_create.denovo': True})
        with self.assertRaises(WizardError):
            Wizards.apply(self.project, MASK_CREATE, 'mask_create', params,
                          {'threshold': 0.3, 'soft_edge': -1})

    def test_pytom_template(self):
        import numpy as np
        from emwrap.base.volume_mask import spherical_mask

        # A ball of radius 20 px off-centre (+10 px in x) in a 120 px box, 2 A/px
        n = 120
        z, y, x = np.mgrid[:n, :n, :n]
        r = np.sqrt((x - 70) ** 2 + (y - 60) ** 2 + (z - 60) ** 2)
        self._write_mrc('Import/job001/map.mrc', (n, n, n), angpix=2.0,
                        data=(r < 20).astype(np.float32))
        T, M = 'pytom_create_template.', 'pytom_create_mask.'
        params = {T + 'input-map': 'Import/job001/map.mrc',
                  T + 'input-voxel-size-angstrom': '',
                  T + 'output-voxel-size-angstrom': '4',
                  T + 'box-size': '', T + 'center': True,
                  T + 'invert': True, T + 'mirror': False,
                  M + 'radius': '', M + 'sigma': ''}
        result = Wizards.open(self.project, PYTOM_TEMPLATE, 'template', params)
        self.assertEqual(result['ui'], 'PyTomTemplateWizard')
        inputs = result['inputs']
        self.assertEqual((inputs['in_angpix'], inputs['out_angpix'], inputs['box']),
                         (2.0, 4.0, 60))  # The map box at the output pixel size
        self.assertEqual(inputs['radius'], 24.0)
        data = result['data']
        self.assertEqual(data['template']['shape'], [60, 60, 60])
        self.assertEqual(data['binning'], 1)

        # Centred and inverted: the minimum at the centre of the box
        from emwrap.base.volume_mask import load_binned_map, compute_template
        binned = load_binned_map(self.project.join('Import/job001/map.mrc'))
        tmpl, f = compute_template(binned, 2.0, 4.0, 60, center=True, invert=True)
        com = [(g * (tmpl < -0.5)).sum() / (tmpl < -0.5).sum()
               for g in np.indices(tmpl.shape)]
        np.testing.assert_allclose(com, [29.5, 29.5, 29.5], atol=1)
        # A larger box is downscaled to at most 100 px
        tmpl, f = compute_template(binned, 2.0, 1.0, 240)
        self.assertEqual((f, tmpl.shape), (3, (80, 80, 80)))

        mask = spherical_mask(60, 20, sigma=2)
        self.assertEqual(mask[30, 30, 30], 1)
        self.assertTrue(((mask > 0.05) & (mask < 0.95)).any())

        result = Wizards.apply(self.project, PYTOM_TEMPLATE, 'template', params,
                               {**inputs, 'box': 64, 'radius': 40, 'sigma': 3})
        self.assertEqual(result['params'][T + 'box-size'], 64)
        self.assertEqual(result['params'][M + 'radius'], 40)
        self.assertEqual(result['params'][M + 'sigma'], 3)
        self.assertEqual(len(result['warnings']), 1)  # radius > box / 2
        with self.assertRaises(WizardError):
            Wizards.apply(self.project, PYTOM_TEMPLATE, 'template', params,
                          {**inputs, 'radius': 0})

    def test_class3d_mask(self):
        self._write_mrc('Refine3D/job010/run_class001.mrc', (40, 40, 40))
        self._write_mrc('MaskCreate/job011/mask.mrc', (40, 40, 40))
        self._write_mrc('MaskCreate/job012/mask.mrc', (32, 32, 32))
        ref = 'Refine3D/job010/run_class001.mrc'

        # Without a mask map: a sphere of the mask diameter
        params = {'fn_ref': ref, 'fn_mask': '', 'particle_diameter': '60'}
        result = Wizards.open(self.project, CLASS3D, 'mask', params)
        self.assertEqual(result['ui'], 'VolumeMaskWizard')
        self.assertEqual(result['inputs'], {'mask_type': 'sphere', 'diameter': 60})
        volume = result['data']['volume']
        self.assertEqual(volume['kind'], 'volume')
        self.assertEqual(volume['dimensions'], [40, 40, 40])
        self.assertEqual(volume['voxelSize'], 2)
        self.assertIsNone(result['data']['mask'])

        result = Wizards.apply(self.project, CLASS3D, 'mask', params,
                               {'diameter': 72.5})
        self.assertEqual(result['params'], {'particle_diameter': 72.5})
        result = Wizards.apply(self.project, CLASS3D, 'mask', params,
                               {'diameter': 100})
        self.assertEqual(len(result['warnings']), 1)  # Larger than the box
        with self.assertRaises(WizardError):
            Wizards.apply(self.project, CLASS3D, 'mask', params, {'diameter': 0})

        # Without a diameter: 80% of the box
        params['particle_diameter'] = ''
        result = Wizards.open(self.project, CLASS3D, 'mask', params)
        self.assertEqual(result['inputs']['diameter'], 64)

        # The mask map takes precedence, OK changes nothing
        params['fn_mask'] = 'MaskCreate/job011/mask.mrc'
        result = Wizards.open(self.project, CLASS3D, 'mask', params)
        self.assertEqual(result['inputs']['mask_type'], 'map')
        self.assertEqual(result['data']['mask']['path'], params['fn_mask'])
        result = Wizards.apply(self.project, CLASS3D, 'mask', params,
                               {'diameter': 50})
        self.assertEqual(result['params'], {})

        # A mask of another box, a missing mask or no map
        for p in [{'fn_mask': 'MaskCreate/job012/mask.mrc'},
                  {'fn_mask': 'missing.mrc'}, {'fn_ref': ''}]:
            with self.assertRaises(WizardError):
                Wizards.open(self.project, CLASS3D, 'mask', {**params, **p})

        # The references of a multi-reference classification (Volumes): the
        # mask over the first one
        from emtools.metadata import StarFile, Table
        refs = Table(['rlnReferenceImage'])
        refs.addRowValues(ref)
        refs.addRowValues('MaskCreate/job011/mask.mrc')
        os.makedirs(self.project.join('EMwrap/job013'), exist_ok=True)
        with StarFile(self.project.join('EMwrap/job013/input_references.star'), 'w') as sf:
            sf.writeTable('references', refs)
        params = {'fn_ref': 'EMwrap/job013/input_references.star', 'fn_mask': '',
                  'particle_diameter': '60'}
        result = Wizards.open(self.project, CLASS3D, 'mask', params)
        self.assertEqual(result['data']['volume']['path'], ref)
        self.assertEqual(len(result['warnings']), 1)

        # Refine3D has the same params and adaptor
        params = {'fn_ref': ref, 'fn_mask': '', 'particle_diameter': '60'}
        result = Wizards.open(self.project, REFINE3D, 'mask', params)
        self.assertEqual(result['inputs'], {'mask_type': 'sphere', 'diameter': 60})
        result = Wizards.apply(self.project, REFINE3D, 'mask', params,
                               {'diameter': 50})
        self.assertEqual(result['params'], {'particle_diameter': 50})

    def test_mtools_create_mask(self):
        self._write('Refine3D/job010/run_data.star', 'data_particles\n')
        self._write('Refine3D/job010/run_it012_data.star', 'data_particles\n')
        self._write_mrc('Refine3D/job010/run_it011_class001.mrc', (40, 40, 40))
        self._write_mrc('Refine3D/job010/run_it012_class001.mrc', (40, 40, 40))
        self._write_mrc('MaskCreate/job011/mask.mrc', (40, 40, 40))
        params = {'create_species.particles_relion': 'Refine3D/job010/run_data.star',
                  'create_species.mask': 'MaskCreate/job011/mask.mrc'}

        # Before the final volume: the last iteration
        result = Wizards.open(self.project, MTOOLS_CREATE, 'mask', params)
        self.assertEqual(result['ui'], 'VolumeMaskWizard')
        self.assertEqual(result['data']['volume']['path'],
                         'Refine3D/job010/run_it012_class001.mrc')
        self.assertEqual(result['data']['mask']['path'], params['create_species.mask'])
        self.assertEqual(Wizards.apply(self.project, MTOOLS_CREATE, 'mask',
                                       params, {})['params'], {})

        # The final volume, or the one of the given iteration
        self._write_mrc('Refine3D/job010/run_class001.mrc', (40, 40, 40))
        result = Wizards.open(self.project, MTOOLS_CREATE, 'mask', params)
        self.assertEqual(result['data']['volume']['path'],
                         'Refine3D/job010/run_class001.mrc')
        result = Wizards.open(self.project, MTOOLS_CREATE, 'mask', {
            **params, 'create_species.particles_relion': 'Refine3D/job010/run_it012_data.star'})
        self.assertEqual(result['data']['volume']['path'],
                         'Refine3D/job010/run_it012_class001.mrc')

        # Missing mask or particles, or particles without a volume
        self._write('Other/job020/run_data.star', 'data_particles\n')
        for p in [{'create_species.mask': ''},
                  {'create_species.mask': 'missing.mrc'},
                  {'create_species.particles_relion': ''},
                  {'create_species.particles_relion': 'missing_data.star'},
                  {'create_species.particles_relion': 'Other/job020/run_data.star'}]:
            with self.assertRaises(WizardError):
                Wizards.open(self.project, MTOOLS_CREATE, 'mask', {**params, **p})

    def test_geometry_mask(self):
        import numpy as np
        from emwrap.base.geometry_mask import parse_shapes, geometry_mask

        dims = [60, 50, 40]
        # A sphere: about 4/3 pi r^3 voxels, at the center of the box
        sphere = parse_shapes([{'shape': 'sphere', 'radius': 10}], dims)
        self.assertEqual(sphere[0]['center_x'], 30)
        self.assertAlmostEqual(geometry_mask(sphere, dims).sum(),
                               4 / 3 * np.pi * 1000, delta=50)
        # A cylinder tilted 90 degrees lies along x
        cyl = parse_shapes([{'shape': 'cylinder', 'radius': 4, 'height': 40,
                             'tilt': 90}], dims)
        z, y, x = np.nonzero(geometry_mask(cyl, dims))
        self.assertEqual((x.min(), x.max()), (10, 50))
        self.assertLessEqual(z.max() - z.min(), 8)
        # A box rotated 90 degrees (rot) swaps x and y
        box = parse_shapes([{'shape': 'box', 'size_x': 20, 'size_y': 10,
                             'size_z': 4, 'rot': 90}], dims)
        z, y, x = np.nonzero(geometry_mask(box, dims))
        self.assertEqual((x.max() - x.min(), y.max() - y.min()), (10, 20))
        with self.assertRaises(ValueError):
            parse_shapes([{'shape': 'cone'}], dims)
        # A soft edge of 6 px: 1 inside, a raised cosine down to 0 outside
        soft = geometry_mask(sphere, dims, soft_edge=6)
        self.assertTrue((soft[geometry_mask(sphere, dims) == 1] == 1).all())
        np.testing.assert_allclose(soft[20, 25, 40:47],
                                   [1, 0.933, 0.75, 0.5, 0.25, 0.067, 0], atol=0.001)

        # Wizard: the box of the volume, a sphere to start with
        self._write_mrc('Refine3D/job020/map.mrc', (40, 50, 300), angpix=2.5)
        params = {'input_volume': 'Refine3D/job020/map.mrc', 'shapes': '[]',
                  'soft_edge': '4'}
        result = Wizards.open(self.project, GEOMETRY_MASK, 'geometry_mask', params)
        self.assertEqual(result['ui'], 'GeometryMaskWizard')
        self.assertEqual(result['data']['dimensions'], [300, 50, 40])
        self.assertEqual(result['data']['binning'], 3)
        self.assertEqual(result['data']['mask']['shape'], [14, 17, 100])
        self.assertEqual(len(result['inputs']['shapes']), 1)
        self.assertEqual(result['inputs']['soft_edge'], 4)

        shapes = result['inputs']['shapes'] + [
            {'shape': 'box', 'center_x': 250, 'size_x': 30, 'size_y': 30, 'size_z': 30}]
        result = Wizards.apply(self.project, GEOMETRY_MASK, 'geometry_mask',
                               params, {'shapes': shapes, 'soft_edge': 3})
        self.assertEqual([s['shape'] for s in result['params']['shapes']],
                         ['sphere', 'box'])
        self.assertEqual(result['params']['soft_edge'], 3)
        with self.assertRaises(WizardError):  # A negative soft edge
            Wizards.apply(self.project, GEOMETRY_MASK, 'geometry_mask',
                          params, {'shapes': shapes, 'soft_edge': -1})
        with self.assertRaises(WizardError):  # A box without sizes
            Wizards.apply(self.project, GEOMETRY_MASK, 'geometry_mask',
                          params, {'shapes': [{'shape': 'box'}]})

        # Without volume: the box size of the form is required
        params = {'box_x': '32', 'box_y': '32', 'box_z': '', 'shapes': ''}
        with self.assertRaises(WizardError):
            Wizards.open(self.project, GEOMETRY_MASK, 'geometry_mask', params)
        params['box_z'] = '16'
        result = Wizards.open(self.project, GEOMETRY_MASK, 'geometry_mask', params)
        self.assertIsNone(result['data']['volume'])
        self.assertEqual(result['data']['dimensions'], [32, 32, 16])

    def test_errors(self):
        with self.assertRaises(WizardError):
            Wizards.open(self.project, WARP, 'export_box', {})
        with self.assertRaises(WizardError):
            Wizards.open(self.project, WARP, 'export_box',
                         {'input_tomograms': 'missing.star'})
        with self.assertRaises(WizardError):
            Wizards.open(self.project, WARP, 'unknown', {})

        params = {'input_tomograms': 'Tomograms/job001/tomograms.star'}
        with self.assertRaises(WizardError) as cm:
            Wizards.apply(self.project, WARP, 'export_box', params,
                          {'in_box': 0, 'out_angpix': -1, 'out_box': '',
                           'diameter': -5})
        self.assertEqual(len(cm.exception.errors), 4)


if __name__ == '__main__':
    unittest.main()
