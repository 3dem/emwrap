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

    def _write_mrc(self, rel, shape, angpix=2.0):
        import mrcfile
        import numpy as np
        fn = self.project.join(rel)
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        with mrcfile.new(fn, overwrite=True) as mrc:
            mrc.set_data(np.zeros(shape, dtype=np.float32))
            mrc.voxel_size = angpix

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

        # Refine3D has the same params and adaptor
        params = {'fn_ref': ref, 'fn_mask': '', 'particle_diameter': '60'}
        result = Wizards.open(self.project, REFINE3D, 'mask', params)
        self.assertEqual(result['inputs'], {'mask_type': 'sphere', 'diameter': 60})
        result = Wizards.apply(self.project, REFINE3D, 'mask', params,
                               {'diameter': 50})
        self.assertEqual(result['params'], {'particle_diameter': 50})

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
