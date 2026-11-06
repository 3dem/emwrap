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

""" Handling of AreTomo3 failures and of the pixel size of the staged stacks
(no datasets nor GPUs needed, a script plays AreTomo3):

    python -m unittest emwrap.tests.test_aretomo3_errors
"""

import os
import shutil
import stat
import tempfile
import unittest

import mrcfile
import numpy as np

from emtools.jobs.batch_manager import Batch
from emtools.metadata import Acquisition, Table

from emwrap.aretomo.aretomo3 import AreTomo3
from emwrap.aretomo.aretomo3_pipeline import AreTomo3Pipeline
from emwrap.aretomo.aretomo3_modular import Aretomo3ModularBase

TS = 'TS_01'

# Fake AreTomo3 (Cmd 1): writes the outputs selected by the environment and
# exits with $AT3_EXIT, e.g. the CTF before crashing like a segfault
FAKE_ARETOMO3 = f"""#!/bin/bash
mkdir -p output/{TS}_Imod
touch output/{TS}_CTF.txt
[ -n "$AT3_ALN" ] && echo aln > output/{TS}.aln
[ -n "$AT3_XF" ] && echo "1 0 0 1 0 0" > output/{TS}_Imod/{TS}_st.xf
echo 0 > output/{TS}_Imod/{TS}_st.tlt
exit ${{AT3_EXIT:-0}}
"""


class TestAreTomo3Errors(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.launcher = os.path.join(self.tmp, 'aretomo3.sh')
        with open(self.launcher, 'w') as f:
            f.write(FAKE_ARETOMO3)
        os.chmod(self.launcher, os.stat(self.launcher).st_mode | stat.S_IEXEC)
        for var in ('AT3_EXIT', 'AT3_ALN', 'AT3_XF'):
            os.environ.pop(var, None)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def _run(self, **env):
        os.environ.update(env)
        path = os.path.join(self.tmp, 'batch')
        shutil.rmtree(path, ignore_errors=True)
        batch = Batch(id='b1', index=1, path=path, items=[], tsName=TS)
        batch.create()
        batch.mkdir('output')
        # The modular pipelines stage the tilt stack in the output folder
        with open(batch.join('output', f'{TS}.mrc'), 'w'):
            pass
        acq = Acquisition(pixel_size=1.35, voltage=300, cs=2.7,
                          amplitude_contrast=0.1)
        at3 = AreTomo3(acq, launcher_aretomo3=self.launcher,
                       **{'aretomo3.OutImod': '1', 'aretomo3.SplitSum': '0'})
        at3.process_batch(batch, gpu=0, cmd=1, input_prefix=f'./{TS}',
                          input_suffix='.mrc', ts_name=TS)
        return batch, batch['results'][0]

    def test_exit_code(self):
        """ A crash fails the tilt series even with all its outputs. """
        batch, result = self._run(AT3_EXIT='139', AT3_ALN='1', AT3_XF='1')
        self.assertIn('exit code 139', result['error'])
        self.assertEqual(batch.info['aretomo_exit_code'], 139)

    def test_missing_imod_xf(self):
        _, result = self._run(AT3_ALN='1')
        self.assertIn(f'{TS}_st.xf', result['error'])

    def test_success(self):
        _, result = self._run(AT3_ALN='1', AT3_XF='1')
        self.assertNotIn('error', result)
        self.assertTrue(result['at3ImodFolder'].endswith(f'{TS}_Imod'))


class _Pipeline(AreTomo3Pipeline):
    """ Pipeline without a job folder, only to register outputs. """
    def __init__(self, names, results):
        self.inputTsTable = Table(['rlnTomoName', 'rlnTomoTiltSeriesStarFile'])
        for name in names:
            self.inputTsTable.addRowValues(rlnTomoName=name,
                                           rlnTomoTiltSeriesStarFile=f'{name}.star')
        self._allResults = results
        self.tables = {}
        self.nodes = None

    def log(self, msg, flush=False):
        pass

    def join(self, *paths):
        return os.path.join('job', *paths)

    def _registeredTsPs(self, tsRow):
        return 1.35

    def write_ts_table(self, tableName, table, starFile):
        self.tables[os.path.basename(starFile)] = [r.rlnTomoName for r in table]

    def writeRelionOutputNodes(self, nodes):
        self.nodes = nodes

    def _registerAretomo3CsvOutputs(self):
        pass

    def _registerTsOutputs(self, tsName, tsRow, result, newTsPs,
                           alignedTable, tomogramsTable):
        if result.get('broken'):
            raise FileNotFoundError(f'{tsName}_st.xf')
        values = {c: '' for c in alignedTable.getColumnNames()}
        values.update(tsRow._asdict())
        alignedTable.addRowValues(**values)
        return False


class TestAreTomo3Outputs(unittest.TestCase):
    def test_register_isolates_failures(self):
        """ A tilt series failing to register does not lose the others. """
        results = {'TS_01': {}, 'TS_02': {'broken': True},
                   'TS_03': {}, 'TS_04': {'error': 'exit code 1'}}
        p = _Pipeline(results, results)
        p._registerOutputs()
        self.assertEqual(p.tables['aligned_tilt_series.star'], ['TS_01', 'TS_03'])
        self.assertEqual(p.tables['failed_tilt_series.star'], ['TS_02', 'TS_04'])
        self.assertIn('st.xf', results['TS_02']['error'])
        # Once failed, it is not registered again
        p._registerOutputs()
        self.assertEqual(p.tables['aligned_tilt_series.star'], ['TS_01', 'TS_03'])

    def test_postrun(self):
        p = _Pipeline([], {'TS_01': {}, 'TS_02': {'error': 'exit code 1'}})
        p.postrun()  # Partial failure, the job finishes
        p = _Pipeline([], {'TS_01': {'error': 'x'}, 'TS_02': {'error': 'y'}})
        with self.assertRaisesRegex(Exception, 'All 2 tilt series failed'):
            p.postrun()
        _Pipeline([], {}).postrun()  # Nothing to process yet


class TestStackPixelSize(unittest.TestCase):
    """ AreTomo3 reads the pixel size of an MRC input from its header. """
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_write_stack(self):
        table = Table(['rlnMicrographName', 'rlnTomoNominalStageTiltAngle',
                       'rlnTomoTiltMovieIndex'])
        for i, angle in enumerate((3.0, -3.0, 0.0), start=1):
            fn = os.path.join(self.tmp, f'tilt{i}.mrc')
            with mrcfile.new(fn) as mrc:
                mrc.set_data(np.full((4, 6), i, dtype=np.float32))
            table.addRowValues(rlnMicrographName=fn,
                               rlnTomoNominalStageTiltAngle=angle,
                               rlnTomoTiltMovieIndex=i)
        stack = os.path.join(self.tmp, f'{TS}.mrc')
        base = object.__new__(Aretomo3ModularBase)
        base._write_stack_from_images(stack, table, pixel_size=1.35)
        with mrcfile.open(stack) as mrc:
            self.assertAlmostEqual(float(mrc.voxel_size.x), 1.35, places=4)
            # Sorted by tilt angle
            self.assertEqual([int(z[0, 0]) for z in mrc.data], [2, 3, 1])

        # Stacks from previous runs, without pixel size
        with mrcfile.new(stack, overwrite=True) as mrc:
            mrc.set_data(np.zeros((2, 4, 6), dtype=np.float32))
        Aretomo3ModularBase._set_stack_pixel_size(stack, 2.7)
        with mrcfile.open(stack) as mrc:
            self.assertAlmostEqual(float(mrc.voxel_size.x), 2.7, places=4)
            self.assertEqual(mrc.data.shape, (2, 4, 6))


if __name__ == '__main__':
    unittest.main()
