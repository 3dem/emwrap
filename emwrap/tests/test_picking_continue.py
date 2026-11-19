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
Streaming and continue behaviour of PickingPipeline (base of emw-pytom and
emw-pytme), with a fake picker instead of a template matching program:
input tomograms arriving while the job runs, failed tomograms kept in
failed_logs and, on continue, picked and failed tomograms skipped while
the remaining and new ones are picked.
"""

import os
import tempfile
import threading
import time
import unittest

import mrcfile
import numpy as np

from emtools.metadata import StarFile, Table
from emwrap.base import PickingPipeline

GLOBAL_COLUMNS = ['rlnTomoName', 'rlnTomoTiltSeriesStarFile', 'rlnVoltage',
                  'rlnSphericalAberration', 'rlnAmplitudeContrast',
                  'rlnTomoTiltSeriesPixelSize', 'rlnTomoTomogramBinning',
                  'rlnTomoReconstructedTomogram']

RELION5 = """
data_particles

loop_
_rlnCenteredCoordinateXAngst
_rlnCenteredCoordinateYAngst
_rlnCenteredCoordinateZAngst
_rlnAngleRot
_rlnAngleTilt
_rlnAnglePsi
_rlnLCCmax
_rlnTomoName
10.0 20.0 30.0 1.0 2.0 3.0 0.5 tomo.mrc
-10.0 0.0 5.0 4.0 5.0 6.0 0.4 tomo.mrc
"""

RELION4 = """
data_particles

loop_
_rlnCoordinateX
_rlnCoordinateY
_rlnCoordinateZ
1 2 3
4 5 6
"""


class FakePicker:
    """ Write fake coordinates files, or fail for the tomograms in FAIL. """
    FAIL = set()
    picked = []  # tomogram names, in picking order (all runs)

    def __init__(self, acq, args):
        pass

    def process_batch(self, batch, launcher=None):
        tsName = batch['tsName']
        os.makedirs(batch.join('output'), exist_ok=True)
        with open(batch.join('batch.log'), 'w') as f:
            f.write(f"picking {tsName}\n")
        if tsName in self.FAIL:
            raise Exception(f"fake picking error for {tsName}")
        for suffix, text in [('particles_relion5.star', RELION5),
                             ('particles_default.star', RELION4)]:
            with open(batch.join('output', f'{tsName}_{suffix}'), 'w') as f:
                f.write(text)
        FakePicker.picked.append(tsName)


class FakePickingPipeline(PickingPipeline):
    name = 'emw-fake-picking'
    LABEL = 'Fake'

    def _createPicker(self, args, gpu):
        return FakePicker(self.acq, args)


class TestPickingContinue(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)
        os.makedirs('Input')
        os.makedirs('Picking')
        with mrcfile.new('Input/tomo.mrc') as mrc:
            mrc.set_data(np.zeros((4, 8, 8), dtype=np.float32))
        self.names = []
        FakePicker.FAIL = {'TS_02'}
        FakePicker.picked = []

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def _addInput(self, *names):
        """ Add tomograms to the input STAR file, re-writing it, as an
        upstream job running on the fly would. """
        for name in names:
            tsStar = f'Input/{name}.star'
            t = Table(['rlnTomoNominalStageTiltAngle', 'rlnMicrographPreExposure'])
            for angle, dose in [(-3, 3), (0, 0), (3, 6)]:
                t.addRowValues(angle, dose)
            with StarFile(tsStar, 'w') as sf:
                sf.writeTable(name, t)
            self.names.append(name)
        t = Table(GLOBAL_COLUMNS)
        for name in self.names:
            t.addRowValues(name, f'Input/{name}.star', 300, 2.7, 0.1, 1.0, 10,
                           'Input/tomo.mrc')
        with StarFile('Input/tomograms.star', 'w') as sf:
            sf.writeTable('global', t)

    def _addLater(self, delay, *names):
        th = threading.Thread(target=lambda: (time.sleep(delay),
                                              self._addInput(*names)))
        th.start()
        return th

    def _run(self):
        job = FakePickingPipeline({'input_tomograms': 'Input/tomograms.star',
                                   'gpus': '2',
                                   'launcher': 'fake',
                                   'wait.timeout': 3,
                                   'wait.sleep': 1}, 'Picking')
        job.run()

    @staticmethod
    def _names(fn, table):
        if not os.path.exists(fn):
            return []
        return [r.rlnTomoName for r in StarFile.getTableFromFile(table, fn)]

    def _check(self, picked, failed):
        tomos = self._names('Picking/tomograms.star', 'global')
        self.assertEqual(sorted(tomos), sorted(picked))
        self.assertEqual(len(tomos), len(set(tomos)), "duplicated tomograms")
        self.assertEqual(sorted(self._names('Picking/failed_tomograms.star',
                                            'global')), sorted(failed))
        particles = self._names('Picking/particles.star', 'particles')
        self.assertEqual(sorted(particles), sorted(n for n in picked for _ in (1, 2)))
        kept = os.listdir('Picking/failed_logs') if failed else []
        self.assertEqual(sorted(k.split('_', 2)[2] for k in kept), sorted(failed))
        for k in kept:  # the failed batch folders keep their logs
            self.assertTrue(os.path.exists(f'Picking/failed_logs/{k}/batch.log'))

    def test_streaming_and_continue(self):
        # First run: 3 tomograms (one fails) and 1 more arriving while running
        self._addInput('TS_01', 'TS_02', 'TS_03')
        th = self._addLater(2, 'TS_04')
        self._run()
        th.join()
        self._check(picked=['TS_01', 'TS_03', 'TS_04'], failed=['TS_02'])
        self.assertEqual(sorted(FakePicker.picked), ['TS_01', 'TS_03', 'TS_04'])

        # Continue: picked and failed tomograms are skipped, the new ones
        # (before starting and while running) are picked
        FakePicker.picked = []
        self._addInput('TS_05')
        th = self._addLater(2, 'TS_06', 'TS_07')
        self._run()
        th.join()
        self.assertEqual(sorted(FakePicker.picked), ['TS_05', 'TS_06', 'TS_07'])
        self._check(picked=['TS_01', 'TS_03', 'TS_04', 'TS_05', 'TS_06', 'TS_07'],
                    failed=['TS_02'])

        # Continue without new tomograms: nothing is picked
        FakePicker.picked = []
        self._run()
        self.assertEqual(FakePicker.picked, [])
        self._check(picked=['TS_01', 'TS_03', 'TS_04', 'TS_05', 'TS_06', 'TS_07'],
                    failed=['TS_02'])

    def test_continue_repicks_unregistered(self):
        """ A tomogram whose particles were written but that was not
        registered (e.g. job killed in between) is picked again, without
        duplicated particles. """
        self._addInput('TS_01', 'TS_03')
        self._run()
        # Remove TS_03 from tomograms.star, keeping its particles
        t = StarFile.getTableFromFile('global', 'Picking/tomograms.star',
                                      guessType=False)
        keep = Table(t.getColumnNames())
        for row in t:
            if row.rlnTomoName != 'TS_03':
                keep.addRowValues(**row._asdict())
        with StarFile('Picking/tomograms.star', 'w') as sf:
            sf.writeTable('global', keep)

        FakePicker.picked = []
        self._run()
        self.assertEqual(FakePicker.picked, ['TS_03'])
        self._check(picked=['TS_01', 'TS_03'], failed=[])


if __name__ == '__main__':
    unittest.main()
