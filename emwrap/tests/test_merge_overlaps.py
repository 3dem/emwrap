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

""" emw-merge-sets of particle sets without their overlaps (overlap_distance,
exclude_parts) and with different columns (common_columns), no datasets:

    python -m unittest emwrap.tests.test_merge_overlaps
"""

import os
import tempfile
import unittest

from emtools.metadata import StarFile

from emwrap.base.merge_sets import MergeSetsPipeline

TOMOGRAMS_STAR = """
data_global

loop_
_rlnTomoName
_rlnTomoTiltSeriesPixelSize
_rlnTomoSizeX
_rlnTomoSizeY
_rlnTomoSizeZ
TS_01 1.0 1000 1000 1000
TS_02 1.0 1000 1000 1000
"""

# A: x = 0, 100 (TS_01), 0 (TS_02); B: x = 5 (near A's 0), 300 (TS_01),
# without origin columns
A = ['TS_01 0 0 0 0', 'TS_01 100 0 0 0', 'TS_02 0 0 0 0']
B = ['TS_01 5 0 0 1', 'TS_01 300 0 0 2']


def _particles(rows, origin):
    cols = ['rlnTomoName', 'rlnCenteredCoordinateXAngst', 'rlnCenteredCoordinateYAngst',
            'rlnCenteredCoordinateZAngst']
    cols += ['rlnOriginXAngst'] if origin else ['rlnLCCmax']
    return ('\ndata_particles\n\nloop_\n' + ''.join(f'_{c}\n' for c in cols)
            + '\n'.join(rows) + '\n')


class TestMergeOverlaps(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)
        for name, rows, origin in (('A', A, True), ('B', B, False)):
            os.makedirs(name)
            self._write(f'{name}/tomograms.star', TOMOGRAMS_STAR)
            self._write(f'{name}/particles.star', _particles(rows, origin))
            self._write(f'{name}/optimisation_set.star',
                        '\ndata_optimisation_set\n\nloop_\n_rlnTomoParticlesFile\n'
                        f'_rlnTomoTomogramsFile\n{name}/particles.star {name}/tomograms.star\n')
        os.makedirs('Merge')

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    @staticmethod
    def _write(path, text):
        with open(path, 'w') as f:
            f.write(text)

    def _merge(self, **args):
        args = dict({'input_sets': ['A/optimisation_set.star', 'B/optimisation_set.star']},
                    **args)
        job = MergeSetsPipeline(args, os.path.abspath('Merge'))
        job.prerun()
        table = StarFile.getTableFromFile('particles', 'Merge/particles.star')
        return table, [(r.rlnTomoName, r.rlnCenteredCoordinateXAngst) for r in table]

    def test_overlaps(self):
        table, rows = self._merge(overlap_distance=10, common_columns=True)
        # A (all: A - B and A & B), then B - A; B's 5 is near A's 0
        self.assertEqual(rows, [('TS_01', 0), ('TS_01', 100), ('TS_02', 0),
                                ('TS_01', 300)])
        # Only the columns of both, and the origin (0 for B)
        self.assertEqual(table.getColumnNames()[-1], 'rlnOriginXAngst')
        self.assertEqual(table[3].rlnOriginXAngst, 0)

    def test_exclude_parts(self):
        # Only the overlaps (consensus)
        _, rows = self._merge(overlap_distance=10, common_columns=True,
                              exclude_parts=[{'tomoName': '*', 'parts': 'set1 set2'}])
        self.assertEqual(rows, [('TS_01', 0)])
        # TS_01 without A - B, TS_02 without anything of A
        _, rows = self._merge(overlap_distance=10, common_columns=True,
                              exclude_parts=[{'tomoName': 'TS_01', 'parts': 'set1'},
                                             {'tomoName': 'TS_02', 'parts': 'set1'}])
        self.assertEqual(rows, [('TS_01', 0), ('TS_01', 300)])

    def test_edge(self):
        # Box of 1000 A: B's 300 A is 200 A from the X edge, the others
        # at least 400 A; before the overlaps
        _, rows = self._merge(overlap_distance=10, common_columns=True, edge_distance=250)
        self.assertEqual(rows, [('TS_01', 0), ('TS_01', 100), ('TS_02', 0)])
        # Only the edges (no overlaps: B's 5 A is merged too)
        _, rows = self._merge(common_columns=True, edge_distance=250)
        self.assertEqual(rows, [('TS_01', 0), ('TS_01', 100), ('TS_02', 0),
                                ('TS_01', 5)])

    def test_errors(self):
        # Different columns without common_columns
        with self.assertRaises(Exception):
            self._merge(overlap_distance=10)
        # Parts without a distance, or invalid
        with self.assertRaises(Exception):
            self._merge(exclude_parts=[{'tomoName': '*', 'parts': 'set1'}])
        with self.assertRaises(Exception):
            self._merge(overlap_distance=10,
                        exclude_parts=[{'tomoName': '*', 'parts': 'set3'}])


if __name__ == '__main__':
    unittest.main()
