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

""" The 'Remove duplicate particles' mode of emw-subset-ts and the duplicates
rule of emw-coordinates-compare (no datasets needed):

    python -m unittest emwrap.tests.test_subset_duplicates
"""

import os
import tempfile
import unittest

import numpy as np

from emtools.metadata import StarFile

from emwrap.base.coordinates_compare import duplicates
from emwrap.base.subset_ts import SubsetTsPipeline

TOMOGRAMS_STAR = """
data_global

loop_
_rlnTomoName
_rlnTomoTiltSeriesPixelSize
_rlnTomoTomogramBinning
_rlnTomoSizeX
_rlnTomoSizeY
_rlnTomoSizeZ
TS_01 1.0 10.0 1000 1000 100
TS_02 1.0 10.0 1000 1000 100
"""

OPTIMISATION_SET = """
data_optimisation_set

loop_
_rlnTomoParticlesFile
_rlnTomoTomogramsFile
Input/particles.star Input/tomograms.star
"""


def _particles(rows, score=True):
    """ particles.star text: (tomo, x, y, z, originX, score) rows, in A. """
    cols = ['rlnTomoName', 'rlnCenteredCoordinateXAngst', 'rlnCenteredCoordinateYAngst',
            'rlnCenteredCoordinateZAngst', 'rlnOriginXAngst']
    cols += ['rlnLCCmax'] if score else []
    lines = [' '.join(str(v) for v in (r if score else r[:-1])) for r in rows]
    return ('\ndata_particles\n\nloop_\n' + ''.join(f'_{c}\n' for c in cols)
            + '\n'.join(lines) + '\n')


# TS_01: a chain 0 - 10 - 20 A (the last one with a 5 A shift: at 15 A),
# and one far away; TS_02: a single one
ROWS = [('TS_01', 0, 0, 0, 0, 1), ('TS_01', 10, 0, 0, 0, 9),
        ('TS_01', 20, 0, 0, 5, 2), ('TS_01', 500, 0, 0, 0, 1),
        ('TS_02', 0, 0, 0, 0, 1)]


class TestSubsetDuplicates(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)
        os.makedirs('Input')
        os.makedirs('Subset')
        self._write('Input/tomograms.star', TOMOGRAMS_STAR)
        self._write('Input/optimisation_set.star', OPTIMISATION_SET)

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    @staticmethod
    def _write(path, text):
        with open(path, 'w') as f:
            f.write(text)

    def _subset(self, distance, score=True, edge=None):
        self._write('Input/particles.star', _particles(ROWS, score))
        job = SubsetTsPipeline({'input_set': 'Input/optimisation_set.star',
                                'mode': '5',
                                'subset_duplicates_distance': distance,
                                'subset_edge_distance': edge},
                               os.path.abspath('Subset'))
        job.prerun()
        table = StarFile.getTableFromFile('particles', 'Subset/particles.star')
        return [(r.rlnTomoName, r.rlnCenteredCoordinateXAngst) for r in table]

    def test_duplicates(self):
        x = np.array([0, 10, 20], float)
        d = np.abs(x[:, None] - x[None])
        self.assertEqual(duplicates(d, 15).tolist(), [True, True, False])
        self.assertEqual(duplicates(d, 15, [1, 5, 1]).tolist(), [True, False, True])
        self.assertEqual(duplicates(d, 15, [5, 1, 5]).tolist(), [False, True, False])
        self.assertEqual(duplicates(d, 15, [2, 2, 2]).tolist(), [False, True, False])

    def test_score(self):
        # 10 A has the highest score: 0 A (10 A away) and the shifted one
        # (at 15 A, 5 A away) are its duplicates
        self.assertEqual(self._subset(12),
                         [('TS_01', 10), ('TS_01', 500), ('TS_02', 0)])
        # At 8 A, only the shifted one (the shift is used)
        self.assertEqual(self._subset(8),
                         [('TS_01', 0), ('TS_01', 10), ('TS_01', 500), ('TS_02', 0)])
        # At 4 A, nothing is a duplicate
        self.assertEqual(len(self._subset(4)), 5)

    def test_no_score(self):
        # Relion's rule: a later one within the distance, so 0 A and 10 A
        self.assertEqual(self._subset(12, score=False),
                         [('TS_01', 20), ('TS_01', 500), ('TS_02', 0)])

    def test_edge(self):
        # Box 1000 x 1000 x 100 A: 500 A (TS_01) is at the X edge, the
        # others 50 A from the Z edges
        self.assertEqual(self._subset(None, edge=10),
                         [('TS_01', 0), ('TS_01', 10), ('TS_01', 20), ('TS_02', 0)])
        # The edges first: at 12 A, the duplicates of the others
        self.assertEqual(self._subset(12, edge=10),
                         [('TS_01', 10), ('TS_02', 0)])
        # Every particle is within 60 A of the Z edges
        with self.assertRaises(Exception):
            self._subset(None, edge=60)

    def test_bad_distance(self):
        # Neither a duplicates nor an edge distance
        with self.assertRaises(Exception):
            SubsetTsPipeline({'input_set': 'Input/optimisation_set.star',
                              'mode': '5'}, os.path.abspath('Subset'))


if __name__ == '__main__':
    unittest.main()
