"""
Particle coordinates written for MTools create_species: pixels of the
rlnImagePixelSize of the optics table (what MTools uses without
--angpix_coords), from Relion 5 centered Angstrom coordinates.

    python -m unittest emwrap.tests.test_mtools_particles
"""

import unittest

from emtools.metadata import Table

from emwrap.warp.warp_mtools_create import WarpMtoolsCreate


class TestMtoolsParticles(unittest.TestCase):

    def setUp(self):
        self.job = WarpMtoolsCreate.__new__(WarpMtoolsCreate)
        tomos = Table(['rlnTomoName', 'rlnTomoTiltSeriesPixelSize',
                       'rlnTomoTomogramBinning', 'rlnTomoSizeX',
                       'rlnTomoSizeY', 'rlnTomoSizeZ', 'wrpTomostar'])
        tomos.addRowValues('TS_45', 1.35, 6.0, 4000, 4000, 2000,
                           'WarpCtfRec/job038/warp_tomostar/TS_45.tomostar')
        self.lookup = {r.rlnTomoName: r for r in tomos}
        # Particles exported by Warp at 4 A/px, after Refine3D (job063)
        self.optics = Table(['rlnOpticsGroup', 'rlnImagePixelSize',
                             'rlnTomoSubtomogramBinning'])
        self.optics.addRowValues(1, 4.0, 5.925926)

    def test_centered_angst(self):
        particles = Table(['rlnTomoName', 'rlnCenteredCoordinateXAngst',
                           'rlnCenteredCoordinateYAngst',
                           'rlnCenteredCoordinateZAngst', 'rlnOpticsGroup',
                           'rlnOriginXAngst'])
        particles.addRowValues('TS_45', 575.1, -1385.1, 534.6, 1, 5.77)
        out = self.job._convertParticlesTableToWarp(
            particles, self.lookup, self.job._opticsPixelSizes(self.optics))
        row = next(iter(out))
        # Same pixels as Warp ts_export_particles wrote at 4 A/px
        self.assertAlmostEqual(row.rlnCoordinateX, 818.775, places=3)
        self.assertAlmostEqual(row.rlnCoordinateY, 328.725, places=3)
        self.assertAlmostEqual(row.rlnCoordinateZ, 471.150, places=3)
        self.assertEqual(row.rlnTomoName, 'TS_45.tomostar')
        self.assertAlmostEqual(row.rlnOriginXAngst, 5.77)
        self.assertFalse(out.hasColumn('rlnCenteredCoordinateXAngst'))

    def test_pixel_coordinates_kept(self):
        particles = Table(['rlnTomoName', 'rlnCoordinateX', 'rlnCoordinateY',
                           'rlnCoordinateZ', 'rlnOpticsGroup'])
        particles.addRowValues('TS_45', 818.775, 328.725, 471.15, 1)
        out = self.job._convertParticlesTableToWarp(
            particles, self.lookup, self.job._opticsPixelSizes(self.optics))
        row = next(iter(out))
        self.assertEqual((row.rlnCoordinateX, row.rlnCoordinateY, row.rlnCoordinateZ),
                         (818.775, 328.725, 471.15))

    def test_missing_pixel_size(self):
        optics = Table(['rlnOpticsGroup'])
        optics.addRowValues(1)
        with self.assertRaises(Exception):
            self.job._opticsPixelSizes(optics)


if __name__ == '__main__':
    unittest.main()
