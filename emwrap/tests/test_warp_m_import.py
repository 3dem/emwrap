"""
Importing the M folder of a previous Warp job: the sources metadata
(tilt-series XML, .source and versions) is a private copy, since MCore
rewrites it in place, while the image data folders stay linked.

    python -m unittest emwrap.tests.test_warp_m_import
"""

import os
import shutil
import tempfile
import unittest

from emwrap.warp.warp import WarpBasePipeline


class TestWarpMImport(unittest.TestCase):

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix='emwrap_m_import_')
        self.prev = os.path.join(self.root, 'WarpMCore', 'job082')
        self.job = os.path.join(self.root, 'WarpMCore', 'job068')
        m = os.path.join(self.prev, 'm')
        ts = os.path.join(m, 'sources', 'HIV', 'warp_tiltseries')
        for d in ('species/HIV_1', 'versions/v1', 'logs'):
            os.makedirs(os.path.join(ts if d != 'species/HIV_1' else m, d))
        os.makedirs(os.path.join(ts, 'tiltstack', 'TS_45'))
        os.makedirs(os.path.join(ts, 'versions', 'v1'), exist_ok=True)
        self._write(os.path.join(m, 'HIV.population'), 'pop')
        self._write(os.path.join(m, 'species', 'HIV_1', 'HIV.species'), 'species')
        self._write(os.path.join(ts, 'TS_45.xml'), 'defocus=3.0')
        self._write(os.path.join(ts, 'HIV.source'), 'source')
        self._write(os.path.join(ts, 'versions', 'v1', 'TS_45.xml'), 'old')
        self._write(os.path.join(ts, 'logs', 'TS_45.log'), 'log')
        self._write(os.path.join(ts, 'tiltstack', 'TS_45', 'TS_45.st'), 'data')
        # A folder that is already a link in the previous job
        frames = os.path.join(self.root, 'WarpExportParticles', 'job040',
                              'warp_frameseries')
        os.makedirs(frames)
        os.symlink(os.path.relpath(frames, os.path.join(m, 'sources', 'HIV')),
                   os.path.join(m, 'sources', 'HIV', 'warp_frameseries'))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    @staticmethod
    def _write(path, text):
        with open(path, 'w') as f:
            f.write(text)

    @staticmethod
    def _read(path):
        with open(path) as f:
            return f.read()

    def test_sources_copied(self):
        WarpBasePipeline.importInputs(self.prev, self.job, keys=['m'])
        sources = os.path.join(self.job, 'm', 'sources')
        ts = os.path.join(sources, 'HIV', 'warp_tiltseries')
        self.assertFalse(os.path.islink(sources))
        self.assertFalse(os.path.islink(os.path.join(ts, 'TS_45.xml')))
        self.assertTrue(os.path.islink(os.path.join(ts, 'tiltstack')))
        self.assertTrue(os.path.exists(os.path.join(ts, 'tiltstack', 'TS_45', 'TS_45.st')))
        self.assertEqual(self._read(os.path.join(ts, 'versions', 'v1', 'TS_45.xml')), 'old')
        self.assertEqual(os.listdir(os.path.join(ts, 'logs')), [])
        frames = os.path.join(sources, 'HIV', 'warp_frameseries')
        self.assertTrue(os.path.islink(frames) and os.path.isdir(frames))

        # MCore rewriting the copy leaves the previous job untouched
        self._write(os.path.join(ts, 'TS_45.xml'), 'defocus=2.7')
        self._write(os.path.join(ts, 'HIV.source'), 'new')
        prevTs = os.path.join(self.prev, 'm', 'sources', 'HIV', 'warp_tiltseries')
        self.assertEqual(self._read(os.path.join(prevTs, 'TS_45.xml')), 'defocus=3.0')
        self.assertEqual(self._read(os.path.join(prevTs, 'HIV.source')), 'source')

    def test_replaces_old_link(self):
        """ A job run before the fix has m/sources linked: it is replaced. """
        os.makedirs(os.path.join(self.job, 'm'))
        os.symlink(os.path.relpath(os.path.join(self.prev, 'm', 'sources'),
                                   os.path.join(self.job, 'm')),
                   os.path.join(self.job, 'm', 'sources'))
        WarpBasePipeline.importInputs(self.prev, self.job, keys=['m'])
        self.assertFalse(os.path.islink(os.path.join(self.job, 'm', 'sources')))


if __name__ == '__main__':
    unittest.main()
