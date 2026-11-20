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
Paths persisted in the job outputs (STAR files, info.json, other JSON files)
by ProcessingPipeline: projectPath, relativizePaths and relativizeStar make
them relative to the project root when inside it, keeping absolute the ones
outside it (programs, external inputs).
"""

import os
import tempfile
import unittest

from emtools.metadata import StarFile, Table
from emwrap.base import ProcessingPipeline


class TestProjectPaths(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        # realpath: on macOS the tmp folder is itself behind a symlink
        self.base = os.path.realpath(self.tmp.name)
        self.root = os.path.join(self.base, 'project')
        self.outside = os.path.join(self.base, 'external')
        os.makedirs(os.path.join(self.root, 'Jobs', 'job001'))
        os.makedirs(self.outside)
        self.job = self._pipeline(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def _pipeline(self, root):
        return ProcessingPipeline({'working_dir': root,
                                   'scratch_dir': self.base},
                                  os.path.join(root, 'Jobs', 'job001'))

    def _in(self, *parts):
        return os.path.join(self.root, *parts)

    # ------------------------------------------------------------------
    # projectPath
    # ------------------------------------------------------------------
    def test_empty(self):
        for value in (None, ''):
            self.assertIs(self.job.projectPath(value), value)

    def test_relative(self):
        p = self.job.projectPath
        self.assertEqual(p('Jobs/job001/out.star'), 'Jobs/job001/out.star')
        self.assertEqual(p('./Jobs//job001/../job001/out.star'),
                         'Jobs/job001/out.star')
        self.assertEqual(p('.'), '.')

    def test_relative_outside(self):
        """ A relative path going out of the project becomes absolute,
        like the os.path.abspath(...) it replaces did. """
        self.assertEqual(self.job.projectPath('../external/config.json'),
                         os.path.join(self.outside, 'config.json'))

    def test_inside(self):
        p = self.job.projectPath
        self.assertEqual(p(self._in('Jobs', 'job001', 'out.star')),
                         'Jobs/job001/out.star')
        self.assertEqual(p(self._in('Jobs', '.', 'job001', '')), 'Jobs/job001')
        self.assertEqual(p(self.root), '.')

    def test_outside(self):
        p = self.job.projectPath
        prog = os.path.join(self.outside, 'bin', 'program')
        self.assertEqual(p(prog), prog)
        self.assertEqual(p('/usr/bin/env'), '/usr/bin/env')
        # A sibling folder sharing the root's name prefix is outside
        sibling = self.root + '_old/file.star'
        self.assertEqual(p(sibling), sibling)
        # toProjectPath, unchanged, makes it a '..' path instead
        self.assertEqual(self.job.toProjectPath(prog),
                         '../external/bin/program')

    def test_symlinked_root(self):
        """ The same project reached through another mount (a symlink):
        paths through either side are inside the project. """
        link = os.path.join(self.base, 'mount')
        os.symlink(self.root, link)
        star = 'Jobs/job001/out.star'
        # Project opened through the symlink, path given with the real one
        job = self._pipeline(link)
        self.assertEqual(job.projectPath(self._in(star)), star)
        self.assertEqual(job.projectPath(os.path.join(link, star)), star)
        # Project opened with the real path, path given through the symlink
        self.assertEqual(self.job.projectPath(os.path.join(link, star)), star)

    def test_mapping(self):
        tmpFile = os.path.join(self.outside, 'scratch', 'batch', 'out.json')
        mapping = {tmpFile: 'Jobs/job001/out.json'}
        p = self.job.projectPath
        self.assertEqual(p(tmpFile, mapping), 'Jobs/job001/out.json')
        # Keys are compared after resolving symlinks
        link = os.path.join(self.base, 'scratch_link')
        os.symlink(os.path.join(self.outside, 'scratch'), link)
        self.assertEqual(p(os.path.join(link, 'batch', 'out.json'), mapping),
                         'Jobs/job001/out.json')
        # Not in the mapping: usual rules
        other = os.path.join(self.outside, 'scratch', 'other.json')
        self.assertEqual(p(other, mapping), other)
        self.assertEqual(p(self._in('a.star'), mapping), 'a.star')

    def test_cwd_relative_same_as_abspath(self):
        """ The pipelines run with the project root as current folder, so
        projectPath(p) gives what relativizePaths(os.path.abspath(p)) did. """
        cwd = os.getcwd()
        os.chdir(self.root)
        try:
            for path in ('config.json', 'Jobs/job001/../job001/x.json',
                         '../external/config.json'):
                self.assertEqual(self.job.projectPath(path),
                                 self.job.relativizePaths(os.path.abspath(path)))
        finally:
            os.chdir(cwd)

    # ------------------------------------------------------------------
    # relativizePaths
    # ------------------------------------------------------------------
    def test_relativize_nested(self):
        prog = os.path.join(self.outside, 'program')
        data = {
            'star': self._in('Jobs', 'job001', 'out.star'),
            'program': prog,
            'relative': 'Jobs//job001/./x.mrc',
            'url': 'http://host//x',
            'number': 1.5,
            'none': None,
            'list': [self._in('a.mrc'), 'b.mrc', [self._in('c', 'd.mrc')]],
            'tuple': (self._in('e.mrc'), prog),
            'dict': {'inner': {'path': self._in('f.mrc')}},
        }
        result = self.job.relativizePaths(data)
        self.assertEqual(result, {
            'star': 'Jobs/job001/out.star',
            'program': prog,
            # Relative strings are not touched: not every string is a path
            'relative': 'Jobs//job001/./x.mrc',
            'url': 'http://host//x',
            'number': 1.5,
            'none': None,
            'list': ['a.mrc', 'b.mrc', ['c/d.mrc']],
            'tuple': ('e.mrc', prog),
            'dict': {'inner': {'path': 'f.mrc'}},
        })
        self.assertIsInstance(result['tuple'], tuple)
        # The input is not modified
        self.assertEqual(data['star'], self._in('Jobs', 'job001', 'out.star'))
        # A single string
        self.assertEqual(self.job.relativizePaths(self._in('g.mrc')), 'g.mrc')

    def test_relativize_mapping(self):
        batch = os.path.join(self.outside, 'scratch', 'batch_001')
        mapping = {os.path.join(batch, 'output'): 'Jobs/job001/Coordinates',
                   os.path.join(batch, 'tomo.mrc'): 'Input/tomo.mrc'}
        data = {'output': os.path.join(batch, 'output'),
                'inputs': [os.path.join(batch, 'tomo.mrc'),
                           os.path.join(batch, 'tmp.log')]}
        self.assertEqual(self.job.relativizePaths(data, mapping), {
            'output': 'Jobs/job001/Coordinates',
            'inputs': ['Input/tomo.mrc', os.path.join(batch, 'tmp.log')]})

    # ------------------------------------------------------------------
    # relativizeStar
    # ------------------------------------------------------------------
    def _writeStar(self, starFile, rows):
        t = Table(['rlnTomoName', 'rlnMicrographName', 'rlnTomoYTilt'])
        for row in rows:
            t.addRowValues(*row)
        with StarFile(starFile, 'w') as sf:
            sf.writeTable('global', t)

    def test_relativize_star(self):
        starFile = self._in('Jobs', 'job001', 'tilt_series.star')
        external = os.path.join(self.outside, 'movie.mrc')
        self._writeStar(starFile, [
            ('TS_01', self._in('Jobs', 'job001', 'TS_01.mrc'), 1.5),
            ('TS_02', external, -2.0),
            ('TS_03', 'Input/TS_03.mrc', 0.0),
        ])
        self.assertTrue(self.job.relativizeStar(starFile))

        table = StarFile.getTableFromFile('global', starFile)
        self.assertEqual([(r.rlnTomoName, r.rlnMicrographName, float(r.rlnTomoYTilt))
                          for r in table], [
            ('TS_01', 'Jobs/job001/TS_01.mrc', 1.5),
            ('TS_02', external, -2.0),
            ('TS_03', 'Input/TS_03.mrc', 0.0),
        ])
        # Nothing left to change: the file is not written again
        mtime = os.stat(starFile).st_mtime_ns
        self.assertFalse(self.job.relativizeStar(starFile))
        self.assertEqual(os.stat(starFile).st_mtime_ns, mtime)

    def test_relativize_star_mapping(self):
        starFile = self._in('Jobs', 'job001', 'out.star')
        tmpFile = os.path.join(self.outside, 'scratch', 'TS_01.mrc')
        self._writeStar(starFile, [('TS_01', tmpFile, 0.0)])
        self.assertTrue(self.job.relativizeStar(
            starFile, {tmpFile: 'Jobs/job001/TS_01.mrc'}))
        row = StarFile.getTableFromFile('global', starFile)[0]
        self.assertEqual(row.rlnMicrographName, 'Jobs/job001/TS_01.mrc')


if __name__ == '__main__':
    unittest.main()
