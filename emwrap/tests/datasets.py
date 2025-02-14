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

import os
import glob

from emtools.metadata import Table, Acquisition, StarFile, RelionStar
from emtools.jobs import BatchManager


class RelionTutorial:
    path = '/jude/facility/jmrt/SCIPION/TESTDATA/relion30_tutorial/'
    acquisition = Acquisition(
        pixel_size=0.885,
        voltage=200,
        cs=1.4,
        amplitude_contrast=0.1
    )

    @classmethod
    def join(cls, *p):
        return os.path.join(cls.path, *p)

    @classmethod
    def optics_table(cls):
        return RelionStar.optics_table(cls.acquisition)

    @classmethod
    def movies_table(cls):
        movies = glob.glob(cls.join('Movies', '*.tiff'))
        t = Table(['rlnMicrographMovieName', 'rlnOpticsGroup'])
        for m in movies:
            t.addRowValues(m, 1)
        return t

    @classmethod
    def write_movies_star(cls, outputStar):
        with StarFile(outputStar, 'w') as sf:
            sf.writeTable('optics', cls.optics_table())
            sf.writeTable('movies', cls.movies_table())

    @classmethod
    def make_batch(cls, outputDir, n):
        table = cls.movies_table()
        batchMgr = BatchManager(n, iter(table[:n]), outputDir,
                                itemFileNameFunc=lambda i: i.rlnMicrographMovieName)
        return next(batchMgr.generate())

    @classmethod
    def _filename(cls, row):
        """ Helper to get unique name from a particle row. """
        pts, stack = row.rlnImageName.split('@')
        return stack.replace('.mrcs', f'_p{pts}.mrcs')
