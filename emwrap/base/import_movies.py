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
import argparse
import shutil
import time
from glob import glob
import threading
from datetime import datetime

from emtools.utils import Color, Pretty, Path, FolderManager
from emtools.metadata import Acquisition, StarFile, RelionStar, Table

from emwrap.base import ProcessingPipeline


class ImportMoviesPipeline(ProcessingPipeline):
    name = 'emw-import-movies'
    input_name = 'in_movies'

    def __init__(self, input_args, output):
        ProcessingPipeline.__init__(self, input_args, output)
        self.acq = self.loadAcquisition()  # from acq.* params
        args = self._args

        # All wait times are in seconds
        self.wait = {
            'timeout': int(args.get('wait.timeout', 7200)),
            'file_change': int(args.get('wait.file_change', 60)),
            'sleep': int(args.get('wait.sleep', 60)),
        }
        self.outputStar = self.join('movies.star')
        self.pattern = args[self.input_name]
        rootParts = []
        for p in Path.splitall(self.pattern):
            if Path.isPattern(p):
                break
            rootParts.append(p)
        self.patternRoot = Path.addslash(os.path.abspath(os.path.sep.join(rootParts)))

    def prerun(self):
        allMovies = set()
        moviesTable = None
        nextId = 0

        # Load already seen movies if we are continuing the job
        if os.path.exists(self.outputStar):
            moviesTable = StarFile.getTableFromFile('movies', self.outputStar)
            allMovies.update(row.rlnMicrographOriginalMovieName for row in moviesTable)
            nextId = moviesTable[-1].rlnImageId
        else:
            self.mkdir('Movies')
            self.mkdir('EPU', 'XML')
            self.mkdir('EPU', 'GridSquares')
            os.symlink(self.patternRoot, self.join('Movies', 'input'))

        xmlFolder = FolderManager(self.join('EPU', 'XML'))
        gsFolder = FolderManager(self.join('EPU', 'GridSquares'))

        self.log(f">>>> STARTING RUN: Monitoring movies with pattern: {Color.cyan(self.pattern)}")
        self.log(f"Existing movies: {Color.cyan(len(allMovies))}")

        self.log(f"Input root: {self.patternRoot}", flush=True)

        now = lastUpdate = datetime.now()

        def _new_file(fn):
            # TODO update if we want to consider modification time
            return fn not in allMovies

        def _grid_square(fn):
            for p in Path.splitall(fn):
                if 'GridSquare' in p:
                    return p
            return 'None'

        if self.pattern.endswith('EER.eer'):
            suffix = '_EER.eer'
        elif self.pattern.endswith('fractions.tiff'):
            suffix = '_fractions.tiff'
        else:
            suffix = Path.getExt(self.pattern)

        # Keep monitoring for new files until the time expires
        while (now - lastUpdate).seconds < self.wait['timeout']:
            now = datetime.now()
            newFiles = [(fn, os.path.getmtime(fn))
                        for fn in glob(self.pattern) if _new_file(fn)]
            if newFiles:
                self.log(f"Found {len(newFiles)} new files", flush=True)

                unwritten = 0

                with StarFile(self.outputStar, 'a') as sf:
                    if moviesTable is None:
                        sf.writeTimeStamp()
                        sf.writeTable('optics', self._opticsTable())
                        moviesTable = Table(['rlnImageId',
                                             'rlnMicrographMovieName',
                                             'rlnMicrographOriginalMovieName',
                                             'rlnOpticsGroup',
                                             'TimeStamp',
                                             'GridSquare'])
                        sf.writeHeader('movies', moviesTable)

                    copiedGs = set()
                    # Sort new files base on modification time
                    for fn, mt in sorted(newFiles, key=lambda x: x[1]):
                        nextId += 1
                        unwritten += 1
                        ext = Path.getExt(fn)
                        newPrefix = f'movie-{nextId:06}'
                        newFn = self.join('Movies', f'{newPrefix}{ext}')
                        absFn = os.path.abspath(fn)
                        baseName = absFn.replace(self.patternRoot, '')
                        # JMRT: 2024/04/04 The newFn should not exist, but some sessions
                        # (e.g. 1815) crashed due to error in movies import, reporting
                        # the target link already existed
                        if not os.path.exists(newFn):
                            os.symlink(os.path.join('input', baseName), newFn)
                        gs = _grid_square(fn)
                        sf.writeRowValues([nextId, newFn, fn, 1, mt, gs])
                        if unwritten == 100:  # Update star file to allow streaming
                            sf.flush()
                            unwritten = 0
                        allMovies.add(fn)
                        absXml = absFn.replace(suffix, '.xml')

                        if os.path.exists(absXml):
                            shutil.copy(absXml, xmlFolder.join(f'{newPrefix}.xml'))
                        if gs not in copiedGs:
                            if gsFiles := glob(os.path.join(self.patternRoot, '*', gs, 'GridSquare_*.???')):
                                gsSubFolder = gsFolder.mkdir(gs)
                                for gsFn in gsFiles:
                                    print(f"   Copying: {gsFn} to {gsSubFolder}")
                                    shutil.copy(gsFn, gsSubFolder)
                                copiedGs.add(gs)

                if not self.outputs:
                    self._registerOutput()
                now = lastUpdate = datetime.now()
            time.sleep(self.wait['sleep'])

        self.log(f"Exiting, no new files detected in: "
                 f"{Color.warn(Pretty.delta(now - lastUpdate))}", flush=True)

    def _opticsTable(self):
        """ Optics table with gain and dose per frame, so they are
        available for the preprocessing of these movies. """
        row = RelionStar.optics_table(self.acq)[0]._asdict()
        if gain := self.acq.get('gain'):
            row['rlnMicrographGainName'] = gain
        if self.acq.dose:
            row['rlnMicrographDoseRate'] = self.acq.dose
        return Table.fromDict(row)

    def _registerOutput(self):
        self.outputs['Movies'] = {
            'label': 'Movies',
            'files': [[self.outputStar, 'MicrographMovieGroupMetadata.star.relion']]
        }
        self.writeRelionOutputNodes(self.outputs['Movies']['files'])
        self.writeInfo()


if __name__ == '__main__':
    ImportMoviesPipeline.main()



