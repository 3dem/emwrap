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
import glob
import threading
from datetime import datetime

from emtools.utils import Color, Pretty, FolderManager
from emtools.metadata import Acquisition, StarFile, RelionStar


class RelionImportMovies(FolderManager, threading.Thread):
    def __init__(self, **kwargs):
        FolderManager.__init__(self, kwargs.pop('output', None))
        threading.Thread.__init__(self)
        #workingDir = kwargs.pop('working_dir', os.getcwd())
        self.acq = Acquisition(kwargs['acquisition'])
        self.pattern = kwargs['movies_pattern']
        self.wait = {
            'new_files': 120,  # 1 hour
            'file_change': 60,  # 1 min
            'sleep': 30,
        }
        self.wait.update(kwargs.get('wait', {}))
        self.outputStar = self.join('movies.star')

    def run(self):
        allMovies = set()
        moviesTable = None

        # Load already seen movies if we are continuing the job
        if os.path.exists(self.outputStar):
            moviesTable = StarFile.getTableFromFile(self.outputStar, 'movies')
            allMovies.update(row.rlnMicrographMovieName for row in moviesTable)

        print(f">>> Monitoring movies with pattern: {Color.cyan(self.pattern)}")
        print(f">>> Existing movies: {Color.cyan(len(allMovies))}")

        now = lastUpdate = datetime.now()

        def _new_file(fn):
            # TODO update if we want to consider modification time
            return fn not in allMovies

        # Keep monitoring for new files until the time expires
        while (now - lastUpdate).seconds < self.wait['new_files']:
            now = datetime.now()
            newFiles = [fn for fn in glob.glob(self.pattern) if _new_file(fn)]
            if newFiles:
                print(f">>> {Pretty.now()}: found {len(newFiles)} new files")

                with StarFile(self.outputStar, 'a') as sf:
                    if moviesTable is None:
                        sf.writeTimeStamp()
                        sf.writeTable('optics', RelionStar.optics_table(self.acq))
                        moviesTable = RelionStar.movies_table()
                        sf.writeHeader('movies', moviesTable)

                    for fn in newFiles:
                        sf.writeRowValues([fn, 1])

                allMovies.update(newFiles)

                now = lastUpdate = datetime.now()
            time.sleep(self.wait['sleep'])

        print(f">>> Exiting, no new files detected in: "
              f"{Color.warn(Pretty.delta(now - lastUpdate))}")


def main():
    p = argparse.ArgumentParser(prog='emw-import-movies')
    p.add_argument('--movies_pattern', '-p',
                   help="Movie files pattern.")
    p.add_argument('--acquisition', '-a', nargs=4,
                   metavar=('pixel_size', 'voltage', 'cs', 'amplitude_contrast'),
                   help="Expected acquisition list: pixel_size voltage cs amplitude_contrast")
    p.add_argument('--output', '-o')
    p.add_argument('--j', help="Just to ignore the threads option from Relion")

    args = p.parse_args()
    acq = args.acquisition

    argsDict = {
        'movies_pattern': args.movies_pattern,
        'output_dir': args.output,
        'working_dir': os.getcwd(),
        'acquisition': Acquisition(pixel_size=acq[0],
                                   voltage=acq[1],
                                   cs=acq[2],
                                   amplitude_contrast=acq[3])
    }

    rim = RelionImportMovies(**argsDict)
    rim.run()


if __name__ == '__main__':
    main()



