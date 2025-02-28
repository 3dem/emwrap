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

"""

import os
import threading
import time
import shutil
import sys
import json
import argparse
from glob import glob

from emtools.utils import Color, Timer, Path, Process, FolderManager
from emtools.metadata import Table, StarFile, Acquisition
from emtools.image import Image

from emwrap.relion.project import RelionProject


class OTF(FolderManager):
    """ Pipeline to run Preprocessing in batches. """
    def __init__(self, path, **kwargs):
        FolderManager.__init__(self, path)

    def _dumpJson(self, fn, obj):
        fullFn = self.join(fn)
        self.log(f"Creating file: {Color.warn(fullFn)}")
        with open(fullFn, 'w') as f:
            json.dump(obj, f, indent=4)
            f.write('\n')

    def create(self, session, sconfig, resources):
        project = RelionProject(self.path)
        project.clean()

        raw = session['extra']['raw']

        for e in ['data', 'args_pp.json', 'args_2d.json']:
            if self.exists('data'):
                self.log(f"Removing: {Color.warn(e)}")
                os.remove(self.join('data'))

        os.symlink(raw['path'], self.join('data'))

        acq = {k: float(v) for k, v in session['acquisition'].items()}
        acq['gain'] = 'FIXME:gain.mrc'

        args_pp = {
            "output": "output",
            "gpu": "0 1",
            "batch_size": 32,
            "in_movies": "FIXME",
            "scratch": "/scr/",
            "acquisition": acq,
            "preprocessing": {
                "motioncor": {
                    "extra_args": {
                        "-FtBin": 2,
                        "-FlipGain": 1,
                        "-Patch": "7 5",
                        "-FmDose": acq['dose']
                    }
                },
                "ctf": {},
                "picking": {
                    "particle_size": None
                },
                "extract": {
                    "extra_args": {
                        "--scale": 100
                    }
                }
            },
            "launcher": "/usr/local/em/scripts/preprocess_batch.sh",
            "input_timeout": 30
        }
        micMap = {r['id']: r['name'] for r in resources}
        microscope = micMap[session['resource_id']]
        conf_acq = sconfig['acquisition'][microscope]
        input_movies = os.path.join('data', conf_acq['images_pattern'])
        args_pp['in_movies'] = input_movies
        gain_pattern = self.join('data', conf_acq['gain_pattern'].format(microscope=microscope))
        if gains := glob(gain_pattern):
            gain = gains[-1]
        else:
            gain = None
        args_pp['acquisition']['gain'] = gain

        # For the Krios02, we don't need to flip gain in Y
        # and the patches are 5x5, since it produces square images
        if microscope == 'Krios02':
            mc_args = args_pp['preprocessing']['motioncor']['extra_args']
            mc_args["-Patch"] = "5 5"
            del mc_args["-FlipGain"]

        self._dumpJson('args_pp.json', args_pp)

        args_2d = {
            "output": "FIXME",
            "gpu": "2,3",
            "batch_size": 100000,
            "in_particles": "FIXME",
            "scratch": "/scr/",
            "launcher": "/usr/local/em/scripts/relion_refine.sh",
            "timeout": 3600,
            "sleep": 120
        }

        self._dumpJson('args_2d.json', args_2d)

        print("raw: ", raw['path'])
        if movies := glob(input_movies):
            first = movies[0]
            print(Image.get_dimensions(first))

    def clean(self):
        """ Create files to start from scratch. """
        pass

    def run(self):
        """
        /tmp/TestPreprocessing.test_pipeline_multigpu__ip9v_n14/output2d/test
        """


def main():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group()
    g.add_argument('--create', '-c', metavar='SESSION_ID',
                   help="Create new OTF project, previous files will be cleaned.")
    # g.add_argument('--update', '-u', action='store_true',
    #                help="Update job status and pipeline star file.")
    # g.add_argument('--run', '-r', nargs=2, metavar=('JOB_TYPE', 'COMMAND'))

    args = p.parse_args()

    otf = OTF(path=os.getcwd())

    if args.create:
        # This option required that emhub client is properly configured
        from emhub.client import open_client
        with open_client() as dc:
            session = dc.get_session(int(args.create))
            sconfig = dc.get_config('sessions')
            resources = dc.get('resources').json()
        otf.create(session, sconfig, resources)

    # elif args.update:
    #     rlnProject.update()
    # elif args.run:
    #     folder, cmd = args.run
    #     rlnProject.run(folder, cmd)


if __name__ == '__main__':
    main()
