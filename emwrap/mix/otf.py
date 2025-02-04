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
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process, FolderManager
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile

from emwrap.base import ProcessingPipeline, Acquisition
from emwrap.relion import RelionStar, RelionImportMovies
from .preprocessing import Preprocessing


class OTF(FolderManager):
    """ Pipeline to run Preprocessing in batches. """
    def __init__(self, **kwargs):
        FolderManager.__init__(self, **kwargs)

    def create(self):
        with open('.gui_projectdir', 'w') as f:
            pass

        with StarFile('default_pipeline.star', 'a') as sf:
            t = Table(['rlnPipeLineJobCounter'])
            t.addRowValues(1)
            sf.writeTable('pipeline_general', t, singleRow=True)

    def clean(self):
        """ Create files to start from scratch. """
        pass

    def run(self):
        """
        /tmp/TestPreprocessing.test_pipeline_multigpu__ip9v_n14/output2d/test
        """


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('batch_folder',
                   help="Batch folder to run the preprocessing")
    args = p.parse_args()
    otf = OTF(path=args.batch_folder)
    otf.create()
