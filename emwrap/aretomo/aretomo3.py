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
import subprocess
import pathlib
import sys
import json
import argparse
import math
from glob import glob
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import MdocBatchManager, Args, Batch
from emtools.metadata import Mdoc, StarFile

from emwrap.base import ProcessingPipeline


class AreTomo3(ProcessingPipeline):
    """ Pipeline specific to AreTomo3 preprocessing. """
    name = 'emw-aretomo3'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)

    def runAreTomo3(self, batch):
        print('----- Running Aretomo3 Test --------')

    def runBatch(self, batch, importInputs=True, **kwargs):
        self.runAreTomo3(batch)
    
    def prerun(self):
        batch = Batch(id=self.name, path=self.path)
        self.runBatch(batch)
        # self._output(batch)


if __name__ == '__main__':
    AreTomo3.main()