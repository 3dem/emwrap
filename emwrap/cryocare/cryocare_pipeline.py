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
from pprint import pprint
from glob import glob

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import MdocBatchManager, Args, Batch
from emtools.metadata import Mdoc, Acquisition

from emwrap.base import ProcessingPipeline


class CryoCarePipeline(ProcessingPipeline):
    """ Pipeline specific to CryoCare processing. """

    def __init__(self, input_args):
        ProcessingPipeline.__init__(self, input_args)
        self.gpuList = [int(g) for g in self._args['gpu'].split()]
        self.inputVolPattern = self._args['in_movies']

    def getInputVols(self):
        # FIXME: Use input STAR file instead of guessing the matching
        # based on glob pattern or names

        inputVols = glob(self.inputVolPattern)

        if not inputVols:
            raise Exception(f"No volumes were found with pattern: "
                            f"{self.inputVolPattern}")

        # Let's match volumes in pairs
        evenVols = [v for v in inputVols if 'EVN' in v]
        oddVols = [v.replace('EVN', 'ODD') for v in evenVols]

        if any(v not in inputVols for v in oddVols):
            raise Exception("Missing some ODD vols.")

        if len(inputVols) != 2 * len(evenVols):
            raise Excetion("There are some input files that can not be matched. ")

        return evenVols, oddVols

