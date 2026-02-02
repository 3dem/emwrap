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

from emtools.utils import FolderManager, Path, Process
from emwrap.base import ProcessingPipeline
from emtools.metadata import StarFile, Acquisition, RelionStar
from emtools.jobs import Batch


class RelionBasePipeline(ProcessingPipeline):
    """ Base class to organize common functions/properties of different
    Relion pipelines.
    """
    PROGRAM = 'RELION'

    def get_subargs(self, prefix, new_prefix='--'):
        subargs = ProcessingPipeline.get_subargs(self, prefix, new_prefix=new_prefix)
        newargs = {}
        # Fix boolean params
        for k, v in subargs.items():
            if v:
                if RelionStar.true_value(v):
                    newargs[k] = ''  # Boolean options will be passed
                elif not RelionStar.false_value(v):
                    newargs[k] = v
        return newargs