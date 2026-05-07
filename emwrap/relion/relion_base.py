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
from emwrap.base import ProcessingPipeline


class RelionBasePipeline(ProcessingPipeline):
    """ Base class to organize common functions/properties of different
    Relion pipelines.
    """
    PROGRAM = 'RELION'

    def get_subargs(self, prefix, new_prefix='--', inverted_booleans=[], possitive=[]):
        return self._args.subset(prefix, new_prefix=new_prefix, 
                                 filters=['remove_empty', 'remove_false'], 
                                 inverted_booleans=inverted_booleans,
                                 possitive=possitive)

    def _get_launcher(self):
        return ProcessingPipeline.get_launcher('RELION')

    def _check_input(self, key, name, allow_empty=False):
        fn = self._args.get(key, '')
        if fn:
            if not os.path.exists(fn):
                raise Exception(f"{name} '{fn}' does not exist.")
        elif not allow_empty:
            raise Exception(f"{name} is required.")