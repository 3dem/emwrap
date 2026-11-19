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
import json

from emwrap.base import PickingPipeline

from .pytom import PyTom


class PyTomPipeline(PickingPipeline):
    """ PyTom template matching in a set of tomograms (see PickingPipeline
    for the inputs and outputs). """
    name = 'emw-pytom'
    PROGRAM = 'PYTOM'
    LABEL = 'PyTom'
    SUBARGS = ('pytom', 'pytom_extract')

    # Expected suffices after running PyTom in one batch
    OUTPUT_SUFFICES = ['angles.mrc',
                       'extraction_graph.svg',
                       'job.json',
                       'particles_default.star',
                       'particles_relion5.star',
                       'scores.mrc']

    def _createPicker(self, args, gpu):
        args['pytom']['g'] = gpu
        return PyTom(self.acq, args)

    def _gpuGroups(self):
        """ Pairs of GPUs per processing thread if there is an even number
        of them, otherwise one GPU per thread. """
        n = len(self.gpuList)
        if n % 2 == 0:
            return [self.gpuList[i:i+2] for i in range(0, n, 2)]
        return [[g] for g in self.gpuList]

    def _copyOutputFile(self, batch, suffix, src, dst, copied):
        if suffix == 'job.json':
            self._writeJobJson(batch, src, dst, copied)
        else:
            PickingPipeline._copyOutputFile(self, batch, suffix, src, dst, copied)

    def _writeJobJson(self, batch, src, dst, copied):
        """ Copy PyTom's job json, that contains absolute paths to the batch
        tmp folder, rewriting them relative to the project root: copied
        outputs point to their file in Coordinates, the linked inputs to
        the original files and the rest to their (tmp) project path. """
        with open(src) as f:
            data = json.load(f)

        mapping = dict(copied)
        mapping[batch.join('output')] = self.join('Coordinates')
        # Inputs linked into the batch folder: point to the original files
        pytomArgs = self._programArgs['pytom']
        for src in [batch.get('tomogram', None),
                    pytomArgs.get('template', None), pytomArgs.get('mask', None)]:
            if src:
                link = batch.join(os.path.basename(src))
                mapping[link] = self.relativizePaths(os.path.abspath(src))

        with open(dst, 'w') as f:
            json.dump(self.relativizePaths(data, mapping), f, indent=4)


if __name__ == '__main__':
    PyTomPipeline.main()
