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

from emtools.utils import Path
from emtools.jobs import Batch
from emwrap.base import ProcessingPipeline

from .pytom import PyTom


class PyTomCreateTemplate(ProcessingPipeline):
    """ Simple wrapper to create a template and a mask with PyTom. """
    name = 'emw-pytom-create_template'

    @classmethod
    def get_launcher(cls):
        return PyTom.get_launcher()

    def create_template_and_mask(self):
        batch = Batch(id=self.name, path=self.path)
        subargs = self.get_subargs('pytom_create_template', '--')
        # Add boolean params only when their value is true
        for a in ['--center', '--invert', '--mirror']:
            if subargs[a]:
                subargs[a] = ''
            else:
                del subargs[a]
        inputMap = batch.link(subargs['--input-map'])
        subargs['--input-map'] = inputMap
        outputVol = Path.replaceBaseExt(inputMap, '.mrc')
        subargs['--output-file'] = outputVol

        # Create the template
        args = {'pytom_create_template.py': ''}
        args.update(subargs)
        self.batch_execute('pytom_create_template', batch, args)

        # Create the mask
        subargs2 = self.get_subargs('pytom_create_mask', '--')
        outPs = float(subargs['--output-voxel-size-angstrom'])
        boxSize = subargs['--box-size']
        subargs2['--box-size'] = boxSize
        subargs2['--voxel-size'] = outPs
        outputMask = Path.replaceExt(outputVol, '_mask.mrc')
        subargs2['--output-file'] = outputMask
        args = {'pytom_create_mask.py': ''}
        args.update(subargs2)
        self.batch_execute('pytom_create_mask', batch, args)

        batch.log("Storing outputs", flush=True)
        infoStr = f"{boxSize} x {boxSize} x {boxSize}, {outPs:0.3f} Å/px"

        self.outputs = {
            'Volume': {
                'label': 'Output Volume',
                'type': 'Volume',
                'info': infoStr,
                'files': [
                    [self.join(outputVol),
                     'TomogramGroupMetadata.star.relion.volume']  # FIXME
                ]
            },
            'VolumeMask': {
                'label': 'Output Mask',
                'type': 'VolumeMask',
                'info': infoStr,
                'files': [
                    [self.join(outputMask),
                     'TomogramGroupMetadata.star.relion.mask3d']  # FIXME
                ]
            }
        }
        self.updateBatchInfo(batch)

    def prerun(self):
        self.create_template_and_mask()


if __name__ == '__main__':
    PyTomCreateTemplate.main()
