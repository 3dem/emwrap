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

from emwrap.base import ProcessingPipeline, PickingPipeline

from .pytme import PyTME


class PyTMEPipeline(PickingPipeline):
    """ pyTME template matching in a set of tomograms.

    Same inputs, outputs and bookkeeping as emw-pytom (see PickingPipeline),
    with the pyTME score as rlnLCCmax in particles.star and the
    per-tomogram pyTME outputs in Coordinates/.
    """
    name = 'emw-pytme'
    PROGRAM = 'PYTME'
    LABEL = 'pyTME'
    SUBARGS = ('pytme', 'pytme_postprocess')

    # Expected suffices after running pyTME in one batch
    OUTPUT_SUFFICES = ['match.pickle',
                       'orientations.tsv',
                       'particles_default.star',
                       'particles_relion5.star']

    @staticmethod
    def get_gpu_list(gpus, as_string=False):
        """ Without GPUs, pyTME runs on CPU (in a single thread). """
        if not str(gpus or '').strip():
            return '' if as_string else []
        return PickingPipeline.get_gpu_list(gpus, as_string=as_string)

    def _gpuGroups(self):
        """ GPUs used by each processing thread (one tomogram at a time),
        see ProcessingPipeline.get_gpu_groups: "2" -> two threads with one
        GPU each, "0,1 2,3" -> two threads with two GPUs each. pyTME splits
        the rotations of one tomogram among the GPUs of its thread. """
        if not self.gpuList:
            return [[]]  # CPU
        return ProcessingPipeline.get_gpu_groups(self._args['gpus'])

    def _createPicker(self, args, gpu):
        # gpu: space separated GPU ids of this thread, pyTME uses commas
        args['pytme']['gpu-indices'] = ','.join(str(gpu).split())
        return PyTME(self.acq, args)

    def _coordinateValues(self, coord):
        values = {c: getattr(coord, c) for c in self.PARTICLES_COLUMNS
                  if c != 'rlnLCCmax'}
        values['rlnLCCmax'] = coord.pytmeScore
        return values

    def prerun(self):
        self._validateTemplate()
        PickingPipeline.prerun(self)

    def _validateTemplate(self):
        """ pyTME only warns if the template and tomogram sampling rates
        differ, but the template must be at the tomograms pixel size. """
        from emwrap.base.wizards import read_volume_header

        args = self._programArgs['pytme']
        tomoPs = float(self.acq.pixel_size)
        for key in ['template', 'template-mask']:
            if fn := args.get(key, None):
                _, ps = read_volume_header(fn)
                if ps and abs(ps - tomoPs) > 0.01 * tomoPs:
                    raise Exception(
                        f"The {key} voxel size ({ps:g} Å) is not the tomograms "
                        f"pixel size ({tomoPs:g} Å), resample it first "
                        f"(e.g. with emw-pytom-create_template).")


if __name__ == '__main__':
    PyTMEPipeline.main()
