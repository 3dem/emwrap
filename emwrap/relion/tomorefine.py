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

from emtools.jobs import Batch, Args
from .relion_base import RelionBasePipeline


class RelionTomoRefine(RelionBasePipeline):
    """ Wrapper around relion_refine for subtomograms 3D refinement. """
    name = 'emw-relion-tomorefine'

    def prerun(self):
        self._check_input('relion_refine.ios', 'Optimization set')
        self._check_input('relion_refine.ref', 'Reference volume')
        self._check_input('relion_refine.solvent_mask', 'Solvent mask', allow_empty=True)

        # FIXME: Get the number of mpis/threads from GUI
        gpus = int(self._args['gpus'])
        mpis = gpus + 1
        threads = 10

        args = Args({
            'relion_refine': mpis,
            '--o': self.join('output/run'),
            '--auto_refine': '',
            '--split_random_halves': '',
            '--flatten_solvent': '',
            '--oversampling': 1,
            '--low_resol_join_halves': 40,
            '--norm': '',
            '--scale': '',
            '--oversampling': 1,
            '--gpu': '',
            '--j': threads
        })

        if self._args.pop('relion_refine.auto_faster', False):
            args['--auto_ignore_angles'] = ''
            args['--auto_resol_angles'] = ''

        # Solvent flattening FSC only when mask is provided
        if self._args.pop('relion_refine.solvent_correct_fsc', False):
            if self._args.get('relion_refine.solvent_mask', ''):
                args['--solvent_correct_fsc'] = ''

        # For some reason, offset_step GUI value is multiplied by 2
        args['--offset_step'] = int(self._args.pop('relion_refine.offset_step', 1)) * 2

        # There are options in Relion GUI that get added when the value is False (Inverted booleans)
        inverted_booleans = [
            'firstiter_cc', 
            'dont_combine_weights_via_disc',
            'no_parallel_disc_io'
            ]
        subargs = self.get_subargs('relion_refine', 
                                   inverted_booleans=inverted_booleans,
                                   possitive=['sigma_tilt', 'ini_high'])

        args.update(subargs)

        if extra := self._args.get('extra_args'):
            args.update(Args.fromString(extra))

        batch = Batch(id=self.name, path=self.workingDir)
        self.mkdir('output')
        self.batch_execute('relion_refine', batch, args)

        runOptSet = self.join('output', 'run_optimisation_set.star')
        if not os.path.exists(runOptSet):
            raise Exception(
                f"ERROR: Output optimisation set '{runOptSet}' was not produced."
            )

        optSet = self.join('optimisation_set.star')
        if os.path.lexists(optSet):
            os.remove(optSet)
        self.link('output/run_optimisation_set.star', name='optimisation_set.star')

        outVol = self.join('output', 'run_class001.mrc')

        outputNodes = [[optSet, 'TomogramGroupMetadata.star.relion.tomo.particles']]
        outputNodes.append([outVol, 'TomogramGroupMetadata.star.relion.volume'])
        self.writeRelionOutputNodes(outputNodes)
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    RelionTomoRefine.main()
