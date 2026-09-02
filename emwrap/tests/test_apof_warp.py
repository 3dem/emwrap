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
import sys

from emwrap.base import ProjectManager
from .test_apof import TestApoF


class TestApoFWarp(TestApoF):
    workflow_template = TestApoF.get_workflow_template('apof-warp-tutorial-part1')
    workflow_template_full = TestApoF.get_workflow_template('apof-warp-tutorial-full')

    job_types = [
        'emw-import-ts',
        'emw-warp-mctf',
        'emw-warp-tsalign',
        'emw-warp-ctfrec',
    ]

    expected_outputs = {
        'emw-import-ts': 'tilt_series.star',
        'emw-warp-mctf': 'tilt_series.star',
        'emw-warp-tsalign': 'aligned_tilt_series.star',
        'emw-warp-ctfrec': 'tomograms.star',
    }

    @classmethod
    def set_args(cls, parser):
        super().set_args(parser)
        parser.add_argument(
            '--workflow', '-w', choices=['small', 'medium', 'full', 'otf'], default='small',
            help='Workflow size: small (preprocessing), medium (part1), full (part1+part2), or otf (preprocessing in OTF mode).')

    def _run_workflow(self):
        """Modify job_types and expected_outputs based on the workflow size."""

        if self.args.workflow == 'medium':
            self.job_types.extend([
                'emw-pytom',
                'emw-warp-export_particles',
                'relion.initialmodel.tomo'
            ])
            self.expected_outputs.update({
                'emw-pytom': 'optimisation_set.star',
                'emw-warp-export_particles': 'optimisation_set.star',
                'relion.initialmodel.tomo': 'output/initial_model.mrc',
            })
        elif self.args.workflow == 'full':
            self.workflow_template = self.get_workflow_template('apof-warp-tutorial-full')
            raise ValueError('Full workflow is not implemented yet.')

        elif self.args.workflow == 'otf':
            self.workflow_template = self.get_workflow_template('apof-warp-tutorial-otf')
            self.job_types = [
                'emw-import-ts',    
                'emw-warp-otf',
            ]
            self.expected_outputs = {
                'emw-import-ts': 'tilt_series.star',
                'emw-warp-otf': ['aligned_tilt_series.star', 'tomograms.star']
            }

        super()._run_workflow()


if __name__ == '__main__':
    TestApoFWarp.run_tests()
