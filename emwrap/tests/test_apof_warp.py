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
        'emw-warp-aretomo',
        'emw-warp-ctfrec',
    ]

    expected_outputs = {
        'emw-import-ts': 'tilt_series.star',
        'emw-warp-mctf': 'tilt_series.star',
        'emw-warp-aretomo': 'aligned_tilt_series.star',
        'emw-warp-ctfrec': 'tomograms.star',
    }

    @classmethod
    def get_parser(cls):
        parser = super().get_parser()
        parser.add_argument(
            '--workflow', '-w', choices=['small', 'medium', 'full'], default='small',
            help='Workflow size: small (preprocessing), medium (part1), or full.')
        return parser

    def _run_workflow(self):
        """Modify job_types and expected_outputs based on the workflow size."""

        if self.args.workflow == 'medium':
            self.job_types.extend([
                'emw-pytom',
                'emw-warp-export_particles',
                'emw-relion-tomoinitial'
            ])
            self.expected_outputs.update({
                'emw-pytom': 'tomograms_coords.star',
                'emw-warp-export_particles': 'optimisation_set.star',
                'emw-relion-tomoinitial': 'output/initial_model.mrc',
            })
        elif self.args.workflow == 'full':
            self.workflow_template = self.get_workflow_template('apof-warp-tutorial-full')
            raise ValueError('Full workflow is not implemented yet.')

        super()._run_workflow()


if __name__ == '__main__':
    TestApoFWarp.run_tests()
