# **************************************************************************
# *
# * Authors:     Daniel Marchan Torres (danielmarchan3@gmail.com)
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

from .test_apof import TestApoF


class TestAretomo3ApoF(TestApoF):
    workflow_template = TestApoF.get_workflow_template('apof-aretomo3')

    job_types = [
        'emw-import-ts'
    ]

    expected_outputs = {
        'emw-import-ts': 'tilt_series.star'
    }

    @classmethod
    def set_args(cls, parser):
        super().set_args(parser)
        parser.add_argument(
            '--workflow', '-w', choices=['pipeline', 'modular', 'full'], default='pipeline',
            help='Workflow type: pipeline (default), modular or full.')

    def _run_workflow(self):
        """Modify job_types and expected_outputs based on the workflow size."""

        if self.args.workflow == 'pipeline':
            self.job_types.extend([
                'emw-aretomo3'
                ])
            self.expected_outputs.update({
                'emw-aretomo3': ['aligned_tilt_series.star', 'tomograms.star'],
            })
        elif self.args.workflow == 'modular':
            self.job_types.extend([
                'emw-warp-mctf',
                'emw-aretomo3-align',
                'emw-aretomo3-reconstruct',
            ])
            self.expected_outputs.update({
                'emw-warp-mctf': 'tilt_series.star',
                'emw-aretomo3-align': 'aligned_tilt_series.star',
                'emw-aretomo3-reconstruct': 'tomograms.star',
            })       
        elif self.args.workflow == 'full':
            self.job_types.extend([
                'emw-aretomo3',
                'emw-warp-mctf',
                'emw-aretomo3-align',
                'emw-aretomo3-reconstruct',
            ])
            self.expected_outputs.update({
                'emw-aretomo3': ['aligned_tilt_series.star', 'tomograms.star'],
                'emw-warp-mctf': 'tilt_series.star',
                'emw-aretomo3-align': 'aligned_tilt_series.star',
                'emw-aretomo3-reconstruct': 'tomograms.star',
            })

        super()._run_workflow()
    

if __name__ == '__main__':
    TestAretomo3ApoF.run_tests()
