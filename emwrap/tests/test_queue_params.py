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

""" Unit tests for the queue params of cluster submissions (no datasets,
GPUs or cluster needed):

    python -m unittest emwrap.tests.test_queue_params
"""

import os
import shutil
import tempfile
import unittest
import unittest.mock

from emtools.utils import FolderManager

from emwrap.base.config import ProcessingConfig
from emwrap.base.project_manager import ProjectManager

TEMPLATE = """#!/bin/bash
#BSUB -q {queue_name}
#BSUB -J {jobId}
#BSUB -n {cpus}
#BSUB -R "rusage[mem={mem_per_core}]"
{gpu_line}
cd {working_dir}
{command}
"""


class TestQueueParams(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='emwrap_queue_')
        self.template = os.path.join(self.tmp, 'lsf_template.sh')
        with open(self.template, 'w') as f:
            f.write(TEMPLATE)

        self.queue = {
            'name': 'cryoem',
            'template': self.template,
            'submit': 'bsub < {job_script}',
            'cancel': 'bkill {job_id}',
            'params': [
                {'name': 'mem_per_core', 'default': 7000},
                {'name': 'gpu_type', 'default': 'any',
                 'paramClass': 'EnumParam'},
                {'name': 'no_default'},
            ]
        }
        config = {'queues': [{'name': 'None'}, self.queue]}
        patcher = unittest.mock.patch.object(ProcessingConfig, '_config', config)
        patcher.start()
        self.addCleanup(patcher.stop)

        # Only the folder is needed, not a Relion project
        self.pm = ProjectManager.__new__(ProjectManager)
        FolderManager.__init__(self.pm, self.tmp)
        self.pm._verbose = 0
        self.jobDir = os.path.join(self.tmp, 'External', 'job001')
        os.makedirs(self.jobDir)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def _submission(self, job_params):
        return self.pm._prepareQueueSubmission(
            'emw-test -i job.star', job_params, self.jobDir,
            job_id='External/job001')

    def test_defaults_without_queue_params(self):
        """ Only queue.name set (API/MCP/workflow jobs): use the defaults. """
        qparams = ProjectManager._queueParams(
            self.queue, {'queue.name': 'cryoem', 'cpus': '4'})
        self.assertEqual(qparams, {'mem_per_core': 7000, 'gpu_type': 'any'})

        script = self._submission({'queue.name': 'cryoem', 'cpus': '4'})['script_content']
        self.assertIn('rusage[mem=7000]', script)
        self.assertIn('#BSUB -n 4', script)

    def test_job_values_override_defaults(self):
        params = {'queue.name': 'cryoem', 'cpus': '2',
                  'queue.param.cryoem.mem_per_core': '16000',
                  'queue.param.cryoem.no_default': 'x',
                  # Values of other queues are ignored
                  'queue.param.cryo.mem_per_core': '1'}
        qparams = ProjectManager._queueParams(self.queue, params)
        self.assertEqual(qparams, {'mem_per_core': '16000', 'gpu_type': 'any',
                                   'no_default': 'x'})
        script = self._submission(params)['script_content']
        self.assertIn('rusage[mem=16000]', script)

    def test_none_takes_default(self):
        params = {'queue.name': 'cryoem', 'cpus': '1',
                  'queue.param.cryoem.mem_per_core': None}
        self.assertIn('rusage[mem=7000]',
                      self._submission(params)['script_content'])

    def test_missing_placeholder_without_default_still_fails(self):
        self.queue['params'] = [{'name': 'mem_per_core'}]
        with self.assertRaisesRegex(Exception, 'mem_per_core'):
            self._submission({'queue.name': 'cryoem', 'cpus': '1'})

    def test_no_queue(self):
        self.assertIsNone(self._submission({'queue.name': 'None', 'cpus': '1'}))


if __name__ == '__main__':
    unittest.main()
