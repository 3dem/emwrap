# **************************************************************************
# *
# * Authors:     J.M. de la Rosa Trevin (delarosatrevin@gmail.com)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# **************************************************************************

""" Unit tests for job references nested in params (lists and dicts, e.g.
the TableParam 'sources' of Warp M create population) when building the
job graph (no datasets, GPUs or cluster needed):

    python -m unittest emwrap.tests.test_nested_inputs
"""

import os
import shutil
import tempfile
import unittest
import unittest.mock

from emtools.jobs import Workflow
from emtools.metadata import RelionStar

from emwrap.base.config import ProcessingConfig
from emwrap.base.job_form import JobForm
from emwrap.base.project_data import ProjectData
from emwrap.base.project_manager import ProjectManager

# A minimal form with a TableParam, as emw-warp-mtools_create
FORM = {'sections': [{'label': 'Input', 'params': [
    {'name': 'particles', 'paramClass': 'PathParam'},
    {'name': 'sources', 'paramClass': 'TableParam', 'params': [
        {'name': 'name'}, {'name': 'warp_folder', 'paramClass': 'PathParam'}]},
]}]}

PARAMS = {
    'particles': 'Refine3D/job009/run_data.star',
    'sources': [{'name': 'ApoF', 'warp_folder': 'WarpExportParticles/job007'}],
}


def _workflow():
    wf = Workflow()
    for jobId in ['WarpExportParticles/job007', 'Refine3D/job009',
                  'WarpMCreatePopulation/job011']:
        wf.registerJob(jobId, jobtype='test')
    return wf


class TestNestedInputs(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='emwrap_nested_')
        patcher = unittest.mock.patch.object(
            ProcessingConfig, 'get_job_form', lambda jobType: FORM)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def _pm(self, wf):
        pm = ProjectManager.__new__(ProjectManager)
        pm._wf = wf
        pm._data = unittest.mock.Mock()
        pm.path = self.tmp
        pm.relpath = lambda p: os.path.relpath(p, self.tmp)
        pm.get_workflow = lambda: wf
        return pm

    def test_update_job_inputs(self):
        """ Saving a job links it to the jobs referenced in table rows. """
        wf = _workflow()
        pm = self._pm(wf)
        job = wf.getJob('WarpMCreatePopulation/job011')
        pm._updateJobInputs(job, PARAMS)
        parents = sorted(d.parent.id for d in job.inputs)
        self.assertEqual(parents, ['Refine3D/job009',
                                   'WarpExportParticles/job007'])

    def test_compute_job_info(self):
        """ The job info read from job.star, where the table is a JSON
        string, lists the inputs referenced in its rows. """
        wf = _workflow()
        pm = self._pm(wf)
        jobId = 'WarpMCreatePopulation/job011'
        jobStar = os.path.join(self.tmp, jobId, 'job.star')
        os.makedirs(os.path.dirname(jobStar))
        RelionStar.write_jobstar('test', JobForm.encode_table_params(FORM, PARAMS),
                                 jobStar)

        data = ProjectData.__new__(ProjectData)
        data._project = pm
        data._wf = wf
        data._computedJobs = set()
        data._debug = lambda *args: None
        data._collectJobOutputIds = lambda jobId, job=None: []
        data._jobNodeTypes = lambda job, outputs: {}
        data._resolveJobStatus = lambda job: 'Succeeded'
        data._readJobTiming = lambda jobId: {}

        info = data._computeJobInfo(jobId, [jobStar])
        self.assertEqual(sorted(info['inputs']), [
            ('Refine3D/job009', 'Refine3D/job009/run_data.star'),
            ('WarpExportParticles/job007', 'WarpExportParticles/job007')])


if __name__ == '__main__':
    unittest.main()
