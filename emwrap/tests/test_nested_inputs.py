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

    def _cachedData(self, wf, cached):
        """ ProjectData with a job info cached in project.json, newer than
        the job files (so it would be reused if its version is current). """
        data = ProjectData.__new__(ProjectData)
        data._project = unittest.mock.Mock(force=False)
        data._wf = wf
        data._jobs = {'WarpMCreatePopulation/job011': cached}
        data._outputs = {}
        data._forcedJobs = set()
        data._debug = lambda *args, **kwargs: None
        data._jobFilesMtimes = lambda jobId: {'job.star': 1.0}
        data.join = lambda *p: os.path.join(self.tmp, *p)
        data._computeJobInfo = unittest.mock.Mock(return_value={
            'inputs': [('WarpExportParticles/job007',
                        'WarpExportParticles/job007')],
            'outputs': [], 'status': 'Succeeded'})
        return data

    def test_cached_inputs_old_version(self):
        """ Inputs cached before nested params were detected (no
        inputsVersion, as in projects processed before) are computed
        again, although the job files did not change. """
        jobId = 'WarpMCreatePopulation/job011'
        data = self._cachedData(_workflow(), {
            'inputs': [], 'outputs': [], 'status': 'Succeeded', 'ts': 10.0})
        info, computed = data._getJobInfo(jobId)
        self.assertTrue(computed)
        self.assertEqual(info['inputs'][0][0], 'WarpExportParticles/job007')
        self.assertEqual(info['inputsVersion'], ProjectData.JOB_INPUTS_VERSION)
        self.assertEqual(data._jobs[jobId]['inputsVersion'],
                         ProjectData.JOB_INPUTS_VERSION)

        # Cached by the current version: reused while the files don't change
        data._computeJobInfo.reset_mock()
        info, computed = data._getJobInfo(jobId)
        self.assertFalse(computed)
        data._computeJobInfo.assert_not_called()

    def test_cached_status_only(self):
        """ A cached status without inputs (e.g. set when saving a job) is
        not a reason to compute the job info. """
        data = self._cachedData(_workflow(), {'status': 'Saved', 'ts': 10.0})
        info, computed = data._getJobInfo('WarpMCreatePopulation/job011')
        self.assertFalse(computed)


if __name__ == '__main__':
    unittest.main()
