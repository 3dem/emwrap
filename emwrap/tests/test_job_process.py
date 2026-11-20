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

"""
Where a job runs and its resource usage, read by ProjectManager from the job
files (job.id, process.id, schedule.id, info.json, job.log, profile/*.csv):
getClusterJobId, getLocalProcess, getScheduleProcess, readScheduleLog and
getJobProfiles.
"""

import os
import json
import shutil
import tempfile
import unittest

from emwrap.base.project_manager import ProjectManager


class FakeJob(dict):
    """ A workflow job: its id and the 'status' key. """
    def __init__(self, jobId, status):
        super().__init__(status=status)
        self.id = jobId


class TestJobProcess(unittest.TestCase):
    def setUp(self):
        self.path = tempfile.mkdtemp(prefix='emwrap_project_')
        self.pm = ProjectManager(self.path, create=True, verbose=0)

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def _write(self, jobId, fn, content):
        path = self.pm.join(jobId, fn)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write(content if isinstance(content, str) else json.dumps(content))

    def test_cluster_job_id(self):
        job = FakeJob('External/job001', 'Finished')
        self.assertEqual(self.pm.getClusterJobId(job), '')
        # From the last run, for older jobs without job.id
        self._write(job.id, 'info.json', {'runs': [
            {'computing': {'cluster': {'system': 'LSF', 'job_id': 1234}}}]})
        self.assertEqual(self.pm.getClusterJobId(job), '1234')
        # Not for a running job (launched locally, the run may be an old one)
        self.assertEqual(self.pm.getClusterJobId(FakeJob(job.id, 'Running')), '')
        self._write(job.id, 'job.id', '5678\n')
        self.assertEqual(self.pm.getClusterJobId(job), '5678')

    def test_local_process(self):
        job = FakeJob('External/job002', 'Finished')
        self.assertEqual(self.pm.getLocalProcess(job), {})
        # From the last run with computing info
        self._write(job.id, 'info.json', {'runs': [
            {'computing': {'pid': 42, 'host': 'wkst03'}}, {}]})
        self.assertEqual(self.pm.getLocalProcess(job), {'pid': '42', 'host': 'wkst03'})
        # process.id: pid and host, or only the pid in older files
        self._write(job.id, 'process.id', '100\n')
        self.assertEqual(self.pm.getLocalProcess(job), {'pid': '100', 'host': 'wkst03'})
        self.assertEqual(self.pm.getLocalProcess(FakeJob(job.id, 'Running')),
                         {'pid': '100', 'host': ''})
        self._write(job.id, 'process.id', '100\nwkst05\n')
        self.assertEqual(self.pm.getLocalProcess(job), {'pid': '100', 'host': 'wkst05'})
        # A cluster run has no local process
        self._write(job.id, 'process.id', '')
        self.assertEqual(self.pm.getLocalProcess(job, runs=[
            {'computing': {'pid': 1, 'cluster': {'job_id': 7}}}]), {})

    def test_schedule(self):
        job = FakeJob('External/job003', 'Scheduled')
        self.assertEqual(self.pm.getScheduleProcess(job), {})
        self._write(job.id, 'schedule.id', '200\nwkst03\n')
        self.assertEqual(self.pm.getScheduleProcess(job), {'pid': '200', 'host': 'wkst03'})
        self.assertEqual(self.pm.getScheduleProcess(FakeJob(job.id, 'Finished')), {})
        self._write(job.id, 'schedule.id', '200\n')
        self.assertEqual(self.pm.getScheduleProcess(job), {'pid': '200', 'host': ''})

        self.assertEqual(self.pm.readScheduleLog(job.id), [])
        self._write(job.id, 'job.log', '\n'.join([
            'old line', f'Job {job.id} scheduled', 'waiting 1', '',
            f'Job {job.id} scheduled', 'waiting 2', 'waiting 3']))
        self.assertEqual(self.pm.readScheduleLog(job.id),
                         [f'Job {job.id} scheduled', 'waiting 2', 'waiting 3'])
        self.assertEqual(self.pm.readScheduleLog(job.id, maxLines=1), ['waiting 3'])

    def test_job_profiles(self):
        jobId = 'External/job004'
        self.assertEqual(self.pm.getJobProfiles(jobId), [])
        csvLines = ['time,procs,cpu,rss_gb,read_mbs,write_mbs,gpu0_util,gpu0_mem_gb'] + [
            f'{i * 2},1,{i * 100},{i / 10},,,{i * 10},{i / 2}' for i in range(10)]
        self._write(jobId, 'profile/run01.csv', '\n'.join(csvLines))
        self._write(jobId, 'profile/run03.csv', '\n'.join(csvLines[:4]))
        self._write(jobId, 'info.json', {'runs': [
            {'start': 's1', 'end': 'e1',
             'profile': {'file': 'profile/run01.csv', 'interval': 2,
                         'gpus': ['0'], 'summary': {'samples': 10}}},
            {'start': 's2', 'end': 'e2'},  # not profiled
            {'start': 's3', 'profile': {'file': 'profile/run03.csv', 'interval': 2}},
        ]})
        profiles = self.pm.getJobProfiles(jobId, maxPoints=5)
        self.assertEqual([p['run'] for p in profiles], [1, 3])
        p1, p3 = profiles
        self.assertEqual(p1['summary'], {'samples': 10})  # from info.json
        self.assertEqual(p1['samples'], 10)
        self.assertEqual(p1['series']['time'], [0, 4, 8, 12, 16])
        self.assertEqual(p1['file'], os.path.join(jobId, 'profile/run01.csv'))
        self.assertNotIn('latest', p3)  # not active
        self.assertEqual(p3['gpus'], ['0'])  # from the CSV columns
        self.assertEqual(p3['summary']['samples'], 3)  # computed from the CSV
        # Active job: the last sample of its last run (no end)
        p3 = self.pm.getJobProfiles(jobId, active=True)[-1]
        self.assertEqual(p3['latest']['time'], '4')
        self.assertIn('age', p3['latest'])
        self.assertEqual(len(self.pm.getJobProfiles(jobId, maxRuns=1)), 1)


if __name__ == '__main__':
    unittest.main()
