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

import argparse
import inspect
import json
import os
import sys
import unittest
from contextlib import contextmanager
import tempfile
from emtools.utils import Color, Path

from emwrap.base import ProjectManager
from emwrap.base.config import ProcessingConfig


def _emwrap_configured():
    return bool(ProcessingConfig.get_jobs())


class TestWarpApoF(unittest.TestCase):
    emwrap_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    workflow_template = os.path.join(
        emwrap_root, 'config', 'workflows', 'apof-warp-tutorial-part1.json.template')

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

    project_path = None
    project_temporary = False

    tilt_series = None
    data_root = None
    ngpus = 1

    @classmethod
    def configure(cls, project_dir=None, tilt_series=None, ngpus=None, dry=False):
        """Set class-level options before running tests."""
        if project_dir is None:
            cls.project_temporary = True
            tmpdir = tempfile.TemporaryDirectory(prefix=f"{cls.__name__}__")
            cls.addClassCleanup(tmpdir.cleanup)
            cls.project_path = tmpdir.name
        else:
            cls.project_temporary = False
            cls.project_path = project_dir
            
        cls.tilt_series = tilt_series or '*'
        cls.ngpus = ngpus or int(os.environ.get('EMWRAP_TEST_GPUS', 1))
        cls.dry = dry
        cls.data_root = ProcessingConfig.get_testdata_path('WarpApofTutorial', validate=True)
        

    @classmethod
    def load_workflow_jobs(cls):
        with open(cls.workflow_template) as f:
            workflow = json.load(f)
        jobs_by_type = {
            j['jobtype']: j for j in workflow['jobs'] if j['jobtype'] in cls.job_types}
        if len(jobs_by_type) != len(cls.job_types):
            missing = set(cls.job_types) - set(jobs_by_type)
            raise ValueError(f"Workflow template is missing jobs: {missing}")
        return [jobs_by_type[job_type] for job_type in cls.job_types]

    def patch_workflow_params(self, jobs):
        """Adjust workflow params for the local test environment."""
        patched = []
        for job in jobs:
            params = dict(job['params'])
            if job['jobtype'] == 'emw-import-ts':
                params['mdoc_files'] = f'data/mdoc/{self.tilt_series}.mrc.mdoc'
            if 'gpus' in params:
                params['gpus'] = str(self.ngpus)
            patched.append({**job, 'params': params})
        return patched

    def _validate_environment(self):
        if not _emwrap_configured():
            self.fail('EMWRAP_CONFIG is not configured (source emwrap.bashrc)')
        if not self.data_root:
            self.fail(
                'Test data path for WarpApofTutorial is not configured in '
                'EMWRAP_CONFIG')
        if not os.path.isdir(self.data_root):
            self.fail(f'Test data folder does not exist: {self.data_root}')

    @classmethod
    def _link_data(cls):
        data_src = os.path.abspath(cls.data_root)
        data_dst = os.path.join(cls.project_path, 'data')
        if os.path.lexists(data_dst):
            os.remove(data_dst)
        os.symlink(data_src, data_dst)

    def _assert_job_succeeded(self, pm, job_id, output_star):
        self.assertTrue(
            pm.exists(job_id, 'RELION_JOB_EXIT_SUCCESS'),
            f"Job {job_id} did not finish successfully")
        self.assertTrue(
            pm.exists(job_id, output_star),
            f"Job {job_id} is missing expected output: {output_star}")
        job = pm._getJob(job_id)
        pm.update()
        self.assertEqual(job['status'], 'Succeeded')

    def _run_workflow(self):
        self._validate_environment()
        caller_name = inspect.currentframe().f_back.f_code.co_name
        test_name = f"{self.__class__.__name__}.{caller_name}"
        print(Color.warn(f"\n============= Running test: {test_name} ============="))
        if self.tilt_series:
            print(Color.warn(f"Restricting to tilt series: {self.tilt_series}"))

        jobs = self.load_workflow_jobs()
        workflow = {'jobs': self.patch_workflow_params(jobs)}
        pm = ProjectManager(self.project_path, create=True)
        self._link_data()
        id_map =pm.loadWorkflow(workflow=workflow)
        job_ids = [id_map[job['jobid']] for job in jobs]
        for job_type, job_id in zip(self.job_types, job_ids):
            if self.dry:
                print(Color.warn(f"Dry run: would run job {job_id} for {job_type}"))
                pm.saveJob
                continue
            else:
                pm.runJob(job_id, wait=True)
                pm.update()
                self._assert_job_succeeded(pm, job_id, self.expected_outputs[job_type])
            
    def test_apof_warp_preprocessing(self):
        self._run_workflow()

    @classmethod
    def run_tests(cls, verbosity=2):
        suite = unittest.TestLoader().loadTestsFromTestCase(cls)
        return unittest.TextTestRunner(verbosity=verbosity).run(suite)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run Warp ApoF integration tests.')
    parser.add_argument(
        '--project', '-p', metavar='PATH',
        help='Project folder for the test run. Outputs are kept for inspection.')
    parser.add_argument(
        '--ts', metavar='NAME',
        help='Run a single tilt series (e.g. TS_11). Sets mdoc_files to data/mdoc/NAME.mdoc')
    parser.add_argument(
        '--gpus', '-g', metavar='GPUs', type=int, default=1,
        help='Number of GPUs to use for the test run.')
    parser.add_argument(
        '-v', '--verbose', action='count', default=0,
        help='Increase unittest output verbosity.')
    parser.add_argument(
        '--dry', action='store_true',
        help='Dry run: do not actually run the jobs, just print what would be done.')
    args = parser.parse_args()

    TestWarpApoF.configure(project_dir=args.project, tilt_series=args.ts, ngpus=args.gpus, dry=args.dry)
    verbosity = min(2, args.verbose) if args.verbose else 1
    result = TestWarpApoF.run_tests(verbosity=verbosity)
    sys.exit(0 if result.wasSuccessful() else 1)
