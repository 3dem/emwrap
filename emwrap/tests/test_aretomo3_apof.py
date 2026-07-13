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

from emtools.utils import Color, Path

from emwrap.base import ProjectManager
from emwrap.base.config import ProcessingConfig

EMWRAP_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WORKFLOW_TEMPLATE = os.path.join(
    EMWRAP_ROOT, 'config', 'workflows', 'apof-aretomo3.json.template')

DATA_ROOT = ProcessingConfig.get_testdata_path('WarpApofTutorial')
PROJECT_DIR = None
TILT_SERIES = None

JOB_TYPES = [
    'emw-import-ts',
    'emw-aretomo3'
]

EXPECTED_OUTPUTS = {
    'emw-import-ts': ['tilt_series.star'],
    'emw-aretomo3': ['aligned_tilt_series.star', 'tomograms.star'],
}


def _emwrap_configured():
    return bool(ProcessingConfig.get_jobs())


def _test_ngpus():
    return int(os.environ.get('EMWRAP_TEST_GPUS', '1'))


def _load_workflow_jobs():
    with open(WORKFLOW_TEMPLATE) as f:
        workflow = json.load(f)
    jobs_by_type = {
        j['jobtype']: j for j in workflow['jobs'] if j['jobtype'] in JOB_TYPES}
    if len(jobs_by_type) != len(JOB_TYPES):
        missing = set(JOB_TYPES) - set(jobs_by_type)
        raise ValueError(f"Workflow template is missing jobs: {missing}")
    return [jobs_by_type[job_type] for job_type in JOB_TYPES]


def _patch_workflow_params(jobs, ngpus, ts_name=None):
    """Adjust workflow params for the local test environment."""
    patched = []
    for job in jobs:
        params = dict(job['params'])
        if job['jobtype'] == 'emw-import-ts':
            if ts_name:
                params['mdoc_files'] = f'data/mdoc/{ts_name}.mrc.mdoc'
            else:
                params['mdoc_files'] = 'data/mdoc/*.mdoc'
        if 'gpus' in params:
            params['gpus'] = str(ngpus)
    
        # Let's launch the test always in the local machine
        if 'queue.name' in params:
            params['queue.name'] = 'NO-QUEUE'
        patched.append({**job, 'params': params})
    return patched


def _resolve_data_dir(data_root):
    """Return the folder that should be linked as project data/."""
    data_sub = os.path.join(data_root, 'data')
    if os.path.isdir(data_sub):
        return os.path.abspath(data_sub)

    for rel in ('gain_ref.mrc', 'mdoc', 'frames'):
        if not os.path.exists(os.path.join(data_root, rel)):
            raise FileNotFoundError(
                f"EMWRAP_WARP_TUTORIAL must contain data/ or "
                f"{{gain_ref.mrc, mdoc, frames}}: missing {rel}")

    return os.path.abspath(data_root)


def _link_data(project_dir, data_root):
    data_src = _resolve_data_dir(data_root)
    data_dst = os.path.join(project_dir, 'data')
    if os.path.lexists(data_dst):
        os.remove(data_dst)
    os.symlink(data_src, data_dst)


@contextmanager
def _project_manager(test_name, project_dir=None):
    """Use a persistent project folder or a temporary one."""
    if project_dir:
        project_path = os.path.abspath(project_dir)
        os.makedirs(project_path, exist_ok=True)
        pipeline_star = os.path.join(project_path, 'default_pipeline.star')
        if os.path.exists(pipeline_star):
            pm = ProjectManager(project_path)
            pm.clean()
        else:
            pm = ProjectManager(project_path, create=True)
        yield pm

        print(Color.warn(f"Project kept at: {project_path}"))
    else:
        with Path.tmpDir(prefix=f"{test_name}__", chdir=True) as project_path:
            yield ProjectManager(project_path, create=True)


class TestAretomo3ApoF(unittest.TestCase):

    def _validate_environment(self):
        if not _emwrap_configured():
            self.fail('EMWRAP_CONFIG is not configured (source emwrap.bashrc)')
        if not DATA_ROOT:
            self.fail(
                'Test data path for WarpApofTutorial is not configured in '
                'EMWRAP_CONFIG')
        if not os.path.isdir(DATA_ROOT):
            self.fail(f'Test data folder does not exist: {DATA_ROOT}')

    def _assert_job_succeeded(self, pm, job_id, output_stars):
        self.assertTrue(
            pm.exists(job_id, 'RELION_JOB_EXIT_SUCCESS'),
            f"Job {job_id} did not finish successfully")

        if isinstance(output_stars, str):
            output_stars = [output_stars]

        for output_star in output_stars:
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
        if TILT_SERIES:
            print(Color.warn(f"Restricting to tilt series: {TILT_SERIES}"))

        jobs = _patch_workflow_params(
            _load_workflow_jobs(), _test_ngpus(), ts_name=TILT_SERIES)
        workflow = {'jobs': jobs}

        with _project_manager(test_name, project_dir=PROJECT_DIR) as pm:
            pm.link(DATA_ROOT, 'data')
            _link_data(pm.path, DATA_ROOT)
            id_map = pm.loadWorkflow(workflow=workflow)
            job_ids = [id_map[job['jobid']] for job in jobs]

            for job_type, job_id in zip(JOB_TYPES, job_ids):
                pm.runJob(job_id, wait=True)
                pm.update()
                self._assert_job_succeeded(pm, job_id, EXPECTED_OUTPUTS[job_type])

    def test_apof_aretomo3_preprocessing(self):
        self._run_workflow()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run Aretomo3 ApoF integration tests.')
    parser.add_argument(
        '--project', '-p', metavar='PATH',
        help='Project folder for the test run. Outputs are kept for inspection.')
    parser.add_argument(
        '--ts', metavar='NAME',
        help='Run a single tilt series (e.g. TS_11). Sets mdoc_files to data/mdoc/NAME.mdoc')
    args, unittest_argv = parser.parse_known_args()
    PROJECT_DIR = args.project
    TILT_SERIES = args.ts
    sys.argv = [sys.argv[0]] + unittest_argv
    unittest.main()
