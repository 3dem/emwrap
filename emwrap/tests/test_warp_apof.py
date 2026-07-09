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
    EMWRAP_ROOT, 'config', 'workflows', 'apof-warp-tutorial-part1.json.template')

DATA_ROOT = os.environ.get('EMWRAP_WARP_TUTORIAL', '')
PROJECT_DIR = None
TILT_SERIES = None

JOB_TYPES = [
    'emw-import-ts',
    'emw-warp-mctf',
    'emw-warp-aretomo',
    'emw-warp-ctfrec',
]

EXPECTED_OUTPUTS = {
    'emw-import-ts': 'tilt_series.star',
    'emw-warp-mctf': 'tilt_series.star',
    'emw-warp-aretomo': 'aligned_tilt_series.star',
    'emw-warp-ctfrec': 'tomograms.star',
}


def _emwrap_configured():
    return bool(ProcessingConfig.get_jobs())


def _test_ngpus():
    return int(os.environ.get('EMWRAP_TEST_GPUS', '1'))


def _load_workflow_jobs():
    with open(WORKFLOW_TEMPLATE) as f:
        workflow = json.load(f)
    jobs = [j for j in workflow['jobs'] if j['jobtype'] in JOB_TYPES]
    if len(jobs) != len(JOB_TYPES):
        missing = set(JOB_TYPES) - {j['jobtype'] for j in jobs}
        raise ValueError(f"Workflow template is missing jobs: {missing}")
    return jobs


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
def _project_dir(test_name):
    """Use a persistent project folder or a temporary one."""
    if PROJECT_DIR:
        project_dir = os.path.abspath(PROJECT_DIR)
        os.makedirs(project_dir, exist_ok=True)
        cwd = os.getcwd()
        os.chdir(project_dir)
        try:
            print(Color.warn(f"Using project dir: {project_dir}"))
            yield project_dir
        finally:
            os.chdir(cwd)
        print(Color.warn(f"Project kept at: {project_dir}"))
    else:
        with Path.tmpDir(prefix=f"{test_name}__", chdir=True) as project_dir:
            yield project_dir


def _create_project(project_dir):
    pipeline_star = os.path.join(project_dir, 'default_pipeline.star')
    if os.path.exists(pipeline_star):
        pm = ProjectManager(project_dir)
        pm.clean()
    else:
        pm = ProjectManager(project_dir, create=True)
    return pm


@unittest.skipUnless(DATA_ROOT and os.path.isdir(DATA_ROOT),
                     'EMWRAP_WARP_TUTORIAL must point to an existing folder')
@unittest.skipUnless(_emwrap_configured(),
                     'EMWRAP_CONFIG is not configured (source emwrap.bashrc)')
class TestWarpApoF(unittest.TestCase):

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
        caller_name = inspect.currentframe().f_back.f_code.co_name
        test_name = f"{self.__class__.__name__}.{caller_name}"
        print(Color.warn(f"\n============= Running test: {test_name} ============="))
        if TILT_SERIES:
            print(Color.warn(f"Restricting to tilt series: {TILT_SERIES}"))

        jobs = _patch_workflow_params(
            _load_workflow_jobs(), _test_ngpus(), ts_name=TILT_SERIES)
        workflow = {'jobs': jobs}

        with _project_dir(test_name) as project_dir:
            _link_data(project_dir, DATA_ROOT)
            pm = _create_project(project_dir)
            id_map = pm.loadWorkflow(workflow=workflow)

            job_ids = []
            for job_type in JOB_TYPES:
                job_id = next(
                    new_id for old_id, new_id in id_map.items()
                    if pm._getJob(new_id)['jobtype'] == job_type)
                job_ids.append(job_id)

            for job_type, job_id in zip(JOB_TYPES, job_ids):
                pm.runJob(job_id, wait=True)
                pm.update()
                self._assert_job_succeeded(pm, job_id, EXPECTED_OUTPUTS[job_type])

    def test_apof_warp_preprocessing(self):
        self._run_workflow()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run Warp ApoF integration tests.')
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
