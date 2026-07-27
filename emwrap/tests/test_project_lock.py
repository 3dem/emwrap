import json
import os
import shutil
import tempfile
import unittest

from emtools.jobs import Workflow
from emtools.metadata import RelionStar

from emwrap.base.project_lock import (
    NoopLockBackend,
    ProjectLock,
    RelionDirLockBackend,
    atomic_write_json,
    relion_lock_paths,
)
from emwrap.base.project_data import ProjectData
from emwrap.base.project_manager import ProjectManager


class TestProjectLock(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = self.tmpdir.name

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_relion_lock_paths(self):
        lock_dir, lock_file = relion_lock_paths(self.path)
        self.assertEqual(lock_dir, os.path.join(self.path, '.relion_lock'))
        self.assertEqual(lock_file, os.path.join(lock_dir, 'lock_default_pipeline.star'))

    def test_relion_dir_lock_acquire_release(self):
        backend = RelionDirLockBackend(stale_seconds=1)
        token = backend.acquire(self.path, message='test')
        lock_dir, lock_file = relion_lock_paths(self.path)
        self.assertTrue(os.path.isdir(lock_dir))
        self.assertTrue(os.path.exists(lock_file))
        self.assertIsNotNone(token)

        with open(lock_file) as f:
            payload = json.load(f)
        self.assertEqual(payload['message'], 'test')
        self.assertIn('pid', payload)

        backend.release(self.path)
        self.assertFalse(os.path.exists(lock_file))
        self.assertFalse(os.path.isdir(lock_dir))

    def test_project_lock_context_manager(self):
        ProjectLock.set_default_backend(NoopLockBackend())
        with ProjectLock(self.path, message='noop'):
            pass

    def test_atomic_write_json(self):
        target = os.path.join(self.path, 'project.json')
        atomic_write_json(target, {'jobs': {}, 'outputs': {}})
        with open(target) as f:
            data = json.load(f)
        self.assertEqual(data, {'jobs': {}, 'outputs': {}})


class TestWorkflowSaveLock(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = self.tmpdir.name
        ProjectLock.set_default_backend(RelionDirLockBackend())
        self.pm = ProjectManager(self.path, create=True)

    def tearDown(self):
        ProjectLock.set_default_backend(None)
        self.tmpdir.cleanup()

    def test_save_workflow_data_uses_lock(self):
        wf = self.pm.get_workflow()
        job = wf.registerJob(
            'Import/job001',
            status=ProjectData.STATUS_SAVED,
            alias='None',
            jobtype='emw-import-ts',
            jobindex=1,
        )
        self.pm._data.setJobInfo(job.id, {
            'status': ProjectData.STATUS_SAVED,
            'inputs': [],
            'outputs': [],
        })

        lock_dir, lock_file = relion_lock_paths(self.path)
        self.pm._save_workflow_data()

        self.assertTrue(os.path.exists(self.pm.pipeline_star))
        self.assertTrue(os.path.exists(self.pm.join('project.json')))
        self.assertFalse(os.path.exists(lock_file))
        self.assertFalse(os.path.isdir(lock_dir))

    def test_reload_preserves_pipeline_outputs(self):
        wf = self.pm.get_workflow()
        job = wf.registerJob(
            'Import/job001',
            status=ProjectData.STATUS_SAVED,
            alias='None',
            jobtype='emw-import-ts',
            jobindex=1,
        )
        output_id = 'Import/job001/movies.star'
        job.registerOutput(output_id, datatype='File')
        self.pm._save_workflow_data()

        reloaded = RelionStar.pipeline_to_workflow(self.pm.pipeline_star)
        self.assertTrue(reloaded.getJob(job.id).hasOutput(output_id))

        # Simulate stale in-memory state (lost update without reload).
        job.removeOutput(output_id)
        self.assertFalse(wf.getJob(job.id).hasOutput(output_id))

        self.pm._save_workflow_data()
        final = RelionStar.pipeline_to_workflow(self.pm.pipeline_star)
        self.assertTrue(final.getJob(job.id).hasOutput(output_id))

    def test_cached_job_info_does_not_prune_outputs(self):
        wf = self.pm.get_workflow()
        job = wf.registerJob(
            'Import/job001',
            status=ProjectData.STATUS_SAVED,
            alias='None',
            jobtype='emw-import-ts',
            jobindex=1,
        )
        output_id = 'Import/job001/movies.star'
        job.registerOutput(output_id, datatype='File')
        self.pm._save_workflow_data()

        self.pm._data.setJobInfo(job.id, {
            'status': ProjectData.STATUS_SAVED,
            'inputs': [],
            'outputs': [],
            'ts': 9999999999.0,
        })

        self.pm.reload_from_disk()
        job = self.pm.get_workflow().getJob(job.id)
        self.assertTrue(job.hasOutput(output_id))

        self.pm._data.updateWorkflow()
        self.assertTrue(job.hasOutput(output_id))

    def test_remove_missing_job_folder(self):
        wf = self.pm.get_workflow()
        job = wf.registerJob(
            'Import/job001',
            status=ProjectData.STATUS_SAVED,
            alias='None',
            jobtype='emw-import-ts',
            jobindex=1,
        )
        self.pm.mkdir(job.id)
        self.pm._data.setJobInfo(job.id, {
            'status': ProjectData.STATUS_SAVED,
            'inputs': [],
            'outputs': [],
        })
        self.pm._save_workflow_data()

        shutil.rmtree(self.pm.join(job.id))
        self.pm.update()

        self.assertFalse(self.pm.get_workflow().hasJob(job.id))
        reloaded = RelionStar.pipeline_to_workflow(self.pm.pipeline_star)
        self.assertFalse(reloaded.hasJob(job.id))

        with open(self.pm.join('project.json')) as f:
            data = json.load(f)
        self.assertNotIn(job.id, data.get('jobs', {}))


if __name__ == '__main__':
    unittest.main()
