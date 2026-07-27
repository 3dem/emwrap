import json
import os
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


if __name__ == '__main__':
    unittest.main()
