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

""" Unit tests for the project lock (no datasets needed):

    python -m unittest emwrap.tests.test_project_lock

Redis lock tests are skipped unless Redis is reachable from the redis.conf
in $EMWRAP_TEST_REDIS_CONF (not the EMhub instance one), e.g.:

    EMWRAP_TEST_REDIS_CONF=/path/to/redis.conf python -m unittest emwrap.tests.test_project_lock
"""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
import unittest.mock
import uuid

from emwrap.base.project_lock import (RelionDirLockBackend, RedisLockBackend,
                                      ProjectLock, create_lock_backend,
                                      redis_lock_client, relion_lock_paths,
                                      read_consistent)


class TestRelionDirLock(unittest.TestCase):

    def setUp(self):
        self.path = tempfile.mkdtemp(prefix='emwrap_lock_')
        self.lock_dir, self.lock_file = relion_lock_paths(self.path)

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def _backend(self, **kwargs):
        kwargs.setdefault('stale_seconds', 2)
        kwargs.setdefault('heartbeat_seconds', 0.2)
        return RelionDirLockBackend(**kwargs)

    def _in_thread(self, func):
        """ Run func in another thread and return its result or exception. """
        result = {}

        def _run():
            try:
                result['value'] = func()
            except Exception as e:
                result['error'] = e

        t = threading.Thread(target=_run)
        t.start()
        t.join()
        return result

    def _write_foreign_lock(self, payload, age=0):
        os.mkdir(self.lock_dir)
        with open(self.lock_file, 'w') as f:
            f.write(payload if isinstance(payload, str) else json.dumps(payload))
        if age:
            t = time.time() - age
            os.utime(self.lock_file, (t, t))

    def test_acquire_release(self):
        b = self._backend()
        b.acquire(self.path, message='test')
        with open(self.lock_file) as f:
            payload = json.load(f)
        self.assertEqual(payload['pid'], os.getpid())
        self.assertTrue(payload['token'])
        b.release(self.path)
        self.assertFalse(os.path.exists(self.lock_dir))

    def test_reentrant_same_thread(self):
        b = self._backend()
        b.acquire(self.path)
        b.acquire(self.path, timeout=1)
        b.release(self.path)
        self.assertTrue(os.path.exists(self.lock_dir))
        b.release(self.path)
        self.assertFalse(os.path.exists(self.lock_dir))

    def test_other_thread_waits(self):
        b = self._backend()
        b.acquire(self.path)
        try:
            r = self._in_thread(lambda: b.acquire(self.path, timeout=1))
            self.assertIsInstance(r.get('error'), TimeoutError)
        finally:
            b.release(self.path)

    def test_heartbeat_keeps_long_lock_alive(self):
        b = self._backend(stale_seconds=1)
        b.acquire(self.path)
        try:
            time.sleep(2.5)  # Longer than stale_seconds
            other = self._backend(stale_seconds=1)
            self.assertFalse(other.break_stale(self.path))
            self.assertTrue(os.path.exists(self.lock_file))
            self.assertFalse(b.is_lost(self.path))
        finally:
            b.release(self.path)
        self.assertFalse(os.path.exists(self.lock_dir))

    def test_break_old_relion_lock(self):
        # Relion writes a plain text lock, without pid or token
        self._write_foreign_lock('Relion GUI lock', age=10)
        b = self._backend()
        b.acquire(self.path, timeout=2)
        b.release(self.path)
        self.assertEqual([], [d for d in os.listdir(self.path)
                              if d.startswith('.relion_lock')])

    def test_recent_relion_lock_is_kept(self):
        self._write_foreign_lock('Relion GUI lock')
        b = self._backend(stale_seconds=30)
        self.assertFalse(b.break_stale(self.path))
        with self.assertRaises(TimeoutError):
            b.acquire(self.path, timeout=1)
        self.assertTrue(os.path.exists(self.lock_file))

    def test_break_lock_of_dead_process(self):
        import socket
        self._write_foreign_lock({'pid': 999999, 'token': 'x',
                                  'hostname': socket.gethostname()})
        b = self._backend(stale_seconds=30)
        b.acquire(self.path, timeout=2)
        b.release(self.path)

    def test_release_keeps_lock_taken_by_others(self):
        b = self._backend(heartbeat_seconds=0)
        b.acquire(self.path)
        with open(self.lock_file, 'w') as f:
            json.dump({'token': 'someone-else'}, f)
        b.release(self.path)
        self.assertTrue(os.path.exists(self.lock_file))

    def test_heartbeat_detects_lost_lock(self):
        b = self._backend()
        b.acquire(self.path)
        with open(self.lock_file, 'w') as f:
            json.dump({'token': 'someone-else'}, f)
        time.sleep(0.6)
        self.assertTrue(b.is_lost(self.path))
        b.release(self.path)
        self.assertTrue(os.path.exists(self.lock_file))

    def test_release_waits_for_nfs_temporary_file(self):
        # On NFS, a removed file still open elsewhere becomes .nfsXXXX until
        # closed, so rmdir fails for a moment; the directory must not be left
        b = self._backend(heartbeat_seconds=0)
        b.acquire(self.path)
        nfs_file = os.path.join(self.lock_dir, '.nfs000000000001')
        open(nfs_file, 'w').close()
        threading.Timer(0.3, os.remove, args=(nfs_file,)).start()
        b.release(self.path)
        self.assertFalse(os.path.exists(self.lock_dir))

    def test_release_keeps_dir_with_other_lock_file(self):
        b = self._backend(heartbeat_seconds=0)
        b.acquire(self.path)
        other = os.path.join(self.lock_dir, 'lock_other.star')
        open(other, 'w').close()
        start = time.time()
        b.release(self.path)
        self.assertLess(time.time() - start, 1)  # No retries
        self.assertTrue(os.path.exists(other))

    def test_break_empty_lock_dir_soon(self):
        # Empty lock directory left behind: stale after empty_stale_seconds
        os.mkdir(self.lock_dir)
        t = time.time() - 3
        os.utime(self.lock_dir, (t, t))
        b = self._backend(stale_seconds=30, empty_stale_seconds=2)
        start = time.time()
        b.acquire(self.path, timeout=5)
        b.release(self.path)
        self.assertLess(time.time() - start, 2)

    def test_recent_empty_lock_dir_is_kept(self):
        # Owner may not have written its lock file yet
        os.mkdir(self.lock_dir)
        b = self._backend(stale_seconds=30, empty_stale_seconds=2)
        self.assertFalse(b.break_stale(self.path))
        self.assertTrue(os.path.isdir(self.lock_dir))

    def test_wait_until_free(self):
        b = self._backend()
        acquired = threading.Event()

        def _hold():
            b.acquire(self.path)
            acquired.set()
            time.sleep(0.5)
            b.release(self.path)

        t = threading.Thread(target=_hold)
        t.start()
        acquired.wait()
        b.wait_until_free(self.path, timeout=5)
        t.join()
        self.assertFalse(os.path.exists(self.lock_dir))

    def test_project_lock_readonly(self):
        b = self._backend()
        with ProjectLock(self.path, backend=b, readonly=True):
            self.assertFalse(os.path.exists(self.lock_dir))
        with ProjectLock(self.path, backend=b):
            self.assertTrue(os.path.exists(self.lock_dir))
        self.assertFalse(os.path.exists(self.lock_dir))


TEST_REDIS_CONF = os.environ.get('EMWRAP_TEST_REDIS_CONF')


def _redis_client():
    """ Redis client from $EMWRAP_TEST_REDIS_CONF, None if Redis is not
    available, so Redis lock tests are skipped. $EMHUB_INSTANCE is not used,
    to avoid running the tests against an instance Redis by accident. """
    if not TEST_REDIS_CONF:
        return None
    try:
        return redis_lock_client(TEST_REDIS_CONF)
    except Exception:
        return None


def _increment_counter(path, prefix, n):
    """ Read-increment-write a counter file n times under the Redis lock
    (run in other processes to check mutual exclusion). """
    b = RedisLockBackend(redis=redis_lock_client(TEST_REDIS_CONF),
                         key_prefix=prefix)
    counter = os.path.join(path, 'counter')
    for _ in range(n):
        with ProjectLock(path, backend=b):
            with open(counter) as f:
                value = int(f.read())
            time.sleep(0.001)
            with open(counter, 'w') as f:
                f.write(str(value + 1))


class TestRedisLock(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.redis = _redis_client()
        if cls.redis is None:
            raise unittest.SkipTest('Redis not available, set EMWRAP_TEST_REDIS_CONF '
                                    'to a redis.conf file to run these tests')

    def setUp(self):
        self.path = tempfile.mkdtemp(prefix='emwrap_lock_')
        self.prefix = f'emwrap:test_lock:{uuid.uuid4().hex}:'

    def tearDown(self):
        for key in self.redis.scan_iter(self.prefix + '*'):
            self.redis.delete(key)
        shutil.rmtree(self.path, ignore_errors=True)

    def _backend(self, **kwargs):
        kwargs.setdefault('ttl_seconds', 2)
        kwargs.setdefault('heartbeat_seconds', 0.2)
        return RedisLockBackend(redis=self.redis, key_prefix=self.prefix, **kwargs)

    def _in_thread(self, func):
        """ Run func in another thread and return its result or exception. """
        result = {}

        def _run():
            try:
                result['value'] = func()
            except Exception as e:
                result['error'] = e

        t = threading.Thread(target=_run)
        t.start()
        t.join()
        return result

    def test_acquire_release(self):
        b = self._backend()
        key = b.acquire(self.path, message='test')
        payload = json.loads(self.redis.get(key + ':info'))
        self.assertEqual(payload['pid'], os.getpid())
        self.assertEqual(payload['token'], self.redis.get(key))
        b.release(self.path)
        self.assertFalse(self.redis.exists(key, key + ':info'))
        self.assertEqual([], os.listdir(self.path))  # No files in the project

    def test_reentrant_same_thread(self):
        b = self._backend()
        key = b.acquire(self.path)
        b.acquire(self.path, timeout=1)
        b.release(self.path)
        self.assertTrue(self.redis.exists(key))
        b.release(self.path)
        self.assertFalse(self.redis.exists(key))

    def test_other_thread_waits(self):
        b = self._backend()
        b.acquire(self.path)
        try:
            r = self._in_thread(lambda: b.acquire(self.path, timeout=1))
            self.assertIsInstance(r.get('error'), TimeoutError)
        finally:
            b.release(self.path)

    def _hold_in_thread(self, b, seconds):
        """ Hold the lock in another thread for some seconds (locks are per thread). """
        acquired = threading.Event()

        def _hold():
            b.acquire(self.path)
            acquired.set()
            time.sleep(seconds)
            b.release(self.path)

        t = threading.Thread(target=_hold)
        t.start()
        acquired.wait()
        return t

    def test_waiter_gets_lock_soon_after_release(self):
        b = self._backend()
        t = self._hold_in_thread(b, 0.3)
        start = time.time()
        b.acquire(self.path, timeout=5)
        b.release(self.path)
        t.join()
        # Waiters poll with backoff up to MAX_POLL_INTERVAL, not 0.5s
        self.assertLess(time.time() - start, 0.3 + 2 * b.MAX_POLL_INTERVAL)

    def test_heartbeat_keeps_long_lock_alive(self):
        b = self._backend(ttl_seconds=1)
        key = b.acquire(self.path)
        try:
            time.sleep(2.5)  # Longer than the ttl
            self.assertTrue(self.redis.exists(key))
            self.assertFalse(b.is_lost(self.path))
        finally:
            b.release(self.path)
        self.assertFalse(self.redis.exists(key))

    def test_lock_of_dead_process_expires(self):
        b = self._backend(ttl_seconds=0.5, heartbeat_seconds=0)
        b.acquire(self.path)  # Never released nor refreshed, like a crash
        other = self._backend(ttl_seconds=0.5)
        start = time.time()
        r = self._in_thread(lambda: other.acquire(self.path, timeout=5))
        self.assertNotIn('error', r)
        self.assertLess(time.time() - start, 2)

    def test_release_keeps_lock_taken_by_others(self):
        b = self._backend(heartbeat_seconds=0)
        key = b.acquire(self.path)
        self.redis.set(key, 'someone-else')
        b.release(self.path)
        self.assertEqual('someone-else', self.redis.get(key))

    def test_heartbeat_detects_lost_lock(self):
        b = self._backend()
        key = b.acquire(self.path)
        self.redis.set(key, 'someone-else')
        time.sleep(0.6)
        self.assertTrue(b.is_lost(self.path))
        b.release(self.path)
        self.assertEqual('someone-else', self.redis.get(key))

    def test_wait_until_free(self):
        b = self._backend()
        t = self._hold_in_thread(b, 0.5)
        b.wait_until_free(self.path, timeout=5)
        t.join()
        self.assertFalse(self.redis.exists(b.lock_key(self.path)))

    def test_project_lock_readonly(self):
        b = self._backend()
        key = b.lock_key(self.path)
        with ProjectLock(self.path, backend=b, readonly=True):
            self.assertFalse(self.redis.exists(key))
        with ProjectLock(self.path, backend=b):
            self.assertTrue(self.redis.exists(key))
        self.assertFalse(self.redis.exists(key))

    def test_mutual_exclusion_between_processes(self):
        import multiprocessing
        with open(os.path.join(self.path, 'counter'), 'w') as f:
            f.write('0')
        procs = [multiprocessing.Process(target=_increment_counter,
                                         args=(self.path, self.prefix, 25))
                 for _ in range(4)]
        for p in procs:
            p.start()
        for p in procs:
            p.join(60)
            self.assertEqual(0, p.exitcode)
        with open(os.path.join(self.path, 'counter')) as f:
            self.assertEqual(100, int(f.read()))

    def test_create_backend_from_env(self):
        # The default client reads redis.conf from the EMhub instance folder
        instance = os.path.join(self.path, 'instance')
        os.mkdir(instance)
        shutil.copy(TEST_REDIS_CONF, os.path.join(instance, 'redis.conf'))
        with unittest.mock.patch.dict(os.environ, {'EMHUB_INSTANCE': instance}):
            b = create_lock_backend('redis')
            self.assertIsInstance(b, RedisLockBackend)
            self.assertTrue(b.redis.ping())


class TestReadConsistent(unittest.TestCase):

    def setUp(self):
        fd, self.file = tempfile.mkstemp(prefix='emwrap_read_')
        os.close(fd)

    def tearDown(self):
        os.remove(self.file)

    def test_retry_until_valid(self):
        calls = []

        def _reader(path):
            calls.append(1)
            if len(calls) == 1:
                with open(path, 'w') as f:  # Simulate a writer finishing
                    f.write('{"ok": true}')
                raise ValueError('incomplete file')
            with open(path) as f:
                return json.load(f)

        self.assertEqual({'ok': True}, read_consistent(self.file, _reader, delay=0.01))
        self.assertEqual(2, len(calls))

    def test_raise_when_always_invalid(self):
        def _reader(path):
            raise ValueError('bad file')

        with self.assertRaises(ValueError):
            read_consistent(self.file, _reader, retries=2, delay=0.01)

    def test_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            read_consistent(self.file + '.missing', open)


class TestProjectManagerLocking(unittest.TestCase):

    def setUp(self):
        from emwrap.base.project_manager import ProjectManager
        self.path = tempfile.mkdtemp(prefix='emwrap_project_')
        self.pm = ProjectManager(self.path, create=True, verbose=0)
        self.backend = RelionDirLockBackend(stale_seconds=30)
        self._prev_backend = ProjectLock._default_backend
        ProjectLock.set_default_backend(self.backend)

    def tearDown(self):
        ProjectLock.set_default_backend(self._prev_backend)
        shutil.rmtree(self.path, ignore_errors=True)

    def _hold_lock_in_thread(self, release_event):
        acquired = threading.Event()

        def _hold():
            self.backend.acquire(self.pm.path)
            acquired.set()
            release_event.wait()
            self.backend.release(self.pm.path)

        t = threading.Thread(target=_hold)
        t.start()
        acquired.wait()
        return t

    def test_update_without_changes_does_not_lock(self):
        release = threading.Event()
        t = self._hold_lock_in_thread(release)
        try:
            start = time.time()
            self.pm.update()  # Would block on the lock held by the thread
            self.assertLess(time.time() - start, 5)
        finally:
            release.set()
            t.join()

    def _updates_persisting(self, n=1):
        """ Run update() n times, each from a new ProjectManager (as EMhub
        does on every request), and return how many saved the project. """
        from emwrap.base.project_manager import ProjectManager
        saved = 0
        for _ in range(n):
            pm = ProjectManager(self.pm.path, verbose=0)
            persist = pm._persist_workflow
            calls = []
            pm._persist_workflow = lambda: (calls.append(1), persist())
            pm.update()
            saved += len(calls)
        return saved

    def test_pending_output_does_not_persist_every_update(self):
        # A running job whose output is not written yet ('Pending')
        from emtools.metadata import RelionStar
        job_id, output_id = 'External/job001', 'External/job001/out.star'
        wf = self.pm.get_workflow()
        job = wf.registerJob(job_id, jobtype='emw-test', status='Running', alias='None')
        job.registerOutput(output_id, datatype='File')
        RelionStar.workflow_to_pipeline(wf, self.pm.pipeline_star)
        os.makedirs(self.pm.join(job_id))
        RelionStar.write_jobstar('emw-test', {'param': 'x'}, self.pm.join(job_id, 'job.star'))
        open(self.pm.join(job_id, 'RELION_JOB_RUNNING'), 'w').close()

        self.assertEqual(1, self._updates_persisting())  # First time: new info
        self.assertEqual(0, self._updates_persisting(3))  # Nothing changed

        # Once written, the output info is computed (no longer 'Pending')
        with open(self.pm.join(output_id), 'w') as f:
            f.write('data_\n')
        self.assertEqual(1, self._updates_persisting())
        from emwrap.base.project_manager import ProjectManager
        info = ProjectManager(self.pm.path, verbose=0)._data._outputs[output_id]
        self.assertNotEqual('Pending', info['info'])

    def test_clean_keeps_lock_and_removes_trash(self):
        os.makedirs(self.pm.join('External', 'job001'))
        self.pm.clean()
        entries = os.listdir(self.pm.path)
        self.assertNotIn('External', entries)
        self.assertNotIn('.relion_lock', entries)
        self.assertEqual([], [e for e in entries if e.startswith('.emwrap_clean_')])
        self.assertIn('default_pipeline.star', entries)


if __name__ == '__main__':
    unittest.main()
