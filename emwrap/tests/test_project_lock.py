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
"""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest

from emwrap.base.project_lock import (RelionDirLockBackend, ProjectLock,
                                      relion_lock_paths, read_consistent)


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
