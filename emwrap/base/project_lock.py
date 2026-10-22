# **************************************************************************
# *
# * Authors:     J.M. de la Rosa Trevin (delarosatrevin@gmail.com)
# *
# * Project-level locking for .emhub/project.json / default_pipeline.star updates.
# *
# * Default backend uses Relion's .relion_lock layout so emwrap, Relion GUI
# * and emw CLI coordinate on NFS-friendly directory locks. Additional backends
# * (fcntl, noop, future server/redis) can be selected via EMWRAP_PROJECT_LOCK.
# *
# * Only writers take the lock. Readers do not lock: files written by emwrap
# * are replaced atomically, and files that Relion rewrites in place are read
# * with read_consistent(), which retries if the file changes while reading.
# *
# **************************************************************************

import errno
import json
import os
import shutil
import socket
import threading
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone

DEFAULT_PIPELINE = 'default_pipeline.star'
DEFAULT_STALE_SECONDS = 30
DEFAULT_HEARTBEAT_SECONDS = 5
DEFAULT_MAX_HOLD_SECONDS = 3600
DEFAULT_POLL_INTERVAL = 0.5
DEFAULT_TIMEOUT = 60
CLOCK_PROBE = '.emwrap_lock_clock'


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _hostname():
    try:
        return socket.gethostname()
    except Exception:
        return 'unknown'


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def _pid_alive(pid):
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _fresh_fstat(path):
    """Stat a path revalidating attributes with the server. On NFS, a plain
    stat() can return cached attributes for up to a minute, while open()
    forces close-to-open revalidation. Return None if the path is missing."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return None
    try:
        return os.fstat(fd)
    finally:
        os.close(fd)


def _fresh_mtime(path):
    st = _fresh_fstat(path)
    return None if st is None else st.st_mtime


def server_now(directory):
    """Current time according to the filesystem holding directory.
    os.utime(path, None) sets the mtime with the NFS server clock, so lock
    ages are not affected by clock skew between hosts."""
    probe = os.path.join(directory, CLOCK_PROBE)
    try:
        with open(probe, 'a'):
            pass
        os.utime(probe, None)
        return os.stat(probe).st_mtime
    except OSError:
        return time.time()


def file_signature(path):
    """(inode, size, mtime) of a file, revalidated with the server, or None."""
    st = _fresh_fstat(path)
    return None if st is None else (st.st_ino, st.st_size, st.st_mtime_ns)


def read_consistent(path, reader, retries=5, delay=0.2):
    """Read a file without locking, retrying when parsing fails or the file
    changes while being read (e.g. default_pipeline.star rewritten in place
    by Relion). Missing files raise FileNotFoundError right away."""
    error = None
    for _ in range(retries):
        before = file_signature(path)
        if before is None:
            raise FileNotFoundError(errno.ENOENT, 'File not found', path)
        try:
            result = reader(path)
            if file_signature(path) == before:
                return result
            error = None
        except FileNotFoundError:
            raise
        except Exception as e:
            error = e
        time.sleep(delay)
    if error is not None:
        raise error
    return reader(path)


def relion_lock_paths(project_path, pipeline_name=DEFAULT_PIPELINE):
    """Return Relion-compatible lock directory and lock file paths."""
    base = os.path.basename(pipeline_name)
    lock_dir = os.path.join(project_path, '.relion_lock')
    lock_file = os.path.join(lock_dir, f'lock_{base}')
    return lock_dir, lock_file


def _tmp_path(directory, path):
    """Unique temporary path (also across threads of the same process)."""
    name = os.path.basename(path)
    return os.path.join(directory, f'.{name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp')


def atomic_write_json(path, data, indent=4):
    """Write JSON atomically via temp file + replace."""
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    tmp = _tmp_path(directory, path)
    with open(tmp, 'w') as f:
        json.dump(data, f, indent=indent)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_write_text(path, content):
    """Write text atomically via temp file + replace."""
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    tmp = _tmp_path(directory, path)
    with open(tmp, 'w') as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class ProjectLockBackend(ABC):
    """Lock backend interface; swap implementations without changing callers."""

    @abstractmethod
    def acquire(self, project_path, message='', timeout=DEFAULT_TIMEOUT):
        """Acquire the project lock. Raises TimeoutError on failure."""

    @abstractmethod
    def release(self, project_path):
        """Release the project lock acquired by this process."""


class NoopLockBackend(ProjectLockBackend):
    """No locking (tests or explicitly disabled environments)."""

    def acquire(self, project_path, message='', timeout=DEFAULT_TIMEOUT):
        return None

    def release(self, project_path):
        return None


class FcntlLockBackend(ProjectLockBackend):
    """Local POSIX flock lock (.emwrap_project.lock). Not ideal for all NFS mounts."""

    def __init__(self):
        self._handles = {}

    def _lock_path(self, project_path):
        return os.path.join(project_path, '.emwrap_project.lock')

    def acquire(self, project_path, message='', timeout=DEFAULT_TIMEOUT):
        import fcntl

        path = self._lock_path(project_path)
        handle = open(path, 'a+')
        handle.write(f'{os.getpid()} {_hostname()} {_utc_now()} {message}\n')
        handle.flush()

        deadline = time.time() + timeout
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() >= deadline:
                    handle.close()
                    raise TimeoutError(f'Timed out waiting for lock {path}')
                time.sleep(DEFAULT_POLL_INTERVAL)

        self._handles[project_path] = handle
        return handle

    def release(self, project_path):
        import fcntl

        handle = self._handles.pop(project_path, None)
        if handle is None:
            return
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


class _HeldLock:
    """State of a lock held by this process (re-entrant, heartbeated)."""

    def __init__(self, lock_dir, lock_file, token):
        self.lock_dir = lock_dir
        self.lock_file = lock_file
        self.token = token
        self.depth = 1
        self.lost = False
        self.stop = threading.Event()
        self.thread = None


class RelionDirLockBackend(ProjectLockBackend):
    """Relion-compatible mkdir lock under .relion_lock/.

    On top of Relion's layout, locks taken by emwrap are:
      * heartbeated: a thread touches the lock file while it is held, so long
        operations are not mistaken for stale locks;
      * owned: a token in the lock file is checked before touching or
        releasing, so a process never removes a lock taken by someone else;
      * re-entrant within the same thread (other threads of the process
        wait like any other process).
    Staleness is measured with the filesystem (NFS server) clock, and stale
    locks are broken with an atomic rename, so only one process wins.
    """

    def __init__(self, pipeline_name=DEFAULT_PIPELINE,
                 stale_seconds=None, heartbeat_seconds=None,
                 max_hold_seconds=None):
        self._pipeline_name = pipeline_name
        self._stale_seconds = (stale_seconds if stale_seconds is not None else
                               _env_float('EMWRAP_LOCK_STALE_SECONDS',
                                          DEFAULT_STALE_SECONDS))
        self._heartbeat_seconds = (
            heartbeat_seconds if heartbeat_seconds is not None else
            _env_float('EMWRAP_LOCK_HEARTBEAT_SECONDS', DEFAULT_HEARTBEAT_SECONDS))
        self._max_hold_seconds = (
            max_hold_seconds if max_hold_seconds is not None else
            _env_float('EMWRAP_LOCK_MAX_HOLD_SECONDS', DEFAULT_MAX_HOLD_SECONDS))
        self._held = {}
        self._mutex = threading.RLock()

    def _lock_payload(self, message, token):
        payload = {
            'pid': os.getpid(),
            'hostname': _hostname(),
            'timestamp': _utc_now(),
            'token': token,
            'message': message or 'emwrap project lock',
        }
        return json.dumps(payload, indent=2)

    def _read_lock_payload(self, lock_file):
        try:
            with open(lock_file) as f:
                text = f.read().strip()
            if not text:
                return {}
            if text.startswith('{'):
                return json.loads(text)
            return {'message': text}  # Lock written by Relion
        except (OSError, json.JSONDecodeError):
            return {}

    def _read_token(self, lock_file):
        return self._read_lock_payload(lock_file).get('token')

    def _is_stale(self, project_path, lock_dir, lock_file, payload):
        pid = payload.get('pid')
        hostname = payload.get('hostname')
        if hostname in (None, '', _hostname()) and pid is not None:
            if not _pid_alive(pid):
                return True

        mtime = _fresh_mtime(lock_file)
        if mtime is None:
            mtime = _fresh_mtime(lock_dir)
        if mtime is None:  # Lock vanished, let the caller retry
            return False
        return server_now(project_path) - mtime >= self._stale_seconds

    def break_stale(self, project_path):
        """Remove the project lock if it is stale. Return True if the lock
        is gone (broken by us or released meanwhile), False if it is alive."""
        lock_dir, lock_file = relion_lock_paths(project_path, self._pipeline_name)
        if not os.path.isdir(lock_dir):
            return True

        payload = self._read_lock_payload(lock_file)
        if not self._is_stale(project_path, lock_dir, lock_file, payload):
            return False

        # Rename is atomic on the server: only one process can move the lock
        stale_dir = f'{lock_dir}.stale.{uuid.uuid4().hex}'
        try:
            os.rename(lock_dir, stale_dir)
        except FileNotFoundError:
            return True
        except OSError:
            return False

        # The lock could have been released and re-acquired between our
        # check and the rename; if we moved a live lock, put it back.
        stale_file = os.path.join(stale_dir, os.path.basename(lock_file))
        moved = self._read_lock_payload(stale_file)
        if not self._is_stale(project_path, stale_dir, stale_file, moved):
            try:
                os.rename(stale_dir, lock_dir)
                return False
            except OSError:
                print(f"WARNING: could not restore live project lock "
                      f"{lock_dir} ({moved})", flush=True)

        print(f"WARNING: breaking stale project lock {lock_dir} "
              f"({moved or 'no lock info'})", flush=True)
        shutil.rmtree(stale_dir, ignore_errors=True)
        return True

    def wait_until_free(self, project_path, timeout=DEFAULT_TIMEOUT):
        """Wait until nobody holds the project lock, breaking stale locks."""
        lock_dir, _ = relion_lock_paths(project_path, self._pipeline_name)
        deadline = time.time() + timeout
        while not self.break_stale(project_path):
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for project lock to be released: {lock_dir}")
            time.sleep(DEFAULT_POLL_INTERVAL)

    def _heartbeat(self, held):
        started = time.time()
        missing = 0
        while not held.stop.wait(self._heartbeat_seconds):
            if time.time() - started > self._max_hold_seconds:
                print(f"WARNING: project lock {held.lock_dir} held for more "
                      f"than {self._max_hold_seconds}s, stop refreshing it",
                      flush=True)
                return
            token = self._read_token(held.lock_file)
            if token is None and missing == 0:
                # Might be a transient rename by a process checking staleness
                missing += 1
                continue
            if token != held.token:
                held.lost = True
                print(f"WARNING: project lock {held.lock_dir} was taken by "
                      f"another process", flush=True)
                return
            missing = 0
            try:
                os.utime(held.lock_file, None)
            except OSError:
                pass

    def acquire(self, project_path, message='', timeout=DEFAULT_TIMEOUT):
        key = (project_path, threading.get_ident())
        with self._mutex:
            if held := self._held.get(key):
                held.depth += 1
                return held.lock_file

        lock_dir, lock_file = relion_lock_paths(project_path, self._pipeline_name)
        deadline = time.time() + timeout
        warned = False

        while True:
            try:
                os.mkdir(lock_dir, 0o700)
                break
            except FileExistsError:
                if self.break_stale(project_path):
                    continue

                if not warned and (time.time() + 10) >= deadline:
                    payload = self._read_lock_payload(lock_file)
                    print(f"WARNING: waiting for project lock {lock_dir} "
                          f"({payload or 'no lock info'})", flush=True)
                    warned = True

                if time.time() >= deadline:
                    raise TimeoutError(
                        f"Timed out waiting for Relion project lock: {lock_dir}. "
                        f"If no process is using the project, remove {lock_file} "
                        f"and {lock_dir} manually.")

                time.sleep(DEFAULT_POLL_INTERVAL)
            except OSError as e:
                if e.errno in (errno.EACCES, errno.EPERM):
                    raise PermissionError(
                        f"No permission to create project lock directory: {lock_dir}"
                    ) from e
                if e.errno == errno.ENOSPC:
                    raise OSError(errno.ENOSPC,
                                  f"No space to create project lock directory: {lock_dir}"
                                  ) from e
                if e.errno == errno.EROFS:
                    raise OSError(errno.EROFS,
                                  f"Read-only filesystem; cannot lock project: {lock_dir}"
                                  ) from e
                raise

        token = uuid.uuid4().hex
        with open(lock_file, 'w') as f:
            f.write(self._lock_payload(message, token))
            f.flush()
            os.fsync(f.fileno())

        held = _HeldLock(lock_dir, lock_file, token)
        if self._heartbeat_seconds > 0:
            held.thread = threading.Thread(target=self._heartbeat, args=(held,),
                                           name='emwrap-lock-heartbeat',
                                           daemon=True)
            held.thread.start()

        with self._mutex:
            self._held[key] = held
        return lock_file

    def is_lost(self, project_path):
        """True if the lock held for this project was taken by another process."""
        held = self._held.get((project_path, threading.get_ident()))
        return bool(held and held.lost)

    def release(self, project_path):
        key = (project_path, threading.get_ident())
        with self._mutex:
            held = self._held.get(key)
            if held is None:
                return
            held.depth -= 1
            if held.depth > 0:
                return
            del self._held[key]

        held.stop.set()
        if held.thread is not None:
            held.thread.join(timeout=self._heartbeat_seconds + 1)

        if self._read_token(held.lock_file) != held.token:
            print(f"WARNING: project lock {held.lock_dir} is not owned by this "
                  f"process anymore, leaving it in place", flush=True)
            return

        try:
            os.remove(held.lock_file)
        except FileNotFoundError:
            pass
        try:
            os.rmdir(held.lock_dir)
        except OSError:
            # Another lock file may exist; leave directory in place.
            pass


def wait_project_lock_free(project_path, timeout=DEFAULT_TIMEOUT):
    """Wait until the Relion project lock is released (breaking stale ones).
    Useful before running Relion programs that take the lock themselves."""
    RelionDirLockBackend().wait_until_free(os.path.abspath(project_path),
                                           timeout=timeout)


class ServerLockBackend(ProjectLockBackend):
    """Placeholder for a future central lock service (Redis, emhub worker, etc.)."""

    def acquire(self, project_path, message='', timeout=DEFAULT_TIMEOUT):
        raise NotImplementedError(
            'ServerLockBackend is not implemented yet. '
            'Set EMWRAP_PROJECT_LOCK=relion_dir or fcntl.')

    def release(self, project_path):
        raise NotImplementedError('ServerLockBackend is not implemented yet.')


_BACKENDS = {
    'noop': NoopLockBackend,
    'fcntl': FcntlLockBackend,
    'relion_dir': RelionDirLockBackend,
    'server': ServerLockBackend,
}


def create_lock_backend(name=None):
    """Factory for lock backends; extensible via EMWRAP_PROJECT_LOCK env var."""
    name = (name or os.environ.get('EMWRAP_PROJECT_LOCK', 'relion_dir')).lower()
    try:
        return _BACKENDS[name]()
    except KeyError as e:
        valid = ', '.join(sorted(_BACKENDS))
        raise ValueError(
            f"Unknown project lock backend '{name}'. Valid values: {valid}") from e


class ProjectLock:
    """Context manager for project-level exclusive locks."""

    _default_backend = None

    @classmethod
    def default_backend(cls):
        if cls._default_backend is None:
            cls._default_backend = create_lock_backend()
        return cls._default_backend

    @classmethod
    def set_default_backend(cls, backend):
        cls._default_backend = backend

    def __init__(self, project_path, message='', timeout=DEFAULT_TIMEOUT,
                 backend=None, readonly=False):
        self.project_path = os.path.abspath(project_path)
        self.message = message
        self.timeout = timeout
        self.backend = backend or self.default_backend()
        self.readonly = readonly
        self._token = None

    def acquire(self):
        if self.readonly:
            return None
        self._token = self.backend.acquire(
            self.project_path, message=self.message, timeout=self.timeout)
        return self._token

    def release(self):
        if self.readonly:
            return
        self.backend.release(self.project_path)
        self._token = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False


@contextmanager
def project_lock(project_path, message='', timeout=DEFAULT_TIMEOUT,
                 backend=None, readonly=False):
    """Functional wrapper around ProjectLock."""
    with ProjectLock(project_path, message=message, timeout=timeout,
                     backend=backend, readonly=readonly):
        yield
