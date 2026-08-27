# **************************************************************************
# *
# * Authors:     J.M. de la Rosa Trevin (delarosatrevin@gmail.com)
# *
# * Project-level locking for project.json / default_pipeline.star updates.
# *
# * Default backend uses Relion's .relion_lock layout so emwrap, Relion GUI
# * and emw CLI coordinate on NFS-friendly directory locks. Additional backends
# * (fcntl, noop, future server/redis) can be selected via EMWRAP_PROJECT_LOCK.
# *
# **************************************************************************

import errno
import json
import os
import socket
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone

DEFAULT_PIPELINE = 'default_pipeline.star'
DEFAULT_STALE_SECONDS = 30
DEFAULT_POLL_INTERVAL = 0.5
DEFAULT_TIMEOUT = 60


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _hostname():
    try:
        return socket.gethostname()
    except Exception:
        return 'unknown'


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


def relion_lock_paths(project_path, pipeline_name=DEFAULT_PIPELINE):
    """Return Relion-compatible lock directory and lock file paths."""
    base = os.path.basename(pipeline_name)
    lock_dir = os.path.join(project_path, '.relion_lock')
    lock_file = os.path.join(lock_dir, f'lock_{base}')
    return lock_dir, lock_file


def atomic_write_json(path, data, indent=4):
    """Write JSON atomically via temp file + replace."""
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    tmp = os.path.join(directory, f'.{os.path.basename(path)}.{os.getpid()}.tmp')
    with open(tmp, 'w') as f:
        json.dump(data, f, indent=indent)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_write_text(path, content):
    """Write text atomically via temp file + replace."""
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    tmp = os.path.join(directory, f'.{os.path.basename(path)}.{os.getpid()}.tmp')
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


class RelionDirLockBackend(ProjectLockBackend):
    """Relion-compatible mkdir lock under .relion_lock/."""

    def __init__(self, pipeline_name=DEFAULT_PIPELINE,
                 stale_seconds=DEFAULT_STALE_SECONDS):
        self._pipeline_name = pipeline_name
        self._stale_seconds = stale_seconds
        self._held = set()

    def _lock_payload(self, message):
        payload = {
            'pid': os.getpid(),
            'hostname': _hostname(),
            'timestamp': _utc_now(),
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
            return {'message': text}
        except (OSError, json.JSONDecodeError):
            return {}

    def _path_age_seconds(self, path):
        try:
            return max(0, time.time() - os.stat(path).st_mtime)
        except OSError:
            return None

    def _is_stale(self, lock_dir, lock_file, payload):
        pid = payload.get('pid')
        hostname = payload.get('hostname')
        if hostname in (None, '', _hostname()) and pid is not None:
            if not _pid_alive(pid):
                return True

        age_path = lock_file if os.path.exists(lock_file) else lock_dir
        age = self._path_age_seconds(age_path)
        if age is not None and age >= self._stale_seconds:
            return True
        return False

    def _try_break_stale_lock(self, lock_dir, lock_file):
        if not os.path.isdir(lock_dir):
            return False

        payload = self._read_lock_payload(lock_file) if os.path.exists(lock_file) else {}
        if not self._is_stale(lock_dir, lock_file, payload):
            return False

        try:
            os.remove(lock_file)
        except FileNotFoundError:
            pass
        try:
            os.rmdir(lock_dir)
        except OSError:
            pass
        return True

    def acquire(self, project_path, message='', timeout=DEFAULT_TIMEOUT):
        lock_dir, lock_file = relion_lock_paths(project_path, self._pipeline_name)
        deadline = time.time() + timeout
        warned = False

        while True:
            try:
                os.mkdir(lock_dir, 0o700)
                break
            except FileExistsError:
                if self._try_break_stale_lock(lock_dir, lock_file):
                    continue

                if not warned and (time.time() + 10) >= deadline:
                    print(f"WARNING: waiting for project lock {lock_dir}", flush=True)
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

        with open(lock_file, 'w') as f:
            f.write(self._lock_payload(message))
            f.flush()
            os.fsync(f.fileno())

        self._held.add(project_path)
        return lock_file

    def release(self, project_path):
        if project_path not in self._held:
            return

        lock_dir, lock_file = relion_lock_paths(project_path, self._pipeline_name)
        self._held.discard(project_path)

        if os.path.exists(lock_file):
            os.remove(lock_file)
        if os.path.isdir(lock_dir):
            try:
                os.rmdir(lock_dir)
            except OSError:
                # Another lock file may exist; leave directory in place.
                pass


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
