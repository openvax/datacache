"""Small local-filesystem primitives shared by resumable and bundle installs."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat
import tempfile

from .integrity import FileValidationError


def path_present(path):
    """Like lexists, but never turn permission errors into absence."""
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    return True


def open_regular(path, flags=os.O_RDONLY, mode=0o600, *, private=False):
    """Open without following links or blocking on special files."""
    flags |= getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    before = None
    try:
        before = os.lstat(path)
    except FileNotFoundError:
        pass
    if before is not None and not stat.S_ISREG(before.st_mode):
        raise FileValidationError(path, 'expected a regular file, not a link or special file')
    fd = os.open(path, flags, mode)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise FileValidationError(path, 'expected a regular file with a single link')
        if private and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077):
            raise FileValidationError(path, 'resume state must be owner-only')
        return fd
    except BaseException:
        os.close(fd)
        raise


@contextmanager
def file_lock(path, *, private=False):
    """Permanent inode, advisory POSIX lock; never unlink a live lock file."""
    if os.name != 'posix':
        raise NotImplementedError('Resumable and bundle installs require a POSIX local filesystem')
    import fcntl
    fd = open_regular(path, os.O_RDWR | os.O_CREAT, 0o600 if private else 0o666,
                      private=private)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def read_json(path, limit=1024 * 1024):
    with os.fdopen(open_regular(path), 'rb') as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise ValueError('JSON record is too large')
    return json.loads(data)


def write_json(path, value, mode=0o600):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.datacache-json-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(value, handle, sort_keys=True)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), mode)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)
