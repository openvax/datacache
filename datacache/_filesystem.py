"""Small local-filesystem primitives shared by resumable and bundle installs."""

from contextlib import contextmanager
import errno
import json
import os
from pathlib import Path
import stat
import sys
import tempfile

from .integrity import FileValidationError


def _rename_noreplace(source, destination):
    """Atomically rename a file without replacing any destination entry.

    No check-then-rename emulation: writers outside DataCache must be protected
    too. The source and destination must be on the same filesystem.
    """
    if os.name == 'nt':
        # Unlike POSIX rename(), Windows rename() refuses existing destinations.
        os.rename(source, destination)
        return
    import ctypes
    libc = ctypes.CDLL(None, use_errno=True)
    source_bytes, destination_bytes = os.fsencode(source), os.fsencode(destination)
    if b'\0' in source_bytes or b'\0' in destination_bytes:
        raise ValueError('embedded null byte')
    if sys.platform.startswith('linux') and hasattr(libc, 'renameat2'):
        rename = libc.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
                           ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        # Linux AT_FDCWD and RENAME_NOREPLACE (renameat2(2)).
        result = rename(-100, source_bytes, -100, destination_bytes, 1)
    elif sys.platform == 'darwin' and hasattr(libc, 'renamex_np'):
        rename = libc.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        # Darwin RENAME_EXCL, from <sys/stdio.h>.
        result = rename(source_bytes, destination_bytes, 4)
    else:
        raise OSError(errno.ENOTSUP, 'atomic no-replace rename is unavailable', destination)
    if result != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), destination)


def publish_noclobber(source, destination):
    """Publish a closed staged file atomically, never clobbering a creator.

    Prefer hard links, falling back only for unsupported-operation errors.
    The caller must remove the staging name if it still exists afterwards.
    """
    unsupported = {errno.EPERM, errno.ENOSYS, errno.ENOTSUP, errno.EOPNOTSUPP}
    try:
        os.link(source, destination)
        return
    except OSError as error:
        # Windows ERROR_INVALID_FUNCTION (1), ERROR_NOT_SUPPORTED (50), and
        # ERROR_CALL_NOT_IMPLEMENTED (120) map to EINVAL in CPython. Inspect
        # winerror so unrelated EINVAL failures still propagate unchanged.
        if (error.errno not in unsupported and
                getattr(error, 'winerror', None) not in (1, 50, 120)):
            raise
    try:
        _rename_noreplace(source, destination)
    except OSError as error:
        if error.errno not in unsupported | {errno.EINVAL}:
            raise
        raise OSError(
            errno.ENOTSUP,
            'Cannot atomically publish a new database: hard links and atomic '
            'no-replace renames are unavailable. Build the database on local '
            'scratch and copy the closed database into a materialize() bundle; '
            'see https://openvax.github.io/datacache/data/#filesystems-without-hard-links',
            destination) from error


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
    """Read a regular JSON file; ``limit=None`` permits growing tree manifests."""
    with os.fdopen(open_regular(path), 'rb') as handle:
        data = handle.read() if limit is None else handle.read(limit + 1)
    if limit is not None and len(data) > limit:
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
            if hasattr(os, 'fchmod'):
                os.fchmod(handle.fileno(), mode)
            else:  # Windows: no descriptor chmod; staging is still private.
                os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)
