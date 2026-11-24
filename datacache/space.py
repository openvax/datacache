# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Disk-space queries and typed ``ENOSPC`` error reporting."""

from contextlib import contextmanager
import errno
import os
import shutil
import stat

from .common import format_bytes

# Windows Win32 error codes mapped to disk-full failures.
_WINDOWS_DISK_FULL_ERRORS = (39, 112)


def _validate_optional_bytes(name, value):
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
        raise ValueError("%s must be a non-negative integer or None" % name)


class InsufficientSpaceError(OSError):
    """Raised when a filesystem operation runs out of space (``ENOSPC``).

    Subclasses ``OSError`` with ``errno == errno.ENOSPC`` so callers already
    catching ``OSError`` continue to work while CLIs and pipelines can
    distinguish disk-full failures from network or permission errors without
    parsing exception strings.

    Parameters
    ----------
    path : str or os.PathLike
        Target destination or staging path affected by the disk-full condition.
    needed_bytes : int or None, optional
        Estimated or expected bytes required for the operation, when known.
    free_bytes : int or None, optional
        Available bytes reported on the target filesystem when the failure was
        detected, or ``None`` if disk usage could not be queried.
    message : str or None, optional
        Optional explicit summary replacing the default formatted description.

    Attributes
    ----------
    path : str
        Path string passed to the constructor (also stored on ``filename``).
    needed_bytes : int or None
        Required bytes when known, else ``None``.
    free_bytes : int or None
        Available bytes when known, else ``None``.
    errno : int
        Always ``errno.ENOSPC``.
    """

    def __init__(self, path, needed_bytes=None, free_bytes=None, message=None):
        _validate_optional_bytes("needed_bytes", needed_bytes)
        _validate_optional_bytes("free_bytes", free_bytes)
        self.path = os.fspath(path)
        self.needed_bytes = needed_bytes
        self.free_bytes = free_bytes
        if message is None:
            details = []
            if needed_bytes is not None:
                details.append("need %s (%d bytes)" % (format_bytes(needed_bytes), needed_bytes))
            if free_bytes is not None:
                details.append("%s (%d bytes) free" % (format_bytes(free_bytes), free_bytes))
            if details:
                message = "Not enough disk space (%s)" % ", ".join(details)
            else:
                message = "Not enough disk space"
        super().__init__(errno.ENOSPC, message, self.path)


def is_enospc(error):
    """Return ``True`` if ``error`` is an ``OSError`` caused by no space left on device.

    Parameters
    ----------
    error : BaseException
        Exception instance to inspect.

    Returns
    -------
    bool
        ``True`` when ``error`` is an ``OSError`` with ``errno == errno.ENOSPC``
        (or a Windows disk-full ``winerror``), ``False`` otherwise.
    """
    if not isinstance(error, OSError):
        return False
    return (
        error.errno == errno.ENOSPC
        or getattr(error, "winerror", None) in _WINDOWS_DISK_FULL_ERRORS
    )


def nearest_existing_parent(path):
    """Return the nearest existing directory at or above ``path`` without creating anything.

    Resolves ``path`` with ``os.path.abspath`` and walks upward until an
    existing directory is found (following symlinks, so mounted volumes under a
    symlinked parent report that volume). Never creates directories or writes
    to disk.

    Parameters
    ----------
    path : str or os.PathLike
        File or directory path whose target volume should be located.

    Returns
    -------
    str
        Absolute path string of the nearest existing directory, or the
        filesystem root if no ancestor could be stated as a directory.
    """
    current = os.path.abspath(os.fspath(path))
    while True:
        try:
            info = os.stat(current)
        except OSError:
            pass
        else:
            if stat.S_ISDIR(info.st_mode):
                return current
        parent = os.path.dirname(current)
        if parent == current:
            return current
        current = parent


def free_disk_bytes(path):
    """Return free bytes available on the volume containing ``path``, or ``None`` if unavailable.

    Queries ``shutil.disk_usage`` on ``nearest_existing_parent(path)`` without
    creating any directories or modifying disk.

    Parameters
    ----------
    path : str or os.PathLike
        Destination file or directory path.

    Returns
    -------
    int or None
        Available free space in bytes, or ``None`` if disk usage cannot be
        determined.
    """
    target = nearest_existing_parent(path)
    try:
        return shutil.disk_usage(target).free
    except OSError:
        return None


@contextmanager
def translate_enospc(path, needed_bytes=None):
    """Convert ``OSError(ENOSPC)`` raised inside the block into ``InsufficientSpaceError``.

    Already-typed ``InsufficientSpaceError`` instances propagate unchanged so
    more specific inner paths or byte counts are preserved. Place this context
    manager outside staging cleanup ``finally`` blocks so ``free_bytes`` is
    measured after temporary files are removed.

    Parameters
    ----------
    path : str or os.PathLike
        Destination path to attach to any raised ``InsufficientSpaceError``.
    needed_bytes : int or None, optional
        Known or estimated byte count required by the operation.
    """
    try:
        yield
    except InsufficientSpaceError:
        raise
    except OSError as error:
        if is_enospc(error):
            raise InsufficientSpaceError(
                path,
                needed_bytes=needed_bytes,
                free_bytes=free_disk_bytes(path),
            ) from error
        raise
