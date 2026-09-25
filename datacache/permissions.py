"""Explicit, narrowly scoped maintenance of shared-cache access."""

import os
import stat

from .integrity import FileValidationError


def make_file_readable(path, *, group=True, others=False):
    """Add read access to one existing regular file, without replacing it.

    On POSIX, its owner can grant group read access (default), or explicitly
    grant others read access. No write/execute bits are added or removed, and
    contents are unchanged. Symlinks and non-regular files are rejected.
    Downloads and cache hits never call this function automatically.

    Return the path on success. Permission errors propagate: this cannot grant
    access to a file the caller cannot open or chmod. Parent directories and
    owning groups must already permit the intended readers to reach the file.
    """
    if not isinstance(group, bool) or not isinstance(others, bool):
        raise ValueError("group and others must be booleans")
    if not hasattr(os, "fchmod") or not hasattr(os, "O_NOFOLLOW"):
        raise NotImplementedError("Explicit shared-file permissions require POSIX file descriptors")
    path = os.fspath(path)
    if not stat.S_ISREG(os.lstat(path).st_mode):
        raise FileValidationError(path, "expected a regular file, not a symlink or directory")
    # Operate on the opened inode rather than a path that could be swapped to a
    # symlink between checking and chmod. Nonblocking open also avoids a FIFO
    # substituted after lstat hanging this maintenance operation.
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise FileValidationError(path, "expected a regular file")
        old_mode = stat.S_IMODE(info.st_mode)
        mode = old_mode | (stat.S_IRGRP if group else 0) | (stat.S_IROTH if others else 0)
        if mode != old_mode:
            os.fchmod(descriptor, mode)
    finally:
        os.close(descriptor)
    return path
