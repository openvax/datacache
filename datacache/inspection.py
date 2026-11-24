"""Read-only presence and integrity checks; creation and repair belong to fetch."""

from dataclasses import dataclass
import os
from pathlib import PurePosixPath, PureWindowsPath
import stat
from typing import Dict, Optional

from . import provenance
from .integrity import FileValidationError, validate_expectations, validate_file_stat


@dataclass(frozen=True)
class FileInspection:
    """Frozen read-only inspection result for one file.

    Returned by ``inspect_file`` and ``Cache.inspect``.

    Attributes
    ----------
    path : str
        Inspected path string.
    status : {"available", "missing", "corrupt", "inaccessible"}
        ``"available"`` (readable regular file matching supplied expectations),
        ``"missing"`` (absent), ``"corrupt"`` (wrong file type or failed
        expectations), or ``"inaccessible"`` (other filesystem error).
    verified : bool
        ``True`` only when a supplied ``expected_sha256`` matched the current
        bytes. Readability, matching size alone, or a recorded provenance
        digest leaves this ``False``.
    error : Exception or None
        Original validation or OS exception for an unavailable file, or
        ``None`` when ``status == "available"``.
    size : int or None
        Size in bytes of an available file; otherwise ``None``.
    mtime : float or None
        Modification time of an available file (seconds since the epoch);
        otherwise ``None``.
    source_url : str or None
        Source URL recorded by ``fetch_file(..., record_provenance=True)``
        (without user name, password, query string, or fragment) while the
        file's size and modification time remain unchanged; otherwise ``None``.
    fetched_at : str or None
        Recorded UTC ISO 8601 fetch timestamp under the same condition;
        otherwise ``None``.
    recorded_sha256 : str or None
        SHA-256 verified at download time under the same condition; otherwise
        ``None``. This is a record, not a fresh check of the current bytes;
        pass it back as ``expected_sha256`` to verify now.
    """

    path: str
    status: str
    verified: bool = False
    error: Optional[Exception] = None
    size: Optional[int] = None
    mtime: Optional[float] = None
    source_url: Optional[str] = None
    fetched_at: Optional[str] = None
    recorded_sha256: Optional[str] = None


@dataclass(frozen=True)
class CacheInspection:
    """Frozen read-only status of a cache directory and its required files.

    Returned by ``inspect_files``.

    Attributes
    ----------
    path : str
        Inspected cache root path string.
    status : {"available", "missing", "corrupt", "inaccessible"}
        ``"available"`` (all required files available), ``"missing"`` (root
        absent), ``"corrupt"`` (wrong root type or missing/corrupt required
        files), or ``"inaccessible"`` (root or required file access error).
    files : dict[str, FileInspection]
        Per-file inspection results keyed by normalized relative name. Empty
        if the root itself could not be inspected as a directory.
    verified : bool
        ``True`` only when every required file matched a supplied SHA-256.
    error : Exception or None
        Root failure or representative failing file's exception; ``None`` on
        success. Inspect ``files`` for all individual failures.
    """

    path: str
    status: str
    files: Dict[str, FileInspection]
    verified: bool = False
    error: Optional[Exception] = None


def path_exists(path):
    """Check presence without creating directories or hiding permission errors.

    Presence is independent of file type, readability, and integrity. A broken
    symlink is absent; other OS errors propagate to the caller.
    """
    try:
        os.stat(path)
    except FileNotFoundError:
        return False
    return True


def inspect_file(path, expected_sha256=None, expected_size=None):
    """Report file availability and integrity offline without writes, locks, or repair.

    Filesystem and validation failures become result statuses rather than being
    raised. For an available file, ``size`` and ``mtime`` are read from the
    same open file descriptor that was validated, along with any provenance
    recorded by ``fetch_file(..., record_provenance=True)`` while size and
    modification time remain unchanged. Missing, unreadable, or malformed
    provenance sidecars are ignored safely.

    Parameters
    ----------
    path : str or os.PathLike
        Path to inspect (symlinks are followed).
    expected_sha256 : str or None, optional
        Trusted 64-character hexadecimal SHA-256 digest (case-insensitive).
    expected_size : int or None, optional
        Expected non-negative integer size in bytes.

    Returns
    -------
    FileInspection
        Frozen inspection result describing availability, verification, and
        optional provenance.

    Raises
    ------
    ValueError
        If ``expected_sha256`` or ``expected_size`` is invalid.
    TypeError
        If ``path`` is not path-like.
    """
    validate_expectations(expected_sha256, expected_size)
    path = os.fspath(path)
    try:
        _, info = validate_file_stat(path, expected_sha256, expected_size)
    except FileNotFoundError as error:
        return FileInspection(path, "missing", error=error)
    except (FileValidationError, NotADirectoryError, IsADirectoryError) as error:
        return FileInspection(path, "corrupt", error=error)
    except OSError as error:
        return FileInspection(path, "inaccessible", error=error)
    record = provenance.read(path, info) or {}
    return FileInspection(
        path, "available", verified=expected_sha256 is not None,
        size=info.st_size, mtime=info.st_mtime, source_url=record.get("url"),
        fetched_at=record.get("fetched_at"), recorded_sha256=record.get("sha256"))


def inspect_files(cache_root, files):
    """Inspect a required-file inventory under ``cache_root``, entirely offline.

    Checks a caller-supplied inventory without writes, network access, manifest
    parsing, or snapshots across concurrent edits. Symlinks are followed.
    A missing root reports ``"missing"``; an existing root missing a required
    file reports ``"corrupt"``; inaccessibility takes precedence over corrupt
    or missing files. ``verified=True`` requires a matching SHA-256 for every
    required file.

    Parameters
    ----------
    cache_root : str or os.PathLike
        Root directory containing the required files.
    files : Mapping[str, dict or None]
        Nonempty mapping from normalized relative POSIX file names (using
        ``/``; no leading ``/``, ``..``, backslashes, redundant separators, or
        colons) to dicts containing optional ``expected_sha256`` and/or
        ``expected_size`` (or ``{}`` / ``None`` when unavailable).

    Returns
    -------
    CacheInspection
        Aggregate status and per-file ``FileInspection`` results.

    Raises
    ------
    ValueError
        If ``files`` is empty, contains invalid relative names, unexpected
        metadata keys, or invalid integrity expectations.
    """
    root = os.fspath(cache_root)
    if not files:
        raise ValueError("files must contain at least one required file")
    expectations = {}
    for name, metadata in files.items():
        if (not isinstance(name, str) or not name or "\\" in name or ":" in name or
                PurePosixPath(name).is_absolute() or PureWindowsPath(name).drive or
                ".." in PurePosixPath(name).parts or
                "/".join(PurePosixPath(name).parts) != name):
            raise ValueError("Required file names must be normalized relative paths: %r" % name)
        metadata = {} if metadata is None else dict(metadata)
        if set(metadata) - {"expected_sha256", "expected_size"}:
            raise ValueError("Unexpected integrity metadata for %s" % name)
        validate_expectations(metadata.get("expected_sha256"), metadata.get("expected_size"))
        expectations[name] = metadata
    try:
        info = os.stat(root)
        if not stat.S_ISDIR(info.st_mode):
            raise FileValidationError(root, "expected a cache directory")
    except FileNotFoundError as error:
        return CacheInspection(root, "missing", {}, error=error)
    except (FileValidationError, NotADirectoryError) as error:
        return CacheInspection(root, "corrupt", {}, error=error)
    except OSError as error:
        return CacheInspection(root, "inaccessible", {}, error=error)
    results = {
        name: inspect_file(os.path.join(root, name), **metadata)
        for name, metadata in expectations.items()
    }
    for status in ("inaccessible", "corrupt", "missing"):
        failed = next((result for result in results.values() if result.status == status), None)
        if failed is not None:
            return CacheInspection(
                root, "inaccessible" if status == "inaccessible" else "corrupt",
                results, error=failed.error)
    return CacheInspection(root, "available", results, verified=all(r.verified for r in results.values()))
