"""Read-only presence and integrity checks; creation and repair belong to fetch."""

from dataclasses import dataclass
import os
from pathlib import PurePosixPath, PureWindowsPath
import stat
from typing import Dict, Optional

from . import provenance
from .integrity import FileValidationError, _validate_expectations, validate_file


@dataclass(frozen=True)
class FileInspection:
    """File status: available, missing, corrupt, or inaccessible.

    available means readable, regular, and matching any supplied expectations.
    verified is True when a supplied SHA-256 expectation matched, or when
    fetch_file verified the file's SHA-256 when it downloaded it and the file is
    unchanged since. error retains the exception for unavailable files.
    size (bytes) and mtime (seconds since the epoch) describe an available
    file. source_url (without credentials or query text) and fetched_at (UTC,
    ISO 8601) are the provenance fetch_file recorded, while the file is
    unchanged since. All four are None when unknown.
    """

    path: str
    status: str
    verified: bool = False
    error: Optional[Exception] = None
    size: Optional[int] = None
    mtime: Optional[float] = None
    source_url: Optional[str] = None
    fetched_at: Optional[str] = None


@dataclass(frozen=True)
class CacheInspection:
    """Status of a directory and its required files, without manifest parsing.

    An absent root is missing; a present directory missing required files is
    corrupt/incomplete. files contains per-file results when the root is readable.
    verified requires supplied SHA-256 expectations for every required file.
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
    """Report file availability without writes, network, locks, or recovery.

    Invalid expectation arguments raise ValueError. Filesystem and integrity
    problems are returned as distinct statuses with their original exceptions.
    An available file's size and mtime are reported, along with any provenance
    fetch_file(..., record_provenance=True) recorded for it. A SHA-256 verified
    at that download counts as verified, without rehashing, while the file's
    size and mtime are unchanged.
    """
    _validate_expectations(expected_sha256, expected_size)
    path = os.fspath(path)
    try:
        validate_file(path, expected_sha256, expected_size)
    except FileNotFoundError as error:
        return FileInspection(path, "missing", error=error)
    except (FileValidationError, NotADirectoryError, IsADirectoryError) as error:
        return FileInspection(path, "corrupt", error=error)
    except OSError as error:
        return FileInspection(path, "inaccessible", error=error)
    try:
        info = os.stat(path)
    except FileNotFoundError as error:  # removed since it was validated
        return FileInspection(path, "missing", error=error)
    except OSError as error:
        return FileInspection(path, "inaccessible", error=error)
    record = provenance.read(path, info)
    verified = expected_sha256 is not None or bool(record and record["sha256"])
    return FileInspection(
        path, "available", verified=verified, size=info.st_size, mtime=info.st_mtime,
        source_url=record["url"] if record else None,
        fetched_at=record["fetched_at"] if record else None)


def inspect_files(cache_root, files):
    """Inspect required relative files in a cache directory, entirely offline.

    files maps relative names to dicts containing expected_sha256/expected_size;
    use {} or None when integrity expectations are unavailable. Include a
    manifest as a required file to detect incomplete installations. This does
    not parse manifests or provide a snapshot across concurrent external edits.
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
        _validate_expectations(metadata.get("expected_sha256"), metadata.get("expected_size"))
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
