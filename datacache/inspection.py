"""Read-only presence and integrity checks; creation and repair belong to fetch."""

from dataclasses import dataclass
import os
from pathlib import PurePosixPath, PureWindowsPath
import stat
from typing import Dict, Optional

from .integrity import FileValidationError, _validate_expectations, validate_file


@dataclass(frozen=True)
class FileInspection:
    """File status: available, missing, corrupt, or inaccessible.

    available means readable, regular, and matching any supplied expectations.
    verified is True only when a supplied SHA-256 expectation was checked.
    error retains the validation or OS exception for unavailable files.
    """

    path: str
    status: str
    verified: bool = False
    error: Optional[Exception] = None


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
    return FileInspection(path, "available", verified=expected_sha256 is not None)


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
