"""Read-only presence, integrity, and fetch-preflight checks; creation and repair belong to fetch."""

from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
import logging
import os
from pathlib import PurePosixPath, PureWindowsPath
import stat
from typing import Dict, Optional
import urllib.parse

from . import common, provenance
from .common import parse_content_length
from .integrity import FileValidationError, validate_expectations, validate_file_stat
from .offline import offline_source, require_online
from .space import free_disk_bytes

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class FetchPlan:
    """Frozen read-only preflight plan for a prospective ``fetch_file`` call.

    Returned by ``plan_fetch`` and ``Cache.plan_fetch`` without performing any
    filesystem writes, directory creation, locking, or repair.

    Attributes
    ----------
    url : str
        Source URL that would be fetched.
    destination : str
        Exact local destination path string.
    action : {"reuse", "download", "replace"}
        Planned action: ``"reuse"`` when a valid file is already present (and
        ``force=False``), ``"download"`` when ``destination`` is absent, or
        ``"replace"`` when an existing entry is corrupt, inaccessible, or
        ``force=True`` was requested.
    inspection : FileInspection
        Read-only ``FileInspection`` for the current ``destination``.
    expected_size : int or None
        Expected total size in bytes, from the caller's ``expected_size``
        argument or (only when ``probe_remote_size=True``) the remote
        ``Content-Length`` or local ``file://`` size; otherwise ``None``.
    partial_bytes : int
        Reusable bytes already stored in a matching private resumable partial
        for ``destination`` (``0`` when reusing or when no matching partial
        exists).
    needed_bytes : int or None
        Estimated additional free bytes required on the destination volume:
        ``0`` for ``"reuse"``, ``max(0, expected_size - partial_bytes)`` when
        ``expected_size`` is known, or ``None`` when ``expected_size`` is
        unknown.
    free_bytes : int or None
        Available free space in bytes on the destination's filesystem
        (queried via ``shutil.disk_usage`` on the nearest existing parent
        directory), or ``None`` if disk usage could not be queried.
    sufficient : bool or None
        ``True`` when ``action == "reuse"`` or ``free_bytes >= needed_bytes``,
        ``False`` when ``free_bytes < needed_bytes``, or ``None`` when either
        ``needed_bytes`` or ``free_bytes`` is unknown.
    """

    url: str
    destination: str
    action: str
    inspection: FileInspection
    expected_size: Optional[int] = None
    partial_bytes: int = 0
    needed_bytes: Optional[int] = None
    free_bytes: Optional[int] = None
    sufficient: Optional[bool] = None


def probe_remote_content_length(url, *, timeout=30, headers=None, offline=None):
    """Probe the expected byte size of ``url`` via HTTP ``HEAD`` or local ``file://`` stat.

    Performs no filesystem writes. For ``http`` / ``https`` URLs, sends an
    HTTP ``HEAD`` request with ``Accept-Encoding: identity`` and returns a
    parsed non-negative ``Content-Length`` only when the response is 2xx,
    uses identity content encoding, and does not use chunked transfer
    encoding. Transport errors or unsupported ``HEAD`` responses return
    ``None``; explicit offline blocks raise ``OfflineError``.

    Parameters
    ----------
    url : str
        Source URL to probe.
    timeout : float or tuple[float, float] or None, optional
        Per-request timeout in seconds passed to ``requests.head``. Default is
        ``30``.
    headers : Mapping[str, str] or None, optional
        Optional extra HTTP headers merged onto the default request headers.
    offline : bool or None, optional
        Override ``DATACACHE_OFFLINE`` / ``OPENVAX_OFFLINE``.

    Returns
    -------
    int or None
        Known byte size, or ``None`` when unavailable.

    Raises
    ------
    OfflineError
        If offline mode forbids probing a remote HTTP(S) URL.
    ValueError
        If ``headers`` is not a mapping or ``None``, or ``offline`` is invalid.
    """
    if headers is not None and not isinstance(headers, Mapping):
        raise ValueError("headers must be a mapping or None")
    offline_source(offline)
    parts = urllib.parse.urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme in ("http", "https"):
        require_online(url, offline=offline)
        import requests
        request_headers = {"User-Agent": common.USER_AGENT, "Accept-Encoding": "identity"}
        if headers is not None:
            lower_custom = {key.lower() for key in headers}
            request_headers = {
                key: value for key, value in request_headers.items()
                if key.lower() not in lower_custom
            }
            request_headers.update(headers)
        try:
            with closing(requests.head(
                url, headers=request_headers, timeout=timeout, allow_redirects=True
            )) as response:
                if not (200 <= response.status_code < 300):
                    return None
                encoding = response.headers.get("Content-Encoding", "identity").strip().lower()
                if encoding != "identity":
                    return None
                if "Transfer-Encoding" in response.headers:
                    return None
                return parse_content_length(response.headers.get("Content-Length"))
        except requests.RequestException as error:
            logger.debug("Could not probe remote size for %s: %s", provenance.redact_url(url), error)
            return None
    if scheme == "file":
        if parts.netloc not in ("", "localhost") or parts.query or parts.fragment:
            return None
        from urllib.request import url2pathname
        try:
            info = os.stat(url2pathname(parts.path))
        except OSError:
            return None
        return info.st_size if stat.S_ISREG(info.st_mode) else None
    return None


def plan_fetch(
        url,
        destination,
        *,
        expected_sha256=None,
        expected_size=None,
        probe_remote_size=False,
        timeout=30,
        headers=None,
        force=False,
        offline=None):
    """Build a read-only preflight ``FetchPlan`` without writes, locks, or recovery.

    Inspects ``destination`` with ``inspect_file``, checks available space on
    the nearest existing parent directory via ``shutil.disk_usage``, and
    credits any matching private resumable partial bytes toward the required
    space. Never creates directories, acquires locks, repairs files, or makes
    network requests unless ``probe_remote_size=True`` is explicitly requested
    and ``expected_size`` is omitted for a file that would be downloaded or
    replaced.

    Parameters
    ----------
    url : str
        Source URL that would be fetched.
    destination : str or os.PathLike
        Exact destination file path to plan for.
    expected_sha256 : str or None, optional
        Trusted 64-character hexadecimal SHA-256 digest (case-insensitive).
    expected_size : int or None, optional
        Expected non-negative integer size in bytes. Takes precedence over
        ``probe_remote_size``.
    probe_remote_size : bool, optional
        If ``True`` and ``expected_size`` is ``None`` for a ``"download"`` or
        ``"replace"`` plan, probe the size with an HTTP ``HEAD`` request (or
        ``os.stat`` for ``file://`` URLs). Default is ``False`` (strictly
        offline).
    timeout : float or tuple[float, float] or None, optional
        Per-request timeout in seconds for the optional HTTP ``HEAD`` probe.
        Default is ``30``.
    headers : Mapping[str, str] or None, optional
        Optional extra HTTP headers sent on the ``HEAD`` probe when
        ``probe_remote_size=True``.
    force : bool, optional
        If ``True``, plan ``"replace"`` instead of ``"reuse"`` when a valid
        file is already present at ``destination``. Default is ``False``.
    offline : bool or None, optional
        Override ``DATACACHE_OFFLINE`` / ``OPENVAX_OFFLINE`` for optional
        remote size probing.

    Returns
    -------
    FetchPlan
        Frozen preflight plan reporting ``action``, ``expected_size``,
        ``partial_bytes``, ``needed_bytes``, ``free_bytes``, ``sufficient``,
        and the current ``FileInspection``.

    Raises
    ------
    ValueError
        If ``url``, ``destination``, expectations, boolean flags, ``headers``,
        or ``offline`` are invalid.
    TypeError
        If ``destination`` is not path-like.
    OfflineError
        If ``probe_remote_size=True`` requires a remote HTTP ``HEAD`` request
        while offline mode is active.
    """
    if not isinstance(url, str) or not url:
        raise ValueError("URL not provided")
    dest = os.fspath(destination)
    if not isinstance(dest, str) or not dest:
        raise ValueError("destination must be a non-empty text path")
    validate_expectations(expected_sha256, expected_size)
    if not isinstance(probe_remote_size, bool):
        raise ValueError("probe_remote_size must be a boolean")
    if not isinstance(force, bool):
        raise ValueError("force must be a boolean")
    if headers is not None and not isinstance(headers, Mapping):
        raise ValueError("headers must be a mapping or None")
    offline_source(offline)

    inspection = inspect_file(dest, expected_sha256=expected_sha256, expected_size=expected_size)
    if inspection.status == "available":
        action = "replace" if force else "reuse"
    elif inspection.status == "missing":
        action = "download"
    else:
        action = "replace"

    free_bytes = free_disk_bytes(dest)
    if action == "reuse":
        return FetchPlan(
            url=url,
            destination=dest,
            action="reuse",
            inspection=inspection,
            expected_size=expected_size,
            partial_bytes=0,
            needed_bytes=0,
            free_bytes=free_bytes,
            sufficient=True,
        )

    resolved_size = expected_size
    if resolved_size is None and probe_remote_size:
        resolved_size = probe_remote_content_length(
            url, timeout=timeout, headers=headers, offline=offline
        )

    partial_bytes = 0
    if resolved_size is not None:
        from .resume import inspect_partial_bytes
        partial_bytes = inspect_partial_bytes(
            dest,
            download_url=url,
            expected_sha256=expected_sha256,
            expected_size=resolved_size,
        )
        needed_bytes = max(0, resolved_size - partial_bytes)
        sufficient = None if free_bytes is None else (free_bytes >= needed_bytes)
    else:
        needed_bytes = None
        sufficient = None

    return FetchPlan(
        url=url,
        destination=dest,
        action=action,
        inspection=inspection,
        expected_size=resolved_size,
        partial_bytes=partial_bytes,
        needed_bytes=needed_bytes,
        free_bytes=free_bytes,
        sufficient=sufficient,
    )
