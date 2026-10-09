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

from contextlib import closing
from datetime import datetime, timedelta, timezone
import gzip
import io
import logging
import math
import os
import stat
import time
import warnings
from shutil import copyfileobj
from tempfile import gettempdir, TemporaryDirectory
from uuid import uuid4
import zipfile
import urllib.parse
import urllib.request

from . import common, provenance
from .common import _source_suffix, build_local_filename
from .integrity import FileValidationError, _validate_expectations, validate_file
from .inspection import path_exists
from .progress import Progress
from .retries import (
    DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BACKOFF, DEFAULT_RETRY_MAX_DELAY,
    error_description, is_retryable_http_error, retry_delay, validate_retry_options,
)

logger = logging.getLogger(__name__)


def __getattr__(name):
    """Keep download.pd and download.requests, now imported only when needed.

    pandas and requests are imported inside the functions that use them, so
    importing datacache stays fast. Existing references to these module
    attributes, such as test patches, still reach the same modules.
    """
    if name == "pd":
        import pandas
        return pandas
    if name == "requests":
        import requests
        return requests
    raise AttributeError("module %r has no attribute %r" % (__name__, name))

# Number of bytes to read/write at a time when streaming a download to disk.
DEFAULT_CHUNK_SIZE = 2 ** 20  # 1 MB


class EmptyResponse(Exception):
    """A transfer completed without error but delivered no bytes.

    A withdrawn upstream record or a misbehaving edge server can answer with a
    clean, empty 200. HTTP attempts retry it like a transient failure.
    """


class MaxBytesExceeded(Exception):
    """Writing more would make a file larger than the caller's max_bytes.

    Never retried: the same request would deliver the same oversized bytes.
    """


def validate_limit(name, value):
    """Raise ValueError unless value is None (no limit) or a non-negative integer."""
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
        raise ValueError("%s must be a non-negative integer" % name)


def validate_size_within_limit(size, max_bytes, what="expected_size"):
    """Raise ValueError when a known size can never fit within max_bytes."""
    if max_bytes is not None and size is not None and size > max_bytes:
        raise ValueError("%s %d is larger than max_bytes %d" % (what, size, max_bytes))


def refuse_if_over(size, max_bytes):
    """Raise MaxBytesExceeded if size is more than max_bytes."""
    if max_bytes is not None and size > max_bytes:
        raise MaxBytesExceeded("more than max_bytes=%d bytes" % max_bytes)


class LimitedWriter:
    """A file that refuses any write that would take it past max_bytes.

    The refused write raises MaxBytesExceeded before anything is written, so
    the file never holds more than max_bytes bytes.
    """

    def __init__(self, file, max_bytes):
        self.file = file
        self.max_bytes = max_bytes
        self.written = 0

    def write(self, data):
        refuse_if_over(self.written + len(data), self.max_bytes)
        self.written += len(data)
        return self.file.write(data)


def _content_length(header_value):
    """Parse a Content-Length header value into an int, or None if it's
    absent or not a valid integer."""
    if header_value is None:
        return None
    try:
        length = int(header_value)
        return length if length >= 0 else None
    except (TypeError, ValueError):
        return None


def _stream_to_file(
        download_url,
        file_handle,
        timeout=None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        progress_callback=None,
        max_bytes=None):
    """
    Stream the contents of `download_url` into an already-open binary file
    handle, one chunk at a time, so the entire payload never has to be held in
    memory at once.

    This performs one attempt. _download_to_temp_file owns retries and creates
    a fresh staging file for each attempt.

    If `progress_callback` is given it is called as
    ``progress_callback(bytes_downloaded, total_bytes)`` after each chunk is
    written, where `total_bytes` is taken from the server's Content-Length
    header (or None when the server doesn't report a size). This lets callers
    drive an application's own progress display instead of the built-in bar.

    max_bytes, if given, is checked before each write, so no more than that
    many bytes are ever written; a larger Content-Length is refused before
    the body is read. Exceeding it raises MaxBytesExceeded.

    Returns the total number of bytes written.
    """
    bytes_downloaded = 0
    if max_bytes is not None:
        file_handle = LimitedWriter(file_handle, max_bytes)
        # Never read, and so never decode, much more than the limit at once.
        chunk_size = min(chunk_size, max_bytes + 1)

    def report(total_bytes):
        if progress_callback is not None:
            progress_callback(bytes_downloaded, total_bytes)

    if urllib.parse.urlsplit(download_url).scheme.lower() in ("http", "https"):
        import requests
        with closing(requests.get(download_url, headers={"User-Agent": common.USER_AGENT},
                                  timeout=timeout, stream=True)) as response:
            response.raise_for_status()
            # Requests decodes content encoding before yielding chunks; a wire
            # length is not a valid total for the bytes we write in that case.
            encoding = response.headers.get("Content-Encoding", "identity").lower()
            total_bytes = (_content_length(response.headers.get("Content-Length"))
                           if encoding == "identity" else None)
            # Chunked transfer makes Content-Length meaningless (RFC 9112).
            if "Transfer-Encoding" not in response.headers:
                refuse_if_over(total_bytes or 0, max_bytes)
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    # skip keep-alive chunks that carry no data
                    continue
                file_handle.write(chunk)
                bytes_downloaded += len(chunk)
                report(total_bytes)
    else:
        req = urllib.request.Request(download_url)
        with urllib.request.urlopen(req, data=None, timeout=timeout) as response:
            total_bytes = _content_length(response.headers.get("Content-Length"))
            refuse_if_over(total_bytes or 0, max_bytes)
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                file_handle.write(chunk)
                bytes_downloaded += len(chunk)
                report(total_bytes)
    return bytes_downloaded


def _with_retries(
        download_url,
        attempt,
        *,
        max_retries=DEFAULT_MAX_RETRIES,
        retry_backoff=DEFAULT_RETRY_BACKOFF,
        retry_max_delay=DEFAULT_RETRY_MAX_DELAY,
        may_retry=None):
    """Return attempt(), retrying transient HTTP(S) failures.

    Connection errors, 408/429/5xx responses and empty bodies (EmptyResponse)
    are retried with doubling backoff capped by retry_max_delay; Retry-After is
    honored within that cap. Other schemes and other errors are not retried,
    nor is any failure for which may_retry(error) is false. Options must
    already be validated.
    """
    http = urllib.parse.urlsplit(download_url).scheme.lower() in ("http", "https")
    backoff = retry_backoff
    for number in range(max_retries + 1):
        try:
            return attempt()
        except BaseException as error:
            retryable = isinstance(error, EmptyResponse) or is_retryable_http_error(error)
            if not http or not retryable or (may_retry is not None and not may_retry(error)):
                raise
            if number == max_retries:
                logger.warning("HTTP download failed after %d attempt(s): %s",
                               number + 1, error_description(error))
                raise
            delay = retry_delay(error, backoff, retry_max_delay)
            if delay is None:
                logger.warning("HTTP download attempt %d/%d failed (%s); Retry-After exceeds retry_max_delay",
                               number + 1, max_retries + 1, error_description(error))
                raise
            logger.warning("HTTP download attempt %d/%d failed (%s); retrying in %.3g seconds",
                           number + 1, max_retries + 1, error_description(error), delay)
            if delay:
                time.sleep(delay)
            backoff *= 2  # retry_delay caps it at retry_max_delay.


def _download_to_temp_file(
        download_url,
        timeout=None,
        base_name="download",
        chunk_size=DEFAULT_CHUNK_SIZE,
        progress_callback=None,
        directory=None,
        *,
        max_retries=DEFAULT_MAX_RETRIES,
        retry_backoff=DEFAULT_RETRY_BACKOFF,
        retry_max_delay=DEFAULT_RETRY_MAX_DELAY,
        show_progress=False,
        allow_empty=True,
        max_bytes=None):

    retry_backoff, retry_max_delay = validate_retry_options(max_retries, retry_backoff, retry_max_delay)
    if not download_url:
        raise ValueError("URL not provided")

    callback_failed = False

    def attempt():
        nonlocal callback_failed
        callback_failed = False
        tmp_path = None

        def report(done, total):
            nonlocal callback_failed
            try:
                progress(done, total)
                if progress_callback is not None:
                    progress_callback(done, total)
            except BaseException:
                callback_failed = True
                raise

        try:
            with Progress(show_progress, "Downloading") as progress, _open_staging_file(
                    directory=directory, suffix='.tmp', prefix=base_name) as tmp:
                tmp_path = tmp.name
                count = _stream_to_file(
                    download_url,
                    tmp,
                    timeout=timeout,
                    chunk_size=chunk_size,
                    progress_callback=report if progress_callback is not None or show_progress else None,
                    max_bytes=max_bytes)
                if count == 0:
                    progress(0, 0)
                    if not allow_empty:
                        raise EmptyResponse("the transfer delivered no bytes")
            return tmp_path
        except BaseException:
            if tmp_path is not None:
                _remove_staging_file(tmp_path)
            raise

    return _with_retries(
        download_url, attempt, max_retries=max_retries, retry_backoff=retry_backoff,
        retry_max_delay=retry_max_delay, may_retry=lambda error: not callback_failed)


def fetch_bytes(
        download_url,
        *,
        timeout=None,
        max_retries=DEFAULT_MAX_RETRIES,
        retry_backoff=DEFAULT_RETRY_BACKOFF,
        retry_max_delay=DEFAULT_RETRY_MAX_DELAY,
        allow_empty=False,
        max_bytes=None):
    """
    Return a remote resource's bytes in memory, transferred and retried
    exactly as fetch_file downloads are. Nothing is written to disk.

    Meant for small resources such as directory listings and metadata: the
    whole body is held in memory. It also works where a cache is read-only.

    Parameters
    ----------
    download_url : str
        HTTP, HTTPS, FTP, or file:// URL.

    timeout, max_retries, retry_backoff, retry_max_delay : optional
        As for fetch_file: a per-attempt timeout (None waits indefinitely),
        and retries of connection errors, 408/429/5xx responses and empty
        bodies with capped backoff that honors Retry-After. Only HTTP(S)
        transfers are retried.

    allow_empty : bool, optional
        Accept an empty body, default False. An empty HTTP response is
        otherwise retried as transient, then rejected with ValueError.

    max_bytes : int, optional
        The largest body to accept, after HTTP transfer decoding. It is
        checked before each chunk is kept, so the returned bytes never grow
        past it, and the body is read in chunks no larger than the limit. A
        larger body raises ValueError without retrying. Default None: no limit.

    Raises ValueError for invalid options, an empty body or one larger than
    max_bytes; Requests
    exceptions for HTTP failures, preserved after retry exhaustion; and
    urllib.error.URLError for file and FTP failures.
    """
    retry_backoff, retry_max_delay = validate_retry_options(max_retries, retry_backoff, retry_max_delay)
    if not isinstance(download_url, str) or not download_url:
        raise ValueError("URL not provided")
    if not isinstance(allow_empty, bool):
        raise ValueError("allow_empty must be a boolean")
    validate_limit("max_bytes", max_bytes)
    if max_bytes == 0 and not allow_empty:
        raise ValueError("max_bytes=0 allows only an empty body; pass allow_empty=True")

    def attempt():
        buffer = io.BytesIO()
        _stream_to_file(download_url, buffer, timeout=timeout, max_bytes=max_bytes)
        if not buffer.getbuffer().nbytes and not allow_empty:
            raise EmptyResponse("the transfer delivered no bytes")
        return buffer.getvalue()

    try:
        return _with_retries(
            download_url, attempt, max_retries=max_retries,
            retry_backoff=retry_backoff, retry_max_delay=retry_max_delay)
    except EmptyResponse as error:
        raise ValueError(
            "%s returned no bytes; pass allow_empty=True if an empty body is expected"
            % provenance.redact_url(download_url)) from error
    except MaxBytesExceeded as error:
        raise ValueError("%s returned %s" % (provenance.redact_url(download_url), error)) from error


def _open_staging_file(directory=None, prefix=".datacache-", suffix="", mode=0o600):
    """Create a unique sibling file exclusively, honoring umask for its mode.

    Unlike chmod after NamedTemporaryFile, exclusive creation with the desired
    mode lets the OS apply umask without reading/changing process-global state.
    Callers own cleanup after closing the returned binary file.
    """
    directory = gettempdir() if directory is None else directory
    for _ in range(100):
        path = os.path.join(directory, prefix + uuid4().hex + suffix)
        try:
            return open(path, "x+b", opener=lambda name, flags: os.open(name, flags, mode))
        except FileExistsError:
            continue
    raise FileExistsError("Could not create a unique staging file in %s" % directory)


def _remove_staging_file(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def normal_creation_mode(directory):
    """Measure ordinary creation permissions using an empty, disposable file.

    Never put data in this file: another user might open it before removal.
    Actual download/conversion files remain private through validation.
    """
    probe_path = None
    try:
        with _open_staging_file(
                directory=directory, prefix=".datacache-mode-", mode=0o666) as probe:
            probe_path = probe.name
            return stat.S_IMODE(os.fstat(probe.fileno()).st_mode) & 0o777
    finally:
        if probe_path is not None:
            _remove_staging_file(probe_path)


def _publish_staged_file(staged_path, full_path, mode=None):
    """Apply normal creation or existing access permissions before publication.

    Call only after writing and validation, so staging data stays private until
    it is ready to publish. New files honor the destination's creation mode;
    replacements preserve the existing regular file's access permissions. An
    explicit mode overrides both. Returns the mode applied.
    """
    try:
        existing = os.stat(full_path)
    except FileNotFoundError:
        if mode is None:
            mode = normal_creation_mode(os.path.dirname(full_path) or ".")
    else:
        if not stat.S_ISREG(existing.st_mode):
            raise FileValidationError(full_path, "expected a regular file")
        if mode is None:
            # Preserve rwx permissions, not setuid/setgid/sticky bits on new content.
            mode = stat.S_IMODE(existing.st_mode) & 0o777
    os.chmod(staged_path, mode)
    os.replace(staged_path, full_path)
    return mode


def _publish_file(staged_path, full_path, record=None):
    """Publish a staged file, keeping its provenance record truthful.

    Any record of the previous bytes is removed first, so an interruption
    leaves the new file unrecorded, never misdescribed. record (JSON text from
    provenance.describe) is then written with the file's own permissions,
    without execute bits, so a private file keeps a private record. Records are
    best effort: failing to write one, or to remove an old one, never fails the
    publication.
    """
    try:
        provenance.remove(full_path)
    except OSError as error:
        logger.warning("Could not remove the provenance record for %s: %s", full_path, error)
    mode = _publish_staged_file(staged_path, full_path)
    if record is None:
        return
    staged_record = None
    try:
        with _open_staging_file(
                directory=os.path.dirname(full_path) or ".",
                prefix=".datacache-provenance-") as output:
            staged_record = output.name
            output.write(record.encode("utf-8"))
        _publish_staged_file(staged_record, provenance.sidecar_path(full_path), mode & 0o666)
    except (OSError, ValueError) as error:
        logger.debug("Could not record provenance for %s: %s", full_path, error)
    finally:
        if staged_record is not None:
            _remove_staging_file(staged_record)


def _copy_with_progress(source, destination, show_progress, total=None, max_bytes=None):
    if max_bytes is not None:
        destination = LimitedWriter(destination, max_bytes)
    if not show_progress:
        return copyfileobj(source, destination)
    with Progress(True, "Decompressing", total) as progress:
        completed = 0
        while True:
            chunk = source.read(DEFAULT_CHUNK_SIZE)
            if not chunk:
                break
            destination.write(chunk)
            completed += len(chunk)
            progress(completed, total)


def _choose_zip_member(infos, filename, warn=True):
    """Pick the archive member to install as filename.

    Prefer the member stored at exactly that name, then a member with that
    name, ignoring case, in any folder: nearest the archive root first, then
    exact case, then the largest. Otherwise install the largest member,
    warning (if warn) when that was a guess among several.
    """
    chosen = next((info for info in infos if info.filename == filename), None)
    if chosen is not None:
        return chosen
    paths = {info: info.filename.replace("\\", "/") for info in infos}
    names = {info: path.rsplit("/", 1)[-1] for info, path in paths.items()}
    depths = {info: sum(part not in ("", ".") for part in path.split("/")[:-1])
              for info, path in paths.items()}
    named = [info for info in infos if names[info].casefold() == filename.casefold()]
    if named:
        return min(named, key=lambda info: (
            depths[info], names[info] != filename, -info.file_size))
    chosen = max(infos, key=lambda info: info.file_size)
    if warn and len(infos) > 1:
        logger.warning("No ZIP member is named %s; installing the largest of %d members, %s",
                       filename, len(infos), chosen.filename)
    return chosen


def _download_and_decompress_if_necessary(
        full_path,
        download_url,
        timeout=None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        progress_callback=None,
        *,
        decompress=None,
        convert_html=None,
        explicit_output=None,
        expected_sha256=None,
        expected_size=None,
        max_retries=DEFAULT_MAX_RETRIES,
        retry_backoff=DEFAULT_RETRY_BACKOFF,
        retry_max_delay=DEFAULT_RETRY_MAX_DELAY,
        show_progress=False,
        record_provenance=False,
        allow_empty=False,
        validator=None,
        max_bytes=None):
    """
    Download, transform and validate in sibling staging files, then publish
    with one atomic replace. Expectations always describe installed bytes.

    Unspecified transform flags retain the pre-1.8 literal-URL heuristics for
    downstream callers (including pyensembl) using this private entry point.
    Explicit flags use the parsed URL format, including download endpoints.
    explicit_output=False marks an inferred cache key, which no archive member
    can match, so installing the largest ZIP member is not reported as a guess.
    Publishing always removes a stale provenance record; record_provenance
    writes a new one describing the staged bytes.
    An empty download or installed file is rejected unless allow_empty is true
    or expected_size is 0; empty HTTP responses are retried first.
    """
    logger.info("Downloading %s to %s", download_url, full_path)
    full_path = os.fspath(full_path)
    filename = os.path.basename(full_path)
    out_dir = os.path.dirname(full_path) or "."
    source_suffix = _source_suffix(download_url)
    output_suffix = os.path.splitext(filename)[1].lower()
    if decompress is None:
        unzip = download_url.endswith("zip") and not filename.endswith("zip")
        gunzip = download_url.endswith("gz") and not filename.endswith("gz")
    else:
        unzip = source_suffix == ".zip" and decompress
        gunzip = source_suffix == ".gz" and decompress
    if convert_html is None:
        html = download_url.endswith(("html", "htm")) and full_path.endswith(".csv")
    else:
        html = convert_html and source_suffix in (".html", ".htm") and output_suffix == ".csv"
    if html and max_bytes is not None:
        raise ValueError("max_bytes can't bound HTML-to-CSV conversion; use raw=True for the HTML itself")
    # expected_size=0 states that an empty file is the right result.
    reject_empty = not allow_empty and expected_size != 0
    try:
        tmp_path = _download_to_temp_file(
            download_url=download_url,
            timeout=timeout,
            base_name=".datacache-download-",
            directory=out_dir,
            chunk_size=chunk_size,
            progress_callback=progress_callback,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            retry_max_delay=retry_max_delay,
            show_progress=show_progress,
            allow_empty=not reject_empty,
            max_bytes=max_bytes)
    except EmptyResponse as error:
        raise FileValidationError(full_path, "downloaded file is empty; pass allow_empty=True if an empty file is expected") from error
    except MaxBytesExceeded as error:
        raise FileValidationError(full_path, "download has %s" % error) from error

    staged_path = tmp_path
    try:
        if unzip or gunzip or html:
            with _open_staging_file(
                    directory=out_dir, prefix=".datacache-install-") as tmp:
                staged_path = tmp.name
            if unzip:
                with zipfile.ZipFile(tmp_path) as z:
                    infos = [info for info in z.infolist() if not info.is_dir()]
                    if not infos:
                        raise ValueError("Empty zip archive")
                    # Never extract stored paths: stream one member's contents.
                    chosen = _choose_zip_member(infos, filename, warn=explicit_output is not False)
                    refuse_if_over(chosen.file_size, max_bytes)
                    with z.open(chosen) as src, open(staged_path, "wb") as dst:
                        _copy_with_progress(src, dst, show_progress, chosen.file_size, max_bytes)
            elif gunzip:
                with gzip.GzipFile(tmp_path) as src, open(staged_path, "wb") as dst:
                    _copy_with_progress(src, dst, show_progress, max_bytes=max_bytes)
            else:
                import pandas as pd
                df = pd.read_html(tmp_path, header=0)[0]
                df.to_csv(staged_path, sep=',', index=False, encoding='utf-8')
            if reject_empty and os.path.getsize(staged_path) == 0:
                raise FileValidationError(full_path, "installed file is empty; pass allow_empty=True if an empty file is expected")
        try:
            if show_progress:
                validate_file(staged_path, expected_sha256, expected_size, show_progress=True)
            else:
                validate_file(staged_path, expected_sha256, expected_size)
        except FileValidationError as error:
            raise FileValidationError(full_path, "downloaded file " + error.reason) from error
        _run_validator(validator, staged_path, full_path, "downloaded file")
        record = None
        if record_provenance:
            # Describe the private staged bytes, not whatever is at full_path
            # after publication, which a concurrent writer may have replaced.
            # os.replace keeps their size and modification time.
            try:
                record = provenance.describe(download_url, os.stat(staged_path), expected_sha256)
            except OSError as error:
                logger.debug("Could not describe provenance for %s: %s", full_path, error)
        _publish_file(staged_path, full_path, record)
    except MaxBytesExceeded as error:
        # Only decompression writes here; the download was limited above.
        raise FileValidationError(full_path, "decompressed file has %s" % error) from error
    finally:
        if staged_path != tmp_path:
            _remove_staging_file(staged_path)
        _remove_staging_file(tmp_path)


def _run_validator(validator, path, reported_path, description):
    """Raise FileValidationError for reported_path if validator(path) raises or
    returns False."""
    if validator is None:
        return
    try:
        accepted = validator(path)
    except Exception as error:
        raise FileValidationError(
            reported_path, "%s failed validation: %s" % (description, error)) from error
    if accepted is False:
        raise FileValidationError(reported_path, "%s failed validation" % description)


def _expiry_seconds(expire_after):
    """expire_after as seconds, or None for no expiry; ValueError if invalid."""
    if expire_after is None:
        return None
    if isinstance(expire_after, timedelta):
        expire_after = expire_after.total_seconds()
    if (isinstance(expire_after, bool) or not isinstance(expire_after, (int, float)) or
            not math.isfinite(expire_after) or expire_after < 0):
        raise ValueError("expire_after must be a non-negative number of seconds or a timedelta")
    return expire_after


def _check_cached_file(full_path, expected_sha256, expected_size, reject_empty, validator):
    """Raise unless full_path is a usable cache hit; FileNotFoundError if absent."""
    validate_file(full_path, expected_sha256, expected_size)
    if reject_empty and os.stat(full_path).st_size == 0:
        raise FileValidationError(full_path, "cached file is empty (pass allow_empty=True if an empty file is expected)")
    _run_validator(validator, full_path, full_path, "cached file")


def _age_seconds(full_path):
    """Seconds since full_path was fetched: from its provenance record when one
    describes it, otherwise from its modification time, which atomic
    publication sets when the download is written."""
    info = os.stat(full_path)
    fetched = info.st_mtime
    record = provenance.read(full_path, info)
    if record is not None:
        try:
            recorded = datetime.fromisoformat(record["fetched_at"])
            if recorded.tzinfo is None:
                recorded = recorded.replace(tzinfo=timezone.utc)  # Records are UTC.
            fetched = recorded.timestamp()
        except (TypeError, ValueError, OverflowError):
            pass
    # A modification time in the future (clock skew, rsync) counts as new.
    return max(0.0, time.time() - fetched)


def expected_path(
        download_url=None,
        filename=None,
        decompress=False,
        subdir=None,
        *,
        destination=None,
        cache_root=None):
    """Resolve the fetch destination without filesystem access or mutations.

    cache_root overrides the platform location selected by subdir. destination
    is an exact path and cannot be combined with filename, subdir, or cache_root.
    """
    if destination is not None:
        if filename is not None or subdir is not None or cache_root is not None:
            raise ValueError("destination cannot be combined with filename, subdir, or cache_root")
        result = os.fspath(destination)
        if not isinstance(result, str) or not result:
            raise ValueError("destination must be a non-empty text path")
        return result
    filename = build_local_filename(download_url, filename, decompress)
    return common.resolve_path(filename, subdir, cache_root=cache_root)


def file_exists(
        download_url=None,
        filename=None,
        decompress=False,
        subdir=None,
        *,
        destination=None,
        cache_root=None):
    """Check presence without writes/network; permission errors propagate.

    This does not check file type, readability, or integrity. Use inspect_file
    or validate_file on expected_path(...) for those checks.
    """
    return path_exists(expected_path(
        download_url, filename, decompress, subdir,
        destination=destination, cache_root=cache_root))


DOWNLOAD_OPTIONS = frozenset((
    "timeout", "chunk_size", "progress_callback", "show_progress",
    "max_retries", "retry_backoff", "retry_max_delay", "resume", "max_bytes"))


def validate_download_options(options, kind):
    """Check the download_options an install passes to each fetch_file call.

    Bundle, archive and materialization installs accept the transfer settings
    named in DOWNLOAD_OPTIONS and nothing else. Each is checked as fetch_file
    checks it, so a bad value fails before an install creates anything rather
    than at its first download. options may be None. kind names the install
    in messages, such as "bundle". Returns a new dict of the options.
    """
    options = dict(options or {})
    unsupported = set(options) - DOWNLOAD_OPTIONS
    if unsupported:
        raise ValueError("unsupported %s download options: %s" % (kind, sorted(unsupported)))
    if options.get("max_bytes") == 0:
        raise ValueError("max_bytes=0 allows only empty downloads")
    validate_transfer_settings(
        chunk_size=options.get("chunk_size", DEFAULT_CHUNK_SIZE),
        progress_callback=options.get("progress_callback"),
        show_progress=options.get("show_progress", False),
        resume=options.get("resume", False),
        max_retries=options.get("max_retries", DEFAULT_MAX_RETRIES),
        retry_backoff=options.get("retry_backoff", DEFAULT_RETRY_BACKOFF),
        retry_max_delay=options.get("retry_max_delay", DEFAULT_RETRY_MAX_DELAY),
        max_bytes=options.get("max_bytes"))
    return options


def validate_transfer_settings(
        *, chunk_size, progress_callback, show_progress, resume,
        max_retries, retry_backoff, retry_max_delay, max_bytes=None):
    """Check fetch_file's transfer settings; return the retry delays as floats."""
    validate_limit("max_bytes", max_bytes)
    if not isinstance(resume, bool):
        raise ValueError("resume must be a boolean")
    if not isinstance(show_progress, bool):
        raise ValueError("show_progress must be a boolean")
    if progress_callback is not None and not callable(progress_callback):
        raise ValueError("progress_callback must be callable")
    if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")
    return validate_retry_options(max_retries, retry_backoff, retry_max_delay)


def fetch_file(
        download_url,
        filename=None,
        decompress=False,
        subdir=None,
        force=False,
        timeout=None,
        use_wget_if_available=None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        progress_callback=None,
        *,
        destination=None,
        cache_root=None,
        expected_sha256=None,
        expected_size=None,
        max_bytes=None,
        max_retries=DEFAULT_MAX_RETRIES,
        retry_backoff=DEFAULT_RETRY_BACKOFF,
        retry_max_delay=DEFAULT_RETRY_MAX_DELAY,
        show_progress=False,
        record_provenance=False,
        allow_empty=False,
        resume=False,
        raw=False,
        expire_after=None,
        return_stale_on_error=False,
        validator=None):
    """
    Download a remote file and store it locally in a cache directory. Don't
    download it again if it's already present (unless `force` is True, or the
    cached copy is older than `expire_after`).

    Parameters
    ----------
    download_url : str
        Remote URL of file to download.

    filename : str, optional
        Local filename, used as cache key. If omitted, then determine the local
        filename from the URL.

    decompress : bool, optional
        With an inferred filename, archives are retained by default, including
        for URLs with query strings or fragments. Set True to decompress them
        under a distinct cache key. An explicit filename or destination lacking
        the source's .zip/.gz suffix still implies decompression for compatibility.

    raw : bool, optional
        Disable archive decompression and HTML-to-CSV conversion regardless of
        the output name. Incompatible with decompress=True. Defaults to False,
        preserving legacy output-name inference. Integrity expectations then
        describe the unchanged payload, after HTTP transfer decoding.

    subdir : str, optional
        Application name selecting a platform cache directory, "datacache" by
        default. It is not nested inside the default cache. Ignored when
        cache_root is supplied.

    force : bool, optional
        By default, a remote file is not downloaded if it's already present.
        However, with this argument set to True, it will be overwritten.

    timeout : float or (float, float), optional
        Per-attempt connect/read timeout in seconds. The default None waits
        indefinitely. HTTP(S) also accepts a Requests (connect, read) tuple.

    use_wget_if_available : bool, optional
        Deprecated and ignored. datacache now always uses its streaming Python
        downloader (which handles http(s) and ftp); the legacy `wget` path was
        removed. Passing this argument emits a DeprecationWarning.

    chunk_size : int, optional
        Number of bytes to stream from the server to disk at a time.
        Defaults to 1 MB.

    progress_callback : callable, optional
        If provided, called as ``progress_callback(bytes_downloaded,
        total_bytes)`` after each chunk is written, where `total_bytes` is the
        server-reported size or None if unknown. Lets callers render a progress
        display in their application's own UI instead of the built-in bar.

    show_progress : bool, optional
        Show tqdm download, decompression, and hash-verification progress.
        tqdm is installed with datacache. Defaults to False; cache hits are quiet.
        Can be combined with progress_callback. Each retry starts a fresh bar.

    destination : str or os.PathLike, optional
        Exact output path, including filename, instead of the default cache.
        Mutually exclusive with filename, subdir, and cache_root. Parent directories are
        created only when downloading. With decompress=True, the destination
        name is kept exactly as supplied while archive contents are installed.

    cache_root : str or os.PathLike, optional
        Directory containing cached files, overriding the location selected by
        subdir. Cannot be combined with destination.

    expected_sha256 : str, optional
        Trusted SHA-256 hex digest of the installed bytes, after decompression
        or HTML conversion (not of the compressed archive or HTTP wire bytes).
        Checked both on cache reuse and before publishing a replacement.

    expected_size : int, optional
        Expected non-negative byte count of the same installed bytes.

    max_bytes : int, optional
        The most bytes any file this fetch writes may hold, checked before
        every write. It bounds the download itself, which is the body after
        HTTP transfer decoding such as gzip Content-Encoding (whether or not
        Content-Length is sent), and the installed file after decompress=True,
        so allow for the compressed download too; together they never need
        more than twice max_bytes. A larger transfer raises FileValidationError
        without retrying: an existing file stays and temporary files are
        removed. Cache hits write nothing and are unaffected. expected_size
        must not be larger, and HTML-to-CSV conversion can't be bounded;
        options that can't work together raise ValueError, even on a cache
        hit. Default None: no limit.

    max_retries : int, optional
        Additional attempts after transient HTTP/transport failures, default 2.
        Set 0 to disable retries. File and FTP transfers are not retried.

    retry_backoff : float, optional
        Initial retry delay in seconds, default 1. Doubles after each retry,
        capped by retry_max_delay. Progress counts restart for each attempt.

    retry_max_delay : float, optional
        Maximum delay between attempts, default 30 seconds. Retry-After is
        honored when within this limit; longer server waits stop retries.
        timeout still applies per attempt, not as a total download deadline.

    resume : bool, optional
        Keep private partials and resume raw HTTP(S) transfers, default False.
        Requires expected_size and a POSIX local filesystem. If expected_sha256
        is omitted, the server must supply a strong ETag on every accepted
        response; it is sent as If-Range when resuming. ETags prevent mixing
        representations, but do not verify a trusted checksum. Cache hits with
        size alone check only their byte count. Does not support decompression
        or HTML conversion; raw=True permits arbitrary output names. Retries
        report cumulative bytes, resetting only when the server requires a
        fresh transfer. discard_partial(destination) explicitly drops partials.
        Interrupted downloads retain at most expected_size partial bytes.

    record_provenance : bool, optional
        After publishing a download, also write a hidden ".<name>.datacache.json"
        record of the source URL (without user name, password, query, or
        fragment; the path is kept as is), fetch time, size, and the SHA-256
        when expected_sha256 verified it, which inspect_file reports offline.
        The record has the file's permissions. Off by default: a caller that
        downloads to a temporary name and then moves the file would leave the
        record behind. Cache hits never write one, and any new download removes
        a previous record first.
    allow_empty : bool, optional
        Accept an empty file, default False. A complete but empty response,
        such as a withdrawn upstream record, is otherwise never published: HTTP
        retries it as transient, then FileValidationError is raised. An empty
        cached file is likewise invalid. expected_size=0 also allows it.

    expire_after : float or datetime.timedelta, optional
        How long a cached file stays fresh, in seconds or as a timedelta, like
        requests-cache's option of the same name. An older cached file is
        downloaded again, as is one that no longer validates; 0 refreshes
        every time. Its age comes from the provenance record's fetch time when
        one describes the file (see record_provenance), otherwise from its
        modification time; a future time counts as new. Default None reuses a
        valid file however old and raises for an invalid one.

    return_stale_on_error : bool, optional
        When a refresh (force=True or an expired expire_after) fails with an
        exception and a valid cached file exists, log a warning and return the
        cached path instead, as HTTP's stale-if-error directive allows.
        Without a valid cached file the error
        propagates, as does an exception from progress_callback, which
        cancels the fetch. Default False.

    validator : callable, optional
        validator(path) checks content that a successful transfer can still
        get wrong, such as an HTTP 200 error page. It rejects the file by
        raising (normally ValueError) or returning False. It runs on the
        installed bytes before publication, so a rejected download never
        replaces the cached file, and on cache hits. Rejection raises
        FileValidationError, chained to any exception. Not supported with
        resume=True.

    A corrupt cache hit raises FileValidationError; use force=True for an
    explicit repair. Missing files are downloaded. Transport, decompression
    and filesystem exceptions propagate. Failed downloads leave an existing
    destination unchanged and remove their staging files. Atomic replacement
    requires a local filesystem supporting os.replace; concurrent writers
    publish complete files with the last successful replacement winning.
    Staging files remain private until publication. New files use normal
    creation permissions (0666 filtered by umask); replacements preserve the
    existing file's read/write/execute permission bits.

    Returns the local path, which is relative when destination or cache_root is.
    """
    retry_backoff, retry_max_delay = validate_transfer_settings(
        chunk_size=chunk_size, progress_callback=progress_callback, show_progress=show_progress,
        resume=resume, max_retries=max_retries, retry_backoff=retry_backoff,
        retry_max_delay=retry_max_delay, max_bytes=max_bytes)
    if not isinstance(raw, bool):
        raise ValueError("raw must be a boolean")
    if raw and decompress:
        raise ValueError("raw=True cannot be combined with decompress=True")
    if resume:
        from .resume import validate_resume
        validate_resume(download_url, expected_sha256, expected_size)
    _validate_expectations(expected_sha256, expected_size)
    validate_size_within_limit(expected_size, max_bytes)
    if max_bytes == 0 and not allow_empty and expected_size != 0:
        raise ValueError("max_bytes=0 allows only an empty file; pass allow_empty=True")
    if not isinstance(record_provenance, bool):
        raise ValueError("record_provenance must be a boolean")
    if not isinstance(allow_empty, bool):
        raise ValueError("allow_empty must be a boolean")
    expiry = _expiry_seconds(expire_after)
    if not isinstance(return_stale_on_error, bool):
        raise ValueError("return_stale_on_error must be a boolean")
    if validator is not None and not callable(validator):
        raise ValueError("validator must be callable")
    if validator is not None and resume:
        raise ValueError("validator cannot be combined with resume=True")
    reject_empty = not allow_empty and expected_size != 0
    # Query/fragment text in an inferred cache key is not an output-format request.
    explicit_output = destination is not None or bool(filename)
    if use_wget_if_available is not None:
        warnings.warn(
            "use_wget_if_available is deprecated and ignored; datacache now always "
            "uses its streaming Python downloader (handles http(s) and ftp).",
            DeprecationWarning,
            stacklevel=2)
    full_path = expected_path(
        download_url, filename, decompress, subdir,
        destination=destination, cache_root=cache_root)
    source_suffix = _source_suffix(download_url)
    output_suffix = os.path.splitext(full_path)[1].lower()
    archive_decompression = not raw and bool(
        decompress or (explicit_output and output_suffix != source_suffix))
    html_conversion = (not raw and explicit_output and source_suffix in (".htm", ".html")
                       and output_suffix == ".csv")
    if html_conversion and max_bytes is not None:
        raise ValueError("max_bytes can't bound HTML-to-CSV conversion; use raw=True for the HTML itself")
    if resume and (decompress or
                   (source_suffix in (".gz", ".zip") and archive_decompression) or
                   html_conversion):
        raise ValueError("resume=True supports raw downloads only; use raw=True or retain the archive suffix")
    # Whether the cached file is known valid, for a failed refresh to fall
    # back to (return_stale_on_error); with force=True it is checked only on failure.
    cached = False
    refresh = force
    if not force:
        try:
            _check_cached_file(full_path, expected_sha256, expected_size, reject_empty, validator)
        except FileNotFoundError:
            pass
        except FileValidationError as error:
            # force=True replaces a mismatched file, but never a directory or
            # other non-regular path, so only suggest it when it can help.
            try:
                replaceable = stat.S_ISREG(os.stat(full_path).st_mode)
            except OSError:
                replaceable = False
            if not replaceable:
                raise
            if expiry is None:
                raise FileValidationError(
                    full_path, error.reason + "; use force=True to explicitly replace it") from error
            # An expiring cache replaces content that no longer validates.
            logger.info("Cached file %s is invalid (%s); fetching it again", full_path, error.reason)
            refresh = True
        else:
            if expiry is None or (expiry > 0 and _age_seconds(full_path) < expiry):
                logger.info("Cached file %s from URL %s", full_path, download_url)
                return full_path
            cached = True
            refresh = True
            logger.info("Cached file %s has expired; fetching it again", full_path)
    # A progress callback's exception cancels the fetch; never fall back.
    callback_errors = []
    if progress_callback is not None:
        caller_callback = progress_callback

        def progress_callback(done, total):
            try:
                caller_callback(done, total)
            except BaseException as error:
                callback_errors.append(error)
                raise
    try:
        os.makedirs(os.path.dirname(full_path) or ".", exist_ok=True)
        logger.info("Fetching %s from URL %s", full_path, download_url)
        if resume:
            from .resume import download_resumable
            download_resumable(
                download_url, full_path, expected_sha256=expected_sha256,
                expected_size=expected_size, timeout=timeout, chunk_size=chunk_size,
                progress_callback=progress_callback, show_progress=show_progress,
                max_retries=max_retries, retry_backoff=retry_backoff,
                retry_max_delay=retry_max_delay, record_provenance=record_provenance,
                force=refresh)
        else:
            _download_and_decompress_if_necessary(
                full_path=full_path,
                download_url=download_url,
                timeout=timeout,
                chunk_size=chunk_size,
                progress_callback=progress_callback,
                decompress=archive_decompression,
                convert_html=html_conversion,
                explicit_output=explicit_output,
                expected_sha256=expected_sha256,
                expected_size=expected_size,
                max_retries=max_retries,
                retry_backoff=retry_backoff,
                retry_max_delay=retry_max_delay,
                show_progress=show_progress,
                record_provenance=record_provenance,
                allow_empty=allow_empty,
                validator=validator,
                max_bytes=max_bytes)
    except Exception as error:
        if not return_stale_on_error or any(error is cancel for cancel in callback_errors):
            raise
        if not cached:
            try:
                # A failed download leaves the destination untouched.
                _check_cached_file(full_path, expected_sha256, expected_size, reject_empty, validator)
                cached = True
            except (OSError, ValueError):
                pass
        if not cached:
            raise  # The refresh's own error, with its cause.
        logger.warning("Could not refresh %s (%s); using the cached copy",
                       full_path, error_description(error))
    return full_path


def fetch_and_transform(
        transformed_filename,
        transformer,
        loader,
        source_filename,
        source_url,
        subdir=None,
        *,
        force=False,
        cache_root=None,
        download_options=None,
        show_progress=False):
    """
    Download a source and cache a successful single-file transformation.

    transformer(source_path, output_path) must create and close output_path.
    It receives an absent path in a private sibling directory, with the final
    basename and extension. Its result is returned on success; cache hits call
    loader(final_path). A directly returned output path is remapped to the
    final path. Prefer in-memory results over objects containing staged paths.

    force rebuilds the transformed output while reusing the source. Set
    download_options={"force": True} to refresh the source too. Other fetch_file
    settings (timeouts, retries, hashes, callbacks) go in download_options.
    cache_root selects the root for both artifacts. For legacy compatibility,
    a nonempty subdir also defaults source decompression to True; override it
    explicitly in download_options. Existing sources misplaced in the default
    cache by older versions can be reused without moving them. Failed
    transformations preserve previous output and remove their partial files.
    """
    transformed_path = common.resolve_path(transformed_filename, subdir, cache_root=cache_root)
    try:
        if force:
            raise FileNotFoundError(transformed_path)
        validate_file(transformed_path)
    except FileNotFoundError:
        options = dict(download_options or {})
        options.setdefault("cache_root", cache_root)
        options.setdefault("show_progress", show_progress)
        options.setdefault("decompress", bool(subdir))
        source_path = None
        # Older versions accidentally passed subdir as decompress and cached
        # the source in the default root. Check only that exact legacy path,
        # after preferring the requested root, and never move/chmod it.
        if (subdir and options["cache_root"] is None and not options.get("force") and
                "destination" not in options):
            current = expected_path(source_url, source_filename, options["decompress"], subdir)
            legacy = expected_path(source_url, source_filename, options["decompress"])
            if not path_exists(current):
                try:
                    validate_file(legacy, options.get("expected_sha256"), options.get("expected_size"))
                except FileNotFoundError:
                    pass
                else:
                    source_path = legacy
        if source_path is None:
            source_path = fetch_file(source_url, filename=source_filename, subdir=subdir, **options)
        logger.info("Generating data file %s from %s", transformed_path, source_path)
        os.makedirs(os.path.dirname(transformed_path) or ".", exist_ok=True)
        # A private directory keeps arbitrary transformer output private while
        # preserving the filename/extension and the absent-output contract.
        with TemporaryDirectory(
                dir=os.path.dirname(transformed_path) or ".",
                prefix=".datacache-transform-") as staging_directory:
            staged_path = os.path.join(staging_directory, os.path.basename(transformed_path))
            result = transformer(source_path, staged_path)
            try:
                validate_file(staged_path)
            except FileNotFoundError as error:
                raise RuntimeError("Transformer did not create %s" % transformed_path) from error
            _publish_file(staged_path, transformed_path)
            if isinstance(result, (str, os.PathLike)) and os.fspath(result) == staged_path:
                result = type(result)(transformed_path)
    else:
        logger.info("Cached data file: %s", transformed_path)
        result = loader(transformed_path)
    return result


def fetch_csv_dataframe(
        download_url,
        filename=None,
        subdir=None,
        *,
        download_options=None,
        show_progress=False,
        **pandas_kwargs):
    """
    Download `download_url` (cached under the key `filename`, if given) and
    load it with pandas.read_csv, passing extra keyword arguments such as sep='\t'.

    Archives are decompressed before parsing. Pass fetch_file settings such as
    cache_root, timeout, expected_sha256, and progress_callback in a separate
    download_options dictionary. show_progress enables optional tqdm displays.
    The remaining keyword arguments are passed only to pandas.read_csv.
    """
    options = dict(download_options or {})
    options.setdefault("show_progress", show_progress)
    path = fetch_file(
        download_url=download_url,
        filename=filename,
        decompress=True,
        subdir=subdir,
        **options)
    import pandas as pd
    return pd.read_csv(path, **pandas_kwargs)
