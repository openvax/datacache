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
import gzip
import logging
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

import requests
import pandas as pd

from . import common
from .common import _source_suffix, build_local_filename
from .integrity import FileValidationError, _validate_expectations, validate_file
from .inspection import path_exists
from .progress import Progress
from .retries import (
    DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BACKOFF, DEFAULT_RETRY_MAX_DELAY,
    error_description, is_retryable_http_error, retry_delay, validate_retry_options,
)

logger = logging.getLogger(__name__)

# Number of bytes to read/write at a time when streaming a download to disk.
DEFAULT_CHUNK_SIZE = 2 ** 20  # 1 MB


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
        progress_callback=None):
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
    drive e.g. a tqdm progress bar without datacache depending on tqdm.

    Returns the total number of bytes written.
    """
    bytes_downloaded = 0

    def report(total_bytes):
        if progress_callback is not None:
            progress_callback(bytes_downloaded, total_bytes)

    if urllib.parse.urlsplit(download_url).scheme.lower() in ("http", "https"):
        with closing(requests.get(download_url, timeout=timeout, stream=True)) as response:
            response.raise_for_status()
            # Requests decodes content encoding before yielding chunks; a wire
            # length is not a valid total for the bytes we write in that case.
            encoding = response.headers.get("Content-Encoding", "identity").lower()
            total_bytes = (_content_length(response.headers.get("Content-Length"))
                           if encoding == "identity" else None)
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
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                file_handle.write(chunk)
                bytes_downloaded += len(chunk)
                report(total_bytes)
    return bytes_downloaded


def _download_to_temp_file(
        download_url,
        timeout=None,
        base_name="download",
        ext="tmp",
        chunk_size=DEFAULT_CHUNK_SIZE,
        progress_callback=None,
        directory=None,
        *,
        max_retries=DEFAULT_MAX_RETRIES,
        retry_backoff=DEFAULT_RETRY_BACKOFF,
        retry_max_delay=DEFAULT_RETRY_MAX_DELAY,
        show_progress=False):

    retry_backoff, retry_max_delay = validate_retry_options(max_retries, retry_backoff, retry_max_delay)
    if not download_url:
        raise ValueError("URL not provided")

    http = urllib.parse.urlsplit(download_url).scheme.lower() in ("http", "https")
    backoff = min(retry_backoff, retry_max_delay)
    for attempt in range(max_retries + 1):
        tmp_path = None
        callback_failed = False

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
                    directory=directory, suffix='.' + ext, prefix=base_name) as tmp:
                tmp_path = tmp.name
                count = _stream_to_file(
                    download_url,
                    tmp,
                    timeout=timeout,
                    chunk_size=chunk_size,
                    progress_callback=report if progress_callback is not None or show_progress else None)
                if count == 0:
                    progress(0, 0)
            return tmp_path
        except BaseException as error:
            if tmp_path is not None:
                _remove_staging_file(tmp_path)
            if not http or callback_failed or not is_retryable_http_error(error):
                raise
            if attempt == max_retries:
                logger.warning("HTTP download failed after %d attempt(s): %s",
                               attempt + 1, error_description(error))
                raise
            delay = retry_delay(error, backoff, retry_max_delay)
            if delay is None:
                logger.warning("HTTP download attempt %d/%d failed (%s); Retry-After exceeds retry_max_delay",
                               attempt + 1, max_retries + 1, error_description(error))
                raise
            logger.warning("HTTP download attempt %d/%d failed (%s); retrying in %.3g seconds",
                           attempt + 1, max_retries + 1, error_description(error), delay)
            if delay:
                time.sleep(delay)
            backoff = min(backoff * 2, retry_max_delay)


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


def _normal_creation_mode(directory):
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


def _publish_staged_file(staged_path, full_path):
    """Apply normal creation or existing access permissions before publication.

    Call only after writing and validation, so staging data stays private until
    it is ready to publish. New files honor the destination's creation mode;
    replacements preserve the existing regular file's access permissions.
    """
    try:
        existing = os.stat(full_path)
    except FileNotFoundError:
        mode = _normal_creation_mode(os.path.dirname(full_path) or ".")
    else:
        if not stat.S_ISREG(existing.st_mode):
            raise FileValidationError(full_path, "expected a regular file")
        # Preserve rwx permissions, not setuid/setgid/sticky bits on new content.
        mode = stat.S_IMODE(existing.st_mode) & 0o777
    os.chmod(staged_path, mode)
    os.replace(staged_path, full_path)


def _decompress_to_file(src_stream, full_path):
    """Compatibility entry point for atomically copying a decompressed stream."""
    staged_path = None
    try:
        with _open_staging_file(
                directory=os.path.dirname(full_path) or ".",
                prefix=".datacache-decompress-") as output:
            staged_path = output.name
            copyfileobj(src_stream, output)
        _publish_staged_file(staged_path, full_path)
    finally:
        if staged_path is not None:
            _remove_staging_file(staged_path)


def _copy_with_progress(source, destination, show_progress, total=None):
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
        show_progress=False):
    """
    Download, transform and validate in sibling staging files, then publish
    with one atomic replace. Expectations always describe installed bytes.

    Unspecified transform flags retain the pre-1.8 literal-URL heuristics for
    downstream callers (including pyensembl) using this private entry point.
    Explicit flags use the parsed URL format, including download endpoints.
    explicit_output=False marks an inferred cache key, which no archive member
    can match, so installing the largest ZIP member is not reported as a guess.
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
        show_progress=show_progress)

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
                    with z.open(chosen) as src, open(staged_path, "wb") as dst:
                        _copy_with_progress(src, dst, show_progress, chosen.file_size)
            elif gunzip:
                with gzip.GzipFile(tmp_path) as src, open(staged_path, "wb") as dst:
                    _copy_with_progress(src, dst, show_progress)
            else:
                df = pd.read_html(tmp_path, header=0)[0]
                df.to_csv(staged_path, sep=',', index=False, encoding='utf-8')
        try:
            if show_progress:
                validate_file(staged_path, expected_sha256, expected_size, show_progress=True)
            else:
                validate_file(staged_path, expected_sha256, expected_size)
        except FileValidationError as error:
            raise FileValidationError(full_path, "downloaded file " + error.reason) from error
        _publish_staged_file(staged_path, full_path)
    finally:
        if staged_path != tmp_path:
            _remove_staging_file(staged_path)
        _remove_staging_file(tmp_path)


def expected_path(
        download_url=None,
        filename=None,
        decompress=False,
        subdir=None,
        *,
        destination=None,
        cache_root=None):
    """Resolve the fetch destination without filesystem access or mutations.

    cache_root overrides the appdirs location selected by subdir. destination
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
        max_retries=DEFAULT_MAX_RETRIES,
        retry_backoff=DEFAULT_RETRY_BACKOFF,
        retry_max_delay=DEFAULT_RETRY_MAX_DELAY,
        show_progress=False):
    """
    Download a remote file and store it locally in a cache directory. Don't
    download it again if it's already present (unless `force` is True.)

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
        bar (e.g. tqdm) without datacache taking on that dependency.

    show_progress : bool, optional
        Show tqdm download, decompression, and hash-verification progress.
        Requires datacache[progress]. Defaults to False; cache hits are quiet.
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
    _validate_expectations(expected_sha256, expected_size)
    if not isinstance(show_progress, bool):
        raise ValueError("show_progress must be a boolean")
    if progress_callback is not None and not callable(progress_callback):
        raise ValueError("progress_callback must be callable")
    retry_backoff, retry_max_delay = validate_retry_options(max_retries, retry_backoff, retry_max_delay)
    if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")
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
    if not force:
        try:
            validate_file(full_path, expected_sha256, expected_size)
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
            raise FileValidationError(
                full_path, error.reason + "; use force=True to explicitly replace it") from error
        else:
            logger.info("Cached file %s from URL %s", full_path, download_url)
            return full_path
    source_suffix = _source_suffix(download_url)
    output_suffix = os.path.splitext(full_path)[1].lower()
    archive_decompression = bool(decompress or (explicit_output and output_suffix != source_suffix))
    os.makedirs(os.path.dirname(full_path) or ".", exist_ok=True)
    logger.info("Fetching %s from URL %s", full_path, download_url)
    _download_and_decompress_if_necessary(
        full_path=full_path,
        download_url=download_url,
        timeout=timeout,
        chunk_size=chunk_size,
        progress_callback=progress_callback,
        decompress=archive_decompression,
        convert_html=explicit_output,
        explicit_output=explicit_output,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        retry_max_delay=retry_max_delay,
        show_progress=show_progress)
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
            _publish_staged_file(staged_path, transformed_path)
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
    return pd.read_csv(path, **pandas_kwargs)
