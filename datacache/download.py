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
import warnings
from shutil import copyfileobj
from tempfile import gettempdir
from uuid import uuid4
import zipfile
import urllib.parse
import urllib.request

import requests
import pandas as pd

from . import common
from .common import _source_suffix, build_path, build_local_filename
from .integrity import FileValidationError, _validate_expectations, validate_file
from .inspection import path_exists

logger = logging.getLogger(__name__)

# Number of bytes to read/write at a time when streaming a download to disk.
DEFAULT_CHUNK_SIZE = 2 ** 20  # 1 MB


def _content_length(header_value):
    """Parse a Content-Length header value into an int, or None if it's
    absent or not a valid integer."""
    if header_value is None:
        return None
    try:
        return int(header_value)
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

    if download_url.startswith("http"):
        with closing(requests.get(download_url, timeout=timeout, stream=True)) as response:
            response.raise_for_status()
            total_bytes = _content_length(response.headers.get("Content-Length"))
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
        directory=None):

    if not download_url:
        raise ValueError("URL not provided")

    tmp_path = None
    try:
        with _open_staging_file(
                directory=directory, suffix='.' + ext, prefix=base_name) as tmp:
            tmp_path = tmp.name
            _stream_to_file(
                download_url,
                tmp,
                timeout=timeout,
                chunk_size=chunk_size,
                progress_callback=progress_callback)
        return tmp_path
    except BaseException:
        if tmp_path is not None:
            _remove_staging_file(tmp_path)
        raise


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


def _publish_staged_file(staged_path, full_path, normal_creation_mode=False):
    """Preserve an existing regular file's access mode before atomic replacement."""
    try:
        existing = os.stat(full_path)
    except FileNotFoundError:
        mode = _normal_creation_mode(os.path.dirname(full_path) or ".") if normal_creation_mode else None
    else:
        if not stat.S_ISREG(existing.st_mode):
            raise FileValidationError(full_path, "expected a regular file")
        # Preserve rwx permissions, not setuid/setgid/sticky bits on new content.
        mode = stat.S_IMODE(existing.st_mode) & 0o777
    if mode is not None:
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


def _download_and_decompress_if_necessary(
        full_path,
        download_url,
        timeout=None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        progress_callback=None,
        *,
        decompress=None,
        convert_html=None,
        expected_sha256=None,
        expected_size=None):
    """
    Download, transform and validate in sibling staging files, then publish
    with one atomic replace. Expectations always describe installed bytes.

    Unspecified transform flags retain the pre-1.8 literal-URL heuristics for
    downstream callers (including pyensembl) using this private entry point.
    Explicit flags use the parsed URL format, including download endpoints.
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
        progress_callback=progress_callback)

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
                    chosen = next(
                        (info for info in infos if info.filename == filename),
                        max(infos, key=lambda info: info.file_size))
                    with z.open(chosen) as src, open(staged_path, "wb") as dst:
                        copyfileobj(src, dst)
            elif gunzip:
                with gzip.GzipFile(tmp_path) as src, open(staged_path, "wb") as dst:
                    copyfileobj(src, dst)
            else:
                df = pd.read_html(tmp_path, header=0)[0]
                df.to_csv(staged_path, sep=',', index=False, encoding='utf-8')
        try:
            validate_file(staged_path, expected_sha256, expected_size)
        except FileValidationError as error:
            raise FileValidationError(full_path, "downloaded file " + error.reason) from error
        _publish_staged_file(staged_path, full_path, normal_creation_mode=html)
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
        expected_size=None):
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
        Group downloads in a single subdirectory.

    force : bool, optional
        By default, a remote file is not downloaded if it's already present.
        However, with this argument set to True, it will be overwritten.

    timeout : float, optional
        Timeout for download in seconds, default is None which uses
        global timeout.

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

    A corrupt cache hit raises FileValidationError; use force=True for an
    explicit repair. Missing files are downloaded. Transport, decompression
    and filesystem exceptions propagate. Failed downloads leave an existing
    destination unchanged and remove their staging files. Atomic replacement
    requires a local filesystem supporting os.replace; concurrent writers
    publish complete files with the last successful replacement winning.

    Returns the full path of the local file.
    """
    _validate_expectations(expected_sha256, expected_size)
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
        expected_sha256=expected_sha256,
        expected_size=expected_size)
    return full_path


def fetch_and_transform(
        transformed_filename,
        transformer,
        loader,
        source_filename,
        source_url,
        subdir=None):
    """
    Fetch a remote file from `source_url`, save it locally as `source_filename` and then use
    the `loader` and `transformer` function arguments to turn this saved data into an in-memory
    object.
    """
    transformed_path = build_path(transformed_filename, subdir)
    if not os.path.exists(transformed_path):
        source_path = fetch_file(source_url, source_filename, subdir)
        logger.info("Generating data file %s from %s", transformed_path, source_path)
        result = transformer(source_path, transformed_path)
    else:
        logger.info("Cached data file: %s", transformed_path)
        result = loader(transformed_path)
    if not os.path.exists(transformed_path):
        raise RuntimeError(
            "Expected transformed file %s to exist after fetch_and_transform" % (
                transformed_path,))
    return result


def fetch_csv_dataframe(
        download_url,
        filename=None,
        subdir=None,
        **pandas_kwargs):
    """
    Download a remote file from `download_url` and save it locally as `filename`.
    Load that local file as a CSV into Pandas using extra keyword arguments such as sep='\t'.
    """
    path = fetch_file(
        download_url=download_url,
        filename=filename,
        decompress=True,
        subdir=subdir)
    return pd.read_csv(path, **pandas_kwargs)
