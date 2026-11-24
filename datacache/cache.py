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

from os.path import realpath
from os import fspath, remove, scandir, stat
from shutil import rmtree

from . import common
from . import download
from . import provenance
from .download import expected_path
from .inspection import inspect_file, path_exists, plan_fetch
from .permissions import make_file_readable


class Cache:
    """Download, inspect, and delete files under one cache directory.

    Selects a root once for file, inspection, permission, and deletion
    operations without creating directories or downloading anything at
    construction time. A relative ``cache_root`` remains relative to the
    working directory on every call; pass an absolute path if the working
    directory may change.

    Parameters
    ----------
    subdir : str, optional
        Nonempty application name selecting the platform cache directory via
        ``get_data_dir``. Default is ``"datacache"``.
    cache_root : str or os.PathLike or None, optional
        Exact directory path overriding the platform location selected by
        ``subdir``.

    Attributes
    ----------
    subdir : str
        Application name passed to the constructor.
    cache_directory_path : str
        Selected cache root directory string.

    Raises
    ------
    ValueError
        If ``subdir`` is empty.
    """

    def __init__(self, subdir="datacache", *, cache_root=None):
        if not subdir:
            raise ValueError("Cache subdir must be a non-empty string")
        self.subdir = subdir
        self.cache_directory_path = (
            common.get_data_dir(subdir) if cache_root is None else fspath(cache_root))

        # Track explicit filenames as well as inferred names for deletion.
        # TODO: handle decompression separately from download,
        # so we can use copies of compressed files we've already downloaded
        self._local_paths = {}

    def delete_url(self, url):
        """Delete this root's local copies of ``url`` and their provenance records.

        Removes paths fetched by this ``Cache`` instance (including explicit
        filenames) plus the URL-derived raw and decompressed paths under this
        root that may have been created by other instances. Missing files are
        ignored; symlinks are unlinked without deleting their targets. Explicit
        filenames used by earlier instances are not persisted and cannot be
        rediscovered by URL alone. Deletion is not transactional.

        Parameters
        ----------
        url : str
            Source URL whose cached files should be removed.

        Raises
        ------
        ValueError
            If ``url`` is empty or invalid.
        OSError
            For permission or directory-removal errors.
        """
        keys = [key for key in self._local_paths if key[0] == url]
        paths = {self._local_paths[key] for key in keys}
        # Include inferred paths created by another Cache instance or fetch_file.
        paths.update(self.local_path(url, decompress=value) for value in (False, True))
        for path in paths:
            try:
                remove(path)
            except FileNotFoundError:
                pass
            provenance.remove(path)
        for key in keys:
            del self._local_paths[key]

    def delete_all(self):
        """Recursively delete all contents of this cache root while preserving the root.

        Removes every entry inside ``cache_directory_path`` (including files
        not created by DataCache), so use only a dedicated directory. The root
        directory and its permissions are preserved. A root symlink is followed
        to the selected directory, while symlinks inside the cache are unlinked
        without clearing their external targets. Remembered download paths on
        this instance are cleared after success. Deletion is not transactional.

        Raises
        ------
        FileNotFoundError
            If the cache root does not exist.
        NotADirectoryError
            If the cache root is not a directory.
        OSError
            For access or removal failures.
        """
        # Validate before resolving: missing/.. must not select an existing
        # parent. Resolve once so deleting a child in a root like child/..
        # cannot invalidate the paths used for the remaining entries.
        stat(self.cache_directory_path)
        directory = realpath(self.cache_directory_path)
        with scandir(directory) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    rmtree(entry.path)
                else:
                    remove(entry.path)
        self._local_paths.clear()

    def exists(self, url=None, filename=None, decompress=False):
        """Check whether the cache path for these arguments exists on disk.

        Performs no download, repair, write, or directory creation. Directories
        count as present and dangling symlinks as absent.

        Parameters
        ----------
        url : str or None, optional
            Source URL.
        filename : str or None, optional
            Explicit cache key filename.
        decompress : bool, optional
            Whether to check the decompressed cache key. Default is ``False``.

        Returns
        -------
        bool
            ``True`` if the path exists, ``False`` if absent.

        Raises
        ------
        ValueError
            If neither ``url`` nor ``filename`` is supplied.
        OSError
            For filesystem errors other than ``FileNotFoundError`` (such as
            ``PermissionError``).
        """
        return path_exists(self.local_path(url, filename, decompress))

    def inspect(self, url=None, filename=None, decompress=False, *,
                expected_sha256=None, expected_size=None):
        """Call ``inspect_file`` on the path selected by ``url``, ``filename``, and ``decompress``.

        Performs no network access, writes, or repair.

        Parameters
        ----------
        url : str or None, optional
            Source URL.
        filename : str or None, optional
            Explicit cache key filename.
        decompress : bool, optional
            Whether to inspect the decompressed cache key. Default is ``False``.
        expected_sha256 : str or None, optional
            Trusted 64-character hexadecimal SHA-256 digest.
        expected_size : int or None, optional
            Expected non-negative byte size.

        Returns
        -------
        FileInspection
            Inspection status, verification flag, and optional provenance.

        Raises
        ------
        ValueError
            For invalid naming arguments or invalid integrity expectations.
        """
        return inspect_file(
            self.local_path(url, filename, decompress),
            expected_sha256=expected_sha256, expected_size=expected_size)

    def plan_fetch(
            self,
            url,
            filename=None,
            decompress=False,
            *,
            expected_sha256=None,
            expected_size=None,
            probe_remote_size=False,
            timeout=30,
            headers=None,
            force=False,
            offline=None):
        """Call ``plan_fetch`` on the cache path selected by ``url``, ``filename``, and ``decompress``.

        Performs no writes, directory creation, locks, or repair. Network
        access occurs only when ``probe_remote_size=True`` and
        ``expected_size`` is omitted for a file that would be downloaded or
        replaced.

        Parameters
        ----------
        url : str
            Source URL that would be fetched.
        filename : str or None, optional
            Explicit cache key filename.
        decompress : bool, optional
            Whether to target the decompressed cache key. Default is ``False``.
        expected_sha256 : str or None, optional
            Trusted 64-character hexadecimal SHA-256 digest.
        expected_size : int or None, optional
            Expected non-negative byte size.
        probe_remote_size : bool, optional
            If ``True`` and ``expected_size`` is omitted, probe the remote
            ``Content-Length`` (or local ``file://`` size). Default is ``False``.
        timeout : float or tuple[float, float] or None, optional
            Timeout in seconds for optional remote ``HEAD`` probing. Default is ``30``.
        headers : Mapping[str, str] or None, optional
            Optional extra HTTP headers for remote ``HEAD`` probing.
        force : bool, optional
            If ``True``, plan ``"replace"`` instead of ``"reuse"`` when a valid
            file is already cached. Default is ``False``.
        offline : bool or None, optional
            Override ``DATACACHE_OFFLINE`` / ``OPENVAX_OFFLINE``.

        Returns
        -------
        FetchPlan
            Frozen preflight plan for the selected cache path.

        Raises
        ------
        ValueError
            For invalid naming arguments, expectations, or options.
        OfflineError
            If ``probe_remote_size=True`` requires a remote probe while offline.
        """
        return plan_fetch(
            url,
            self.local_path(url, filename, decompress),
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            probe_remote_size=probe_remote_size,
            timeout=timeout,
            headers=headers,
            force=force,
            offline=offline,
        )

    def make_readable(self, url=None, filename=None, decompress=False, *, group=True, others=False):
        """Explicitly add POSIX read access to one cached regular file.

        Explicit POSIX maintenance only: does not download, replace, or recurse
        into directories. Intended for a file owner sharing an older private
        cache file.

        Parameters
        ----------
        url : str or None, optional
            Source URL.
        filename : str or None, optional
            Explicit cache key filename.
        decompress : bool, optional
            Whether to target the decompressed cache key. Default is ``False``.
        group : bool, optional
            Grant group read permission. Default is ``True``.
        others : bool, optional
            Grant world read permission. Default is ``False``.

        Returns
        -------
        str
            Path string of the updated file.
        """
        return make_file_readable(
            self.local_path(url, filename, decompress), group=group, others=others)

    def fetch(
            self,
            url,
            filename=None,
            decompress=False,
            force=False,
            timeout=None,
            use_wget_if_available=None,
            *,
            chunk_size=download.DEFAULT_CHUNK_SIZE,
            progress_callback=None,
            expected_sha256=None,
            expected_size=None,
            max_bytes=None,
            max_retries=download.DEFAULT_MAX_RETRIES,
            retry_backoff=download.DEFAULT_RETRY_BACKOFF,
            retry_max_delay=download.DEFAULT_RETRY_MAX_DELAY,
            show_progress=False,
            record_provenance=False,
            allow_empty=False,
            resume=False,
            raw=False,
            http_representation="decoded",
            expire_after=None,
            return_stale_on_error=False,
            validator=None,
            offline=None):
        """Instance form of ``fetch_file`` using this ``Cache``'s root directory.

        Every parameter after ``url`` has the same meaning and default as in
        ``fetch_file``. Explicit filenames are remembered in memory on this
        instance for later ``delete_url`` calls (not written to disk).

        Returns
        -------
        str
            Local path string of the cached or downloaded file.

        Raises
        ------
        ValueError, FileValidationError, OfflineError, OSError
            Same errors as documented for ``fetch_file``.
        """
        key = (url, filename, decompress)
        path = download.fetch_file(
            url,
            filename=filename,
            decompress=decompress,
            subdir=self.subdir,
            cache_root=self.cache_directory_path,
            force=force,
            timeout=timeout,
            use_wget_if_available=use_wget_if_available,
            chunk_size=chunk_size,
            progress_callback=progress_callback,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            max_bytes=max_bytes,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            retry_max_delay=retry_max_delay,
            show_progress=show_progress,
            record_provenance=record_provenance,
            allow_empty=allow_empty,
            resume=resume,
            raw=raw,
            http_representation=http_representation,
            expire_after=expire_after,
            return_stale_on_error=return_stale_on_error,
            validator=validator,
            offline=offline)

        self._local_paths[key] = path
        return path

    def local_filename(
            self,
            url=None,
            filename=None,
            decompress=False):
        """Instance form of ``build_local_filename`` without the root directory.

        Does not access the filesystem.

        Parameters
        ----------
        url : str or None, optional
            Source URL (equivalent to ``download_url``).
        filename : str or None, optional
            Explicit cache key filename.
        decompress : bool, optional
            Whether to return the decompressed cache key. Default is ``False``.

        Returns
        -------
        str
            Normalized cache filename within this cache.

        Raises
        ------
        ValueError
            If neither ``url`` nor ``filename`` is supplied.
        """
        return common.build_local_filename(url, filename, decompress)

    def local_path(self, url=None, filename=None, decompress=False, download=False):
        """Instance form of ``expected_path`` under this cache's root.

        With ``download=False`` (default), only computes the path without
        touching the filesystem. With ``download=True``, delegates to
        ``Cache.fetch`` with its defaults; call ``Cache.fetch`` directly when
        you need timeouts, integrity expectations, or other download options.

        Parameters
        ----------
        url : str or None, optional
            Source URL.
        filename : str or None, optional
            Explicit cache key filename.
        decompress : bool, optional
            Whether to target the decompressed cache key. Default is ``False``.
        download : bool, optional
            If ``True``, fetch the file first using ``Cache.fetch`` defaults.
            Default is ``False``.

        Returns
        -------
        str
            Full path string under ``cache_directory_path``.

        Raises
        ------
        ValueError
            For invalid naming arguments, plus ``Cache.fetch`` errors when
            ``download=True``.
        """
        if download:
            return self.fetch(url=url, filename=filename, decompress=decompress)
        else:
            return expected_path(
                url, filename, decompress, cache_root=self.cache_directory_path)
