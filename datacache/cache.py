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

from os.path import isabs, join, realpath
from os import fspath, getcwd, remove, scandir, stat
from shutil import rmtree

from . import common
from . import download
from .download import expected_path
from .database_helpers import db_from_dataframe
from .inspection import inspect_file, path_exists
from .permissions import make_file_readable


class Cache(object):
    def __init__(self, subdir="datacache", *, cache_root=None):
        """Select a cache directory without creating it.

        cache_root overrides the platform cache location selected by subdir.
        """
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
        """
        Delete local files downloaded from given URL
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
        for key in keys:
            del self._local_paths[key]

    def delete_all(self):
        """Clear cached contents while preserving the root and its permissions."""
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
        """
        Check presence without writes or network. Permission errors propagate.
        """
        return path_exists(self.local_path(url, filename, decompress))

    def inspect(self, url=None, filename=None, decompress=False, *,
                expected_sha256=None, expected_size=None):
        """Report availability/integrity without writes, network, or repair."""
        return inspect_file(
            self.local_path(url, filename, decompress),
            expected_sha256=expected_sha256, expected_size=expected_size)

    def make_readable(self, url=None, filename=None, decompress=False, *, group=True, others=False):
        """Explicitly add read access to one cached regular file on POSIX.

        Defaults to group read access; does not download, replace, or recurse.
        The file owner must invoke this to share an older private cache file.
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
            max_retries=download.DEFAULT_MAX_RETRIES,
            retry_backoff=download.DEFAULT_RETRY_BACKOFF,
            retry_max_delay=download.DEFAULT_RETRY_MAX_DELAY,
            show_progress=False):
        """
        Return the local path to the downloaded copy of a given URL.
        Don't download the file again if it's already present,
        unless `force` is True.

        Retry options have the same meanings as in fetch_file; max_retries=0
        disables automatic retries of transient HTTP failures.
        show_progress=True enables optional tqdm displays; callbacks remain
        supported independently. Existing cache hits are quiet.

        `use_wget_if_available` is deprecated and ignored (datacache always uses
        its streaming Python downloader now); passing it emits a warning.
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
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            retry_max_delay=retry_max_delay,
            show_progress=show_progress)

        self._local_paths[key] = path
        return path

    def local_filename(
            self,
            url=None,
            filename=None,
            decompress=False):
        """
        What local filename will we use within the cache directory
        for the given URL/filename/decompress options.
        """
        return common.build_local_filename(url, filename, decompress)

    def local_path(self, url=None, filename=None, decompress=False, download=False):
        """
        What will the full local path be if we download the given file?
        """
        if download:
            return self.fetch(url=url, filename=filename, decompress=decompress)
        else:
            return expected_path(
                url, filename, decompress, cache_root=self.cache_directory_path)

    def db_from_dataframe(
            self,
            db_filename,
            table_name,
            df,
            key_column_name=None,
            *,
            overwrite=False,
            version=1,
            show_progress=False):
        """Build or reuse a database in this cache and return its connection.

        The caller must close the connection. Change version or set overwrite
        to rebuild; failed rebuilds preserve the previous database.
        """
        db_path = join(self.cache_directory_path, db_filename)
        if not isabs(db_path):
            # Prefix the cwd without abspath's lexical removal of symlink/..
            # components; the filesystem must determine their destination.
            db_path = join(getcwd(), db_path)
        return db_from_dataframe(
            db_filename=db_path,
            table_name=table_name,
            df=df,
            primary_key=key_column_name,
            subdir=self.subdir,
            overwrite=overwrite,
            version=version,
            show_progress=show_progress)
