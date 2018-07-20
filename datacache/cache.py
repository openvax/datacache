# Copyright (c) 2015-2018. Mount Sinai School of Medicine
#
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

from __future__ import print_function, division, absolute_import

from os.path import exists, join
from os import remove

from . import common
from . import download
from .database_helpers import db_from_dataframe


class Cache(object):
    def __init__(self, subdir="datacache"):
        assert subdir
        self.subdir = subdir
        self.cache_directory_path = common.get_data_dir(subdir)

        # dictionary mapping from (URL, decompress) pair to local paths
        # TODO: handle decompression separately from download,
        # so we can use copies of compressed files we've already downloaded
        self._local_paths = {}

    def delete_url(self, url):
        """
        Delete local files downloaded from given URL
        """
        # file may exist locally in compressed and decompressed states
        # delete both
        for decompress in [False, True]:
            key = (url, decompress)
            if key in self._local_paths:
                path = self._local_paths[key]
                remove(path)
                del self._local_paths[key]

            # possible that file was downloaded via the download module without
            # using the Cache object, this wouldn't end up in the local_paths
            # but should still be deleted
            path = self.local_path(
                url, decompress=decompress, download=False)

            if exists(path):
                remove(path)

    def delete_all(self):
        self._local_paths.clear()
        common.clear_cache(self.cache_directory_path)
        common.ensure_dir(self.cache_directory_path)

    def exists(self, url, filename=None, decompress=False):
        """
        Return True if a local file corresponding to these arguments
        exists.
        """
        return download.file_exists(
            url,
            filename=filename,
            decompress=decompress,
            subdir=self.subdir)

    def fetch(
            self,
            url,
            filename=None,
            decompress=False,
            force=False,
            timeout=None,
            use_wget_if_available=True):
        """
        Return the local path to the downloaded copy of a given URL.
        Don't download the file again if it's already present,
        unless `force` is True.
        """
        key = (url, decompress)
        if not force and key in self._local_paths:
            path = self._local_paths[key]
            if exists(path):
                return path
            else:
                del self._local_paths[key]
        path = download.fetch_file(
            url,
            filename=filename,
            decompress=decompress,
            subdir=self.subdir,
            force=force,
            timeout=timeout,
            use_wget_if_available=use_wget_if_available)

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

    def local_path(self, url, filename=None, decompress=False, download=False):
        """
        What will the full local path be if we download the given file?
        """
        if download:
            return self.fetch(url=url, filename=filename, decompress=decompress)
        else:
            filename = self.local_filename(url, filename, decompress)
            return join(self.cache_directory_path, filename)

    def db_from_dataframe(
            self,
            db_filename,
            table_name,
            df,
            key_column_name=None):
        return db_from_dataframe(
            db_filename=db_filename,
            table_name=table_name,
            df=df,
            primary_key=key_column_name,
            subdir=self.subdir)
