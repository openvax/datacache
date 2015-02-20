from os.path import split, exists, join
from os import remove

from . import common
from . import db
from . import download


class Cache(object):
    def __init__(self, subdir="datacache"):
        assert subdir
        self.subdir = subdir
        self.cache_directory_path = common.get_data_dir(subdir)

        # dictionary mapping from URL to local paths
        self.local_paths = {}

    def delete_key(self, url):
        if url in self.local_paths:
            path = self.local_paths[url]
            del self.local_paths[url]
            if exists(path):
                remove(path)

    def delete_all(self):
        self.local_paths.clear()
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

    def fetch(self, url, filename=None, decompress=False):
        """
        Return the local path to the downloaded copy of a given URL.
        Don't download the file again if it's already present.
        """
        if url in self.local_paths:
            path = self.local_paths[url]
            if exists(path):
                return path
            else:
                del self.local_paths[url]
        path = download.fetch_file(
            url,
            filename=filename,
            decompress=decompress,
            subdir=self.subdir)
        self.local_paths[url] = path
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
            filename =  self.local_filename(url, filename, decompress)
            return join(self.cache_directory_path, filename)

    def db_from_dataframe(
            self,
            db_filename,
            table_name,
            df,
            key_column_name = None):
        return db_from_dataframe(
            db_filename = db_filename,
            table_name = table_name,
            df = df,
            key_column_name = key_column_name,
            subdir = self.subdir)
