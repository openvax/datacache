import common
import download

class Cache(object):
    def __init__(self, subdir="datacache"):
        assert subdir
        self.subdir = subdir
        self.directory_path = common.get_data_dir(subdir)

    def delete_all(self):
        common.clear_cache(self.directory_path)
        common.ensure_dir(self.directory_path)

    def local_filename(self, url=None, filename=None, decompress=False):
        """
        What local filename will we use within the cache directory
        for the given URL/filename/decompress options
        """
        return common.build_local_filename(url, filename, decompress)

    def path(self, url, filename=None, decompress=False):
        """
        What will the full local path be if we download the given file?
        """
        filename =  self.local_filename(url, filename, decompress)
        return join(self.directory_path, filename)

    def fetch(self, url, filename=None, decompress=False):
        return download.fetch_file(
            url,
            filename=filename,
            decompress=decompress,
            subdir=self.subdir)
