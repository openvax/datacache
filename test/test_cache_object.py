from os.path import exists
from mock import patch

from datacache import Cache


def test_cache_object_path():
    cache = Cache("datacache_test")
    assert cache.cache_directory_path.endswith("datacache_test"), \
        cache.cache_directory_path

def test_cache_object_local_filename():
    filename = Cache("datacache_test").local_filename(filename="test")
    assert filename.endswith("test")

def test_cache_fetch_google():
    cache = Cache("datacache_test")
    path = cache.fetch("http://www.google.com", filename="google")
    assert path.endswith("google")
    assert exists(path)
    assert path == cache.local_path("http://www.google.com", filename="google")

@patch('datacache.cache.download._download')
def test_cache_fetch_force(mock_download):
    cache = Cache("datacache_test")
    cache.fetch("http://www.google.com", filename="google", force=True)
    cache.fetch("http://www.google.com", filename="google", force=True)
    assert len(mock_download.call_args_list) == 2, \
        "Expected two separate calls to _download, given force=True"
