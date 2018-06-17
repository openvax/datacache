from os import remove
from os.path import exists
from mock import patch
from nose.tools import eq_

from datacache import Cache

CACHE_DIR = "datacache_test"
TEST_URL = "http://www.google.com"
TEST_FILENAME = "google"

def test_cache_object_path():
    cache = Cache(CACHE_DIR)
    assert cache.cache_directory_path.endswith(CACHE_DIR), \
        "Expected directory path to end with %s but got %s" % (
            CACHE_DIR, cache.cache_directory_path)

def test_cache_object_local_filename():
    filename = Cache(CACHE_DIR).local_filename(filename="test")
    assert filename.endswith("test")

def test_cache_fetch_google():
    cache = Cache(CACHE_DIR)
    path = cache.fetch(TEST_URL, filename=TEST_FILENAME)
    assert path.endswith(TEST_FILENAME), \
        "Expected local file to be named %s but got %s" % (
            TEST_FILENAME, path)
    assert exists(path), "File not found: %s" % path
    eq_(path, cache.local_path(TEST_URL, filename=TEST_FILENAME))


@patch('datacache.cache.download._download_and_decompress_if_necessary')
def test_cache_fetch_force(mock_download):
    cache = Cache("datacache_test")
    cache.fetch("http://www.google.com", filename="google", force=True)
    cache.fetch("http://www.google.com", filename="google", force=True)
    assert len(mock_download.call_args_list) == 2, \
        "Expected two separate calls to _download, given force=True"

def test_cache_delete_url():
    cache = Cache(CACHE_DIR)
    path = cache.fetch(TEST_URL, filename=TEST_FILENAME)
    assert exists(path), "Expected %s to exist after download" % path
    cache.delete_url(TEST_URL)
    assert not exists(path), \
        "Expected %s to be deleted after call to delete_url" % path

def test_cache_missing_file():
    """test_cache_missing_file : Files can be deleted from the file system,
    Cache should be aware that these files no longer exist
    """
    cache = Cache(CACHE_DIR)
    path = cache.fetch(TEST_URL, filename=TEST_FILENAME)
    # does the filename exist?
    assert exists(path)
    # does the cache know the URL has been downloaded?
    assert cache.exists(TEST_URL, filename=TEST_FILENAME)
    remove(path)
    assert not cache.exists(TEST_URL, filename=TEST_FILENAME), \
        "Local file for %s has been deleted from the file system" % TEST_URL
