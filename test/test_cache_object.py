from os.path import exists

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

def test_cache_delete_key():
    cache = Cache("datacache_test")
    url = "http://www.google.com"
    path = cache.fetch(url, filename="google")
    assert exists(path)
    cache.delete_key(url)
    assert not exists(path)
    assert url not in cache.local_paths
