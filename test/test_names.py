from datacache import build_local_filename

def test_url_without_filename():
    filename = build_local_filename(download_url="http://www.google.com/")
    assert filename
    assert "google" in filename

def test_multiple_domains_same_file():
    filename_google = build_local_filename(
        download_url="http://www.google.com/index.html")
    filename_yahoo = build_local_filename(
        download_url="http://www.yahoo.com/index.html")

    assert "index" in filename_google
    assert "index" in filename_yahoo
    assert filename_yahoo != filename_google