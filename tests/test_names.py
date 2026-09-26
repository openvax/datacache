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


def test_long_url_can_be_used_as_cache_key():
    url = "file:///" + "long-directory/" * 20 + "sequences.fa.gz"
    filename = build_local_filename(download_url=url)
    assert filename == build_local_filename(download_url=url)
    assert filename.endswith("sequences.fa.gz")
    assert len(filename) < len(url)


def test_names_over_255_utf8_bytes_are_shortened_keeping_their_end():
    # 104 characters but 304 bytes: too long for ext4, which counts bytes.
    long_name = "測" * 100 + ".csv"
    key = build_local_filename(filename=long_name)
    assert len(key.encode("utf-8")) <= 255
    assert key.endswith("測.csv")
    assert key == build_local_filename(filename=long_name)
    assert key != build_local_filename(filename="數" + long_name[1:])


def test_names_within_255_utf8_bytes_are_unchanged():
    within = "é" * 125 + ".csv"  # 254 bytes
    assert build_local_filename(filename=within) == within
    at_limit = "é" * 125 + "x.csv"  # 255 bytes
    assert build_local_filename(filename=at_limit) == at_limit
    over = "é" * 126 + ".csv"  # 256 bytes
    assert build_local_filename(filename=over) != over


def test_shortened_decompressed_names_keep_their_extension():
    key = build_local_filename(filename="測" * 100 + ".csv.gz", decompress=True)
    assert key.endswith("測.csv") and len(key.encode("utf-8")) <= 255
