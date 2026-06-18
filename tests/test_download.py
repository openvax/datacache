# Copyright (c) 2014. Mount Sinai School of Medicine
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

"""
Tests for ``fetch_file`` download + decompression.

These serve a local ``.gz`` over a ``file://`` URL and redirect the cache to an
isolated tmp dir, so they don't depend on a live FTP server or on cross-run
cached state -- the two sources of flakiness in the old live-Ensembl test (#42).
"""

import gzip
import os
import zipfile

import pytest

from datacache import common, fetch_file


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    """Point datacache's cache dir at an isolated tmp dir (so the test neither
    depends on nor pollutes the user's real cache). Fixtures live in ``tmp_path``;
    the cache lives in ``tmp_path/cache``."""
    cache = tmp_path / "cache"
    monkeypatch.setattr(common, "get_data_dir", lambda subdir=None, envkey=None: str(cache))
    return tmp_path


def _write_gz(path, text):
    with gzip.open(str(path), "wb") as f:
        f.write(text.encode())
    return "file://" + str(path)


def test_fetch_decompress(isolated_cache):
    url = _write_gz(isolated_cache / "seq.fa.gz", "ACGTACGT TCAATTTCGTGCCAG\n")
    path = fetch_file(url, filename="seq.fa.gz", decompress=True)
    assert path.endswith("seq.fa")
    with open(path) as f:
        assert "TCAATTTCGTGCCAG" in f.read()


def test_fetch_decompress_caches_then_force(isolated_cache):
    # Deterministic cache behaviour. The old test looped over
    # use_wget_if_available/timeout but the file was downloaded once and reused,
    # so the result depended on iteration order (#42). Here we assert it
    # explicitly: without force the cached copy is reused even if the source
    # changes; with force it's re-fetched.
    src = isolated_cache / "data.fa.gz"
    url = _write_gz(src, "FIRST\n")
    p1 = fetch_file(url, filename="data.fa.gz", decompress=True)
    with open(p1) as f:
        assert f.read() == "FIRST\n"

    _write_gz(src, "SECOND\n")  # change source; cached copy should win
    p2 = fetch_file(url, filename="data.fa.gz", decompress=True)
    assert p2 == p1
    with open(p2) as f:
        assert f.read() == "FIRST\n"

    p3 = fetch_file(url, filename="data.fa.gz", decompress=True, force=True)
    with open(p3) as f:
        assert f.read() == "SECOND\n"


def test_fetch_decompress_zip_picks_named_member(isolated_cache):
    # Multi-member zip: the member matching the local filename is chosen and
    # streamed into the cache -- never extracted into the working directory
    # (the cwd-pollution / path-traversal footgun of the old ZipFile.extract).
    archive = isolated_cache / "table.tsv.zip"
    with zipfile.ZipFile(str(archive), "w") as z:
        z.writestr("table.tsv", "the wanted member\n")
        z.writestr("readme.txt", "ignore me\n")
    url = "file://" + str(archive)

    path = fetch_file(url, filename="table.tsv.zip", decompress=True)
    assert path.endswith("table.tsv")
    with open(path) as f:
        assert f.read() == "the wanted member\n"
    # Nothing leaked into the current working directory.
    assert not os.path.exists("table.tsv")
    assert not os.path.exists("readme.txt")


def test_corrupt_gz_leaves_no_partial_cache(isolated_cache):
    # A truncated gzip decompresses partway then fails its trailing CRC check.
    # fetch_file must surface the error and leave NO file at the destination,
    # so the next call re-fetches instead of serving a silent partial.
    good = gzip.compress(b"the full payload that must not be half-cached\n" * 50)
    corrupt = good[:-8]  # drop the CRC32 + ISIZE trailer
    archive = isolated_cache / "trunc.fa.gz"
    archive.write_bytes(corrupt)
    url = "file://" + str(archive)

    with pytest.raises(Exception):
        fetch_file(url, filename="trunc.fa.gz", decompress=True)

    dest = isolated_cache / "cache" / "trunc.fa"
    assert not dest.exists(), "corrupt decompress must not leave a cached partial"


def test_fetch_subdirs(tmp_path, monkeypatch):
    # Different subdirs resolve to different cache locations. Hermetic: route
    # each subdir to its own tmp dir.
    src = tmp_path / "seq.fa.gz"
    url = _write_gz(src, "ACGT\n")
    dirs = {"datacache": tmp_path / "a", "datacache_test": tmp_path / "b"}
    monkeypatch.setattr(
        common,
        "get_data_dir",
        lambda subdir=None, envkey=None: str(dirs.get(subdir, tmp_path / "default")),
    )

    path = fetch_file(url, filename="seq.fa.gz", decompress=True, subdir="datacache")
    assert path.endswith("seq.fa")
    other_path = fetch_file(url, filename="seq.fa.gz", decompress=True, subdir="datacache_test")
    assert other_path.endswith("seq.fa")
    assert other_path != path


def test_use_wget_if_available_is_deprecated(isolated_cache):
    # The legacy wget path was removed; passing the argument is accepted for
    # backwards compatibility but warns and is otherwise ignored.
    url = _write_gz(isolated_cache / "w.fa.gz", "ACGT\n")
    with pytest.warns(DeprecationWarning, match="use_wget_if_available is deprecated"):
        path = fetch_file(url, filename="w.fa.gz", decompress=True, use_wget_if_available=True)
    assert path.endswith("w.fa")
