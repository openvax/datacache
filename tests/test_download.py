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
import logging
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


def test_fetch_decompress_zip_matches_member_inside_a_folder(isolated_cache):
    # Archives often wrap their files in a top-level folder. The member named
    # like the output must win over a larger sibling rather than having the
    # largest member's contents silently installed under the requested name.
    archive = isolated_cache / "release.zip"
    with zipfile.ZipFile(str(archive), "w") as z:
        z.writestr("release/wanted.csv", "the wanted member\n")
        z.writestr("release/other.csv", "a much larger member\n" * 100)
    path = fetch_file("file://" + str(archive), filename="wanted.csv", decompress=True)
    with open(path) as f:
        assert f.read() == "the wanted member\n"


def test_fetch_decompress_zip_prefers_the_exact_member_path(isolated_cache):
    archive = isolated_cache / "release.zip"
    with zipfile.ZipFile(str(archive), "w") as z:
        z.writestr("wanted.csv", "top level\n")
        z.writestr("nested/wanted.csv", "a larger nested copy\n" * 100)
    path = fetch_file("file://" + str(archive), filename="wanted.csv", decompress=True)
    with open(path) as f:
        assert f.read() == "top level\n"


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


def test_fetch_decompress_zip_matches_member_names_ignoring_case(isolated_cache, caplog):
    archive = isolated_cache / "cased.zip"
    with zipfile.ZipFile(str(archive), "w") as z:
        z.writestr("Data.CSV", "the wanted member\n")
        z.writestr("notes.txt", "a much larger member\n" * 100)
    with caplog.at_level(logging.WARNING, logger="datacache.download"):
        path = fetch_file("file://" + str(archive), filename="data.csv", decompress=True)
    with open(path) as f:
        assert f.read() == "the wanted member\n"
    assert not caplog.records


def test_fetch_decompress_zip_prefers_the_copy_nearest_the_root(isolated_cache):
    archive = isolated_cache / "nested.zip"
    with zipfile.ZipFile(str(archive), "w") as z:
        z.writestr("release/data.csv", "current\n")
        z.writestr("release/archive/2019/data.csv", "older and larger\n" * 100)
    path = fetch_file("file://" + str(archive), filename="data.csv", decompress=True)
    with open(path) as f:
        assert f.read() == "current\n"


def test_fetch_decompress_zip_warns_only_when_guessing_among_members(isolated_cache, caplog):
    several = isolated_cache / "several.zip"
    with zipfile.ZipFile(str(several), "w") as z:
        z.writestr("readme.txt", "short\n")
        z.writestr("table.tsv", "the largest member\n" * 10)
    single = isolated_cache / "single.zip"
    with zipfile.ZipFile(str(single), "w") as z:
        z.writestr("anything.bin", "the only member\n")
    with caplog.at_level(logging.WARNING, logger="datacache.download"):
        guessed = fetch_file("file://" + str(several), filename="guessed.csv", decompress=True)
        assert "table.tsv" in caplog.text
        caplog.clear()
        only = fetch_file("file://" + str(single), filename="only.csv", decompress=True)
        assert not caplog.records
    with open(guessed) as f:
        assert f.read().startswith("the largest member")
    with open(only) as f:
        assert f.read() == "the only member\n"


def test_fetch_decompress_zip_is_quiet_for_inferred_names(isolated_cache, caplog):
    # An inferred cache key can never match a member name, so installing the
    # largest member is the expected behavior, not a guess worth reporting.
    archive = isolated_cache / "inferred.zip"
    with zipfile.ZipFile(str(archive), "w") as z:
        z.writestr("data.csv", "the largest member\n" * 10)
        z.writestr("README", "short\n")
    with caplog.at_level(logging.WARNING, logger="datacache.download"):
        path = fetch_file("file://" + str(archive), decompress=True)
    assert not caplog.records
    with open(path) as f:
        assert f.read().startswith("the largest member")
