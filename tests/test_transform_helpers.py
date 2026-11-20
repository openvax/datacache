"""Offline regression tests for composed download/transform helpers."""

import gzip
import inspect
import logging
import os
from pathlib import Path
import re
import sqlite3
import stat

import pytest

import pandas as pd

from datacache import common, fetch_and_transform


@pytest.fixture
def source(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "get_data_dir", lambda subdir=None: str(tmp_path / (subdir or "default")))
    source = tmp_path / "source.txt"
    source.write_text("complete data")
    return source


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
@pytest.mark.parametrize("existing", [False, True])
def test_failed_transform_preserves_cache_and_cleans_up(source, tmp_path, failure, existing):
    destination = tmp_path / "project" / "result.txt"
    if existing:
        destination.parent.mkdir()
        destination.write_text("old contents")
        destination.chmod(0o640)

    def transformer(source_path, output_path):
        assert Path(source_path).parent == destination.parent
        assert Path(output_path).name == "result.txt"
        assert not Path(output_path).exists()
        assert stat.S_IMODE(Path(output_path).parent.stat().st_mode) & 0o077 == 0
        Path(output_path).write_text("partial")
        raise failure("injected failure")

    with pytest.raises(failure):
        fetch_and_transform("result.txt", transformer, lambda p: Path(p).read_text(),
                            "download.txt", source.as_uri(), subdir="project", force=existing,
                            decompress=False)
    if existing:
        assert destination.read_text() == "old contents"
        assert stat.S_IMODE(destination.stat().st_mode) == 0o640
    else:
        assert not destination.exists()
    assert not list(destination.parent.glob(".datacache-*"))


def test_transform_success_reuse_and_force(source, tmp_path):
    root = tmp_path / "cache"
    calls = []

    def transform(source_path, output_path):
        calls.append(source_path)
        Path(output_path).write_text(Path(source_path).read_text().upper())
        return Path(output_path)

    options = dict(transformed_filename="result.txt", transformer=transform,
                   loader=lambda p: Path(p), source_filename="input.txt",
                   source_url=source.as_uri(), cache_root=root)
    result = fetch_and_transform(**options)
    assert result == root / "result.txt"
    assert result.read_text() == "COMPLETE DATA"
    source.unlink()
    assert fetch_and_transform(**options) == result
    assert len(calls) == 1
    assert fetch_and_transform(**options, force=True) == result
    assert len(calls) == 2
    assert set(root.iterdir()) == {root / "input.txt", result}


def test_noop_transform_does_not_cache_empty_placeholder(source, tmp_path):
    with pytest.raises(RuntimeError, match="did not create"):
        fetch_and_transform("result.txt", lambda *args: None, lambda p: p,
                            "input.txt", source.as_uri(), cache_root=tmp_path / "cache")
    assert not (tmp_path / "cache" / "result.txt").exists()
    assert not list((tmp_path / "cache").glob(".datacache-*"))


@pytest.fixture
def gzip_source(source):
    """A gzip source whose transformer reports the bytes it was handed."""
    compressed = source.with_name("source.txt.gz")
    compressed.write_bytes(gzip.compress(b"complete data"))

    def transformer(source_path, output_path):
        fetch.builds += 1
        received = Path(source_path).read_bytes()
        Path(output_path).write_bytes(received)
        return received

    def fetch(source_filename="source.txt.gz", **arguments):
        return fetch_and_transform(
            "result.bin", transformer, lambda p: Path(p).read_bytes(),
            source_filename, compressed.as_uri(), **arguments)

    fetch.builds = 0
    fetch.compressed = compressed.read_bytes()
    return fetch


@pytest.mark.parametrize("location", ["subdir", "cache_root", "default"])
@pytest.mark.parametrize("decompress", [False, True])
@pytest.mark.parametrize("through_options", [False, True])
def test_explicit_decompress_does_not_depend_on_cache_location(
        gzip_source, tmp_path, recwarn, location, decompress, through_options):
    arguments = {"subdir": {"subdir": "project"}, "cache_root": {"cache_root": tmp_path / "cache"},
                 "default": {}}[location]
    if through_options:
        arguments["download_options"] = {"decompress": decompress}
    else:
        arguments["decompress"] = decompress
    assert gzip_source(**arguments) == (b"complete data" if decompress else gzip_source.compressed)
    assert not [w for w in recwarn if issubclass(w.category, DeprecationWarning)]


@pytest.mark.parametrize("location", ["cache_root", "default"])
def test_unspecified_decompress_defaults_to_raw_source_without_subdir(
        gzip_source, tmp_path, recwarn, location):
    arguments = {"cache_root": tmp_path / "cache"} if location == "cache_root" else {}
    assert gzip_source(**arguments) == gzip_source.compressed
    assert not [w for w in recwarn if issubclass(w.category, DeprecationWarning)]


def test_unspecified_decompress_with_subdir_is_deprecated_but_unchanged(gzip_source):
    with pytest.warns(DeprecationWarning, match="pass decompress=True"):
        assert gzip_source(subdir="project") == b"complete data"
    assert gzip_source.builds == 1
    # Cache hits warn too, so a warm cache does not hide the pending change.
    with pytest.warns(DeprecationWarning, match="subdir='project'"):
        assert gzip_source(subdir="project") == b"complete data"
    assert gzip_source.builds == 1
    with pytest.warns(DeprecationWarning, match="subdir='project'"):
        assert gzip_source(subdir="project", force=True) == b"complete data"
    assert gzip_source.builds == 2


def test_deprecation_warning_points_at_the_calling_line(source):
    with pytest.warns(DeprecationWarning) as caught:
        expected_line = inspect.currentframe().f_lineno + 1
        fetch_and_transform("result.txt", lambda s, o: Path(o).write_text("x"), lambda p: p,
                            "input.txt", source.as_uri(), subdir="project")
    assert (caught[0].filename, caught[0].lineno) == (__file__, expected_line)


@pytest.mark.parametrize("unset", [None, False, 0])
def test_falsy_download_option_still_means_no_decompression(gzip_source, recwarn, unset):
    # Released callers could disable the subdir rule with any falsy value.
    assert gzip_source(subdir="project",
                       download_options={"decompress": unset}) == gzip_source.compressed
    assert gzip_source(subdir="project", force=True, decompress=False,
                       download_options={"decompress": unset}) == gzip_source.compressed
    assert not [w for w in recwarn if issubclass(w.category, DeprecationWarning)]


def test_source_name_without_compression_suffix_still_implies_decompression(gzip_source, tmp_path):
    # fetch_file's naming rule is unchanged; raw=True is the unconditional switch.
    root = tmp_path / "cache"
    assert gzip_source("source.txt", cache_root=root, decompress=False) == b"complete data"
    assert gzip_source("raw.txt", cache_root=root, decompress=False, force=True,
                       download_options={"raw": True}) == gzip_source.compressed


@pytest.mark.parametrize("decompress", [False, True])
def test_conflicting_decompress_settings_are_rejected(gzip_source, tmp_path, decompress):
    with pytest.raises(ValueError, match="conflicts with download_options"):
        gzip_source(cache_root=tmp_path / "cache", decompress=decompress,
                    download_options={"decompress": not decompress})
    assert not (tmp_path / "cache").exists()
    assert gzip_source.builds == 0
    assert gzip_source(cache_root=tmp_path / "cache", decompress=decompress,
                       download_options={"decompress": decompress}) == (
        b"complete data" if decompress else gzip_source.compressed)


