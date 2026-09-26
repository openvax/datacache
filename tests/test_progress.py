"""Progress must be accurate, optional, retry-aware, and exception-safe."""

import builtins
from contextlib import closing, redirect_stderr
import gzip
import hashlib
import io
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pandas as pd
import pytest
import requests
import tqdm.auto

from datacache import Cache, db_from_dataframe, download, fetch_file


@pytest.fixture
def bars(monkeypatch):
    created = []

    class Bar:
        def __init__(self, **options):
            self.options = options
            self.total = options["total"]
            self.n = 0
            self.closed = False
            self.counts = []
            created.append(self)

        def update(self, count):
            assert count >= 0
            self.n += count
            self.counts.append((self.n, self.total))

        def reset(self, total=None):
            self.n = 0
            self.total = total

        def close(self):
            self.closed = True

    monkeypatch.setattr(tqdm.auto, "tqdm", Bar)
    return created


@pytest.mark.parametrize("kind", ["raw", "gz", "zip"])
def test_download_decompression_and_verification_progress(tmp_path, bars, kind):
    data = b"complete data\n" * 100
    source = tmp_path / ("source." + kind)
    if kind == "gz":
        source.write_bytes(gzip.compress(data))
    elif kind == "zip":
        with zipfile.ZipFile(source, "w") as archive:
            archive.writestr("data", data)
    else:
        source.write_bytes(data)
    reports = []
    cache = Cache(cache_root=tmp_path / "cache")
    path = cache.fetch(source.as_uri(), filename="data", show_progress=True, chunk_size=10,
                       expected_sha256=hashlib.sha256(data).hexdigest(),
                       progress_callback=lambda done, total: reports.append((done, total)))
    assert Path(path).read_bytes() == data
    descriptions = [bar.options["desc"] for bar in bars]
    assert descriptions == ["Downloading"] + ([] if kind == "raw" else ["Decompressing"]) + ["Verifying"]
    assert bars[0].n == source.stat().st_size
    assert bars[-1].n == len(data)
    assert all(bar.closed for bar in bars)
    assert reports[-1] == (source.stat().st_size, source.stat().st_size)
    count = len(bars)
    cache.fetch(source.as_uri(), filename="data", show_progress=True)
    assert len(bars) == count


def test_each_retry_gets_fresh_progress_even_with_larger_first_chunk(tmp_path, bars, monkeypatch):
    calls = []

    def stream(url, output, progress_callback, **kwargs):
        calls.append(url)
        if len(calls) == 1:
            output.write(b"part")
            progress_callback(4, 10)
            raise requests.ConnectionError("injected interruption")
        output.write(b"complete")
        progress_callback(8, 8)
        return 8

    monkeypatch.setattr(download, "_stream_to_file", stream)
    destination = tmp_path / "data"
    fetch_file("https://host/file", destination=destination, show_progress=True, retry_backoff=0)
    assert destination.read_bytes() == b"complete"
    assert [bar.counts for bar in bars] == [[(4, 10)], [(8, 8)]]
    assert all(bar.closed for bar in bars)
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("failure", ["callback", "hash", "gzip"])
def test_failure_closes_bars_and_preserves_old_file(tmp_path, bars, failure):
    source = tmp_path / ("source.gz" if failure == "gzip" else "source")
    source.write_bytes(b"new data")
    destination = tmp_path / "data"
    destination.write_bytes(b"old data")
    options = {}
    if failure == "callback":
        def reject(*args):
            raise RuntimeError("callback failed")
        options["progress_callback"] = reject
    elif failure == "hash":
        options["expected_sha256"] = "0" * 64
    with pytest.raises((RuntimeError, ValueError, OSError)):
        fetch_file(source.as_uri(), destination=destination, force=True, show_progress=True, **options)
    assert bars and all(bar.closed for bar in bars)
    assert destination.read_bytes() == b"old data"
    assert not list(tmp_path.glob(".datacache-*"))


def test_empty_download_reports_zero_total(tmp_path, bars):
    source = tmp_path / "source"
    source.touch()
    fetch_file(source.as_uri(), destination=tmp_path / "output", show_progress=True)
    assert bars[0].counts == [(0, 0)]
    assert bars[0].closed


def test_http_content_encoding_uses_unknown_decoded_total(tmp_path, bars, monkeypatch):
    data = b"uncompressed payload" * 100
    response = SimpleNamespace(
        headers={"Content-Length": "20", "Content-Encoding": "gzip"},
        raise_for_status=lambda: None, iter_content=lambda **kw: iter([data]), close=lambda: None)
    monkeypatch.setattr(download.requests, "get", lambda *a, **kw: response)
    fetch_file("https://host/data", destination=tmp_path / "data", show_progress=True)
    assert bars[0].counts == [(len(data), None)]


def test_database_progress_reports_completed_batches(tmp_path, bars):
    with closing(db_from_dataframe(tmp_path / "data.db", "records",
                                   pd.DataFrame({"id": range(2501)}), show_progress=True)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM records").fetchone() == (2501,)
    assert bars[0].options["unit"] == "rows"
    assert bars[0].counts == [(1000, 2501), (2000, 2501), (2501, 2501)]
    assert bars[0].closed


def test_tqdm_is_optional_and_missing_extra_has_actionable_error(tmp_path, monkeypatch):
    original_import = builtins.__import__

    def no_tqdm(name, *args, **kwargs):
        if name.startswith("tqdm"):
            raise ImportError("not installed")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_tqdm)
    source = tmp_path / "source"
    source.write_bytes(b"data")
    fetch_file(source.as_uri(), destination=tmp_path / "quiet")
    with pytest.raises(ImportError, match=r"datacache\[progress\]"):
        fetch_file(source.as_uri(), destination=tmp_path / "progress", show_progress=True)
    assert not (tmp_path / "progress").exists()
    assert not list(tmp_path.glob(".datacache-*"))


def test_real_tqdm_writes_only_to_stderr(tmp_path, capsys):
    source = tmp_path / "source"
    source.write_bytes(b"data")
    captured = io.StringIO()
    with redirect_stderr(captured):
        fetch_file(source.as_uri(), destination=tmp_path / "output", show_progress=True)
    assert "Downloading" in captured.getvalue()
    assert capsys.readouterr().out == ""
