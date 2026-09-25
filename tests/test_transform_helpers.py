"""Offline regression tests for composed download/transform helpers."""

from contextlib import closing
import gzip
import hashlib
from pathlib import Path
import stat

import pytest

from datacache import common, fetch_and_transform, fetch_csv_dataframe, fetch_csv_db


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
                            "download.txt", source.as_uri(), subdir="project", force=existing)
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


@pytest.mark.parametrize("explicit_csv", [False, True])
@pytest.mark.parametrize("explicit_db", [False, True])
def test_csv_database_filename_defaults(source, tmp_path, explicit_csv, explicit_db):
    source.write_text("id;value\n1;alpha\n2;beta\n")
    options = dict(csv_filename="records.csv" if explicit_csv else None,
                   db_filename="records.db" if explicit_db else None,
                   subdir="project", sep=";")
    with closing(fetch_csv_db("records", source.as_uri(), **options)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(1, "alpha"), (2, "beta")]
    assert len(list((tmp_path / "project").glob("*.db"))) == 1


def test_dataframe_download_options_do_not_leak_to_pandas(tmp_path):
    source = tmp_path / "records.csv.gz"
    data = b"id;value\n1;hello\n"
    source.write_bytes(gzip.compress(data))
    options = dict(cache_root=tmp_path / "cache", expected_sha256=hashlib.sha256(data).hexdigest(),
                   expected_size=len(data), timeout=2, max_retries=0)
    frame = fetch_csv_dataframe(source.as_uri(), download_options=options, sep=";")
    assert frame.to_dict("list") == {"id": [1], "value": ["hello"]}
    with closing(fetch_csv_db("records", source.as_uri(), download_options=options, sep=";")) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(1, "hello")]
    assert list((tmp_path / "cache").glob("*.db"))


def test_real_html_table_conversion(tmp_path):
    from datacache import fetch_file

    source = tmp_path / "records.html"
    source.write_text("<table><tr><th>id</th><th>label</th></tr>"
                      "<tr><td>1</td><td>case</td></tr></table>")
    expected = b"id,label\n1,case\n"
    destination = tmp_path / "records.csv"
    fetch_file(source.as_uri(), destination=destination,
               expected_sha256=hashlib.sha256(expected).hexdigest())
    assert destination.read_bytes() == expected
