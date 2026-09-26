"""Offline regression tests for composed download/transform helpers."""

from contextlib import closing
import gzip
import hashlib
import logging
import os
from pathlib import Path
import re
import sqlite3
import stat

import pytest

import pandas as pd

from datacache import common, fetch_and_transform, fetch_csv_dataframe, fetch_csv_db
from datacache.common import build_local_filename
from datacache import database_helpers
from datacache.database_helpers import _db_filenames_from_dataframe, _name_length, _truncate_name


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


def test_inferred_database_names_keep_their_historical_spelling():
    # Existing databases are found by name, so ordinary schemas must keep
    # producing exactly the old key.
    frame = pd.DataFrame({"id": [1], "label": ["x"], "with space": [1.5], "Éclair μ (%)": [2]})
    assert _db_filenames_from_dataframe("records", frame) == (
        "records_nrows1.id_INT.label_TEXT.with_space_FLOAT.Éclair_μ_(%)_INT.db", None)


def test_csv_headers_cannot_place_the_database_outside_the_cache(tmp_path):
    cache_root = tmp_path / "deep" / "cache"
    source = tmp_path / "headers.csv"
    source.write_text('id,"/../../../escaped/x"\n1,2\n')
    with closing(fetch_csv_db("records", source.as_uri(), csv_filename="headers.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(1, 2)]
    [database] = tmp_path.rglob("*.db")
    assert database.parent == cache_root


def test_wide_csv_infers_a_database_name_the_filesystem_accepts(tmp_path):
    columns = ["measurement_%02d" % i for i in range(40)]
    source = tmp_path / "wide.csv"
    source.write_text(",".join(columns) + "\n" + ",".join("1" for _ in columns) + "\n")
    with closing(fetch_csv_db("records", source.as_uri(), csv_filename="wide.csv",
                              download_options={"cache_root": tmp_path / "cache"})) as connection:
        assert connection.execute("SELECT COUNT(*) FROM records").fetchone() == (1,)
    [database] = (tmp_path / "cache").glob("*.db")
    assert len(database.name) < 255


def test_shortened_database_names_still_distinguish_schemas():
    frame = pd.DataFrame({"a": [1], "/../x": [2]})
    name, historical = _db_filenames_from_dataframe("records", frame)
    assert re.fullmatch(r"records_nrows1\.[0-9a-f]{32}\.db", name)
    assert historical is None  # a name that could leave the cache is never reused
    assert _db_filenames_from_dataframe("records", frame)[0] == name
    assert _db_filenames_from_dataframe("records", frame.astype({"a": float}))[0] != name


@pytest.mark.parametrize("db_filename", [None, "unnamed.db"])
def test_unnamed_csv_columns_raise_the_same_clear_error(tmp_path, db_filename):
    source = tmp_path / "unnamed.csv"
    source.write_text("1,2\n3,4\n")
    with pytest.raises(ValueError, match="non-empty strings"):
        fetch_csv_db("records", source.as_uri(), csv_filename="unnamed.csv", db_filename=db_filename,
                     download_options={"cache_root": tmp_path / "cache"}, header=None)



def _write_legacy_database(path, version, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            'CREATE TABLE records (peptide TEXT, "m/z" FLOAT);'
            "CREATE TABLE _datacache_metadata (version INT);"
            "INSERT INTO _datacache_metadata VALUES (%d);" % version)
        connection.executemany("INSERT INTO records VALUES (?, ?)", rows)
        connection.commit()


@pytest.fixture
def mz_source(tmp_path):
    source = tmp_path / "mz.csv"
    source.write_text("peptide,m/z\nAAA,1.5\n")
    return source


def test_slash_in_a_column_name_does_not_nest_directories(tmp_path, mz_source):
    cache_root = tmp_path / "cache"
    with closing(fetch_csv_db("records", mz_source.as_uri(), csv_filename="mz.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute('SELECT "m/z" FROM records').fetchall() == [(1.5,)]
    assert [path for path in cache_root.iterdir() if path.is_dir()] == []
    assert len(list(cache_root.glob("*.db"))) == 1


def test_nested_database_from_an_older_release_is_reused_in_place(tmp_path, mz_source):
    cache_root = tmp_path / "cache"
    legacy = cache_root / "mz_nrows1.peptide_TEXT.m" / "z_FLOAT.db"
    _write_legacy_database(legacy, 1, [("OLD", 9.5)])
    before = legacy.read_bytes()
    with closing(fetch_csv_db("records", mz_source.as_uri(), csv_filename="mz.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [("OLD", 9.5)]
    assert legacy.read_bytes() == before
    assert list(cache_root.glob("*.db")) == []


def test_new_version_of_a_nested_database_is_built_flat(tmp_path, mz_source):
    # The older copy is left alone: a process running an older release may
    # still have it open, and deleting an open SQLite file can corrupt data.
    cache_root = tmp_path / "cache"
    legacy = cache_root / "mz_nrows1.peptide_TEXT.m" / "z_FLOAT.db"
    _write_legacy_database(legacy, 1, [("OLD", 9.5)])
    before = legacy.read_bytes()
    with closing(fetch_csv_db("records", mz_source.as_uri(), csv_filename="mz.csv", version=2,
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [("AAA", 1.5)]
    assert legacy.read_bytes() == before
    assert len(list(cache_root.glob("*.db"))) == 1


def test_legacy_lookup_never_opens_a_database_outside_the_cache(tmp_path):
    # Even when the directories a hostile header names already exist, an old
    # nested name is reused only if it stays inside the cache directory.
    cache_root = tmp_path / "deep" / "cache"
    (cache_root / "headers_nrows1.a_INT.").mkdir(parents=True)
    outside = tmp_path / "escaped" / "x_INT.db"
    outside.parent.mkdir()
    with closing(sqlite3.connect(outside)) as connection:
        connection.executescript("CREATE TABLE records (a INT, b INT);"
                                 "INSERT INTO records VALUES (99, 99);"
                                 "CREATE TABLE _datacache_metadata (version INT);"
                                 "INSERT INTO _datacache_metadata VALUES (1);")
    before = outside.read_bytes()
    source = tmp_path / "headers.csv"
    source.write_text('a,"/../../../escaped/x"\n1,2\n')
    with closing(fetch_csv_db("records", source.as_uri(), csv_filename="headers.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(1, 2)]
    assert outside.read_bytes() == before


def test_rebuilds_leave_room_for_the_sqlite_journal(tmp_path):
    # SQLite creates "<database>-journal" to rebuild. A 250-byte name could be
    # built once but never rebuilt, so such names are shortened up front, even
    # when a long CSV filename leaves no room for the usual digest name.
    schema = ".id_INT.value_INT.db"
    base = "r" * (250 - len("_nrows1") - len(schema))
    source = tmp_path / "source.csv"
    source.write_text("id,value\n1,10\n")
    options = dict(csv_filename=base + ".csv", download_options={"cache_root": tmp_path / "cache"})
    for version in (1, 2):
        with closing(fetch_csv_db("records", source.as_uri(), version=version, **options)) as connection:
            assert connection.execute("SELECT * FROM records").fetchall() == [(1, 10)]
    [database] = (tmp_path / "cache").glob("*.db")
    assert len(database.name) <= 255 - len("-journal")
    assert database.name.startswith("rrrr")


def test_database_at_a_name_too_long_to_rebuild_is_still_reused(tmp_path):
    # Older releases could create a 248-255 byte name. It is still reused in
    # place; only a rebuild moves to the shorter name.
    cache_root = tmp_path / "cache"
    source = tmp_path / "mass.csv"
    source.write_text("peptide,mass\nAAA,1.5\n")
    base = "m" * (250 - len("_nrows1") - len(".peptide_TEXT.mass_FLOAT.db"))
    name, historical = _db_filenames_from_dataframe(base, pd.read_csv(source))
    assert len(historical) == 250 and len(name) <= 255 - len("-journal")
    # SQLite cannot write at this name (its journal would not fit), so, like
    # older releases, build under a short name and publish it with a link.
    _write_legacy_database(cache_root / "staged.db", 1, [("OLD", 9.5)])
    os.link(cache_root / "staged.db", cache_root / historical)
    os.remove(cache_root / "staged.db")
    with closing(fetch_csv_db("records", source.as_uri(), csv_filename=base + ".csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [("OLD", 9.5)]


def test_characters_some_platform_forbids_are_kept_out_of_names(tmp_path):
    source = tmp_path / "times.csv"
    source.write_text('id,time:s,"a?b",c*d,e|f\n1,2,3,4,5\n')
    cache_root = tmp_path / "cache"
    with closing(fetch_csv_db("records", source.as_uri(), csv_filename="times.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute('SELECT "time:s" FROM records').fetchall() == [(2,)]
    [database] = cache_root.glob("*.db")
    assert not set(':?*|<>"') & set(database.name)


@pytest.mark.skipif(os.name == "nt", reason="':' names an alternate data stream on Windows")
def test_database_with_a_colon_from_an_older_release_is_reused(tmp_path):
    cache_root = tmp_path / "cache"
    legacy = cache_root / "times_nrows1.peptide_TEXT.m:z_FLOAT.db"
    _write_legacy_database(legacy, 1, [("OLD", 9.5)])
    source = tmp_path / "times.csv"
    source.write_text("peptide,m:z\nAAA,1.5\n")
    with closing(fetch_csv_db("records", source.as_uri(), csv_filename="times.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [("OLD", 9.5)]


@pytest.mark.skipif(os.name == "nt", reason="backslash separates paths on Windows")
def test_backslashes_in_an_older_name_are_ordinary_characters_on_posix(tmp_path):
    # On POSIX, "a\..\b" was one harmless file name, so it is reused in place.
    cache_root = tmp_path / "cache"
    legacy = cache_root / "slashes_nrows1.peptide_TEXT.a\\..\\b_FLOAT.db"
    _write_legacy_database(legacy, 1, [("OLD", 9.5)])
    source = tmp_path / "slashes.csv"
    source.write_text("peptide,a\\..\\b\nAAA,1.5\n")
    with closing(fetch_csv_db("records", source.as_uri(), csv_filename="slashes.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [("OLD", 9.5)]


@pytest.mark.parametrize("obstacle", ["file", "directory"])
def test_anything_else_at_an_older_name_is_not_reused(tmp_path, mz_source, obstacle):
    # Looking for an older release's database must never stop a new build.
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    if obstacle == "file":  # the old name's parent directory is a regular file
        (cache_root / "mz_nrows1.peptide_TEXT.m").write_text("not a directory")
    else:  # a directory sits where the old database would be
        (cache_root / "mz_nrows1.peptide_TEXT.m" / "z_FLOAT.db").mkdir(parents=True)
    with closing(fetch_csv_db("records", mz_source.as_uri(), csv_filename="mz.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [("AAA", 1.5)]
    assert len(list(cache_root.glob("*.db"))) == 1


def test_cache_names_work_where_md5_is_disabled_for_security(monkeypatch):
    # FIPS-mode OpenSSL rejects md5 unless it is declared not to be for security.
    real_md5 = hashlib.md5

    def fips_md5(*args, usedforsecurity=True, **kwargs):
        if usedforsecurity:
            raise ValueError("[digital envelope routines] unsupported")
        return real_md5(*args, usedforsecurity=False, **kwargs)

    monkeypatch.setattr(hashlib, "md5", fips_md5)
    assert build_local_filename("https://example.org/data?id=1")
    assert len(build_local_filename(filename="x" * 200)) < 200
    assert _db_filenames_from_dataframe("records", pd.DataFrame({"m/z": [1.5]}))[0].endswith(".db")


@pytest.mark.parametrize("message", ["database is locked", "unable to open database file"])
def test_only_a_locked_older_database_stops_a_new_build(tmp_path, mz_source, monkeypatch, message):
    # An older release may be writing to its database; building a second copy
    # beside it would let the two diverge, so a lock error propagates. A file
    # SQLite cannot open at all just means there is nothing to reuse.
    cache_root = tmp_path / "cache"
    legacy = cache_root / "mz_nrows1.peptide_TEXT.m" / "z_FLOAT.db"
    _write_legacy_database(legacy, 1, [("OLD", 9.5)])
    real_cached_connection = database_helpers._cached_connection

    def failing(path, *args):
        if os.fspath(path) == str(legacy):
            raise sqlite3.OperationalError(message)
        return real_cached_connection(path, *args)

    monkeypatch.setattr(database_helpers, "_cached_connection", failing)
    options = dict(csv_filename="mz.csv", download_options={"cache_root": cache_root})
    if "locked" in message:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            fetch_csv_db("records", mz_source.as_uri(), **options)
        assert list(cache_root.glob("*.db")) == []
    else:
        with closing(fetch_csv_db("records", mz_source.as_uri(), **options)) as connection:
            assert connection.execute("SELECT * FROM records").fetchall() == [("AAA", 1.5)]


def test_csv_filename_characters_some_platform_forbids_stay_out_of_names(tmp_path):
    source = tmp_path / "run.csv"
    source.write_text("id\n1\n")
    cache_root = tmp_path / "cache"
    with closing(fetch_csv_db("records", source.as_uri(), csv_filename="run:2.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(1,)]
    [database] = cache_root.glob("*.db")
    assert database.name.startswith("run_2_nrows1.") and ":" not in database.name


@pytest.mark.skipif(os.name == "nt", reason="':' names an alternate data stream on Windows")
def test_database_named_after_a_csv_filename_with_a_colon_is_reused(tmp_path):
    cache_root = tmp_path / "cache"
    _write_legacy_database(cache_root / "run:2_nrows1.peptide_TEXT.mass_FLOAT.db", 1, [("OLD", 9.5)])
    source = tmp_path / "run.csv"
    source.write_text("peptide,mass\nAAA,1.5\n")
    with closing(fetch_csv_db("records", source.as_uri(), csv_filename="run:2.csv",
                              download_options={"cache_root": cache_root})) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [("OLD", 9.5)]


def test_database_names_need_named_columns():
    with pytest.raises(ValueError, match="non-empty strings"):
        _db_filenames_from_dataframe("records", pd.DataFrame())


def test_truncation_counts_utf8_bytes_on_every_platform():
    assert _truncate_name("abc", 5) == "abc"
    assert _truncate_name("é" * 10, 5) == "éé"  # two bytes each
    assert _name_length("é" * 130) == 260


def test_inferred_names_measure_utf8_bytes_on_every_platform():
    # 130 accented characters are 260 UTF-8 bytes but only 130 characters or
    # UTF-16 units. Every platform must still pick the same (digest) name.
    name, historical = _db_filenames_from_dataframe("records", pd.DataFrame({"é" * 130: [1]}))
    assert re.fullmatch(r"records_nrows1\.[0-9a-f]{32}\.db", name)
    assert historical == "records_nrows1.%s_INT.db" % ("é" * 130)


def test_schemas_that_read_alike_get_different_digest_names():
    # One column "a_INT.b:c" of text and two columns "a" and "b:c" spell out
    # the same way; their digest names must still differ.
    one = pd.DataFrame({"a_INT.b:c": ["x"]})
    two = pd.DataFrame({"a": [1], "b:c": ["x"]})
    assert _db_filenames_from_dataframe("records", one)[1] == _db_filenames_from_dataframe("records", two)[1]
    assert _db_filenames_from_dataframe("records", one)[0] != _db_filenames_from_dataframe("records", two)[0]


def test_a_superseded_older_database_is_reported_once(tmp_path, mz_source, caplog):
    cache_root = tmp_path / "cache"
    legacy = cache_root / "mz_nrows1.peptide_TEXT.m" / "z_FLOAT.db"
    _write_legacy_database(legacy, 1, [("OLD", 9.5)])
    options = dict(csv_filename="mz.csv", download_options={"cache_root": cache_root})
    with caplog.at_level(logging.WARNING, logger="datacache.database_helpers"):
        fetch_csv_db("records", mz_source.as_uri(), version=2, **options).close()
        assert str(legacy) in caplog.text and "can be deleted" in caplog.text
        caplog.clear()
        fetch_csv_db("records", mz_source.as_uri(), version=3, **options).close()
        assert not caplog.records
    assert legacy.exists()


def test_no_report_without_an_older_database(tmp_path, mz_source, caplog):
    with caplog.at_level(logging.WARNING, logger="datacache.database_helpers"):
        fetch_csv_db("records", mz_source.as_uri(), csv_filename="mz.csv",
                     download_options={"cache_root": tmp_path / "cache"}).close()
    assert not caplog.records


def test_the_older_copy_is_called_deletable_only_after_a_successful_build(
        tmp_path, mz_source, caplog, monkeypatch):
    cache_root = tmp_path / "cache"
    _write_legacy_database(cache_root / "mz_nrows1.peptide_TEXT.m" / "z_FLOAT.db", 1, [("OLD", 9.5)])

    def failing_build(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(database_helpers, "db_from_dataframe", failing_build)
    with caplog.at_level(logging.WARNING, logger="datacache.database_helpers"):
        with pytest.raises(OSError, match="disk full"):
            fetch_csv_db("records", mz_source.as_uri(), csv_filename="mz.csv", version=2,
                         download_options={"cache_root": cache_root})
    assert "can be deleted" not in caplog.text


def test_older_names_whose_dots_stay_in_the_cache_are_still_reused():
    # "records_nrows1../x_INT.db" has components "records_nrows1.." and
    # "x_INT.db": it never leaves the cache, so an existing copy is reused.
    name, historical = _db_filenames_from_dataframe("records", pd.DataFrame({"./x": [1]}))
    assert historical == "records_nrows1../x_INT.db" and name != historical
    assert _db_filenames_from_dataframe("records", pd.DataFrame({"/../x": [1]}))[1] is None
