"""Data fidelity, rollback, publication, and downstream SQLite contracts."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import os
import sqlite3
import stat
import threading

import pandas as pd
import pytest

from datacache import connect_if_correct_version, db_from_dataframe
from datacache.database import Database, quote_identifier
from datacache.database_helpers import _create_cached_db, db_from_dataframes_with_absolute_path
from datacache.database_table import DatabaseTable


@pytest.mark.parametrize("dtype", ["int8", "int32", "int64", "uint32", "Int64", "boolean"])
def test_numeric_scalars_are_stored_as_integers(tmp_path, dtype):
    values = [True, False] if dtype == "boolean" else [1, 2]
    frame = pd.DataFrame({"value": pd.Series(values, dtype=dtype)})
    with closing(db_from_dataframe(tmp_path / "data.db", "records", frame)) as connection:
        assert connection.execute("SELECT value, typeof(value) FROM records").fetchall() == [
            (int(value), "integer") for value in values]


def test_mixed_numbers_keep_integer_precision(tmp_path):
    value = 2**53 + 1
    frame = pd.DataFrame({"id": [value], "weight": [1.5]})
    with closing(db_from_dataframe(tmp_path / "data.db", "records", frame)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(value, 1.5)]
        assert connection.execute("SELECT id FROM records WHERE id = ?", (value,)).fetchone() == (value,)


def test_missing_scalars_and_datetime_roundtrip(tmp_path):
    frame = pd.DataFrame({
        "int": pd.Series([1, None], dtype="Int64"),
        "flag": pd.Series([True, None], dtype="boolean"),
        "float": pd.Series([1.5, None], dtype="Float64"),
        "string": pd.Series(["hello", None], dtype="string"),
        "category": pd.Series(["value", None], dtype="category"),
        "date": pd.to_datetime(["2026-01-02", None]),
    })
    with closing(db_from_dataframe(tmp_path / "data.db", "records", frame)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [
            (1, 1, 1.5, "hello", "value", "2026-01-02T00:00:00"), (None,) * 6]


@pytest.mark.parametrize("key", ["sample id", "sample_id"])
def test_spaced_names_apply_to_keys_nullability_and_indices(tmp_path, key):
    frame = pd.DataFrame({"sample id": [1, 2], "group by": ["a", None]})
    with closing(db_from_dataframe(
            tmp_path / "data.db", "records", frame, primary_key=key,
            indices=[("group by",)])) as connection:
        assert connection.execute("SELECT sample_id, group_by FROM records").fetchall() == [(1, "a"), (2, None)]
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO records VALUES (1, 'duplicate')")
        indices = connection.execute("PRAGMA index_info(records_index_group_by)").fetchall()
        assert indices[0][2] == "group_by"


@pytest.mark.parametrize("frame,options", [
    (pd.DataFrame([[1, 2]], columns=["a b", "a_b"]), {}),
    (pd.DataFrame([[1, 2]], columns=["value", "VALUE"]), {}),
    (pd.DataFrame({"value": [1]}), {"primary_key": "missing"}),
    (pd.DataFrame({"value": [1]}), {"indices": [("missing",)]}),
    (pd.DataFrame({"value": [1]}), {"indices": ["value"]}),
])
def test_invalid_schema_does_not_destroy_existing_database(tmp_path, frame, options):
    path = tmp_path / "data.db"
    with closing(db_from_dataframe(path, "records", pd.DataFrame({"value": [42]}))):
        pass
    previous = path.read_bytes()
    with pytest.raises(ValueError):
        db_from_dataframe(path, "records", frame, overwrite=True, **options)
    assert path.read_bytes() == previous


@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("failure", ["duplicate", "overflow", "interrupt"])
def test_failed_rebuild_preserves_data_version_and_permissions(tmp_path, monkeypatch, overwrite, failure):
    path = tmp_path / "data.db"
    with closing(db_from_dataframe(path, "records", pd.DataFrame({"value": [42]}), version=1)):
        pass
    path.chmod(0o640)
    mode = stat.S_IMODE(path.stat().st_mode)
    before = path.stat().st_ino
    values = list(range(1001)) + [1]
    error = sqlite3.IntegrityError
    if failure == "overflow":
        values = [2**63]
        error = OverflowError
    elif failure == "interrupt":
        values = list(range(1001))
        original = Database._create_indices

        def interrupt(self, *args, **kwargs):
            original(self, *args, **kwargs)
            raise KeyboardInterrupt()

        monkeypatch.setattr(Database, "_create_indices", interrupt)
        error = KeyboardInterrupt
    with pytest.raises(error):
        db_from_dataframe(path, "records", pd.DataFrame({"value": values}),
                          primary_key="value", version=2, overwrite=overwrite)
    with closing(connect_if_correct_version(path, 1)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(42,)]
    assert path.stat().st_ino == before
    assert stat.S_IMODE(path.stat().st_mode) == mode
    assert list(tmp_path.iterdir()) == [path]


def test_empty_dataframe_is_a_complete_database(tmp_path):
    path = tmp_path / "empty.db"
    with closing(db_from_dataframe(path, "records", pd.DataFrame({"id": pd.Series([], dtype="int64")}))) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == []
    with closing(connect_if_correct_version(path, 1)) as connection:
        assert connection is not None


def test_failed_new_database_is_not_published(tmp_path):
    with pytest.raises(sqlite3.IntegrityError):
        db_from_dataframe(tmp_path / "bad.db", "records", pd.DataFrame({"id": [1, 1]}), primary_key="id")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("overwrite", [False, True])
def test_successful_rebuild_keeps_existing_inode_and_readers(tmp_path, overwrite):
    path = tmp_path / "data.db"
    with closing(db_from_dataframe(path, "records", pd.DataFrame({"value": [1]}))) as reader:
        inode = path.stat().st_ino
        with closing(db_from_dataframe(path, "records", pd.DataFrame({"value": [2]}),
                                       version=1 if overwrite else 2, overwrite=overwrite)):
            pass
        assert reader.execute("SELECT * FROM records").fetchall() == [(2,)]
        assert path.stat().st_ino == inode


def database_with_views(path, view_name="records"):
    quoted_view = quote_identifier(view_name)
    with closing(db_from_dataframe(path, "legacy", pd.DataFrame({"value": [42]}))) as connection:
        connection.execute("CREATE VIEW %s AS SELECT value FROM legacy" % quoted_view)
        connection.execute("CREATE VIEW summary AS SELECT * FROM %s" % quoted_view)
        connection.execute("CREATE TRIGGER remove_summary INSTEAD OF DELETE ON summary "
                           "BEGIN DELETE FROM legacy WHERE value = OLD.value; END")
        connection.commit()
    path.chmod(0o640)
    return path.stat()


@pytest.mark.parametrize("view_name", ["records", 'records "view"'])
def test_explicit_overwrite_replaces_views_with_tables(tmp_path, view_name):
    path = tmp_path / "data.db"
    before = database_with_views(path, view_name)
    with closing(db_from_dataframe(path, view_name, pd.DataFrame({"value": [99]}),
                                   overwrite=True, version=2)) as connection:
        assert connection.execute("SELECT * FROM %s" % quote_identifier(view_name)).fetchall() == [(99,)]
        assert connection.execute("SELECT type, name FROM sqlite_master ORDER BY name").fetchall() == [
            ("table", "_datacache_metadata"), ("table", view_name)]
        assert connection.execute("SELECT version FROM _datacache_metadata").fetchone() == (2,)
    assert path.stat().st_ino == before.st_ino
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


@pytest.mark.parametrize("failure", ["constraint", "interrupt"])
def test_failed_overwrite_restores_views_and_triggers(tmp_path, monkeypatch, failure):
    path = tmp_path / "data.db"
    before = database_with_views(path)
    original_bytes = path.read_bytes()
    error = sqlite3.IntegrityError
    if failure == "interrupt":
        def interrupt(*args, **kwargs):
            raise KeyboardInterrupt()

        monkeypatch.setattr(Database, "_create_indices", interrupt)
        error = KeyboardInterrupt
    with pytest.raises(error):
        db_from_dataframe(path, "records", pd.DataFrame({"value": [1, 1]}),
                          primary_key="value" if failure == "constraint" else None,
                          overwrite=True, version=2)
    assert path.read_bytes() == original_bytes
    assert path.stat().st_ino == before.st_ino
    assert stat.S_IMODE(path.stat().st_mode) == 0o640
    with closing(connect_if_correct_version(path, 1)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(42,)]
        assert connection.execute("SELECT * FROM summary").fetchall() == [(42,)]
        connection.execute("DELETE FROM summary")
        assert connection.execute("SELECT * FROM legacy").fetchall() == []
        connection.rollback()
    assert list(tmp_path.iterdir()) == [path]


def test_cache_reuse_and_version_rebuild_preserve_views(tmp_path):
    path = tmp_path / "data.db"
    database_with_views(path)
    original_bytes = path.read_bytes()
    with closing(db_from_dataframe(path, "legacy", None)) as connection:
        assert connection.execute("SELECT * FROM summary").fetchall() == [(42,)]
    assert path.read_bytes() == original_bytes
    with closing(db_from_dataframe(path, "legacy", pd.DataFrame({"value": [99]}), version=2)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(99,)]
        assert connection.execute("SELECT * FROM summary").fetchall() == [(99,)]


@pytest.mark.parametrize("symlink", [False, True])
def test_concurrent_new_database_creators_reuse_winner(tmp_path, symlink):
    path = tmp_path / "data.db"
    target = tmp_path / "target.db"
    if symlink:
        path.symlink_to(target.name)
    barrier = threading.Barrier(2, timeout=10)

    def create(value):
        def rows():
            assert not path.exists()
            barrier.wait()
            yield (value,)

        table = DatabaseTable("records", [("value", "INT")], rows)
        with closing(_create_cached_db(path, [table])) as connection:
            return connection.execute("SELECT value FROM records").fetchone()[0]

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(create, 1)
        second = executor.submit(create, 2)
        assert first.result(timeout=15) == second.result(timeout=15)
    assert set(tmp_path.iterdir()) == ({path, target} if symlink else {path})
    if symlink:
        assert path.is_symlink()
        assert os.readlink(path) == target.name


def test_version_lookup_does_not_create_and_can_open_read_only(tmp_path):
    path = tmp_path / "data.db"
    assert connect_if_correct_version(path, 1) is None
    assert not path.exists()
    with closing(db_from_dataframe(path, "records", pd.DataFrame({"value": [1]}))):
        pass
    assert connect_if_correct_version(path, 2) is None
    with closing(connect_if_correct_version(path, 1, read_only=True)) as connection:
        assert connection.execute("SELECT value FROM records").fetchall() == [(1,)]
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("DELETE FROM records")


def test_direct_database_create_rolls_back_partial_tables(tmp_path):
    db = Database(tmp_path / "data.db")
    table = DatabaseTable("records", [("id", "INT")], lambda: [(1,), (1,)], primary_key="id")
    try:
        with pytest.raises(sqlite3.IntegrityError):
            db.create([table], 1)
        assert db.table_names() == []
        assert not db.connection.in_transaction
    finally:
        db.connection.close()


def test_primary_key_cannot_be_null(tmp_path):
    with pytest.raises(sqlite3.IntegrityError):
        db_from_dataframe(tmp_path / "data.db", "records",
                          pd.DataFrame({"sample id": pd.Series([1, None], dtype="Int64")}),
                          primary_key="sample id")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("operation", ["link", "chmod"])
def test_failed_new_database_publication_cleans_up(tmp_path, monkeypatch, operation):
    def reject(*args):
        raise PermissionError("injected publication failure")

    monkeypatch.setattr(os, operation, reject)
    with pytest.raises(PermissionError):
        db_from_dataframe(tmp_path / "data.db", "records", pd.DataFrame({"id": [1]}))
    assert list(tmp_path.iterdir()) == []


@pytest.mark.skipif(os.name != "posix", reason="POSIX umask semantics")
@pytest.mark.parametrize("mask", [0o022, 0o002, 0o077])
def test_new_database_honors_umask(tmp_path, mask):
    path = tmp_path / "data.db"
    previous = os.umask(mask)
    try:
        with closing(db_from_dataframe(path, "records", pd.DataFrame({"id": [1]}))):
            pass
    finally:
        os.umask(previous)
    assert stat.S_IMODE(path.stat().st_mode) == 0o666 & ~mask


def test_mismatched_version_closes_rejected_connection(tmp_path, monkeypatch):
    from datacache import database_helpers

    path = tmp_path / "data.db"
    with closing(db_from_dataframe(path, "records", pd.DataFrame({"id": [1]}))):
        pass
    opened = []

    def observed(*args, **kwargs):
        db = Database(*args, **kwargs)
        opened.append(db.connection)
        return db

    monkeypatch.setattr(database_helpers, "Database", observed)
    assert connect_if_correct_version(path, 2) is None
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")


def test_new_failed_creator_does_not_remove_concurrent_winner(tmp_path):
    path = tmp_path / "data.db"
    started = threading.Event()
    published = threading.Event()

    def failed_rows():
        started.set()
        assert published.wait(timeout=10)
        yield (2,)
        raise RuntimeError("failed after another creator published")

    def fail():
        table = DatabaseTable("records", [("id", "INT")], failed_rows)
        with pytest.raises(RuntimeError, match="another creator"):
            _create_cached_db(path, [table])

    with ThreadPoolExecutor(max_workers=1) as executor:
        failed = executor.submit(fail)
        try:
            assert started.wait(timeout=10)
            with closing(db_from_dataframe(path, "records", pd.DataFrame({"id": [1]}))):
                pass
        finally:
            published.set()
        failed.result(timeout=10)
    with closing(connect_if_correct_version(path, 1)) as connection:
        assert connection.execute("SELECT id FROM records").fetchall() == [(1,)]
    assert list(tmp_path.iterdir()) == [path]


def test_reuse_matches_table_names_ignoring_ascii_case(tmp_path):
    # SQLite treats "Records" and "records" as one table, so a caller spelling
    # it differently must reuse the database instead of rebuilding it.
    with closing(db_from_dataframe("cased.db", "Records", pd.DataFrame({"id": [1]}), cache_root=tmp_path)):
        pass
    with closing(db_from_dataframe(
            "cased.db", "records", pd.DataFrame({"id": [2]}), cache_root=tmp_path)) as connection:
        assert connection.execute("SELECT id FROM records").fetchall() == [(1,)]


def test_reuse_keeps_non_ascii_case_distinct_like_sqlite(tmp_path):
    # SQLite folds ASCII letters only: "Éclair" and "éclair" are two tables.
    # Matching them with str.lower() would reuse a table SQLite cannot find.
    with closing(db_from_dataframe("accented.db", "Éclair", pd.DataFrame({"id": [1]}), cache_root=tmp_path)):
        pass
    with closing(db_from_dataframe(
            "accented.db", "éclair", pd.DataFrame({"id": [2]}), cache_root=tmp_path)) as connection:
        assert connection.execute('SELECT id FROM "éclair"').fetchall() == [(2,)]
