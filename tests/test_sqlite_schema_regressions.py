"""Full-text cache rebuilds and distinct indexes must survive schema cleanup."""

from contextlib import closing
import sqlite3
import stat

import pandas as pd
import pytest

from datacache import connect_if_correct_version, db_from_dataframe, db_from_dataframes_with_absolute_path
from datacache.database import Database, quote_identifier


def index_columns(connection, table_name):
    return {
        row[1]: tuple(info[2] for info in connection.execute(
            "PRAGMA index_info(%s)" % quote_identifier(row[1])))
        for row in connection.execute("PRAGMA index_list(%s)" % quote_identifier(table_name))
    }


@pytest.fixture(params=["fts3", "fts4", "fts5"])
def full_text_database(tmp_path, request):
    path = tmp_path / "records.db"
    with closing(db_from_dataframe(path, "records", pd.DataFrame({"value": [42]}))) as connection:
        try:
            connection.execute('CREATE VIRTUAL TABLE "full text" USING %s(text)' % request.param)
        except sqlite3.OperationalError as error:
            if "no such module" in str(error):
                pytest.skip("SQLite build does not provide " + request.param)
            raise
        connection.execute('INSERT INTO "full text" VALUES (?)', ("original cached text",))
        connection.commit()
    path.chmod(0o640)
    return path


@pytest.mark.parametrize("overwrite", [False, True])
def test_rebuild_with_full_text_tables(full_text_database, overwrite):
    path = full_text_database
    before = path.stat()
    with closing(db_from_dataframe(path, "records", pd.DataFrame({"value": [99]}),
                                   overwrite=overwrite, version=2)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(99,)]
        assert connection.execute("SELECT version FROM _datacache_metadata").fetchone() == (2,)
        assert connection.execute("SELECT name FROM sqlite_master ORDER BY name").fetchall() == [
            ("_datacache_metadata",), ("records",)]
    assert path.stat().st_ino == before.st_ino
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


@pytest.mark.parametrize("failure", ["constraint", "interrupt"])
def test_failed_full_text_rebuild_restores_search(full_text_database, monkeypatch, failure):
    path = full_text_database
    before = path.stat()
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
    with closing(connect_if_correct_version(path, 1)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(42,)]
        assert connection.execute(
            'SELECT text FROM "full text" WHERE "full text" MATCH ?', ("original",)
        ).fetchall() == [("original cached text",)]
    assert path.read_bytes() == original_bytes
    assert path.stat().st_ino == before.st_ino
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


def test_colliding_composite_indexes_are_all_created(tmp_path):
    frame = pd.DataFrame({"a_b": [1], "c": [2], "a": [3], "b_c": [4]})
    groups = [("a_b", "c"), ("a", "b_c"), ("a_b", "c"), ("a", "b_c")]
    with closing(db_from_dataframe(tmp_path / "records.db", "records", frame, indices=groups)) as connection:
        indexes = index_columns(connection, "records")
        assert len(indexes) == 2
        assert set(indexes.values()) == {("a_b", "c"), ("a", "b_c")}
        assert indexes["records_index_a_b_c"] == ("a_b", "c")


def test_index_names_colliding_across_tables(tmp_path):
    # Both legacy names are a_index_b_index_c; different tables still need
    # separate indexes because index names share the database namespace.
    frames = {"a": pd.DataFrame({"b_index_c": [1]}), "a_index_b": pd.DataFrame({"c": [2]})}
    groups = {"a": [("b_index_c",)], "a_index_b": [("c",)]}
    with closing(db_from_dataframes_with_absolute_path(
            tmp_path / "records.db", frames, table_names_to_indices=groups)) as connection:
        assert list(index_columns(connection, "a").values()) == [("b_index_c",)]
        assert list(index_columns(connection, "a_index_b").values()) == [("c",)]


def test_index_collision_checks_tables_views_and_partial_indexes(tmp_path):
    db = Database(tmp_path / "records.db")
    try:
        db.connection.executescript('''
            CREATE TABLE "odd table" ("quoted\"\"column" INT);
            CREATE VIEW "odd table_index_quoted\"\"column" AS SELECT 1;
            CREATE TABLE "odd table_index_quoted\"\"column_2" (value INT);
            CREATE INDEX "odd table_index_quoted\"\"column_3" ON "odd table" ("quoted\"\"column")
                WHERE "quoted\"\"column" > 0;
        ''')
        db._create_index("odd table", ['quoted"column'])
        db._create_index("odd table", ['quoted"column'])
        indexes = index_columns(db.connection, "odd table")
        assert len(indexes) == 2  # Original partial index plus a full index.
        assert set(indexes.values()) == {('quoted"column',)}
        assert sum(row[4] == 0 for row in db.connection.execute('PRAGMA index_list("odd table")')) == 1
    finally:
        db.connection.close()


def test_legacy_cache_hit_does_not_add_missing_index(tmp_path):
    path = tmp_path / "records.db"
    frame = pd.DataFrame({"a_b": [1], "c": [2], "a": [3], "b_c": [4]})
    with closing(db_from_dataframe(path, "records", frame, indices=[("a_b", "c")])):
        pass
    before = path.read_bytes()
    with closing(db_from_dataframe(path, "records", frame, indices=[("a_b", "c"), ("a", "b_c")])) as connection:
        assert list(index_columns(connection, "records").values()) == [("a_b", "c")]
    assert path.read_bytes() == before
