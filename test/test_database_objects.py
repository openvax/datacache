"""
Test that datacache constructs databases correctly
(separately from downloading/caching them)
"""
import tempfile

import datacache
from datacache.common import build_sqlite_url

from nose.tools import eq_
import pandas as pd
from os.path import dirname

from .util import get_collection_name

TABLE_NAME = "test"
INT_COL_NAME = "int_col"
STR_COL_NAME = "str_col"
COL_TYPES = [(INT_COL_NAME, "INT"), (STR_COL_NAME, "TEXT")]
KEY_COLUMN_NAME = "int_col"
NULLABLE = {STR_COL_NAME}
ROWS = [{INT_COL_NAME: 1, STR_COL_NAME: "darkness"},
        {INT_COL_NAME: 2, STR_COL_NAME: "light"},
        {INT_COL_NAME: 3, STR_COL_NAME: None}]
INDICES = [["str_col"]]
VERSION = 3

def make_table_object():
    return datacache.database_table.DatabaseTable(
        name=TABLE_NAME,
        column_types=COL_TYPES,
        make_rows=lambda: ROWS,
        indices=INDICES,
        nullable=NULLABLE,
        primary_key=INT_COL_NAME)

def test_database_table_object():
    table = make_table_object()
    eq_(table.name, TABLE_NAME)
    eq_(table.indices, INDICES)
    eq_(table.nullable, NULLABLE)
    eq_(table.rows, ROWS)
    eq_(table.indices, INDICES)

def test_table_from_dataframe():
    df = pd.DataFrame({"numbers": [1, 2, 3], "strings": ["a", "b", "c"]})
    table = datacache.database_table.DatabaseTable.from_dataframe(
        name="table_from_dataframe",
        df=df,
        indices=[],
        primary_key="numbers")
    eq_(table.rows, [{'numbers': 1, 'strings': 'a'},
                     {'numbers': 2, 'strings': 'b'},
                     {'numbers': 3, 'strings': 'c'}])

def test_create_db():
    with tempfile.NamedTemporaryFile(suffix="test.db") as f:
        subdir = dirname(f.name)
        db = datacache.database.Database(
            build_sqlite_url(get_collection_name(f), subdir), "test")
        table = make_table_object()
        db.create(tables=[table], overwrite=False, version=VERSION)
        assert db.has_table(TABLE_NAME)
        assert db.has_version()
        assert db.version() == VERSION
        sql = """
            SELECT \"%s\" from \"%s\" WHERE \"%s\" = \"%s\"
        """ % (INT_COL_NAME, TABLE_NAME, STR_COL_NAME, "light")
        cursor = db.connection.execute(sql)
        int_result_tuple = cursor.fetchone()
        int_result = int_result_tuple[0]
        eq_(int_result, 2)
