"""
Test that datacache constructs databases correctly
(separately from downloading/caching them)
"""
import tempfile

import datacache

from nose.tools import eq_

TABLE_NAME = "test"
INT_COL_NAME = "int_col"
STR_COL_NAME = "str_col"
COL_TYPES = [(INT_COL_NAME, "INT"), (STR_COL_NAME, "STR")]
KEY_COLUMN_NAME = "int_col"
NULLABLE = {STR_COL_NAME}
ROWS = [(1, "darkness"), (2, "light"), (3, None)]
INDICES = [["str_col"]]
VERSION = 2

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

def test_create_db():
    with tempfile.NamedTemporaryFile(suffix="test.db") as f:
        db = datacache.database.Database(f.name)
        table = make_table_object()
        db.create(tables=[table], version=VERSION)
        assert db.has_table(TABLE_NAME)
        assert db.has_version()
        assert db.version() == VERSION
        sql = """
            SELECT %s from %s WHERE %s = '%s'
        """ % (INT_COL_NAME, TABLE_NAME, STR_COL_NAME, "light")
        cursor = db.connection.execute(sql)
        int_result_tuple = cursor.fetchone()
        int_result = int_result_tuple[0]
        eq_(int_result, 2)
