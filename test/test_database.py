"""
Test that datacache constructs databases correctly
(separately from downloading/caching them)
"""
from os import remove
from os.path import exists
import sqlite3

import datacache

from nose.tools import eq_

TEST_DB_PATH = "test.db"
TABLE_NAME="test"
INT_COL_NAME="int_col"
STR_COL_NAME="str_col"
COL_TYPES=[(INT_COL_NAME, "INT"), (STR_COL_NAME, "STR")]
KEY_COLUMN_NAME="int_col"
NULLABLE=[]
ROWS=[(1, "darkness"), (2, "light")]
INDICES=[["str_col"]]
VERSION=2

def test_create_db():
    if exists(TEST_DB_PATH):
        remove(TEST_DB_PATH)
    db = datacache.database.Database(TEST_DB_PATH)
    table = datacache.database_table.DatabaseTable(
        name=TABLE_NAME,
        column_types=COL_TYPES,
            make_rows,
            indices=[],
            nullable=set(),
            primary_key=None):
    )
    tables = {TABLE_NAME : table}
    datacache.database_helpers._create_db(
        db=db,
        tables=tables)
        col_types=COL_TYPES,
        key_column_name=KEY_COLUMN_NAME,
        nullable=NULLABLE,
        rows=ROWS,
        indices=INDICES,
        version=VERSION)
    db.commit()
    assert db.table_exists(TABLE_NAME)
    assert db.has_version()
    assert db.version() == VERSION
    sql = """
        SELECT %s from %s WHERE %s = '%s'
    """ % (INT_COL_NAME, TABLE_NAME, STR_COL_NAME, "light")
    print(sql)
    cursor = db.connection.execute(sql)
    int_result_tuple = cursor.fetchone()
    int_result = int_result_tuple[0]
    eq_(int_result, 2)
    db.close()
    remove(TEST_DB_PATH)