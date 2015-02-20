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
    db = sqlite3.connect(TEST_DB_PATH)

    datacache.db._create_db(
        db=db,
        table_name=TABLE_NAME,
        col_types=COL_TYPES,
        key_column_name=KEY_COLUMN_NAME,
        nullable=NULLABLE,
        rows=ROWS,
        indices=INDICES,
        version=VERSION)
    db.commit()
    assert datacache.db.db_table_exists(db, TABLE_NAME)
    assert datacache.db.db_has_version(db)
    assert datacache.db.db_version(db) == VERSION
    sql = """
        SELECT %s from %s WHERE %s = '%s'
    """ % (INT_COL_NAME, TABLE_NAME, STR_COL_NAME, "light")
    print(sql)
    cursor = db.execute(sql)
    int_result_tuple = cursor.fetchone()
    int_result = int_result_tuple[0]
    eq_(int_result, 2)
    db.close()
    remove(TEST_DB_PATH)