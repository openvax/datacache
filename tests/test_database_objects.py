"""
Test that datacache constructs databases correctly
(separately from downloading/caching them)
"""
import tempfile
import threading

import datacache

def eq_(x, y):
    assert x == y

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

def test_query_db_from_another_thread():
    # regression test for https://github.com/openvax/datacache/issues/45:
    # a connection opened in one thread must remain usable from another thread.
    with tempfile.NamedTemporaryFile(suffix="test.db") as f:
        db = datacache.database.Database(f.name)
        table = make_table_object()
        db.create(tables=[table], version=VERSION)

        results = {}

        def query_in_thread():
            cursor = db.connection.execute(
                "SELECT %s FROM %s WHERE %s = '%s'" % (
                    INT_COL_NAME, TABLE_NAME, STR_COL_NAME, "light"))
            results["value"] = cursor.fetchone()[0]

        thread = threading.Thread(target=query_in_thread)
        thread.start()
        thread.join()
        eq_(results["value"], 2)

def test_create_db_with_reserved_and_spaced_identifiers():
    # regression test for https://github.com/openvax/datacache/issues/17:
    # table/column names that are SQL keywords or contain spaces must be
    # quoted so they don't have to be sanitized to alphanumeric + underscore.
    table_name = "weird table"
    column_types = [("order", "INT"), ("group by", "STR")]
    rows = [(1, "a"), (2, "b")]
    table = datacache.database_table.DatabaseTable(
        name=table_name,
        column_types=column_types,
        make_rows=lambda: rows,
        indices=[["group by"]],
        nullable={"group by"},
        primary_key="order")
    with tempfile.NamedTemporaryFile(suffix="test.db") as f:
        db = datacache.database.Database(f.name)
        db.create(tables=[table], version=VERSION)
        assert db.has_table(table_name)
        cursor = db.connection.execute(
            'SELECT "order" FROM "weird table" WHERE "group by" = ?', ("b",))
        eq_(cursor.fetchone()[0], 2)
