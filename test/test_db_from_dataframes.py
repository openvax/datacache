from nose.tools import eq_
import pandas as pd
from tempfile import NamedTemporaryFile
from datacache import db_from_dataframes, db_from_dataframe
from datacache.database_helpers import build_table_name

from .util import get_collection_name

dfA = pd.DataFrame({"numbers": [1, 2, 3], "strings": ["a", "b", "c"]})
dfB = pd.DataFrame({"wuzzles": ["nuzzle", "ruzzle"]})

def test_database_from_dataframes():
    with NamedTemporaryFile(suffix="test.db") as f:
        collection_name = get_collection_name(f)
        db = db_from_dataframes(
            collection_name=collection_name,
            dataframes={"A": dfA, "B": dfB},
            primary_keys={"A": "numbers"},
            indices={"A": [("numbers", "strings")]},
            subdir="test_datacache")
        cursor_A = db.execute("SELECT * FROM \"%s\"" % build_table_name("A", collection_name))
        results_A = cursor_A.fetchall()
        eq_(results_A, [(1, "a"), (2, "b"), (3, "c")])
        cursor_B = db.execute("SELECT * FROM \"%s\"" % build_table_name("B", collection_name))
        results_B = cursor_B.fetchall()
        eq_(results_B, [("nuzzle",), ("ruzzle",)])

def test_database_from_single_dataframe():
    with NamedTemporaryFile(suffix="test.db") as f:
        collection_name = get_collection_name(f)
        db = db_from_dataframe(
            collection_name=collection_name,
            table_name="A",
            df=dfA,
            primary_key="numbers",
            indices=[("numbers", "strings")],
            subdir="test_datacache")
        cursor = db.execute("SELECT * FROM \"%s\"" % build_table_name("A", collection_name))
        results = cursor.fetchall()
        eq_(results, [(1, "a"), (2, "b"), (3, "c")])
