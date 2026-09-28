from contextlib import closing

import numpy as np
import pandas as pd
from datacache import db_from_dataframe
from datacache.database_types import db_type

def test_db_types():
    for int_type in [
            int,
            np.int8, np.int16, np.int32, np.int64,
            np.uint8, np.uint16, np.uint32, np.uint64]:
        assert db_type(int_type) == "INT"

    for float_type in [float, np.float32, np.float64]:
        assert db_type(float) == "FLOAT"

    assert db_type(str) == "TEXT"


def test_float16_columns_are_stored_as_floats(tmp_path):
    assert db_type(np.float16) == "FLOAT"
    frame = pd.DataFrame({"value": np.array([1.5], dtype=np.float16)})
    with closing(db_from_dataframe("half.db", "halves", frame, cache_root=tmp_path)) as connection:
        assert connection.execute("SELECT value FROM halves").fetchall() == [(1.5,)]


def test_numpy_fixed_width_and_bytes_types_are_mapped():
    # dtype('S3').type is bytes_ and dtype('U5').type is str_ on Python 3;
    # the Python 2 name string_ never matched them.
    assert db_type(np.dtype("U5")) == "TEXT"
    assert db_type(np.str_) == "TEXT"
    assert db_type(np.dtype("S3")) == "BLOB"
    assert db_type(np.bytes_) == "BLOB"
    assert db_type(bytes) == "BLOB"


def test_dataframes_of_fixed_width_arrays_keep_text_columns(tmp_path):
    # pandas stores such arrays with the object dtype, so existing schemas
    # built from DataFrames are unchanged.
    frame = pd.DataFrame({"b": np.array([b"ab"], dtype="S2"), "u": np.array(["x"], dtype="U1")})
    with closing(db_from_dataframe("fixed.db", "t", frame, cache_root=tmp_path)) as connection:
        assert [row[2] for row in connection.execute("PRAGMA table_info(t)")] == ["TEXT", "TEXT"]
        assert connection.execute("SELECT * FROM t").fetchall() == [(b"ab", "x")]
