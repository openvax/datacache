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
