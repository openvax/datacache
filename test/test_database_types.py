from nose.tools import eq_
import numpy as np
from datacache.database_types import db_type

def test_db_types():
    for int_type in [
            int,
            np.int8, np.int16, np.int32, np.int64,
            np.uint8, np.uint16, np.uint32, np.uint64]:
        eq_(db_type(int_type), "INT")

    for float_type in [float, np.float32, np.float64]:
        eq_(db_type(float), "FLOAT")

    eq_(db_type(str), "TEXT")
