"""Convert from Python type names to sqlite3 column types"""

import numpy as np
from typechecks import is_string


_dtype_to_db_type_dict = {
 'int' : 'INT',
 'int8' : 'INT',
 'int16' : 'INT',
 'int32' : 'INT',
 'int64' : 'INT',

 'uint8' : 'INT',
 'uint16' : 'INT',
 'uint32' : 'INT',
 'uint64' : 'INT',

 'bool' : 'INT',

 'float' : 'FLOAT',
 'float32' : 'FLOAT',
 'float64' : 'FLOAT',

 'object' : 'TEXT',
 'object_' : 'TEXT',
 'string_' : 'TEXT',
 'str' : 'TEXT',
}

def _lookup_type_name(type_name):
    if type_name in _dtype_to_db_type_dict:
        return _dtype_to_db_type_dict[type_name]
    else:
        return None

def _candidate_type_names(python_type):
    """Generator which yields possible type names to look up in the conversion
    dictionary
    """
    # create list of candidate type names to search through the
    # dictionary of known conversions
    if is_string(python_type):
        yield python_type

    # if we get a single character code we should normalize to a NumPy type
    if python_type in np.typeDict:
        python_type = np.typeDict[python_type]
        yield python_type.__name__

    #if we get a dtype object i.e. dtype('int16'), then pull out its name
    if hasattr(python_type, 'name'):
        candidates.add(python_type.name)

    # convert Python types by adding their type's name
    if hasattr(python_type, '__name__'):
        yield python_type.__name__

    # for a dtype like dtype('S3') need to access dtype.type.__name__
    # to get 'string_'
    if hasattr(python_type, 'type') and hasattr(python_type.type, '__name__'):
        yield python_type.type.__name__

    yield str(dtype)

def db_type(python_type):
    """
    Converts from Python type or NumPy/Pandas dtype to a sqlite3 type name
    """

    for candidate_key in _candidate_type_names(python_type):


    assert False, "Failed to find sqlite3 column type for %s" % dtype