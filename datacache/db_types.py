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

 'float32' : 'FLOAT',
 'float64' : 'FLOAT',

 'object' : 'TEXT',
 'object_' : 'TEXT',
 'string_' : 'TEXT'
}

def dtype_to_db_type(dtype):
    """
    Converts from NumPy/Pandas dtype to a sqlite3 type name
    """
    # create list of candidate type names to search through the
    # dictionary of known conversions
    candidates = [dtype]

    # if we get a single character code we should normalize to a NumPy type
    if dtype in np.typeDict:
        dtype = np.typeDict[dtype]
        candidates.append(dtype.__name__)

    #if we get a dtype object i.e. dtype('int16'), then pull out its name
    if hasattr(dtype, 'name'):
        candidates.append(dtype.name)

    # for a dtype like dtype('S3') need to access dtype.type.__name__ to get 'string_'
    if hasattr(dtype, 'type'):
        candidates.append(dtype.type.__name__)

    # convert Python types by adding their type's name
    if hasattr(dtype, '__name__'):
        candidates.append(dtype.__name__)

    candidates.append(str(dtype))

    for candidate_key in candidates:
        if candidate_key in _dtype_to_db_type_dict:
            return _dtype_to_db_type_dict[candidate_key]

    assert False, "Failed to find sqlite3 column type for %s" % dtype