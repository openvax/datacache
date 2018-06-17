# Copyright (c) 2015-2018. Mount Sinai School of Medicine
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Convert from Python type names to sqlite3 column types"""

from __future__ import print_function, division, absolute_import

import numpy as np

_dtype_to_db_type_dict = {
    'int': 'INT',
    'int8': 'INT',
    'int16': 'INT',
    'int32': 'INT',
    'int64': 'INT',

    'uint8': 'INT',
    'uint16': 'INT',
    'uint32': 'INT',
    'uint64': 'INT',

    'bool': 'INT',

    'float': 'FLOAT',
    'float32': 'FLOAT',
    'float64': 'FLOAT',

    'object': 'TEXT',
    'object_': 'TEXT',
    'string_': 'TEXT',
    'str': 'TEXT',
}

def _lookup_type_name(type_name):
    if type_name in _dtype_to_db_type_dict:
        return _dtype_to_db_type_dict[type_name]
    else:
        return None

def _candidate_type_names(python_type_representation):
    """Generator which yields possible type names to look up in the conversion
    dictionary.

    Parameters
    ----------
    python_type_representation : object
        Any Python object which represents a type, such as `int`,
        `dtype('int8')`, `np.int8`, or `"int8"`.
    """
    # if we get a single character code we should normalize to a NumPy type
    # using np.typeDict, which maps string representations of types to NumPy
    # type objects
    if python_type_representation in np.typeDict:
        python_type_representation = np.typeDict[python_type_representation]
        yield python_type_representation.__name__

    # if we get a dtype object i.e. dtype('int16'), then pull out its name
    if hasattr(python_type_representation, 'name'):
        yield python_type_representation.name

    # convert Python types by adding their type's name
    if hasattr(python_type_representation, '__name__'):
        yield python_type_representation.__name__

    # for a dtype like dtype('S3') need to access dtype.type.__name__
    # to get 'string_'
    if hasattr(python_type_representation, 'type'):
        if hasattr(python_type_representation.type, '__name__'):
            yield python_type_representation.type.__name__

    yield str(python_type_representation)

def db_type(python_type_representation):
    """
    Converts from any of:
        (1) Python type
        (2) NumPy/Pandas dtypes
        (3) string names of types
    ...to a sqlite3 type name
    """
    for type_name in _candidate_type_names(python_type_representation):
        db_type_name = _lookup_type_name(type_name)
        if db_type_name:
            return db_type_name
    raise ValueError("Failed to find sqlite3 column type for %s" % (
        python_type_representation))
