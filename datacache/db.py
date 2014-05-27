# Copyright (c) 2014. Mount Sinai School of Medicine
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

import sqlite3
from os import remove
from os.path import (splitext, split)

import numpy as np
from Bio import SeqIO

from common import build_path
from download import fetch_file, fetch_csv_dataframe


def db_table_exists(db, table_name):
    query = \
        "SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % \
        table_name
    cursor = db.execute(query)
    results = cursor.fetchmany()
    return len(results) > 0


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
 
 'float32' : 'FLOAT', 
 'float64' : 'FLOAT',

 'object' : 'TEXT', 
 'object_' : 'TEXT', 
 'string_' : 'TEXT'
}

def _dtype_to_db_type(dtype):
    """
    Converts from NumPy/Pandas dtype to a sqlite3 type name
    """

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

def create_db(db_path, table_name, col_types, rows):
    """
    Creates a sqlite3 database from the given Python values. 

    Parameters
    ----------

    db_path : str 
        Name of sqlite3 file to create

    col_types : list of (str, str) pairs
        First element of each tuple is the column name, second element is the sqlite3 type

    rows : list of tuples
        Must have as many elements in each tuple as there were col_types
    """
    db = sqlite3.connect(db_path)

    # make sure to delete the database file in case anything goes wrong
    # to avoid leaving behind an empty DB
    try:
        # if we've already create the table in the database
        # then assuming it's complete/correct and return it
        if db_table_exists(db, table_name):
            return db
        col_type_str = ", ".join("%s %s" % (col_name,t) for col_name, t in col_types)
        create = \
            "create table %s (%s)" % (table_name, col_type_str)
        db.execute(create)
        
        blank_slots = ", ".join("?" for _ in col_types)
        db.executemany("insert into %s values (%s)" % (table_name, blank_slots), rows)
        db.commit()
    except:
        db.close()
        remove(db_path)
        raise
    return db

def fetch_fasta_db(
        table_name,
        download_url,
        fasta_filename = None, 
        key_column = 'id',
        value_column = 'seq',
        subdir = None):
    """
    Download a FASTA file from `download_url` and store it locally as a sqlite3 database. 
    """

    fasta_path = fetch_file(
        download_url = download_url, 
        filename = fasta_filename, 
        subdir = subdir)
    fasta_dict = SeqIO.index(fasta_path, 'fasta')


    base_filename = split(fasta_path)[1]
    db_filename = "%s.%s.%s.db" % (base_filename, key_column, value_column)
    db_path = build_path(db_filename, subdir)

    col_types = [(key_column, "TEXT"), (value_column, "TEXT")]
    rows = [
        (idx, str(record.seq))
        for (idx, record)
        in fasta_dict.iteritems()
    ]

    return create_db(db_path, table_name, col_types, rows)


def db_from_dataframe(base_filename, table_name, df, subdir = None):
    """
    Given a dataframe `df`, turn it into a sqlite3 database. 
    Use `base_filename` as the root of the local db filename and store
    values in a table called `table_name`. 
    """
    db_filename = base_filename
    col_types = []
    for col_name in df.columns:
        col = df[col_name]
        col_db_type = _dtype_to_db_type(col.dtype) 
        col_name = col_name.replace(" ", "_")
        col_types.append( (col_name, col_db_type) )
        db_filename += ".%s_%s" % (col_name, col_db_type)
    db_filename += ".db"
    db_path = build_path(db_filename, subdir)
    rows = list(tuple(row) for row in df.values)
    return create_db(db_path, table_name, col_types, rows)

def fetch_csv_db(table_name, download_url, csv_filename = None, subdir = None, **pandas_kwargs):
    """
    Download a remote CSV file and create a local sqlite3 database from its contents
    """
    df = fetch_csv_dataframe(
        download_url = download_url, 
        filename = csv_filename, 
        subdir = subdir, 
        **pandas_kwargs)
    base_filename = splitext(csv_filename)[0]
    return db_from_dataframe(base_filename, table_name, df, subdir = subdir)