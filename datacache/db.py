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
from os.path import (splitext, split, exists)
import logging

import numpy as np
from Bio import SeqIO

from common import build_path, normalize_filename
from download import fetch_file, fetch_csv_dataframe

METADATA_COLUMN_NAME = "_datacache_metadata"


def db_table_exists(db, table_name):
    """
    Does a table named `table_name` exist in the sqlite database `db`?
    """
    query = \
        "SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % \
        table_name
    cursor = db.execute(query)
    results = cursor.fetchmany()
    return len(results) > 0


def db_has_version(db):
    return db_table_exists(db, METADATA_COLUMN_NAME)

def db_version(db):
    query =  "SELECT version FROM %s" % METADATA_COLUMN_NAME
    cursor = db.execute(query)
    version = cursor.fetchone()
    if not version:
        return 0
    else:
        return int(version[0])

def db_has_min_versoin(db, min_version):
    if not db_has_version(db):
        return False
    version = db_version(db)
    return version >= min_version

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

def execute_sql(db, sql, commit=False):
    logging.info("Running sqlite query: \"%s\"", sql)
    db.execute(sql)
    if commit:
        db.commit()

def create_table(
        db,
        table_name,
        col_types,
        primary_key=None,
        nullable=[],
        version=1):
    """
    Creates a sqlite3 database from the given Python values.

    Parameters
    ----------

    db : sqlite3 database

    col_types : list of (str, str) pairs
        First element of each tuple is the column name, second element is the sqlite3 type

    rows : list of tuples
        Must have as many elements in each tuple as there were col_types

    primary_key : str, optional
        Which column is the primary key

    nullable : set/list, optional
        Any column in this collection can have null values

    version : int
        Tag created database with user-specified version number
    """
    assert isinstance(version, int), \
        "Version must be integer, not %s : %s" % (version, type(version))
    assert isinstance(table_name, str), \
        "Table name must be str, not %s : %s" % (table_name, type(table_name))
    assert isinstance(nullable, (list, tuple, set)), \
        "Nullable must be a list|tuple|set, not %s : %s" % (
            nullable, type(nullable))

    create_metadata = \
        "CREATE TABLE %s (version INT)" % METADATA_COLUMN_NAME
    execute_sql(db, create_metadata, commit=False)
    insert_version = \
        "INSERT INTO %s VALUES (%s)" % (METADATA_COLUMN_NAME, version)
    execute_sql(db, insert_version, commit=False)

    col_decls = []
    for col_name, t in col_types:
        decl = "%s %s" % (col_name,t)
        if col_name == primary_key:
            decl += " UNIQUE PRIMARY KEY"
        if col_name not in nullable:
            decl += " NOT NULL"
        col_decls.append(decl)
    col_decl_str = ", ".join(col_decls)
    create_data_table = \
        "create table %s (%s)" % (table_name, col_decl_str)
    execute_sql(db, create_data_table, commit=True)

def fill_table(db, table_name, rows):
    assert isinstance(table_name, str), \
        "Expected table to be str, got %s : %s" % (table_name, type(table_name))
    assert len(rows) > 0, "Rows must be non-empty sequence"
    assert db_table_exists(db, table_name), \
        "Table '%s' does not exist in database" % (table_name,)

    first_row = rows[0]

    n_columns = len(first_row)
    blank_slots = ", ".join("?" for _ in xrange(n_columns))
    logging.info("Inserting %d rows into table %s", len(rows), table_name)
    db.executemany(
        "insert into %s values (%s)" % (table_name, blank_slots),
        rows)
    db.commit()

def create_db_indices(db, table_name, indices):
    assert isinstance(table_name, str), \
        "Expected table_name to be str, got %s : %s" % (table_name, str)
    assert isinstance(indices, (list, tuple, set)), \
        "Expected indices to be sequence (list|tuple|set), got %s : %s" % (
            indices,
            type(indices)
        )
    for i, index_col_set in enumerate(indices):
        logging.info("Creating index on %s (%s)" % (
            table_name,
            ", ".join(index_col_set)
        ))
        index_name = "index%d_%s" % (i, "_".join(index_col_set))
        db.execute(
            "CREATE INDEX IF NOT EXISTS %s ON %s (%s)" % (
                index_name,
                table_name,
                ", ".join(index_col_set)
            )
        )
    db.commit()

def create_cached_db(
        db_filename,
        table_name,
        fn,
        subdir = None,
        nullable = [],
        indices = [],
        version=1,
        min_version=None):
    """
    Either create or retrieve sqlite database.

    Parameters
    --------

    db_filename : str

    table_name : str

    fn : function
        Returns (rows, col_types, key_column_name)

    subdir : str, optional

    nullable : list/seq

    indices : list of string tuples

    version : int

    min_version : int, optional
        Last previous version acceptable as cached data.

    """
    assert isinstance(db_filename, str), \
        "Expected db_filename to be str, got %s : %s" % (
            db_filename,
            type(db_filename)
        )
    assert isinstance(table_name, str), \
        "Expected table_name to be str, got %s : %s" % (
            table_name,
            type(table_name),
        )
    assert subdir is None or isinstance(subdir, str), \
        "Expected subdir to be None or str, got %s : %s" % (
            subdir, type(subdir)
        )

    assert isinstance(indices, (list, tuple, set)), \
        "Expected indices to be sequence (list|tuple|set), got %s : %s" % (
            indices,
            type(indices)
        )
    assert isinstance(version, (int, long)), \
        "Expected version to be int, got %s : %s" % (version, type(version))

    if min_version is None:
        min_version = version
    else:
        assert isinstance(min_version, (int, long)), \
            "Expected min_version to be int, got %s : %s" % (
                min_version, type(min_version)
            )

    db_path = build_path(db_filename, subdir)

    # if we've already create the table in the database
    # then assuming it's complete/correct and return it
    db = sqlite3.connect(db_path)

    # make sure to delete the database file in case anything goes wrong
    # to avoid leaving behind an empty DB
    try:
        if db_table_exists(db, table_name) and \
           db_has_version(db) and \
           db_version(db) >= min_version:
            logging.info("Found existing table in database %s", db_path)
        else:
            logging.info(
                "Creating database table %s at %s", table_name, db_path)
            # col_types is a list of (name, type) pairs
            col_types, rows, key_column_name = fn()

            create_table(
                db, table_name, col_types,
                primary_key=key_column_name,
                nullable=nullable)
            fill_table(db, table_name, rows)
            create_db_indices(db, table_name, indices)
    except:
        logging.warning(
            "Failed to create table %s in database %s",
            table_name, db_path)
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
        subdir = None,
        version = 1,
        min_version = None,):
    """
    Download a FASTA file from `download_url` and store it locally as a sqlite3 database.
    """

    if min_version is None:
        min_version = version

    base_filename = normalize_filename(split(download_url)[1])
    db_filename = "%s.%s.%s.db" % (base_filename, key_column, value_column)

    def load_data():
        fasta_path = fetch_file(
            download_url = download_url,
            filename = fasta_filename,
            subdir = subdir,
            decompress = True)

        fasta_dict = SeqIO.index(fasta_path, 'fasta')
        key_list = list(fasta_dict.keys())
        key_set = set(key_list)
        assert len(key_set) == len(key_list), \
            "FASTA file from %s contains %d non-unique sequence identifiers" % \
            (download_url, len(key_list) - len(key_set))
        col_types = [(key_column, "TEXT"), (value_column, "TEXT")]
        rows = [
            (idx, str(record.seq))
            for (idx, record)
            in fasta_dict.iteritems()
        ]
        return col_types, rows, key_column

    return create_cached_db(
        db_filename,
        table_name,
        fn=load_data,
        subdir=subdir,
        version=version,
        min_version=min_version,)

def construct_db_filename(base_filename, df):
    """
    Generate  filename for a DataFeame
    """
    db_filename = base_filename + ("_nrows%d" % len(df))
    for col_name in df.columns:
        col_db_type = dtype_to_db_type(col.dtype)
        col_name = col_name.replace(" ", "_")
        db_filename += ".%s_%s" % (col_name, col_db_type)
    return db_filename + ".db"

def db_from_dataframe(
        db_filename,
        table_name,
        df,
        key_column_name = None,
        subdir = None,
        overwrite = False,
        indices = [],
        version=1,
        min_version=None):
    """
    Given a dataframe `df`, turn it into a sqlite3 database.
    Store values in a table called `table_name`.
    """
    db_path = build_path(db_filename, subdir)

    if overwrite and exists(db_path):
        remove(db_path)

    col_types = []
    nullable = set([])

    # tag cached database by dataframe's number of rows and columns
    for col_name in df.columns:
        col = df[col_name]
        if col.isnull().any():
            nullable.add(col_name)
        col_db_type = dtype_to_db_type(col.dtype)
        col_name = col_name.replace(" ", "_")
        col_types.append( (col_name, col_db_type) )

    def create_rows():
        rows = list(tuple(row) for row in df.values)
        return col_types, rows, key_column_name

    return create_cached_db(
        db_path,
        table_name,
        fn=create_rows,
        subdir=subdir,
        nullable=nullable,
        indices=indices,
        version=version,
        min_version=min_version)

def fetch_csv_db(
        table_name,
        download_url,
        csv_filename=None,
        db_filename=None,
        subdir=None,
        version=1,
        min_version=None,
        **pandas_kwargs):
    """
    Download a remote CSV file and create a local sqlite3 database from its contents
    """
    df = fetch_csv_dataframe(
        download_url = download_url,
        filename = csv_filename,
        subdir = subdir,
        **pandas_kwargs)
    base_filename = splitext(csv_filename)[0]
    if db_filename is None:
        db_filename = construct_db_filename(base_filename, df)
    return db_from_dataframe(
        db_filename,
        table_name,
        df,
        subdir=subdir,
        version=version,
        min_version=min_version)