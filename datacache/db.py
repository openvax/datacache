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
    query = """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='%s'""" % table_name
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

def _set_database_version(db, version):
    """
    Create metadata table for database with version number.

    Parameters
    ----------
    version : int
        Tag created database with user-specified version number
    """
    if not isinstance(version, int):
        raise TypeError("Version must be integer, not %s : %s" % (
            version, type(version)))
    create_metadata = \
        "CREATE TABLE %s (version INT)" % METADATA_COLUMN_NAME
    execute_sql(db, create_metadata)
    insert_version = \
        "INSERT INTO %s VALUES (%s)" % (METADATA_COLUMN_NAME, version)
    execute_sql(db, insert_version)


def _create_table(
        db,
        table_name,
        col_types,
        primary_key=None,
        nullable=[]):
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

    """
    if not isinstance(table_name, str):
        raise TypeError("Table name must be str, not %s : %s" % (
            table_name, type(table_name)))
    if not isinstance(nullable, (list, tuple, set)):
        raise TypeError("Nullable must be a list|tuple|set, not %s : %s" % (
            nullable, type(nullable)))

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
    execute_sql(db, create_data_table)

def _fill_table(db, table_name, rows):
    if not isinstance(table_name, str):
        raise TypeError(
            "Expected table to be str, got %s : %s" % (
                table_name, type(table_name)))
    if len(rows) == 0:
        raise ValueError("Rows must be non-empty sequence")
    if not db_table_exists(db, table_name):
        raise ValueError(
            "Table '%s' does not exist in database" % (table_name,))

    first_row = rows[0]

    n_columns = len(first_row)
    blank_slots = ", ".join("?" for _ in xrange(n_columns))
    logging.info("Inserting %d rows into table %s", len(rows), table_name)
    db.executemany(
        "insert into %s values (%s)" % (table_name, blank_slots),
        rows)

def _create_db_indices(db, table_name, indices):
    if not isinstance(table_name, str):
        raise TypeError(
            "Expected table_name to be str, got %s : %s" % (table_name, str))
    if not isinstance(indices, (list, tuple, set)):
        raise TypeError(
                "Expected indices to be sequence (list|tuple|set), got %s : %s" % (
                indices,
                type(indices)))

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
                ", ".join(index_col_set)))

def _create_db(
        db,
        table_name,
        col_types,
        key_column_name,
        nullable,
        rows,
        indices,
        version):
    """
    Do the actual work of creating a table in the database, filling it with
    values, creating indices, and setting the datacache version metadata.
    """
    _create_table(
        db,
        table_name,
        col_types,
        primary_key=key_column_name,
        nullable=nullable)
    _fill_table(db, table_name, rows)
    _create_db_indices(db, table_name, indices)
    _set_database_version(db, version)
    db.commit()

def create_cached_db(
        db_filename,
        table_name,
        fn,
        subdir = None,
        nullable = [],
        indices = [],
        version=1):
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

    version : int, optional
        Version acceptable as cached data.

    """
    if not isinstance(db_filename, str):
        raise TypeError("Expected db_filename to be str, got %s : %s" % (
            db_filename, type(db_filename)))
    if not isinstance(table_name, str):
        raise TypeError("Expected table_name to be str, got %s : %s" % (
            table_name, type(table_name)))
    if not (subdir is None or isinstance(subdir, str)):
        raise TypeError("Expected subdir to be None or str, got %s : %s" % (
            subdir, type(subdir)))
    if not isinstance(indices, (list, tuple, set)):
        raise TypeError(
            "Expected indices to be sequence (list|tuple|set), got %s : %s" % (
                indices, type(indices)))
    if not isinstance(version, (int, long)):
        raise TypeError("Expected version to be int, got %s : %s" % (
            version, type(version)))

    db_path = build_path(db_filename, subdir)

    # if we've already create the table in the database
    # then assuming it's complete/correct and return it
    db = sqlite3.connect(db_path)

    # make sure to delete the database file in case anything goes wrong
    # to avoid leaving behind an empty DB
    try:
        if db_table_exists(db, table_name) and \
           db_has_version(db) and \
           db_version(db) == version:
            logging.info("Found existing table in database %s", db_path)
        else:
            logging.info(
                "Creating database table %s at %s", table_name, db_path)
            # col_types is a list of (name, type) pairs
            col_types, rows, key_column_name = fn()
            _create_db(
                db,
                table_name,
                col_types,
                key_column_name,
                nullable,
                rows,
                indices,
                version)
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
        version = 1):
    """
    Download a FASTA file from `download_url` and store it locally as a sqlite3 database.
    """

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
        version=version)

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
        version=1):
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
        version=version)

def fetch_csv_db(
        table_name,
        download_url,
        csv_filename=None,
        db_filename=None,
        subdir=None,
        version=1,
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
        version=version)