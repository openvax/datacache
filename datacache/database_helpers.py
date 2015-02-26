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
from os.path import splitext, split, exists
import logging

import numpy as np
from Bio import SeqIO
from typechecks import (
    require_string,
    require_integer,
    require_iterable_of
)

from .common import build_path, normalize_filename
from .download import fetch_file, fetch_csv_dataframe


def _create_db(db, tables, version=1):
    """Create a database from dicts that map table names to rows and metadata.

    This function does the actual work of creating a table in the database,
    filling its tables with values, creating indices, and setting the datacache
    version metadata.

    Parameters
    ----------
    db : datacache.Database
        Wrapper around sqlite3 connection to an empty database

    tables : dict
        Dictionary mapping table names to datacache.DatabaseTable objects
    """
    for (table_name, table) in tables.items():
        db.create_table(
            table_name=table_name,
            column_types=table.column_types,
            primary=table.primary,
            nullable=table.nullable)
        db.fill_table(table_name, table.rows())
        db.create_indices(table_name, table.indices)
    db.set_version(version)
    db.close()

def create_cached_db(
        db_filename,
        tables,
        subdir=None,
        version=1):
    """
    Either create or retrieve sqlite database.

    Parameters
    --------

    db_filename : str
        Name of sqlite3 database file

    tables : dict
        Dictionary mapping table names to datacache.DatabaseTable objects

    subdir : str, optional

    version : int, optional
        Version acceptable as cached data.
    """
    require_string(db_filename, "db_filename")
    require_iterable_of(table_names, str)
    if not (subdir is None or isinstance(subdir, str)):
        raise TypeError("Expected subdir to be None or str, got %s : %s" % (
            subdir, type(subdir)))
    require_iterable_of(nullable, str, name="nullable")
    require_iterable_of(indices, tuple, name="indices")
    try:
        version = int(version)
    except ValueError:
        raise TypeError("Expected version to be int, got %s : %s" % (
            version, type(version)))

    db_path = build_path(db_filename, subdir)

    # if we've already create the table in the database
    # then assuming it's complete/correct and return it
    db = Database(db_path)

    table_names = [table.name for table in tables]

    # make sure to delete the database file in case anything goes wrong
    # to avoid leaving behind an empty DB
    try:
        if db.has_tables(table_names) and \
           db.has_version()) and \
           db.version() == version:
            logging.info("Found existing table in database %s", db_path)
        else:
            logging.info(
                "Creating database table %s at %s", table_name, db_path)
            _create_db(db,
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
        fasta_filename=None,
        key_column='id',
        value_column='seq',
        subdir=None,
        version=1):
    """
    Download a FASTA file from `download_url` and store it locally as a sqlite3 database.
    """

    base_filename = normalize_filename(split(download_url)[1])
    db_filename = "%s.%s.%s.db" % (base_filename, key_column, value_column)

    def load_data():
        fasta_path = fetch_file(
            download_url=download_url,
            filename=fasta_filename,
            subdir=subdir,
            decompress=True)

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
            in fasta_dict.items()
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
    Generate  filename for a DataFrame
    """
    db_filename = base_filename + ("_nrows%d" % len(df))
    for col_name in df.columns:
        col_db_type = dtype_to_db_type(col.dtype)
        col_name = col_name.replace(" ", "_")
        db_filename += ".%s_%s" % (col_name, col_db_type)
    return db_filename + ".db"

def db_from_dataframes(
        db_filename,
        dataframes_dict,
        key_column_names={}
        indices_dict={},
        subdir=None,
        overwrite=False,
        version=1):
    """Create a sqlite3 database with
    Parameters
    ----------
    db_filename : str
        Name of database file to create

    dataframes_dict : dict
        Dictionary from table names to DataFrame objects

    key_column_names : dict, optional
        Name of primary key column for each table

    indices_dict : dict, optional
        Dictionary from table names to list of column name tuples

    subdir : str, optional

    overwrite : bool, optional
        If the database already exists, overwrite it?

    version : int, optional
    """
    db_path = build_path(db_filename, subdir)

    if overwrite and exists(db_path):
        remove(db_path)

    tables = {}
    for table_name, df in dataframes.items():
        table_indices = indices_dict.get(table_name, [])
        primary_key = key_column_names.get(table_name)
        table = DatabaseTable.from_dataframe(
            name=table_name,
            df=df,
            indices=table_indices,
            primary_key=primary)
        tables[table_name] = table

    return create_cached_db(
        db_path,
        tables,
        subdir=subdir,
        version=version)

def db_from_dataframe(
        db_filename,
        table_name,
        df,
        key_column_name=None,
        subdir=None,
        overwrite=False,
        indices=(),
        version=1):
    """
    Given a dataframe `df`, turn it into a sqlite3 database.
    Store values in a table called `table_name`.
    """
    return db_from_dataframes(
        db_filename=db_filename,
        dataframes={table_name : df},
        key_column_names={table_name : key_column_name},
        indices={table_name : indices},
        subdir=subdir,
        overwrite=overwrite,
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
        download_url=download_url,
        filename=csv_filename,
        subdir=subdir,
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
