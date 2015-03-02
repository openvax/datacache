# Copyright (c) 2015. Mount Sinai School of Medicine
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

from os import remove
from os.path import splitext, split, exists
import logging

from Bio import SeqIO
from typechecks import (
    require_string,
    require_iterable_of
)

from .common import build_path, normalize_filename
from .download import fetch_file, fetch_csv_dataframe
from .database import Database
from .database_table import DatabaseTable
from .database_types import db_type


def connect_if_correct_version(db_path, version):
    """Return a sqlite3 database connection if the version in the database's
    metadata matches the version argument.

    Also implicitly checks for whether the data in this database has
    been completely filled, since we set the version last.

    TODO: Make an explicit 'complete' flag to the metadata.
    """
    db = Database(db_path)
    if db.has_version() and db.version() == version:
        return db.connection
    return None

def _create_cached_db(
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

    Returns sqlite3 connection
    """
    require_string(db_filename, "db_filename")
    require_iterable_of(tables, DatabaseTable)
    if not (subdir is None or isinstance(subdir, str)):
        raise TypeError("Expected subdir to be None or str, got %s : %s" % (
            subdir, type(subdir)))

    try:
        version = int(version)
    except ValueError:
        raise TypeError("Expected version to be int, got %s : %s" % (
            version, type(version)))

    db_path = build_path(db_filename, subdir)

    # if we've already create the table in the database
    # then assuming it's complete/correct and return it
    db = Database(db_path)

    # make sure to delete the database file in case anything goes wrong
    # to avoid leaving behind an empty DB
    table_names = [table.name for table in tables]
    try:
        if db.has_tables(table_names) and \
                db.has_version() and \
                db.version() == version:
            logging.info("Found existing table in database %s", db_path)
        else:
            if len(db.table_names()) > 0:
                logging.info("Dropping tables from database %s: %s",
                    db_path,
                    ", ".join(db.table_names()))
                db.drop_tables()
            logging.info(
                "Creating database %s containing: %s",
                db_path,
                ", ".join(table_names))
            db.create(tables, version)
    except:
        logging.warning(
            "Failed to create tables %s in database %s",
            table_names,
            db_path)
        db.close()
        remove(db_path)
        raise
    return db.connection

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

    fasta_path = fetch_file(
        download_url=download_url,
        filename=fasta_filename,
        subdir=subdir,
        decompress=True)

    fasta_dict = SeqIO.index(fasta_path, 'fasta')
    table = DatabaseTable.from_fasta_dict(
        table_name,
        fasta_dict,
        key_column=key_column,
        value_column=value_column)

    return _create_cached_db(
        db_filename,
        tables=[table],
        subdir=subdir,
        version=version)

def _construct_db_filename(base_filename, df):
    """
    Generate  filename for a DataFrame
    """
    db_filename = base_filename + ("_nrows%d" % len(df))
    for column_name in df.columns:
        column_db_type = db_type(df[column_name].dtype)
        column_name = column_name.replace(" ", "_")
        db_filename += ".%s_%s" % (column_name, column_db_type)
    return db_filename + ".db"

def db_from_dataframes(
        db_filename,
        dataframes,
        primary_keys={},
        indices={},
        subdir=None,
        overwrite=False,
        version=1):
    """Create a sqlite3 database with
    Parameters
    ----------
    db_filename : str
        Name of database file to create

    dataframes : dict
        Dictionary from table names to DataFrame objects

    primary_keys : dict, optional
        Name of primary key column for each table

    indices : dict, optional
        Dictionary from table names to list of column name tuples

    subdir : str, optional

    overwrite : bool, optional
        If the database already exists, overwrite it?

    version : int, optional
    """
    db_path = build_path(db_filename, subdir)

    if overwrite and exists(db_path):
        remove(db_path)

    tables = []
    for table_name, df in dataframes.items():
        table_indices = indices.get(table_name, [])
        primary_key = primary_keys.get(table_name)
        table = DatabaseTable.from_dataframe(
            name=table_name,
            df=df,
            indices=table_indices,
            primary_key=primary_key)
        tables.append(table)
    return _create_cached_db(
        db_path,
        tables=tables,
        subdir=subdir,
        version=version)

def db_from_dataframe(
        db_filename,
        table_name,
        df,
        primary_key=None,
        subdir=None,
        overwrite=False,
        indices=(),
        version=1):
    """
    Given a dataframe `df`, turn it into a sqlite3 database.
    Store values in a table called `table_name`.

    Returns full path to the sqlite database file.
    """
    return db_from_dataframes(
        db_filename=db_filename,
        dataframes={table_name: df},
        primary_keys={table_name: primary_key},
        indices={table_name: indices},
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
    Download a remote CSV file and create a local sqlite3 database
    from its contents
    """
    df = fetch_csv_dataframe(
        download_url=download_url,
        filename=csv_filename,
        subdir=subdir,
        **pandas_kwargs)
    base_filename = splitext(csv_filename)[0]
    if db_filename is None:
        db_filename = _construct_db_filename(base_filename, df)
    return db_from_dataframe(
        db_filename,
        table_name,
        df,
        subdir=subdir,
        version=version)
