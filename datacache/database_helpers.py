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

from sqlalchemy_utils import database_exists, drop_database
from typechecks import (
    require_string,
    require_integer,
    require_iterable_of
)

from .common import (build_path, normalize_filename, get_db_name,
                     build_sqlite_url)
from .download import fetch_file, fetch_csv_dataframe
from .database import Database
from .database_table import DatabaseTable
from .database_types import db_type


def connect_if_correct_version(db_url, collection_name, subdir, version):
    """Return a database connection if the version in the database's
    metadata matches the version argument.

    Also implicitly checks for whether the data in this database has
    been completely filled, since we set the version last.

    Fall back to a sqlite DB file if no DB URL (None) is provided.

    TODO: Make an explicit 'complete' flag to the metadata.
    """
    # Fall back to a sqlite DB file if no DB URL (None) is provided
    db_url = db_url if db_url else build_sqlite_url(
        collection_name, subdir)

    if not exists(db_url):
        return None

    db = Database(db_url=db_url, collection_name=collection_name)
    if db.has_version() and db.version() == version:
        return db.connection
    return None


def exists(db_url):
    try:
        return database_exists(db_url)
    except Exception as e:
        logging.warning(
            "Failed to check if db_url \"%s\" exists with error: %s" %
            (db_url, e.message))
        return False


def _create_cached_db(
        collection_name,
        tables,
        db_url=None,
        overwrite=False,
        subdir=None,
        version=1):
    """
    Either create or retrieve a database.

    Parameters
    --------
    collection_name : str
        A name describing the set of data, e.g.
        Homo_sapiens.GRCh37.62

    db_url : str
        Database connection description: see
        http://docs.sqlalchemy.org/en/latest/core/engines.html
        #database-urls
        Fall back to a sqlite DB file if no DB URL is provided

    tables : iterable
        Iterable of datacache.DatabaseTable objects

    overwrite : bool, optional
        Overwrite existing tables?

    subdir : str, optional

    version : int, optional
        Version acceptable as cached data.

    Returns a database connection
    """
    if db_url is not None:
        require_string(db_url, "db_url")
    require_iterable_of(tables, DatabaseTable)
    if not (subdir is None or isinstance(subdir, str)):
        raise TypeError("Expected subdir to be None or str, got %s : %s" % (
            subdir, type(subdir)))
    require_integer(version, "version")

    # Fall back to a sqlite DB file if no DB URL (None) is provided
    is_db_url_fallback = db_url is None
    db_url = db_url if db_url else build_sqlite_url(
        collection_name, subdir)
    db_name = get_db_name(db_url)

    # If the database is specific to this collection, drop the whole
    # database.
    if overwrite and exists(db_url):
        if db_name == collection_name:
            drop_database(db_url)

    # If the database doesn't already exist and we encounter an error
    # later, delete the database before raising an exception
    drop_db_on_error = is_db_url_fallback and not exists(db_url)

    # If the database already exists, contains all the table
    # names and has the right version, then just return it
    db = Database(db_url, collection_name)

    # Make sure to delete the database in case anything goes wrong
    # to avoid leaving behind an empty DB
    table_names = [table.name for table in tables]
    try:
        if (db.has_tables(table_names) and
            db.has_version() and
            db.version() == version):
            logging.info("Found existing tables in database %s", db_name)
        else:
            logging.info(
                "Creating database %s containing: %s",
                db_name,
                ", ".join(table_names))
            db.create(tables=tables, overwrite=overwrite,
                      version=version)
    except Exception:
        logging.warning(
            "Failed to create tables %s in database %s",
            table_names,
            db_name)
        db.close()
        if drop_db_on_error:
            drop_database(db_url)
        raise
    return db.connection


def fetch_fasta_db(
        table_name,
        download_url,
        db_url=None,
        fasta_filename=None,
        key_column='id',
        value_column='seq',
        subdir=None,
        overwrite=False,
        version=1):
    """
    Download a FASTA file from `download_url` and store it as
    a new database or within an existing database.

    Fall back to a sqlite DB file if no DB URL is provided
    """
    base_filename = normalize_filename(split(download_url)[1])
    collection_name = "%s.%s.%s" % (base_filename, key_column,
                                    value_column)

    fasta_path = fetch_file(
        download_url=download_url,
        filename=fasta_filename,
        subdir=subdir,
        decompress=True)
    fasta_dict = SeqIO.index(fasta_path, 'fasta')

    table = DatabaseTable.from_fasta_dict(
        name=build_table_name(table_name, collection_name),
        fasta_dict=fasta_dict,
        key_column=key_column,
        value_column=value_column)

    return _create_cached_db(
        collection_name=collection_name,
        db_url=db_url,
        tables=[table],
        subdir=subdir,
        overwrite=overwrite,
        version=version)


def db_from_dataframes(
        collection_name,
        dataframes,
        db_url=None,
        primary_keys={},
        indices={},
        subdir=None,
        overwrite=False,
        version=1):
    """Create or load into a database with
    Parameters
    ----------
    collection_name : str
        A name describing all these dataframes, e.g.
        Homo_sapiens.GRCh37.62

    dataframes : dict
        Dictionary from table names (before concatentation with
        collection_name) to DataFrame objects

    db_url : str, optional
        Database connection description: see
        http://docs.sqlalchemy.org/en/latest/core/engines.html
        #database-urls
        Fall back to a sqlite DB file if no DB URL is provided

    primary_keys : dict, optional
        Name of primary key column for each table

    indices : dict, optional
        Dictionary from table names (before concatentation with
        collection_name) to list of column name tuples

    subdir : str, optional

    overwrite : bool, optional
        If the database already exists, and its name matches
        the collection name, overwrite it

    version : int, optional
    """
    tables = []
    for table_name, df in dataframes.items():
        table_indices = indices.get(table_name, [])
        primary_key = primary_keys.get(table_name)
        table = DatabaseTable.from_dataframe(
            name=build_table_name(table_name, collection_name),
            df=df,
            indices=table_indices,
            primary_key=primary_key)
        tables.append(table)

    return _create_cached_db(
        collection_name=collection_name,
        db_url=db_url,
        tables=tables,
        subdir=subdir,
        version=version)


def db_from_dataframe(
        collection_name,
        df,
        table_name,
        db_url=None,
        primary_key=None,
        indices=(),
        subdir=None,
        overwrite=False,
        version=1):
    """
    Given a dataframe `df`, turn it into a database.
    Store values in a table prefixed `table_name` (concatentaed
    with `collection_name`; see db_from_dataframes).

    Returns a database connection.
    """
    return db_from_dataframes(
        collection_name=collection_name,
        dataframes={table_name: df},
        db_url=db_url,
        primary_keys={table_name: primary_key},
        indices={table_name: indices},
        subdir=subdir,
        overwrite=overwrite,
        version=version)


def fetch_csv_db(
        table_name,
        download_url,
        csv_filename=None,
        db_url=None,
        subdir=None,
        version=1,
        **pandas_kwargs):
    """
    Download a remote CSV file and create (or populate) a database
    from its contents.

    If db_url is not provided, a new sqlite DB is created.
    """
    df = fetch_csv_dataframe(
        download_url=download_url,
        filename=csv_filename,
        subdir=subdir,
        **pandas_kwargs)
    base_filename = splitext(csv_filename)[0]

    def get_collection_name(base_filename, df):
        """
        Generate a collection name for a database we're going to
        fill with the contents of a DataFrame, using the DataFrame's
        column names and types.

        This will prefix the table in the database, and will also
        serve as the filename if the database is sqlite.
        """
        collection_name = base_filename + ("_nrows%d" % len(df))
        for column_name in df.columns:
            column_db_type = db_type(df[column_name].dtype)
            column_name = column_name.replace(" ", "_")
            collection_name += ".%s_%s" % (column_name, column_db_type)
        return collection_name
    collection_name = get_collection_name(base_filename, df)

    return db_from_dataframe(
        collection_name=collection_name,
        df=df,
        table_name=table_name,
        db_url=db_url,
        subdir=subdir,
        version=version)


def build_table_name(table_name_prefix, collection_name):
    return "%s_%s" % (table_name_prefix, collection_name)
