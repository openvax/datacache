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

from __future__ import print_function, division, absolute_import

import hashlib
import os
import errno
import re
import stat
from os.path import splitext, lexists
import logging

from typechecks import (
    require_string,
    require_integer,
    require_iterable_of
)

from .common import resolve_path, build_local_filename
from .download import (
    fetch_csv_dataframe, _open_staging_file, _normal_creation_mode,
    _remove_staging_file,
)
from .database import Database, fold_identifier
from .database_table import DatabaseTable
from .database_types import db_type
from .inspection import path_exists


logger = logging.getLogger(__name__)

# Longest file or directory name, in bytes, on common local filesystems.
_MAX_NAME_BYTES = 255


def connect_if_correct_version(db_path, version, *, read_only=False):
    """Return a sqlite3 database connection if the version in the database's
    metadata matches the version argument.

    Also implicitly checks for whether the data in this database has
    been completely filled, since we set the version last.

    Missing files and version mismatches return None, without creating files.
    The caller owns the returned connection. Use read_only=True to explicitly
    open a shared installation without write access.
    """
    try:
        os.stat(db_path)
    except FileNotFoundError:
        return None
    db = Database(db_path, must_exist=True, read_only=read_only)
    try:
        if db.has_version() and db.version() == version:
            return db.connection
    except BaseException:
        db.connection.close()
        raise
    db.connection.close()
    return None


def _cached_connection(db_path, table_names, version):
    """Reuse a matching installation before validating a new build's inputs."""
    require_integer(version, "version")
    connection = connect_if_correct_version(db_path, version)
    if connection is None:
        return None
    try:
        existing = {fold_identifier(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if all(fold_identifier(name) in existing for name in table_names):
            return connection
    except BaseException:
        connection.close()
        raise
    connection.close()
    return None


def _database_target_path(db_path):
    """Follow final symlinks so publication creates their target, not a new link.

    Keep parent components intact: normalizing ``symlink/..`` or ``missing/..``
    lexically could select a different directory than the filesystem would.
    """
    original = db_path
    for depth in range(41):
        try:
            info = os.lstat(db_path)
        except FileNotFoundError:
            return db_path
        if not stat.S_ISLNK(info.st_mode):
            return db_path
        if depth == 40:
            break
        target = os.readlink(db_path)
        db_path = target if os.path.isabs(target) else os.path.join(os.path.dirname(db_path), target)
    raise OSError(errno.ELOOP, "Too many symbolic links", original)


def _create_cached_db(
        db_path,
        tables,
        version=1,
        *,
        overwrite=False,
        show_progress=False):
    """
    Either create or retrieve sqlite database.

    Parameters
    --------
    db_path : str
        Path to sqlite3 database file

    tables : iterable
        datacache.DatabaseTable objects

    version : int, optional
        Version acceptable as cached data.

    Returns sqlite3 connection
    """
    db_path = os.fspath(db_path)
    require_string(db_path, "db_path")
    tables = list(tables)
    require_iterable_of(tables, DatabaseTable)
    require_integer(version, "version")

    table_names = [table.name for table in tables]
    if not all(isinstance(name, str) and name for name in table_names):
        raise ValueError("Database table names must be non-empty strings")
    if not tables or len({name.lower() for name in table_names}) != len(table_names):
        raise ValueError("Database requires non-empty, distinct table names")
    if any(name.lower().startswith("sqlite_") or name.lower() == "_datacache_metadata"
           for name in table_names):
        raise ValueError("Database table name is reserved for metadata")

    db_path = _database_target_path(db_path)
    if not lexists(db_path):
        # Readers never see an empty/partial new database. Hard-link publication
        # is atomic and does not clobber a concurrent creator's database.
        directory = os.path.dirname(db_path) or "."
        staged_path = None
        db = None
        try:
            with _open_staging_file(directory=directory, suffix=".db") as staged:
                staged_path = staged.name
            db = Database(staged_path)
            db.create(tables, version, show_progress=show_progress)
            db.connection.close()
            db = None
            os.chmod(staged_path, _normal_creation_mode(directory))
            try:
                os.link(staged_path, db_path)
            except FileExistsError:
                pass  # Another creator won; use the existing-file path below.
            else:
                return Database(db_path, must_exist=True).connection
        finally:
            if db is not None:
                db.connection.close()
            if staged_path is not None:
                _remove_staging_file(staged_path)

    db = Database(db_path, must_exist=True)

    def reusable():
        return (not overwrite and db.has_tables(table_names) and
                db.has_version() and db.version() == version)

    try:
        if reusable():
            logger.info("Found existing table in database %s", db_path)
        else:
            # Serialize rebuilds and recheck after acquiring SQLite's write
            # lock. Never unlink an existing database or commit drops alone.
            with db.connection:
                db.connection.execute("BEGIN IMMEDIATE")
                if not reusable():
                    db.drop_all_tables(commit=False, include_views=overwrite)
                    logger.info("Creating database %s containing: %s", db_path, ", ".join(table_names))
                    db.create(tables, version, show_progress=show_progress)
    except BaseException:
        logger.warning(
            "Failed to create tables %s in database %s",
            table_names,
            db_path)
        db.connection.rollback()
        db.connection.close()
        raise
    return db.connection

def build_tables(
        table_names_to_dataframes,
        table_names_to_primary_keys=None,
        table_names_to_indices=None):
    """
    Parameters
    ----------
    table_names_to_dataframes : dict
        Dictionary mapping each table name to a DataFrame

    table_names_to_primary_keys : dict
        Dictionary mapping each table to its primary key

    table_names_to_indices : dict
        Dictionary mapping each table to a set of indices

    Returns list of DatabaseTable objects
    """
    table_names_to_primary_keys = table_names_to_primary_keys or {}
    table_names_to_indices = table_names_to_indices or {}
    tables = []
    for table_name, df in table_names_to_dataframes.items():
        table_indices = table_names_to_indices.get(table_name, [])
        primary_key = table_names_to_primary_keys.get(table_name)
        table = DatabaseTable.from_dataframe(
            name=table_name,
            df=df,
            indices=table_indices,
            primary_key=primary_key)
        tables.append(table)
    return tables

def db_from_dataframes_with_absolute_path(
        db_path,
        table_names_to_dataframes,
        table_names_to_primary_keys=None,
        table_names_to_indices=None,
        overwrite=False,
        version=1,
        *,
        show_progress=False):
    """
    Create a sqlite3 database from a collection of DataFrame objects.

    Return an open connection owned by the caller. The parent directory must
    already exist. Rebuilds, including overwrite=True, roll back on failure;
    existing databases are not unlinked. show_progress enables tqdm row counts.

    Parameters
    ----------
    db_path : str
        Path to database file to create

    table_names_to_dataframes : dict
        Dictionary from table names to DataFrame objects

    table_names_to_primary_keys : dict, optional
        Name of primary key column for each table

    table_names_to_indices : dict, optional
        Dictionary from table names to list of column name tuples

    overwrite : bool, optional
        If the database already exists, overwrite it?

    version : int, optional
    """
    if not overwrite:
        connection = _cached_connection(db_path, table_names_to_dataframes, version)
        if connection is not None:
            return connection
    tables = build_tables(
        table_names_to_dataframes,
        table_names_to_primary_keys,
        table_names_to_indices)
    return _create_cached_db(
        db_path,
        tables=tables,
        version=version,
        overwrite=overwrite,
        show_progress=show_progress)

def db_from_dataframes(
        db_filename,
        dataframes,
        primary_keys=None,
        indices=None,
        subdir=None,
        overwrite=False,
        version=1,
        *,
        cache_root=None,
        show_progress=False):
    """
    Create a sqlite3 database from a collection of DataFrame objects.

    Return an open connection owned by the caller. cache_root selects an exact
    cache directory; show_progress enables optional tqdm insertion progress.
    Bump version or set overwrite=True to refresh a matching cached database.

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
        Application name selecting a platform cache directory; ignored when
        cache_root is supplied.

    overwrite : bool, optional
        If the database already exists, overwrite it?

    version : int, optional
    """
    if not (subdir is None or isinstance(subdir, str)):
        raise TypeError("Expected subdir to be None or str, got %s : %s" % (
            subdir, type(subdir)))
    db_path = resolve_path(db_filename, subdir, cache_root=cache_root)
    if not overwrite:
        connection = _cached_connection(db_path, dataframes, version)
        if connection is not None:
            return connection
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    return db_from_dataframes_with_absolute_path(
        db_path,
        table_names_to_dataframes=dataframes,
        table_names_to_primary_keys=primary_keys,
        table_names_to_indices=indices,
        overwrite=overwrite,
        version=version,
        show_progress=show_progress)

def db_from_dataframe(
        db_filename,
        table_name,
        df,
        primary_key=None,
        subdir=None,
        overwrite=False,
        indices=(),
        version=1,
        *,
        cache_root=None,
        show_progress=False):
    """
    Given a dataframe `df`, turn it into a sqlite3 database.
    Store values in a table called `table_name`.

    Returns an open sqlite3.Connection owned by the caller. Close it when done.
    Rebuild failures preserve the previous database, including on overwrite.
    """
    return db_from_dataframes(
        db_filename=db_filename,
        dataframes={table_name: df},
        primary_keys={table_name: primary_key},
        indices={table_name: indices},
        subdir=subdir,
        overwrite=overwrite,
        version=version,
        cache_root=cache_root,
        show_progress=show_progress)


def _schema_filename_parts(base_filename, df):
    """Split an inferred database name into its row-count prefix and schema."""
    schema = ""
    for column_name in df.columns:
        if not isinstance(column_name, str) or not column_name:
            raise ValueError("DataFrame columns must be non-empty strings")
        column_db_type = db_type(df[column_name].dtype)
        schema += ".%s_%s" % (column_name.replace(" ", "_"), column_db_type)
    return base_filename + ("_nrows%d" % len(df)), schema


def _fits_filesystem(filename):
    """Is every component of filename within the filesystem's name limit?"""
    return all(len(os.fsencode(part)) <= _MAX_NAME_BYTES
               for part in filename.replace(os.altsep or os.sep, os.sep).split(os.sep))


def _db_filename_from_dataframe(base_filename, df):
    """
    Generate database filename for a sqlite3 database we're going to
    fill with the contents of a DataFrame, using the DataFrame's
    column names and types.

    Column names come from downloaded data. A schema containing a path
    separator, or too long for a filename, is named by its digest instead, so
    the database is always a file directly inside its cache directory. Every
    other name keeps its historical spelling so existing databases are reused.
    """
    prefix, schema = _schema_filename_parts(base_filename, df)
    db_filename = prefix + schema + ".db"
    if re.search(r"[/\\]", schema) or not _fits_filesystem(db_filename):
        digest = hashlib.md5(schema.encode("utf-8", "surrogatepass")).hexdigest()
        db_filename = "%s.%s.db" % (prefix, digest)
    return db_filename


def _legacy_db_filename_from_dataframe(base_filename, df):
    """Return the nested name older releases used for this schema, or None.

    A column name containing a path separator used to add directories inside
    the cache. Such a database is still reused where it is, but only when its
    name stays inside the cache directory and could have been created.
    """
    prefix, schema = _schema_filename_parts(base_filename, df)
    parts = re.split(r"[/\\]", schema)
    legacy = prefix + schema + ".db"
    if len(parts) == 1 or ".." in parts or not _fits_filesystem(legacy):
        return None
    return legacy

def fetch_csv_db(
        table_name,
        download_url,
        csv_filename=None,
        db_filename=None,
        subdir=None,
        version=1,
        *,
        download_options=None,
        show_progress=False,
        **pandas_kwargs):
    """
    Download CSV data and return an open SQLite connection owned by the caller.

    Omitted CSV/database filenames are inferred from the URL and schema.
    Parser settings go in pandas_kwargs; fetch_file settings go in
    download_options. Its cache_root applies to both files. show_progress
    enables optional download and row-insertion displays. Bump version when
    the data or schema changes to rebuild an existing matching database.
    """
    options = download_options or {}
    cache_root = options.get("cache_root")
    check_source = (options.get("force") or options.get("expected_sha256") is not None or
                    options.get("expected_size") is not None)
    if db_filename is not None and not check_source:
        connection = _cached_connection(
            resolve_path(db_filename, subdir, cache_root=cache_root), [table_name], version)
        if connection is not None:
            return connection
    df = fetch_csv_dataframe(
        download_url=download_url,
        filename=csv_filename,
        subdir=subdir,
        download_options=download_options,
        show_progress=show_progress,
        **pandas_kwargs)
    if db_filename is None:
        # Explicit filenames have always used the caller's spelling here,
        # including the last compressed suffix and any path components. Keep
        # that key so upgrades find existing databases. Only the previously
        # broken csv_filename=None case needs the new inferred name.
        source_filename = (os.fspath(csv_filename) if csv_filename is not None else
                           build_local_filename(download_url, decompress=True))
        base_filename = splitext(source_filename)[0]
        db_filename = _db_filename_from_dataframe(base_filename, df)
        legacy_filename = _legacy_db_filename_from_dataframe(base_filename, df)
        if legacy_filename is not None and not path_exists(
                resolve_path(db_filename, subdir, cache_root=cache_root)):
            # Reuse a matching database an older release nested in the cache,
            # without moving it. Version changes rebuild under the new name.
            connection = _cached_connection(
                resolve_path(legacy_filename, subdir, cache_root=cache_root), [table_name], version)
            if connection is not None:
                return connection
    return db_from_dataframe(
        db_filename,
        table_name,
        df,
        subdir=subdir,
        version=version,
        cache_root=cache_root,
        show_progress=show_progress)
