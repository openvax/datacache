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

import os
import errno
import json
import re
import sqlite3
import stat
from os.path import splitext, lexists
import logging

from .common import MAX_NAME_BYTES, build_local_filename, name_digest, name_length, resolve_path
from .download import (
    fetch_csv_dataframe, _open_staging_file, normal_creation_mode,
    _remove_staging_file,
)
from .database import (
    METADATA_TABLE_NAME, Database, fold_identifier, require_integer, require_iterable_of, require_string, tables_exist,
)
from .database_table import DatabaseTable, validate_column_names
from .database_types import db_type
from .inspection import path_exists
from ._filesystem import publish_noclobber


logger = logging.getLogger(__name__)

# Rebuilding a database creates "<name>-journal" beside it, so database names
# need that much room to spare within the filesystem's name limit.
_MAX_DB_NAME_LENGTH = MAX_NAME_BYTES - len("-journal")
# Characters some supported platform forbids in a file name, separators included.
_UNSAFE_NAME_CHARACTERS = re.compile(r'[\x00-\x1f<>:"/\\|?*\ud800-\udfff]')
# Separators this platform's filesystem interprets.
_SEPARATORS = re.compile("[%s]" % re.escape(os.sep + (os.altsep or "")))


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


def _validate_table_names(table_names):
    """Reject table names that no build could create, compared as SQLite does."""
    if not all(isinstance(name, str) and name for name in table_names):
        raise ValueError("Database table names must be non-empty strings")
    folded = [fold_identifier(name) for name in table_names]
    if not folded or len(set(folded)) != len(folded):
        raise ValueError("Database requires non-empty, distinct table names")
    if any(name.startswith("sqlite_") or name == METADATA_TABLE_NAME for name in folded):
        raise ValueError("Database table name is reserved for metadata")


def _cached_connection(db_path, table_names, version):
    """Reuse a matching installation before parsing or validating new data.

    Table names are checked first, so reuse can never hand back a table that
    no build could create, such as the version metadata.
    """
    require_integer(version, "version")
    table_names = list(table_names)
    if table_names:  # an empty request has always reused any matching database
        _validate_table_names(table_names)
    connection = connect_if_correct_version(db_path, version)
    if connection is None:
        return None
    try:
        if tables_exist(connection, table_names):
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


def _explain_rebuild_failure(db_path, error):
    """Replace SQLite's vague error when its journal name is too long to create.

    Rebuilding writes "<name>-journal" beside the database. Staged creation
    works for a name without room for that suffix, but a rebuild never can.
    Name limits differ by filesystem (bytes on ext4, characters on APFS), so
    ask this one rather than predict it.
    """
    try:
        os.stat(db_path + "-journal")
    except OSError as stat_error:
        # Windows reports ERROR_FILENAME_EXCED_RANGE rather than ENAMETOOLONG.
        if stat_error.errno == errno.ENAMETOOLONG or getattr(stat_error, "winerror", None) == 206:
            name = os.path.basename(db_path)
            raise ValueError(
                "Cannot rebuild database %r: this filesystem has no room for SQLite's "
                "%r file beside it. Use a shorter filename." % (name, name + "-journal")) from error


def recover_cached_database(db_path, tables, version=1, *, page_size=None, show_progress=False):
    """Explicitly replace an existing SQLite cache from DatabaseTable objects.

    Build tables once in a temporary sibling database, validate the completed
    build, then publish with restore_database. db_path may be healthy or corrupt;
    all previous tables are replaced. Its parent and target must already exist.
    version is an integer cache version. Optional page_size is a SQLite page
    size (a power of two from 512 through 65536); None uses SQLite's default.
    show_progress enables row counts. Return an owned sqlite3.Connection that
    the caller must close. Build failures leave the existing file untouched;
    publication preserves its inode, permissions, ACLs and SQLite locking.
    """
    from .sqlite_recovery import restore_database

    db_path = os.fspath(db_path)
    require_string(db_path, 'db_path')
    db_path = _database_target_path(db_path)
    if not stat.S_ISREG(os.stat(db_path).st_mode):
        raise ValueError('database recovery requires an existing regular file')
    tables = list(tables)
    require_iterable_of(tables, DatabaseTable)
    _validate_table_names([table.name for table in tables])
    require_integer(version, 'version')
    if page_size is not None:
        require_integer(page_size, 'page_size')
        if not 512 <= page_size <= 65536 or page_size & (page_size - 1):
            raise ValueError('page_size must be a power of two from 512 through 65536')
    staged_path = None
    staged_db = None
    try:
        with _open_staging_file(directory=os.path.dirname(db_path) or '.', suffix='.db') as staged:
            staged_path = staged.name
        staged_db = Database(staged_path)
        # Keep the replacement compatible with the destination's page size.
        if page_size is not None:
            staged_db.connection.execute('PRAGMA page_size=%d' % page_size)
        staged_db.create(tables, version, show_progress=show_progress)
        if staged_db.connection.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
            raise sqlite3.DatabaseError('replacement database failed SQLite quick_check')
        staged_db.connection.close()
        staged_db = None
        restore_database(staged_path, db_path)
        return Database(db_path, must_exist=True).connection
    finally:
        if staged_db is not None:
            staged_db.connection.close()
        if staged_path is not None:
            _remove_staging_file(staged_path)


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
    _validate_table_names(table_names)

    db_path = _database_target_path(db_path)
    if not lexists(db_path):
        # Readers never see an empty/partial new database. Publication is atomic
        # and does not clobber a concurrent creator's database.
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
            os.chmod(staged_path, normal_creation_mode(directory))
            try:
                publish_noclobber(staged_path, db_path)
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

    preparing = True
    try:
        if reusable():
            logger.info("Found existing table in database %s", db_path)
        else:
            # Serialize rebuilds and recheck after acquiring SQLite's write
            # lock. Never unlink an existing database or commit drops alone.
            try:
                with db.connection:
                    db.connection.execute("BEGIN IMMEDIATE")
                    if not reusable():
                        # Dropping tables need not visit damaged data/freelist
                        # pages. Detect corruption before consuming new rows,
                        # including damage a rebuild could otherwise retain.
                        if db.connection.execute('PRAGMA quick_check(1)').fetchall() != [('ok',)]:
                            error = sqlite3.DatabaseError('existing cache failed SQLite quick_check')
                            error.sqlite_errorcode = sqlite3.SQLITE_CORRUPT
                            raise error
                        db.drop_all_tables(commit=False, include_views=overwrite)
                        logger.info("Creating database %s containing: %s", db_path, ", ".join(table_names))
                        preparing = False
                        db.create(tables, version, show_progress=show_progress)
            except sqlite3.OperationalError as error:
                if not _is_lock_error(error):
                    _explain_rebuild_failure(db_path, error)
                raise
    except BaseException as error:
        recover = (overwrite and preparing and isinstance(error, sqlite3.DatabaseError) and
                   (getattr(error, 'sqlite_errorcode', 0) & 255) in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB))
        try:
            db.connection.rollback()
            if recover:
                page_size = db.connection.execute('PRAGMA page_size').fetchone()[0]
        finally:
            db.connection.close()
        if recover:
            logger.info('Rebuilding corrupt database %s from a complete staged replacement', db_path)
            return recover_cached_database(db_path, tables, version, page_size=page_size, show_progress=show_progress)
        logger.warning(
            "Failed to create tables %s in database %s",
            table_names,
            db_path)
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
    Explicit overwrite can recover corrupt files through a staged replacement;
    ordinary reads and version changes do not repair corruption.

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
    return _build_database(
        db_path, table_names_to_dataframes, table_names_to_primary_keys,
        table_names_to_indices, overwrite, version, show_progress)


def _build_database(db_path, dataframes, primary_keys, indices, overwrite, version, show_progress):
    """Build (or, after a race, reuse) a database once reuse was ruled out."""
    tables = build_tables(dataframes, primary_keys, indices)
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
    return _build_database(db_path, dataframes, primary_keys, indices, overwrite, version, show_progress)

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


def _truncate_name(name, limit):
    """Return the longest prefix of name within limit UTF-8 bytes."""
    return name.encode("utf-8", "surrogatepass")[:limit].decode("utf-8", "ignore")


def _db_filenames_from_dataframe(base_filename, df):
    """
    Generate database filename for a sqlite3 database we're going to
    fill with the contents of a DataFrame, using the DataFrame's
    column names and types.

    Returns the filename and, when it differs, the historical name older
    releases used for the same schema (otherwise None). The historical
    spelling is kept only when every character of the file name is allowed on
    all supported platforms and SQLite's journal still fits beside it. Other
    names use a digest, so column names from downloaded data can never add
    directories or leave the cache directory. Lengths are measured the same way
    on every platform (see common.name_length); only an explicit CSV filename's
    path separators are interpreted by the local platform.
    """
    validate_column_names(df.columns)
    columns = [(column_name, db_type(df[column_name].dtype)) for column_name in df.columns]
    schema = "".join(".%s_%s" % (name.replace(" ", "_"), kind) for name, kind in columns)
    prefix = base_filename + ("_nrows%d" % len(df))
    historical = prefix + schema + ".db"
    # An explicit CSV filename may name directories; those are the caller's.
    cut = max((match.end() for match in _SEPARATORS.finditer(prefix)), default=0)
    directory, stem = prefix[:cut], prefix[cut:]
    if (not _UNSAFE_NAME_CHARACTERS.search(stem + schema) and
            name_length(stem + schema + ".db") <= _MAX_DB_NAME_LENGTH):
        return historical, None
    # The spelled-out schema is ambiguous (column "a_INT.b" of TEXT reads like
    # two columns), so digest an unambiguous encoding instead.
    digest = name_digest(json.dumps([stem, columns]))
    safe_stem = _UNSAFE_NAME_CHARACTERS.sub("_", stem)
    safe_stem = _truncate_name(safe_stem, _MAX_DB_NAME_LENGTH - len(".%s.db" % digest))
    # A historical name with a ".." component could lie outside the cache, and
    # one with a NUL could never have been created, so neither is reused.
    reusable = ".." not in _SEPARATORS.split(stem + schema) and "\x00" not in historical
    return "%s%s.%s.db" % (directory, safe_stem, digest), historical if reusable else None


def _is_lock_error(error):
    """Is this SQLite's busy or locked error, rather than a failure to open?"""
    code = getattr(error, "sqlite_errorcode", None)  # Python 3.11+
    if code is not None:
        return code & 0xFF in (5, 6)  # SQLITE_BUSY, SQLITE_LOCKED
    return "locked" in str(error)


def _open_legacy_database(path, table_name, version):
    """Reuse a matching database an older release stored at path.

    Returns (connection, present): the connection when it can be reused, and
    whether a SQLite database is there at all. A name this platform rejects, a
    file SQLite cannot open, or anything that is not a SQLite database only
    means there is nothing to reuse. A busy or locked database raises instead:
    building a second copy beside one that is in use would let the two diverge.
    """
    try:
        if not stat.S_ISREG(os.stat(path).st_mode):
            return None, False
    except (OSError, UnicodeError):
        return None, False
    try:
        return _cached_connection(path, [table_name], version), True
    except sqlite3.DatabaseError as error:
        if _is_lock_error(error):
            raise
        return None, False


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
    superseded = False
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
        # including the last compressed suffix and any path components, and
        # still do whenever that spelling is a valid name on every platform.
        source_filename = (os.fspath(csv_filename) if csv_filename is not None else
                           build_local_filename(download_url, decompress=True))
        base_filename = splitext(source_filename)[0]
        db_filename, legacy_filename = _db_filenames_from_dataframe(base_filename, df)
        if legacy_filename is not None and not path_exists(
                resolve_path(db_filename, subdir, cache_root=cache_root)):
            # Reuse a matching database an older release stored under its
            # historical name, without moving it; a new version rebuilds under
            # the new name. The older copy is never deleted: another process,
            # perhaps running an older release, may still have it open.
            legacy_path = resolve_path(legacy_filename, subdir, cache_root=cache_root)
            connection, superseded = _open_legacy_database(legacy_path, table_name, version)
            if connection is not None:
                return connection
    connection = db_from_dataframe(
        db_filename,
        table_name,
        df,
        subdir=subdir,
        version=version,
        cache_root=cache_root,
        show_progress=show_progress)
    if superseded:
        # Only now that the replacement exists is the older copy truly unused.
        logger.warning(
            "Built %s under a new name; the older database at %s is no longer used "
            "and can be deleted once no older datacache release uses this cache.",
            db_filename, legacy_path)
    return connection
