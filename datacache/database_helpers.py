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

import errno
import logging
import os
from os.path import lexists
import sqlite3
import stat

from ._filesystem import publish_noclobber
from .database import (
    METADATA_TABLE_NAME, Database, fold_identifier, require_integer,
    require_iterable_of, require_string, tables_exist,
)
from .database_table import DatabaseTable
from .download import open_staging_file, remove_staging_file, normal_creation_mode


logger = logging.getLogger(__name__)


def connect_if_correct_version(db_path, version, *, read_only=False):
    """Open an existing SQLite database if its DataCache metadata matches ``version``.

    Implicitly checks that the database finished building, since version
    metadata is written last. Never creates missing files and closes any
    connection it rejects. Checks metadata only, not application tables, data
    correctness, or a checksum.

    Parameters
    ----------
    db_path : str or os.PathLike
        Path to the SQLite database file.
    version : int
        Expected integer database version recorded in DataCache metadata.
    read_only : bool, optional
        If ``True``, open using SQLite's read-only mode so shared installations
        work without write access. Default is ``False``.

    Returns
    -------
    sqlite3.Connection or None
        Open caller-owned connection on a version match, or ``None`` when the
        file is missing, metadata is absent, or the version differs.

    Raises
    ------
    OSError
        For filesystem access failures other than ``FileNotFoundError``.
    sqlite3.Error
        For invalid or unreadable SQLite files or malformed metadata tables.
    ValueError
        For malformed stored version values.
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


def _is_lock_error(error):
    """Is this SQLite's busy or locked error, rather than a failure to open?"""
    code = getattr(error, "sqlite_errorcode", None)  # Python 3.11+
    if code is not None:
        return code & 0xFF in (5, 6)  # SQLITE_BUSY, SQLITE_LOCKED
    return "locked" in str(error)


def _replace_corrupt_database(db_path, tables, version=1, *, show_progress=False):
    """Replace a corrupt SQLite file atomically from a complete staged sibling."""
    directory = os.path.dirname(db_path) or "."
    staged_path = None
    staged_db = None
    try:
        with open_staging_file(directory=directory, suffix=".db") as staged:
            staged_path = staged.name
        staged_db = Database(staged_path)
        staged_db.create(tables, version, show_progress=show_progress)
        if staged_db.connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            raise sqlite3.DatabaseError("replacement database failed SQLite quick_check")
        staged_db.connection.close()
        staged_db = None
        try:
            mode = stat.S_IMODE(os.stat(db_path).st_mode)
        except OSError:
            mode = normal_creation_mode(directory)
        os.chmod(staged_path, mode)
        os.replace(staged_path, db_path)
        for suffix in ("-wal", "-shm", "-journal"):
            try:
                os.unlink(db_path + suffix)
            except OSError:
                pass
        return Database(db_path, must_exist=True).connection
    finally:
        if staged_db is not None:
            staged_db.connection.close()
        if staged_path is not None:
            remove_staging_file(staged_path)


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
            with open_staging_file(directory=directory, suffix=".db") as staged:
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
                remove_staging_file(staged_path)

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
            # lock. Never unlink a healthy database or commit drops alone.
            try:
                with db.connection:
                    db.connection.execute("BEGIN IMMEDIATE")
                    if not reusable():
                        # Dropping tables need not visit damaged data/freelist
                        # pages. Detect corruption before consuming new rows,
                        # including damage a rebuild could otherwise retain.
                        if db.connection.execute("PRAGMA quick_check(1)").fetchall() != [("ok",)]:
                            error = sqlite3.DatabaseError("existing cache failed SQLite quick_check")
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
                   (getattr(error, "sqlite_errorcode", 0) & 255) in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB))
        try:
            db.connection.rollback()
        finally:
            db.connection.close()
        if recover:
            logger.info("Rebuilding corrupt database %s from a complete staged replacement", db_path)
            return _replace_corrupt_database(db_path, tables, version, show_progress=show_progress)
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
    """Create or reuse a SQLite database at ``db_path`` from pandas DataFrames.

    Uses ``db_path`` directly (relative paths resolve against the current
    working directory) without selecting a platform cache directory. The parent
    directory must already exist (for dangling symlinks, the target's parent
    must exist; publication preserves the symlink).

    A matching ``version`` and all requested tables permit reuse without
    re-reading DataFrames or rewriting the file (table names match
    ASCII-case-insensitively as in SQLite). New databases are staged before
    publication. Rebuilds of healthy databases roll back on failure without
    unlinking the existing file. Explicit ``overwrite=True`` also removes views
    and can atomically rebuild corrupt or non-SQLite files from a private
    staged replacement while preserving inode, links, permissions, and ACLs;
    ordinary reads and version changes never trigger corruption recovery.

    Parameters
    ----------
    db_path : str or os.PathLike
        Target SQLite database file path.
    table_names_to_dataframes : Mapping[str, pandas.DataFrame]
        Mapping from nonempty table names (distinct ignoring ASCII case, not
        ``_datacache_metadata`` or ``sqlite_*``) to DataFrames. DataFrame
        column spaces become underscores; the pandas row index is not stored.
    table_names_to_primary_keys : Mapping[str, str] or None, optional
        Optional mapping from table name to primary-key column name.
    table_names_to_indices : Mapping[str, Sequence[Sequence[str]]] or None, optional
        Optional mapping from table name to sequences of nonempty column-name
        tuples such as ``[("id",), ("id", "value")]``.
    overwrite : bool, optional
        If ``True``, rebuild the database even if it already matches
        ``version`` (or is corrupt). Default is ``False``.
    version : int, optional
        Integer schema/data version stored in DataCache metadata. Default is
        ``1``.
    show_progress : bool, optional
        If ``True``, display ``tqdm`` row-insertion progress bars; reuse is
        quiet. Default is ``False``.

    Returns
    -------
    sqlite3.Connection
        Open caller-owned connection (opened with ``check_same_thread=False``;
        wrap with ``contextlib.closing`` or call ``.close()``).

    Raises
    ------
    ValueError
        For invalid table/column names, constraints, versions, unsupported
        dtypes, or filesystems without room for SQLite's ``<name>-journal``.
    TypeError
        For unsupported argument types.
    sqlite3.IntegrityError
        For duplicate or null primary keys.
    OverflowError
        For integers outside SQLite's signed 64-bit range.
    sqlite3.Error
        For database, schema, binding, or locking failures.
    OSError
        For filesystem or publication failures (including a missing parent
        directory).
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
