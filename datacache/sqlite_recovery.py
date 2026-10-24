"""Publish a built database over corrupt storage using SQLite's own locks.

Use Connection.setconfig() where available, and public SQLite C APIs for
Python 3.11. Isolation matters when Python links SQLite statically: a platform
library must not interfere with another SQLite library's process-local
descriptor/lock bookkeeping. No Python object layouts or pointers to
Python-owned connections are used.

restore_database is the application entry point. The other public APIs let
integrators build isolated workers; see docs/sqlite-recovery.md for contracts.
"""

import ctypes
import ctypes.util
from contextlib import closing, contextmanager, ExitStack
from functools import lru_cache
import json
import mmap
import os
from pathlib import Path
import secrets
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile

__all__ = [
    'restore_database', 'restore_database_in_process', 'copy_file_contents',
    'open_recovery_connection', 'NativeSQLiteConnection', 'SQLiteRows',
    'execute_sql', 'read_schema_version', 'load_sqlite_api',
    'check_sqlite_result', 'open_native_database',
    'read_database_schema', 'database_with_encoding', 'publish_change_counter', 'notify_wal_readers',
]


@lru_cache(maxsize=1)
def load_sqlite_api():
    """Return a cached ctypes SQLite library with recovery function signatures.

    Prefer Python's linked library, then the platform library. Raise
    RuntimeError if neither exports the required APIs. Use native handles only
    in an isolated worker without other SQLite connections to the same files.
    """
    import _sqlite3

    extension = getattr(_sqlite3, '__file__', None)
    candidates = [extension]  # None selects symbols in a statically linked Python.
    if os.name == 'nt' and extension:
        # Windows does not re-export imported DLL functions through the .pyd.
        candidates.append(os.path.join(os.path.dirname(extension), 'sqlite3.dll'))
    platform_library = ctypes.util.find_library('sqlite3')
    if platform_library:
        candidates.append(platform_library)
    handle, integer, string = ctypes.c_void_p, ctypes.c_int, ctypes.c_char_p
    signatures = {
        'open_v2': ([string, ctypes.POINTER(handle), integer, string], integer),
        'close': ([handle], integer),
        'errmsg': ([handle], string),
        'errstr': ([integer], string),
        'errcode': ([handle], integer),
        'busy_timeout': ([handle, integer], integer),
        'exec': ([handle, string, handle, handle, handle], integer),
        # Only the fixed arguments of this variadic function belong in argtypes
        # (notably on Apple ARM64). Supply the remaining arguments explicitly.
        'db_config': ([handle, integer], integer),
        'get_autocommit': ([handle], integer),
    }
    for candidate in candidates:
        try:
            library = ctypes.CDLL(candidate)
            for name, (arguments, result) in signatures.items():
                function = getattr(library, 'sqlite3_' + name)
                function.argtypes, function.restype = arguments, result
            return library
        except (OSError, AttributeError):
            continue
    raise RuntimeError('This Python build does not expose the SQLite recovery APIs; '
                       'the existing database has not been replaced')


def check_sqlite_result(api, connection, code):
    """Return None for SQLITE_OK; otherwise raise a sqlite3 exception with code.

    api must come from load_sqlite_api and connection must be an open native
    handle from that same library. Corruption raises DatabaseError; other
    failures raise OperationalError. Both carry sqlite_errorcode.
    """
    if code == sqlite3.SQLITE_OK:
        return
    message = (api.sqlite3_errmsg(connection) if api.sqlite3_errcode(connection) == code
               else api.sqlite3_errstr(code)).decode('utf-8', errors='replace')
    error_class = (sqlite3.DatabaseError if (code & 255) in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB)
                   else sqlite3.OperationalError)
    error = error_class(message)
    error.sqlite_errorcode = code
    raise error


def open_native_database(api, path, flags):
    """Open path with SQLite open_v2 flags and return an owned c_void_p handle.

    api is returned by load_sqlite_api; flags are SQLite's integer open flags.
    URI and private-cache flags are always added. The caller must close a
    successful handle with api.sqlite3_close. Failed opens close it here and
    raise a sqlite3 exception. Do not pass a Python connection's pointer.
    """
    path = Path(path)
    if '\0' in os.fspath(path):
        raise ValueError('embedded null byte')
    encoded = path.absolute().as_uri().encode('utf-8')
    connection = ctypes.c_void_p()
    # Private cache avoids sharing the destination's pager with other handles.
    code = api.sqlite3_open_v2(encoded, ctypes.byref(connection), flags | 0x40000 | 0x40, None)
    try:
        check_sqlite_result(api, connection, code)
        return connection
    except BaseException:
        if connection:
            api.sqlite3_close(connection)
        raise


class NativeSQLiteConnection:
    """Open an existing database for recovery SQL using public native APIs.

    path is a filesystem path; timeout is lock-wait seconds. This is a small
    control-SQL adapter, not a general DB-API connection: execute accepts SQL
    without bound parameters and returns text/NULL rows. The caller owns close.
    Use only in an isolated process; restore_database handles this for callers.
    """
    def __init__(self, path, timeout):
        self.api = load_sqlite_api()
        self.handle = open_native_database(self.api, path, 2)  # READWRITE, never CREATE
        try:
            check_sqlite_result(self.api, self.handle, self.api.sqlite3_busy_timeout(self.handle, int(timeout * 1000)))
        except BaseException:
            self.close()
            raise

    def setconfig(self, option, enabled):
        """Set a boolean SQLITE_DBCONFIG option; raise on unsupported options."""
        check_sqlite_result(self.api, self.handle, self.api.sqlite3_db_config(
            self.handle, option, ctypes.c_int(enabled), ctypes.c_void_p()))

    @property
    def in_transaction(self):
        """Whether a transaction is active on this open connection."""
        return not self.api.sqlite3_get_autocommit(self.handle)

    def execute(self, sql):
        """Execute trusted control SQL and return SQLiteRows of text/NULL values.

        Numeric results are returned as strings; BLOB queries are unsupported.
        Copy actual cache data with INSERT ... SELECT rather than this adapter's
        row values. SQLite errors propagate with their sqlite_errorcode.
        """
        rows = []
        callback_type = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_int,
                                        ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_char_p))

        @callback_type
        def collect(unused, count, row, names):
            rows.append(tuple(row[index].decode('utf-8') if row[index] is not None else None
                              for index in range(count)))
            return 0

        check_sqlite_result(self.api, self.handle, self.api.sqlite3_exec(
            self.handle, sql.encode('utf-8'), ctypes.cast(collect, ctypes.c_void_p), None, None))
        return SQLiteRows(rows)

    def close(self):
        """Close this connection once work is finished; repeated calls are safe."""
        if self.handle:
            check_sqlite_result(self.api, self.handle, self.api.sqlite3_close(self.handle))
            self.handle = None


class SQLiteRows:
    """Materialized text/NULL row tuples returned by NativeSQLiteConnection."""
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        """Return the first row, or None if empty; this does not advance a cursor."""
        return self.rows[0] if self.rows else None


def open_recovery_connection(path, timeout):
    """Open an existing database, returning an owned recovery connection.

    timeout is in seconds. Python 3.12+ returns an autocommit sqlite3.Connection;
    3.11 returns NativeSQLiteConnection. Both support execute, setconfig,
    in_transaction, and close. Use in an isolated worker and close after use.
    """
    if '\0' in os.fspath(path):
        raise ValueError('embedded null byte')
    if hasattr(sqlite3.Connection, 'setconfig'):
        return sqlite3.connect(Path(path).absolute().as_uri() + '?mode=rw', uri=True,
                               isolation_level=None, timeout=timeout)
    return NativeSQLiteConnection(path, timeout)


def execute_sql(connection, sql):
    """Execute control SQL on a recovery connection, discard rows, return None."""
    connection.execute(sql)


def read_schema_version(connection):
    """Read the current main-database schema cookie as int; propagate errors.

    This value is untrusted if the database is corrupt and cannot establish
    what an existing reader has cached.
    """
    return int(connection.execute('PRAGMA main.schema_version').fetchone()[0])


def copy_file_contents(source, destination):
    """Copy whole regular files through bounded memory mappings.

    Both arguments are open binary files with fileno(); destination must be
    readable and writable. Flush buffered writes first, resize destination to
    match source, and copy all bytes without closing either file. Caller owns
    synchronization: during recovery SQLite's exclusive lock stays held.

    Windows permits mapped access through another handle across SQLite's
    mandatory lock bytes at 1 GiB. Ordinary read()/write() calls do not. Every
    mapping is closed before returning, so SQLite can resize files afterwards.
    Raises ValueError for the same file and propagates mapping or I/O errors.
    """
    source.flush()
    destination.flush()
    source_info = os.fstat(source.fileno())
    if os.path.samestat(source_info, os.fstat(destination.fileno())):
        raise ValueError('source and destination must be different files')
    size = source_info.st_size
    destination.truncate(size)
    chunk_size = max(1024 * 1024, mmap.ALLOCATIONGRANULARITY)
    for offset in range(0, size, chunk_size):
        length = min(chunk_size, size - offset)
        with mmap.mmap(source.fileno(), length, access=mmap.ACCESS_READ, offset=offset) as contents:
            with mmap.mmap(destination.fileno(), length, access=mmap.ACCESS_WRITE, offset=offset) as output:
                output[:] = contents[:]
                output.flush()


def read_database_schema(source):
    """Validate a complete DataCache source and describe its schema.

    Return (schema, encoding, has_sequence), where schema contains (type, name,
    create_sql) tuples ordered with tables before indexes, encoding is SQLite's
    canonical encoding name, and has_sequence indicates sqlite_sequence.
    Source must remain stable throughout recovery. Invalid/incomplete caches
    and unsupported source objects raise sqlite3.Error or ValueError.
    """
    with closing(sqlite3.connect(Path(source).absolute().as_uri() + '?mode=ro', uri=True)) as staged:
        if staged.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
            raise sqlite3.DatabaseError('source database failed SQLite quick_check')
        versions = staged.execute('SELECT version FROM _datacache_metadata').fetchall()
        if len(versions) != 1 or not isinstance(versions[0][0], int):
            raise ValueError('source must contain a complete DataCache version record')
        if staged.execute("SELECT 1 FROM sqlite_master WHERE type IN ('view', 'trigger') "
                          "OR upper(sql) LIKE 'CREATE VIRTUAL TABLE %' LIMIT 1").fetchone():
            raise ValueError('source must contain DataCache tables and indexes, without views, triggers or virtual tables')
        schema = staged.execute("SELECT type, name, sql FROM sqlite_master "
                                "WHERE type IN ('table', 'index') AND sql IS NOT NULL "
                                "AND name NOT GLOB 'sqlite_*' ORDER BY type DESC, name").fetchall()
        encoding = staged.execute('PRAGMA encoding').fetchone()[0]
        has_sequence = staged.execute("SELECT 1 FROM sqlite_master WHERE name='sqlite_sequence'").fetchone() is not None
    return schema, encoding, has_sequence


@contextmanager
def database_with_encoding(source, encoding, *, directory=None):
    """Yield a stable source path compatible with the requested SQLite encoding.

    Source must already be validated by read_database_schema and remain closed
    to writers. Matching encodings reuse source. Otherwise create a temporary
    complete copy using bounded row batches, preserving values, constraints,
    indexes and AUTOINCREMENT state. The copy is deleted on context exit, so
    close all attached connections first. directory chooses temporary storage.
    """
    if encoding not in ('UTF-8', 'UTF-16le', 'UTF-16be'):
        raise ValueError('unsupported SQLite text encoding')
    with closing(sqlite3.connect(Path(source).absolute().as_uri() + '?mode=ro', uri=True)) as reader:
        source_encoding = reader.execute('PRAGMA encoding').fetchone()[0]
        if source_encoding == encoding:
            # Close this Python connection before the native adapter attaches
            # the same file; never mix their descriptor bookkeeping.
            reader.close()
            yield os.fspath(source)
            return
        schema, unused_encoding, has_sequence = read_database_schema(source)
        with tempfile.TemporaryDirectory(dir=directory) as temporary:
            path = os.path.join(temporary, 'encoded.db')
            with closing(sqlite3.connect(path)) as converted:
                converted.execute("PRAGMA encoding='%s'" % encoding)
                with converted:
                    converted.execute('BEGIN')
                    for kind, name, sql in schema:
                        converted.execute(sql)
                        if kind == 'table':
                            quoted = '"' + name.replace('"', '""') + '"'
                            rows = reader.execute('SELECT * FROM ' + quoted)
                            insert = 'INSERT INTO %s VALUES(%s)' % (quoted, ','.join('?' for column in rows.description))
                            while batch := rows.fetchmany(1000):
                                converted.executemany(insert, batch)
                    if has_sequence and converted.execute(
                            "SELECT 1 FROM sqlite_master WHERE name='sqlite_sequence'").fetchone():
                        converted.execute('DELETE FROM sqlite_sequence')
                        converted.executemany('INSERT INTO sqlite_sequence(name,seq) VALUES(?,?)',
                                              reader.execute('SELECT name,seq FROM sqlite_sequence'))
                    if converted.execute('PRAGMA integrity_check').fetchone() != ('ok',):
                        raise sqlite3.DatabaseError('encoded source failed SQLite integrity_check')
            reader.close()
            yield path


def publish_change_counter(connection, database_file, *, wal=False):
    """Invalidate other readers' page caches after a recovery commit.

    Caller must retain SQLite's exclusive lock, have no active transaction,
    and close connection before database_file. wal=True retains an earlier WAL
    installation after header loss; call notify_wal_readers after closing this
    connection to refresh its shared reader state. Checkpoint any WAL, then publish
    a fresh file-change counter and matching version-valid-for field through
    a flushed header mapping. Return the counter. Do not perform further SQL
    writes with this connection. Errors propagate; committed data stays intact.
    """
    if connection.in_transaction:
        raise ValueError('change counter publication requires a completed transaction')
    if wal:
        if connection.execute('PRAGMA main.journal_mode=WAL').fetchone()[0].lower() != 'wal':
            raise sqlite3.OperationalError('could not restore WAL mode after recovery')
        execute_sql(connection, 'PRAGMA main.schema_version=%d' % read_schema_version(connection))
    checkpoint = connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
    if int(checkpoint[0]) != 0:
        raise sqlite3.OperationalError('database is locked during recovery checkpoint')
    with mmap.mmap(database_file.fileno(), 100, access=mmap.ACCESS_WRITE) as header:
        if header[:16] != b'SQLite format 3\0':
            raise sqlite3.DatabaseError('recovered database has an invalid header')
        previous = int.from_bytes(header[24:28], 'big')
        counter = 0x40000000 | secrets.randbits(30)
        while counter == previous:
            counter = 0x40000000 | secrets.randbits(30)
        header[24:28] = header[92:96] = counter.to_bytes(4, 'big')
        header.flush()
    os.fsync(database_file.fileno())
    return counter


def notify_wal_readers(destination, *, timeout=5.0):
    """Refresh WAL's shared reader state after an exclusive recovery worker closes.

    Exclusive-locking SQLite can use a private WAL index. A normal connection
    must publish a page-one frame through the shared index for existing readers.
    Serialize with other writers and rewrite only the current user_version to
    its existing value; preserve any intervening writer's complete cache.
    Return None, propagating SQLite errors. No other worker handles may remain.
    """
    with closing(sqlite3.connect(Path(destination).absolute().as_uri() + '?mode=rw',
                                 uri=True, isolation_level=None, timeout=timeout)) as connection:
        with connection:
            connection.execute('BEGIN IMMEDIATE')
            version = connection.execute('PRAGMA user_version').fetchone()[0]
            connection.execute('PRAGMA user_version=%d' % version)


def restore_database_in_process(source, destination, *, timeout=5.0):
    """Copy a complete staged database while retaining SQLite's exclusive lock.

    The arguments and result match restore_database. Call this only in a
    dedicated process without other connections to either path; normal
    applications should use restore_database for automatic process isolation.
    Tables, sequence state and a fresh schema cookie commit together. A fresh
    pager change counter is published before releasing the exclusive lock.
    """
    if os.path.samefile(source, destination):
        raise ValueError('source and destination must be different files')
    schema, source_encoding, has_sequence = read_database_schema(source)
    connection = original = saved = old_version = old_encoding = None
    resources = ExitStack()
    completed = commit_attempted = snapshot_ready = False
    try:
        original = open(destination, 'r+b', buffering=0)
        descriptor = original.fileno()
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError('database recovery requires an existing regular file')
        connection = open_recovery_connection(destination, timeout)
        # Keep existing WAL descriptors valid, including readers attached to
        # the original shared-memory index while this worker uses private WAL
        # state under exclusive locking. Checkpoint explicitly after commit.
        connection.setconfig(1006, True)  # SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE
        # Some platform SQLite builds enable DEFENSIVE by default, silently
        # ignoring schema_version writes. This private recovery connection
        # must be able to invalidate other readers' old schema caches.
        connection.setconfig(1010, False)  # SQLITE_DBCONFIG_DEFENSIVE
        execute_sql(connection, 'PRAGMA locking_mode=EXCLUSIVE')
        # This initializes any WAL before resetting the b-tree, and obtains
        # its latest schema cookie rather than an older main-file header.
        try:
            execute_sql(connection, 'BEGIN EXCLUSIVE')
            old_version = read_schema_version(connection)
            old_encoding = connection.execute('PRAGMA encoding').fetchone()[0]
        except sqlite3.DatabaseError as error:
            if (error.sqlite_errorcode & 255) not in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB):
                raise
        finally:
            if connection.in_transaction:
                execute_sql(connection, 'ROLLBACK')
        connection.setconfig(1009, True)  # SQLITE_DBCONFIG_RESET_DATABASE
        # Reset inherited incremental-vacuum state before newDatabase creates
        # its header. Setting NONE also clears a stray incremental flag when
        # the old file has no pointer-map pages.
        execute_sql(connection, 'PRAGMA auto_vacuum=NONE')
        execute_sql(connection, 'BEGIN EXCLUSIVE')
        # The main b-tree is now reset inside the uncommitted transaction.
        # Disable the flag before attaching the valid replacement database.
        connection.setconfig(1009, False)
        info = os.fstat(descriptor)
        if not os.path.samestat(info, os.stat(destination)):
            raise sqlite3.OperationalError('database path changed during recovery')
        # RESET_DATABASE can make SQLite treat corrupt pages as outside the
        # old database, so its rollback journal need not include those bytes.
        # Keep an exact snapshot under the retained exclusive lock until the
        # transaction commits. This also preserves a final partial page.
        saved = tempfile.TemporaryFile(dir=os.path.dirname(destination) or '.')
        copy_file_contents(original, saved)
        snapshot_ready = True
        saved.seek(18)
        was_wal = saved.read(2) == b'\x02\x02' or os.path.exists(os.fspath(destination) + '-wal')
        if old_encoding is None:
            saved.seek(56)
            old_encoding = {1: 'UTF-8', 2: 'UTF-16le', 3: 'UTF-16be'}.get(
                int.from_bytes(saved.read(4), 'big'), source_encoding)
        execute_sql(connection, "PRAGMA encoding='%s'" % old_encoding)
        source = resources.enter_context(database_with_encoding(
            source, old_encoding, directory=os.path.dirname(destination) or '.'))
        # Even a readable cookie may be corrupt. Incrementing it could recreate
        # a value cached by an existing reader. Choose a fresh high-range nonce
        # independently of the header, excluding its currently readable value.
        new_version = 0x40000000 | secrets.randbits(30)
        while new_version == old_version:
            new_version = 0x40000000 | secrets.randbits(30)
        execute_sql(connection, 'PRAGMA cache_size=-2000')
        source_uri = Path(source).absolute().as_uri() + '?mode=ro'
        execute_sql(connection, "ATTACH DATABASE '%s' AS replacement" % source_uri.replace("'", "''"))
        for kind, name, sql in schema:
            execute_sql(connection, sql)
            if kind == 'table':
                quoted = '"' + name.replace('"', '""') + '"'
                execute_sql(connection, 'INSERT INTO main.%s SELECT * FROM replacement.%s' % (quoted, quoted))
        if has_sequence and connection.execute(
                "SELECT 1 FROM main.sqlite_master WHERE name='sqlite_sequence'").fetchone():
            execute_sql(connection, 'DELETE FROM main.sqlite_sequence')
            execute_sql(connection, 'INSERT INTO main.sqlite_sequence(name,seq) SELECT name,seq FROM replacement.sqlite_sequence')
        execute_sql(connection, 'PRAGMA main.schema_version=%d' % new_version)
        if read_schema_version(connection) != new_version:
            raise sqlite3.DatabaseError('SQLite did not update the replacement schema version')
        if connection.execute('PRAGMA main.integrity_check').fetchone() != ('ok',):
            raise sqlite3.DatabaseError('replacement database failed SQLite integrity_check')
        commit_attempted = True
        execute_sql(connection, 'COMMIT')
        completed = True
        publish_change_counter(connection, original, wal=was_wal)
    finally:
        try:
            if connection and connection.in_transaction:
                execute_sql(connection, 'ROLLBACK')
            elif connection and commit_attempted and not completed:
                # If an exception arrives just after native COMMIT, preserve
                # the fully published database instead of truncating it.
                try:
                    completed = read_schema_version(connection) == new_version
                except sqlite3.DatabaseError:
                    pass
            if not completed and snapshot_ready:
                copy_file_contents(saved, original)
                os.fsync(descriptor)
        finally:
            if connection:
                connection.close()
            if original is not None:
                original.close()
            if saved is not None:
                saved.close()
            resources.close()
    if was_wal:
        notify_wal_readers(destination, timeout=timeout)


def restore_database(source, destination, *, timeout=5.0):
    """Replace an existing cache from a complete, stable DataCache database.

    source and destination are different existing regular-file paths. source
    must be a completed DataCache cache of ordinary tables/indexes, closed to
    writers for the duration of this call. Its integrity and version record
    are checked before destination changes. All tables, rows, constraints,
    indexes, AUTOINCREMENT high-water marks and the source's DataCache version
    are copied. Preserve a readable destination encoding, converting source
    values when necessary; otherwise use the source encoding. Unsupported
    source objects are rejected rather than silently omitted.

    Return None after committing the replacement. Destination may be healthy
    or corrupt; its previous schema is replaced, while its inode, permissions
    and ACLs survive. SQLite locks serialize writers; timeout is lock-wait
    seconds. Open WAL readers can prevent exclusive access even between read
    transactions. Validate the rebuilt destination before committing. Handled
    failures before commit restore previous bytes; errors after commit leave
    the complete replacement installed. A child process performs publication,
    and cancellation waits for it to finish.
    sqlite3 and OS errors propagate; unavailable recovery APIs raise RuntimeError.
    """
    command = [sys.executable, os.path.abspath(__file__), os.fspath(source), os.fspath(destination), str(timeout)]
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        interrupted = False
        while True:
            try:
                output, errors = process.communicate()
                break
            except KeyboardInterrupt:
                # Finish publication/rollback before propagating cancellation,
                # including repeated Ctrl-C. Keep the staged source alive.
                interrupted = True
        if interrupted:
            raise KeyboardInterrupt()
    if process.returncode == 0:
        return
    try:
        detail = json.loads(output)
    except (ValueError, TypeError):
        raise RuntimeError('SQLite recovery worker failed: ' + errors.strip()) from None
    classes = {'DatabaseError': sqlite3.DatabaseError, 'OperationalError': sqlite3.OperationalError,
               'KeyboardInterrupt': KeyboardInterrupt, 'ValueError': ValueError,
               'OSError': OSError, 'PermissionError': PermissionError, 'FileNotFoundError': FileNotFoundError}
    error_class = classes.get(detail['type'], RuntimeError)
    error = (error_class(detail['errno'], detail['message']) if detail.get('errno') is not None
             else error_class(detail['message']))
    if detail.get('code') is not None:
        error.sqlite_errorcode = detail['code']
    raise error


if __name__ == '__main__':
    # Ctrl-C in the parent may request cancellation, but this short atomic
    # publication must finish or roll back before the worker exits.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        restore_database_in_process(sys.argv[1], sys.argv[2], timeout=float(sys.argv[3]))
    except BaseException as error:
        print(json.dumps({'type': type(error).__name__, 'message': str(error),
                          'code': getattr(error, 'sqlite_errorcode', None),
                          'errno': getattr(error, 'errno', None)}))
        sys.exit(1)
