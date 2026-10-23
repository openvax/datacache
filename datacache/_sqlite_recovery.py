"""Publish a built database over corrupt storage using SQLite's own locks.

Use Connection.setconfig() where available, and public SQLite C APIs for
Python 3.11. Isolation matters when Python links SQLite statically: a platform
library must not interfere with another SQLite library's process-local
descriptor/lock bookkeeping. No Python object layouts or pointers to
Python-owned connections are used.
"""

import ctypes
import ctypes.util
from contextlib import closing
from functools import lru_cache
import json
import os
from pathlib import Path
import secrets
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile


@lru_cache(maxsize=1)
def _sqlite_api():
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


def _check(api, connection, code):
    if code == sqlite3.SQLITE_OK:
        return
    message = (api.sqlite3_errmsg(connection) if api.sqlite3_errcode(connection) == code
               else api.sqlite3_errstr(code)).decode('utf-8', errors='replace')
    error_class = (sqlite3.DatabaseError if (code & 255) in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB)
                   else sqlite3.OperationalError)
    error = error_class(message)
    error.sqlite_errorcode = code
    raise error


def _open(api, path, flags):
    encoded = Path(path).absolute().as_uri().encode('utf-8')
    if b'\0' in encoded:
        raise ValueError('embedded null byte')
    connection = ctypes.c_void_p()
    # Private cache avoids sharing the destination's pager with other handles.
    code = api.sqlite3_open_v2(encoded, ctypes.byref(connection), flags | 0x40000 | 0x40, None)
    try:
        _check(api, connection, code)
        return connection
    except BaseException:
        if connection:
            api.sqlite3_close(connection)
        raise


class _NativeConnection:
    """The small Connection subset needed when setconfig is unavailable."""
    def __init__(self, path, timeout):
        self.api = _sqlite_api()
        self.handle = _open(self.api, path, 2)  # READWRITE, never CREATE
        try:
            _check(self.api, self.handle, self.api.sqlite3_busy_timeout(self.handle, int(timeout * 1000)))
        except BaseException:
            self.close()
            raise

    def setconfig(self, option, enabled):
        _check(self.api, self.handle, self.api.sqlite3_db_config(
            self.handle, option, ctypes.c_int(enabled), ctypes.c_void_p()))

    @property
    def in_transaction(self):
        return not self.api.sqlite3_get_autocommit(self.handle)

    def execute(self, sql):
        rows = []
        callback_type = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_int,
                                        ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_char_p))

        @callback_type
        def collect(unused, count, row, names):
            rows.append(tuple(row[index].decode('utf-8') if row[index] is not None else None
                              for index in range(count)))
            return 0

        _check(self.api, self.handle, self.api.sqlite3_exec(
            self.handle, sql.encode('utf-8'), ctypes.cast(collect, ctypes.c_void_p), None, None))
        return _Rows(rows)

    def close(self):
        self.api.sqlite3_close(self.handle)


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


def _connect(path, timeout):
    if hasattr(sqlite3.Connection, 'setconfig'):
        return sqlite3.connect(Path(path).absolute().as_uri() + '?mode=rw', uri=True,
                               isolation_level=None, timeout=timeout)
    return _NativeConnection(path, timeout)


def _execute(connection, sql):
    connection.execute(sql)


def _schema_version(connection):
    return int(connection.execute('PRAGMA main.schema_version').fetchone()[0])


def _copy_file(source, destination):
    """Copy bytes with bounded memory, without closing either locked file."""
    source.seek(0)
    destination.seek(0)
    while chunk := source.read(1024 * 1024):
        pending = memoryview(chunk)
        while pending:
            written = destination.write(pending)
            if not written:
                raise OSError('short write during SQLite recovery')
            pending = pending[written:]
    destination.truncate()
    destination.flush()


def _restore_database(source, destination, *, timeout=5.0):
    """Copy a complete staged database in one destination write transaction.

    RESET_DATABASE permits opening the corrupt b-tree as an empty one without
    committing an empty database. Set a fresh schema cookie before the same
    commit so pre-existing readers discard their old schema cache. Exclusive
    locking retains the lock through rollback/cleanup too.
    """
    # Read only the schema into Python; rows stay in SQLite's bounded cache.
    with closing(sqlite3.connect(Path(source).absolute().as_uri() + '?mode=ro', uri=True)) as staged:
        schema = staged.execute("SELECT type, name, sql FROM sqlite_master "
                                "WHERE type IN ('table', 'index') AND sql IS NOT NULL "
                                "AND name NOT GLOB 'sqlite_*' ORDER BY type DESC, name").fetchall()
    connection = original = saved = old_version = None
    completed = commit_attempted = snapshot_ready = False
    try:
        original = open(destination, 'r+b', buffering=0)
        descriptor = original.fileno()
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError('database recovery requires an existing regular file')
        connection = _connect(destination, timeout)
        # Some platform SQLite builds enable DEFENSIVE by default, silently
        # ignoring schema_version writes. This private recovery connection
        # must be able to invalidate other readers' old schema caches.
        connection.setconfig(1010, False)  # SQLITE_DBCONFIG_DEFENSIVE
        _execute(connection, 'PRAGMA locking_mode=EXCLUSIVE')
        # This initializes any WAL before resetting the b-tree, and obtains
        # its latest schema cookie rather than an older main-file header.
        try:
            _execute(connection, 'BEGIN EXCLUSIVE')
            old_version = _schema_version(connection)
        except sqlite3.DatabaseError as error:
            if (error.sqlite_errorcode & 255) not in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB):
                raise
        finally:
            if connection.in_transaction:
                _execute(connection, 'ROLLBACK')
        connection.setconfig(1009, True)  # SQLITE_DBCONFIG_RESET_DATABASE
        _execute(connection, 'BEGIN EXCLUSIVE')
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
        _copy_file(original, saved)
        snapshot_ready = True
        if old_version is None:
            # The header may have lost the previous cookie entirely. Avoid the
            # small cookie values commonly cached by pre-existing connections.
            os.lseek(descriptor, 40, os.SEEK_SET)
            old_version = int.from_bytes(os.read(descriptor, 4), 'big')
            new_version = 0x40000000 | secrets.randbits(30)
            while new_version == old_version:
                new_version = 0x40000000 | secrets.randbits(30)
        else:
            new_version = (old_version + 1) & 0x7fffffff
        _execute(connection, 'PRAGMA cache_size=-2000')
        source_uri = Path(source).absolute().as_uri() + '?mode=ro'
        _execute(connection, "ATTACH DATABASE '%s' AS replacement" % source_uri.replace("'", "''"))
        for kind, name, sql in schema:
            _execute(connection, sql)
            if kind == 'table':
                quoted = '"' + name.replace('"', '""') + '"'
                _execute(connection, 'INSERT INTO main.%s SELECT * FROM replacement.%s' % (quoted, quoted))
        _execute(connection, 'PRAGMA main.schema_version=%d' % new_version)
        if _schema_version(connection) != new_version:
            raise sqlite3.DatabaseError('SQLite did not update the replacement schema version')
        commit_attempted = True
        _execute(connection, 'COMMIT')
        completed = True
    finally:
        try:
            if connection and connection.in_transaction:
                _execute(connection, 'ROLLBACK')
            elif connection and commit_attempted and not completed:
                # If an exception arrives just after native COMMIT, preserve
                # the fully published database instead of truncating it.
                try:
                    completed = _schema_version(connection) == new_version
                except sqlite3.DatabaseError:
                    pass
            if not completed and snapshot_ready:
                _copy_file(saved, original)
                os.fsync(descriptor)
        finally:
            if connection:
                connection.close()
            if original is not None:
                original.close()
            if saved is not None:
                saved.close()


def restore_database(source, destination, *, timeout=5.0):
    """Run the isolated publisher and propagate its error without a fallback."""
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
        _restore_database(sys.argv[1], sys.argv[2], timeout=float(sys.argv[3]))
    except BaseException as error:
        print(json.dumps({'type': type(error).__name__, 'message': str(error),
                          'code': getattr(error, 'sqlite_errorcode', None),
                          'errno': getattr(error, 'errno', None)}))
        sys.exit(1)
