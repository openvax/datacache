# SQLite recovery APIs

Use `overwrite=True` on a database builder to recover a corrupt cache from
source data. For direct control, import `recover_cached_database` or
`restore_database` from `datacache`; their parameters, return values, and
exceptions are in the [API reference](api.md#recover_cached_database).

```python
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from datacache import db_from_dataframe, restore_database

with TemporaryDirectory() as directory:
    source = Path(directory) / "complete.db"
    destination = Path(directory) / "broken.db"
    with closing(db_from_dataframe(source, "records", pd.DataFrame({"id": [1, 2]}))):
        pass
    destination.write_bytes(b"not a SQLite database")
    restore_database(source, destination)
    with closing(db_from_dataframe(destination, "records", None)) as connection:
        print(connection.execute("SELECT id FROM records ORDER BY id").fetchall())
```

```text
[(1,), (2,)]
```

Recovery validates the complete replacement before publishing. SQLite's locks
protect the destination throughout publication and rollback. An existing
complete cache, including a concurrent writer's completed replacement, remains
usable if publication fails before commit. Errors after commit leave the complete
replacement installed; reopen and inspect its version before retrying. Corrupt or incomplete source databases are rejected;
they are not copied over a complete cache. Normal matching-cache reuse still
returns the complete cached data without consuming replacement inputs.

The rebuilt destination passes `integrity_check` before commit. Recovery resets
corrupt vacuum metadata, retains AUTOINCREMENT high-water marks, and preserves a
readable destination text encoding. A temporary converted source makes UTF-8
and UTF-16 replacements compatible; if the destination encoding is lost, the
source encoding is used; readers that cached a different encoding must reopen.
Fresh schema and pager counters invalidate existing
readers, including when the original header is gone. WAL recovery also refreshes
the shared reader state. WAL connections can prevent exclusive access
while other connections remain open, even between transactions; a lock timeout
leaves the cache unchanged. Close those connections before retrying.

Snapshots use bounded memory mappings, which
[Windows allows across locked byte ranges](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex#remarks).
This preserves file contents during handled publication failures without raw
reads or writes through a second handle at SQLite's 1 GiB lock boundary. All
mappings close before SQLite continues. Recovery needs disk space for staging,
the original-file snapshot, and the SQLite journal. Process termination,
power loss, or storage failure during rollback is outside the handled-failure
guarantee; do not treat an error as a successfully installed cache.

## Worker interfaces

All APIs below are public imports from `datacache.sqlite_recovery`. Applications
normally need `restore_database`, which creates and waits for the worker. The
lower-level interfaces are for integrations that own their worker lifecycle.
They are documented here rather than hidden behind underscored helper names.

### `restore_database`

`restore_database(source, destination, *, timeout=5.0)` is the same callable
exported from `datacache`. It returns `None` after publication and propagates
the worker's exception on failure. The source must remain stable until the
call returns, including while cancellation waits for the worker to finish.

### `restore_database_in_process`

`restore_database_in_process(source, destination, *, timeout=5.0)` performs the
same validated copy and returns `None`, without starting a child process.
Call it only in a dedicated process with no other connections to either path.
In particular, independently linked SQLite libraries must not manage the
same files in one process: closing one library's descriptors can release the
other library's POSIX locks. Use `restore_database` unless you own that isolation.

### `read_database_schema`

`read_database_schema(source)` validates a completed DataCache source using
`quick_check` and its single integer version record. It returns
`(schema, encoding, has_sequence)`: ordered `(type, name, create_sql)` tuples for
ordinary tables and indexes, SQLite's canonical encoding name, and a boolean
indicating `sqlite_sequence`. Views, triggers, and virtual tables are rejected.
Invalid sources raise `sqlite3.Error` or `ValueError`. Keep the source stable
until publication finishes.

### `database_with_encoding`

`database_with_encoding(source, encoding, *, directory=None)` is a context manager
yielding a stable source path. Validate the source with `read_database_schema`
first and keep it closed to writers. `encoding` must be `UTF-8`, `UTF-16le`, or
`UTF-16be`; other values raise `ValueError`. Matching encodings reuse the source.
Otherwise a temporary copy preserves table values, constraints, indexes, and
AUTOINCREMENT sequence state, transferring rows in batches of at most 1,000.
`directory` selects temporary storage; `None` uses the system default. Close all
connections that attach the yielded database before leaving the context, which
deletes any temporary copy. SQLite and filesystem errors propagate.

### `publish_change_counter`

`publish_change_counter(connection, database_file, *, wal=False)` publishes a
fresh pager change counter after a committed recovery transaction and returns
it as an integer. The caller must retain SQLite's exclusive lock and supply an
open readable/writable file handle for the same destination. Active transactions
raise `ValueError`. The function checkpoints any WAL and updates both the file
change counter and its matching version-valid-for field through a flushed,
synchronized header mapping. No further SQL writes may use this connection.
Close it before closing `database_file`. SQLite and I/O errors propagate; the
already committed replacement remains intact.

With `wal=True`, restore the WAL mode lost with a damaged header. The connection
must have `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE` enabled so existing WAL file handles
remain valid. After closing all worker handles, call `notify_wal_readers`.

### `notify_wal_readers`

`notify_wal_readers(destination, *, timeout=5.0)` opens a normal SQLite connection
and writes a page-one frame through the shared WAL index. Exclusive recovery
can use a private WAL index, so this step notifies existing readers after the
exclusive worker connection closes. It holds a write transaction and rewrites
the current `user_version` to the same value, preserving any completed intervening
writer's data and metadata. No other worker handles may remain open. Returns
`None`; `timeout` is lock-wait seconds, and SQLite errors propagate.

### `copy_file_contents`

`copy_file_contents(source, destination)` copies the entire source file,
resizes the destination to the exact source length, and returns `None`.
The arguments are open binary regular files with `fileno()`; the destination
must be readable and writable. Buffered writes are flushed before mapping.
At most two 1 MiB windows are mapped at once on supported platforms. Mappings
are flushed and closed on exit; the supplied files stay open and their seek
positions are not used. Call `os.fsync(destination.fileno())` when durable
storage synchronization is required.

The caller must prevent concurrent file changes and hold the appropriate
SQLite locks when copying an active database. This API does not acquire locks
and does not provide an atomic copy by itself. Passing the same file twice
raises `ValueError`; mapping and I/O failures propagate. A failed copy can leave
its destination partially written, so the recovery publisher only restores
from snapshots whose copy completed successfully.

### `open_recovery_connection`

`open_recovery_connection(path, timeout)` opens an existing database for writing
without creating it. `timeout` is lock-wait seconds. It returns an autocommit
`sqlite3.Connection` on Python 3.12+, or `NativeSQLiteConnection` on Python 3.11.
Both provide `execute`, `setconfig`, `in_transaction`, and `close`. The caller
owns the returned connection and must close it. Use this in an isolated worker.

### `NativeSQLiteConnection`

`NativeSQLiteConnection(path, timeout)` opens an existing database using public
SQLite C APIs and raises `sqlite3.Error` on failure. It is a control-SQL adapter
for recovery workers, not a replacement for Python's DB-API connection.

| Member | Contract |
| --- | --- |
| `execute(sql)` | Execute trusted SQL without parameters; return `SQLiteRows`. Results are strings or `None`; numeric results are text, and BLOB queries are unsupported. Copy cache rows with SQL `INSERT … SELECT` to preserve their original SQLite types. |
| `setconfig(option, enabled)` | Set an integer boolean SQLite database-configuration option. Unsupported options raise `sqlite3.Error`. |
| `in_transaction` | Boolean property indicating an active transaction. |
| `close()` | Close the connection. Repeated closes are safe; other operations require an open connection. |
| `api`, `handle` | Native library and owned `c_void_p` connection handle. Use only together; never substitute pointers from Python's managed connections. |

### `SQLiteRows`

`SQLiteRows(rows)` stores materialized row tuples returned by the native
adapter. `rows` is the supplied collection. `fetchone()` returns its first
tuple, or `None` if empty; it does not advance a cursor. Values produced by
`NativeSQLiteConnection` are strings or `None`.

### `execute_sql`

`execute_sql(connection, sql)` executes control SQL on either recovery
connection type, discards result rows, and returns `None`. SQLite exceptions
propagate. The connection remains caller-owned.

### `read_schema_version`

`read_schema_version(connection)` returns the main database's schema cookie as
an integer or propagates a SQLite error. A readable cookie is still untrusted
when storage is corrupt; it cannot tell you what another reader has cached.
Recovery chooses a fresh high-range cookie independently of damaged headers.

### `load_sqlite_api`

`load_sqlite_api()` returns a cached `ctypes.CDLL` whose SQLite recovery
functions have configured signatures. It prefers Python's linked SQLite and
falls back to the platform library. If the necessary functions are unavailable,
it raises `RuntimeError`. Use native handles only in an isolated worker.

### `open_native_database`

`open_native_database(api, path, flags)` returns an owned native connection
handle using SQLite's integer `sqlite3_open_v2` flags. It additionally enables
URI paths and private cache. `api` must be returned by `load_sqlite_api`.
Close successful handles with `api.sqlite3_close(handle)`; failed opens close
their handle before raising. `NativeSQLiteConnection` manages that ownership
for ordinary worker code.

### `check_sqlite_result`

`check_sqlite_result(api, connection, code)` returns `None` for `SQLITE_OK`.
Otherwise it raises `sqlite3.DatabaseError` for corruption or
`sqlite3.OperationalError` for other failures, preserving `sqlite_errorcode`.
`connection` must be a live native handle owned by the same `api` library.
