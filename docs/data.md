# SQLite caches and custom transformations

See the [API reference](api.md) for signatures, defaults, return values, and errors.

## DataFrames to SQLite

Database helpers return **open `sqlite3.Connection` objects**, not file paths.
The caller owns them. Use `contextlib.closing`: a connection's own context
manager commits or rolls back, but does not close the connection.

```python
from contextlib import closing
import pandas as pd
from datacache import db_from_dataframe

frame = pd.DataFrame({"sample id": [1, 2], "label": ["case", "control"]})
with closing(db_from_dataframe(
    "samples.db", "samples", frame,
    primary_key="sample id",
    indices=[("label",)],
    cache_root="references",
    version=1,
)) as connection:
    rows = connection.execute("SELECT sample_id, label FROM samples").fetchall()
```

`db_from_dataframes` accepts a mapping of table names to DataFrames, with
`primary_keys` and `indices` mappings for each table. The
`db_from_dataframes_with_absolute_path` helper takes an exact database path
whose parent already exists. `Cache.db_from_dataframe` uses the selected cache
root and also supports `version`, `overwrite`, and `show_progress`.

### Types and names

- Integer and boolean columns use SQLite integers. Mixed numeric DataFrames
  retain integer precision instead of being coerced through a float array.
- Nullable pandas integer, boolean, floating, and string dtypes are supported.
  `None`, `pd.NA`, `NaT`, and floating NaN values become SQL NULL.
- Strings and categorical columns use TEXT. Datetimes are ISO-formatted text.
  Unsupported dtypes raise an error rather than guessing a representation.
- SQLite integer values must fit a signed 64-bit integer. Oversized unsigned
  values raise an error and roll back; they are not silently rounded.
- Column names must be nonempty strings. For compatibility, spaces become
  underscores. Primary-key and index names accept original or normalized
  names; nullability uses the same mapping. Colliding names and unknown
  constraint columns are rejected.
- Primary keys cannot be NULL. Duplicate keys fail the build. Empty DataFrames
  with a supported column schema produce valid empty tables.

Index names retain their historical spelling when available. If two requested
indexes would share a name, the later one gets a numeric suffix so both are
created. Matching cache hits do not add or rename indexes in existing databases;
rebuild explicitly to restore an index omitted by an older release.

DataFrame rows are converted as individual scalars and inserted in batches of
up to 1,000. Building a database does not create another full list of all rows.
The lower-level `DatabaseTable.rows` property still materializes a list for
compatibility; use `iter_rows()` to consume it incrementally.

### Reuse and replacement

A database is reused when its requested tables and version match, even if the
supplied DataFrame's contents have changed. Increment `version` when changing
the schema or dataset, or pass `overwrite=True` to rebuild explicitly. Version
metadata is written in the same transaction after rows and indices succeed.

Existing databases rebuild in one SQLite transaction. A schema error, duplicate
key, insertion failure, or handled interruption rolls back the old tables,
rows, and version. The inode and its permissions remain unchanged, and existing
connections can observe the committed update on their next read. SQLite locks
serialize rebuilds; long-lived transactions can block a writer or cause a
lock-timeout error, which leaves the previous database intact. Connections
may be used across threads, but applications must coordinate simultaneous use
of the same connection and should use separate connections for concurrent work.

An explicit `overwrite=True` also removes existing views and their triggers
so their names can be used for replacement tables. These removals are part of
the transaction: failure restores the previous views and triggers as well as
the tables. Ordinary cache hits and version rebuilds retain existing views.
Rebuilds also handle virtual tables, such as SQLite FTS tables, whose internal
shadow tables disappear automatically when the parent table is dropped.

New databases are built in private sibling files, closed, and published using
an atomic hard link that cannot overwrite a concurrent creator. A losing
creator checks the winner's tables/version and reuses or transactionally
rebuilds it. Failed builds remove their staging files. This requires a local
filesystem supporting hard links and SQLite locking; unsupported publication
fails without installing a partial database.

A database path may be a symlink, including one whose target does not yet
exist. Creation stages and publishes the database beside that target without
replacing the symlink; the target's parent directory must already exist.
Relative and chained links keep their filesystem meaning. Failed creation
leaves the symlink intact and does not install a partial target database.

`connect_if_correct_version(path, version)` returns `None` for a missing file
or mismatched metadata, without creating an empty database. It closes rejected
connections. `read_only=True` explicitly opens a matching database read-only.
Version matching is a metadata check, not a checksum or proof of data validity.

### Upgrading existing caches

No migration or blanket rebuild is required. Existing cache keys, SQLite
metadata, versions, and table layouts remain readable. A matching database
is opened before new-build input validation: its old schema and constraints
are left intact, even if the current builder would create them differently.
Reuse requires no directory creation or write access, and preserves file
contents, inode, and permissions. New databases retain the same metadata
format so older readers can open them too.

Only an explicit `overwrite=True`, a changed version, or missing requested
tables triggers rebuilding. Stronger constraints and numeric conversion
rules apply then. If a rebuild rejects old inputs, the previous database
remains intact; correct the source inputs before retrying.

Older releases could store NumPy integers as BLOBs or round integers in mixed
numeric DataFrames. Such values are not silently decoded or rewritten on
upgrade: their intended type cannot always be inferred, and lost precision
cannot be recovered from the database. If an affected database needs
correction, rebuild it from the original data with `overwrite=True` or a new
cache version. Unaffected databases need no action.

## CSV to SQLite

`fetch_csv_db("records", url)` infers both filenames when omitted. The inferred
database name records the row count and each column's name and type. Because
column names come from the downloaded data, a schema that would add a `..` path
component or exceed the filesystem's 255-byte name limit is named by a digest
instead, so the database always stays in its cache directory. An explicit
`db_filename` also works without `csv_filename`. Supply parser options directly
and download options in `download_options`, as described in the
[progress guide](progress.md#csv-options). A `cache_root` in that dictionary
is used for both the downloaded CSV and its database.

Explicit CSV filenames keep their historical database key, including names
ending in `.csv.gz`. If `db_filename` is supplied and its tables/version match,
the database can be reused offline without a source CSV or parsing it again.
Supplying `expected_sha256`, `expected_size`, or `force` in `download_options`
still requests source validation or refresh. Refreshing a source alone does
not rebuild a matching database; also increment `version` when its data changes.

## Custom single-file transformations

```python
from pathlib import Path
from datacache import fetch_and_transform

def uppercase(source_path, output_path):
    text = Path(source_path).read_text().upper()
    Path(output_path).write_text(text)
    return text

text = fetch_and_transform(
    transformed_filename="uppercase.txt",
    transformer=uppercase,
    loader=lambda path: Path(path).read_text(),
    source_filename="source.txt",
    source_url="https://example.org/source.txt",
    subdir="my-library",
)
```

The transformer receives an absent output path inside a private temporary
directory on the destination filesystem. Its basename and extension match the
final output. It must write and close one regular file at that path. A no-op
transformer raises an error; an intentionally written empty file is valid.
Only successful output is published. Failures remove partial files and leave
any previous transformed output intact.

On a cache hit, only the loader runs. `force=True` reruns the transformation;
it does not implicitly redownload its source. Use
`download_options={"force": True}` to refresh the source as well. New source
downloads and transformed outputs honor `subdir` and `cache_root`. Other settings can be
passed through `download_options` without changing parser/transformer code.

Older versions accidentally stored the source in the default cache when a
`subdir` was supplied. Without an explicit `cache_root`, the helper reuses that
exact legacy source if the requested directory has no source of its own.
It validates supplied integrity expectations and does not move or chmod the
old file. New downloads go into the requested directory. For compatibility,
a nonempty `subdir` continues to default source decompression to enabled;
set `download_options={"decompress": False}` to override it explicitly.

Return in-memory results from the transformer and loader. A transformer that
returns its output path directly receives the final path back; embedded paths
inside arbitrary objects cannot be rewritten. This contract covers one file,
not directory trees, multiple sidecar files, or open database connections.
