# Public API reference

This reference covers every name exported in `datacache.__all__` and every
public `Cache` method in DataCache 1.10.2. Import these names from `datacache`.
Signatures below show all defaults; arguments after `*` are keyword-only.
Method signatures omit `self` and are called on a `Cache` instance.

Start with the [quickstart](../README.md#quickstart) for a first download.
The [download guide](downloads.md), [data guide](data.md),
[progress guide](progress.md), and [shared-cache guide](shared-caches.md)
explain the longer workflows and compatibility guarantees.

## Index

| Area | APIs |
| --- | --- |
| Downloading | [fetch_file](#fetch_file), [fetch_csv_dataframe](#fetch_csv_dataframe), [fetch_and_transform](#fetch_and_transform) |
| Paths and presence | [expected_path](#expected_path), [file_exists](#file_exists), [build_local_filename](#build_local_filename), [get_data_dir](#get_data_dir), [resolve_path](#resolve_path), [build_path](#build_path), [ensure_dir](#ensure_dir), [clear_cache](#clear_cache) |
| Integrity and permissions | [validate_file](#validate_file), [inspect_file](#inspect_file), [inspect_files](#inspect_files), [make_file_readable](#make_file_readable) |
| Results and exceptions | [FileInspection](#fileinspection), [CacheInspection](#cacheinspection), [FileValidationError](#filevalidationerror) |
| SQLite | [db_from_dataframe](#db_from_dataframe), [db_from_dataframes](#db_from_dataframes), [db_from_dataframes_with_absolute_path](#db_from_dataframes_with_absolute_path), [fetch_csv_db](#fetch_csv_db), [connect_if_correct_version](#connect_if_correct_version) |
| Cache object | [Cache](#cache), [fetch](#cachefetch), [local_filename](#cachelocal_filename), [local_path](#cachelocal_path), [exists](#cacheexists), [inspect](#cacheinspect), [make_readable](#cachemake_readable), [db_from_dataframe](#cachedb_from_dataframe), [delete_url](#cachedelete_url), [delete_all](#cachedelete_all) |
| Package version | [__version__](#__version__) |

## Example setup

The examples in this page use this shared setup. Run it once, then run the
examples in order in the same Python session. All downloads use a local file;
no external dataset server is needed. Explicit roots keep example writes in a
temporary directory. Cleanup is shown at the end of the page.

```python
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import os

import pandas as pd
import datacache as dc

temporary = TemporaryDirectory()
root = Path(temporary.name)
cache_root = root / "cache"
source = root / "source.csv"
contents = b"id,value\n1,10\n2,20\n"
source.write_bytes(contents)
url = source.as_uri()
sha256 = hashlib.sha256(contents).hexdigest()
frame = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
```

## Downloading

### `fetch_file`

```text
fetch_file(
    download_url, filename=None, decompress=False, subdir=None, force=False,
    timeout=None, use_wget_if_available=None, chunk_size=1048576,
    progress_callback=None, *, destination=None, cache_root=None, expected_sha256=None,
    expected_size=None, max_retries=2, retry_backoff=1.0, retry_max_delay=30.0,
    show_progress=False
)
```

Download a missing file or reuse a readable regular file at its cache path.
On reuse, supplied integrity expectations are checked without a request. An
invalid hit raises an error; `force=True` explicitly downloads a replacement.
Successful new downloads are validated and published atomically. A failed
replacement leaves the previous file intact.

| Parameter | Meaning |
| --- | --- |
| `download_url` | Source URL string: HTTP, HTTPS, FTP, or `file://`. |
| `filename` | Optional string cache key. Omitted names are inferred from the full URL. Names are normalized by [build_local_filename](#build_local_filename), not treated as exact paths. |
| `decompress` | Request gzip/ZIP contents. With inferred filenames, `False` retains the archive and `True` selects a separate decompressed key. An explicit output lacking the source's compression suffix also implies decompression. |
| `subdir` | Application name selecting a platform cache directory; `None` selects `datacache`. See [get_data_dir](#get_data_dir). |
| `force` | Download even when a cached file exists. Does not automatically broaden its permissions. |
| `timeout` | Per-attempt timeout in seconds; `None` supplies no timeout. HTTP also accepts a Requests `(connect, read)` tuple. Timeouts limit connection/read inactivity, not overall elapsed time. |
| `use_wget_if_available` | Deprecated and ignored. Passing a value other than `None` emits `DeprecationWarning`. |
| `chunk_size` | Positive integer transfer chunk size in bytes; the default is 1 MiB. |
| `progress_callback` | Optional callable receiving `(completed_bytes, total_bytes)` after each transfer chunk. The total can be `None`; retries restart counts. Callback exceptions propagate without retrying. |
| `destination` | Exact output path, as a string or `Path`. Mutually exclusive with `filename`, `subdir`, and `cache_root`. Its spelling is kept even when decompressing. |
| `cache_root` | Exact directory containing cached files, as a string or `Path`. Overrides `subdir`; relative roots stay relative to the working directory. |
| `expected_sha256` | Optional trusted 64-character hexadecimal SHA-256 digest of the **installed bytes**, after decompression or conversion. Case-insensitive. |
| `expected_size` | Optional non-negative integer size of those installed bytes; booleans are rejected. |
| `max_retries` | Non-negative integer number of additional attempts after transient HTTP failures. Default `2` allows three total attempts; `0` disables retries. File and FTP downloads are not retried. |
| `retry_backoff` | Initial retry delay in seconds, doubled for subsequent retries. Must be finite and non-negative. |
| `retry_max_delay` | Maximum retry delay in seconds, finite and non-negative. A server `Retry-After` exceeding this limit stops retries. |
| `show_progress` | Boolean enabling optional tqdm download, decompression, and hash bars. Requires `datacache[progress]` when a bar is needed. Cache hits are quiet. |

ZIP downloads select the member whose stored path matches the output name, then
a member in a folder with that name (the largest, if several), and otherwise the
largest non-directory member. HTML-to-CSV conversion is enabled by an explicit
`.csv` output for an HTML source and requires `datacache[html]`. See
[format selection](downloads.md#verified-downloads) and
[HTTP retry behavior](downloads.md#transient-http-failures).

**Returns:** local path string. It is not necessarily absolute when given a
relative `destination` or `cache_root`.

**Raises:** `ValueError` for conflicting paths or invalid expectations,
chunk size, retry options, callback, or progress flag;
[`FileValidationError`](#filevalidationerror) for non-regular files or integrity
mismatches; `OSError` subclasses for filesystem failures; Requests exceptions
for HTTP failures; `urllib.error.URLError` for URL-handler failures; archive
errors such as `zipfile.BadZipFile`, `gzip.BadGzipFile`, or `EOFError` for damaged
input. Empty ZIPs raise `ValueError`. Missing optional dependencies raise
`ImportError`. Parser and callback exceptions propagate. Transport exceptions
are preserved after retry exhaustion.

```python
path = dc.fetch_file(
    url, filename="records.csv", cache_root=cache_root,
    expected_sha256=sha256, expected_size=len(contents), timeout=30,
)
assert dc.fetch_file(url, filename="records.csv", cache_root=cache_root) == path
```

### `fetch_csv_dataframe`

```text
fetch_csv_dataframe(
    download_url, filename=None, subdir=None, *, download_options=None,
    show_progress=False, **pandas_kwargs
)
```

Fetch and decompress the source, then call `pandas.read_csv` on its local path.
`download_url`, `filename`, and `subdir` have the meanings above. Pass parser
arguments such as `sep`, `dtype`, and `usecols` through `**pandas_kwargs`.
`download_options` is an optional dictionary of other `fetch_file` arguments
(for example `cache_root`, `timeout`, `force`, retry settings, and expectations).
It is copied, not mutated. Keep `download_url`, `filename`, `subdir`, and
`decompress` out of this dictionary: the helper supplies them itself, with
`decompress=True`. Exact destinations still obey `fetch_file`'s exclusions.
An inner `show_progress` overrides the outer setting for the download.

**Returns:** a pandas `DataFrame` normally; a pandas `TextFileReader` when
`chunksize` or `iterator=True` is supplied. Close a reader when finished.

**Raises:** [fetch_file errors](#fetch_file), pandas parser errors such as
`pandas.errors.ParserError` and `pandas.errors.EmptyDataError`, decoding errors,
and `TypeError` for duplicated or unsupported keyword arguments.

```python
loaded = dc.fetch_csv_dataframe(
    url, filename="records.csv", dtype={"id": "int64"},
    download_options={"cache_root": cache_root, "expected_sha256": sha256},
)
assert loaded["id"].tolist() == [1, 2]
```

### `fetch_and_transform`

```text
fetch_and_transform(
    transformed_filename, transformer, loader, source_filename, source_url, subdir=None,
    *, force=False, cache_root=None, download_options=None, show_progress=False
)
```

Cache one transformed file. `transformed_filename` is resolved with
[`resolve_path`](#resolve_path); `source_filename` is a download cache key and
`source_url` is the source URL. `subdir` and `cache_root` select the output root
and the default source root.

`transformer(source_path, output_path)` must write and close one regular file
at the supplied, initially absent path. Its basename and extension match the
final output, but it lives in a private staging directory. The result is
published only after successful validation. `loader(final_path)` loads an
existing readable output. A hit calls only the loader, ignoring source options.

`force=True` rebuilds the output while reusing the source; also pass
`download_options={"force": True}` to refresh the source. The optional options
dictionary is copied and passed to `fetch_file`; do not duplicate its supplied
`download_url`, `filename`, or `subdir` arguments. Integrity expectations in
this dictionary validate the downloaded source, not the transformed output.
`show_progress` sets the source-download default; arbitrary transformer work
has no automatic bar. Inner options can override source `cache_root`,
`show_progress`, and `decompress`.

For compatibility, a nonempty `subdir` defaults source decompression to `True`;
otherwise it defaults to `False`. An explicit `download_options["decompress"]`
overrides this. Without an explicit root, sources misplaced in the default
cache by older releases can be reused without moving or changing them. See
the [transformation guide](data.md#custom-single-file-transformations).

**Returns:** the transformer's result after a build, or the loader's result
on reuse. A transformer returning its output path directly as a string or
`Path` receives the final path back. Paths nested inside other objects are not
rewritten; prefer in-memory results and close files before returning.

**Raises:** [fetch_file errors](#fetch_file), `RuntimeError` if the transformer
does not create its output, `FileValidationError` for invalid output, and any
transformer/loader exception. Failures preserve the previous transformed file.

```python
def double_values(source_path, output_path):
    result = pd.read_csv(source_path)
    result["value"] *= 2
    result.to_csv(output_path, index=False)
    return result

doubled = dc.fetch_and_transform(
    "doubled.csv", double_values, pd.read_csv, "records.csv", url,
    cache_root=cache_root,
)
assert doubled["value"].tolist() == [20, 40]
```

## Paths and presence

### `expected_path`

```text
expected_path(
    download_url=None, filename=None, decompress=False, subdir=None, *,
    destination=None, cache_root=None
)
```

Compute the path that `fetch_file` would use, without filesystem access or
directory creation. The parameters follow [fetch_file](#fetch_file); either
`download_url`, `filename`, or `destination` must identify the output. A
`destination` is used exactly and cannot be combined with `filename`, `subdir`,
or `cache_root`. Otherwise, the name is normalized by `build_local_filename`.

**Returns:** path string, possibly relative. **Raises:** `ValueError` for no
URL/filename, an empty destination, or mutually exclusive path arguments.

```python
assert dc.expected_path(filename="records.csv", cache_root=cache_root) == path
```

### `file_exists`

```text
file_exists(
    download_url=None, filename=None, decompress=False, subdir=None, *,
    destination=None, cache_root=None
)
```

Check the presence of the path selected by [expected_path](#expected_path),
with the same parameters. Follows symlinks. Directories count as present and
dangling symlinks as absent. Does not validate readability or integrity.

**Returns:** `bool`. **Raises:** `expected_path` argument errors and filesystem
errors other than `FileNotFoundError`, such as `PermissionError`. No directories
are created, even when the cache root is absent.

```python
assert dc.file_exists(filename="records.csv", cache_root=cache_root)
```

### `build_local_filename`

```text
build_local_filename(download_url=None, filename=None, decompress=False)
```

Choose a string cache key from a URL string or an explicit filename string.
At least one must be nonempty. An explicit name takes precedence. URL-derived
names include an MD5 digest of the full URL for naming, not integrity checking.
Special characters `/`, `\`, `;`, `:`, `?`, and `=` are replaced with underscores;
long names are shortened with a digest. This is a cache key, not an exact path.

`decompress=True` strips a final `.gz` or `.zip` suffix. Inferred archive
endpoints without a removable suffix instead get `.decompressed`, so archive
and decompressed keys differ. No filesystem or network access occurs.

**Returns:** filename string. **Raises:** `ValueError` if neither a URL nor
a filename is supplied.

```python
assert dc.build_local_filename(filename="records.csv.gz", decompress=True) == "records.csv"
```

### `get_data_dir`

```text
get_data_dir(subdir=None, envkey=None)
```

Select a platform cache directory without creating it. `subdir` is an
application name, defaulting to `datacache` when omitted or empty. It is not a
directory nested under the default datacache cache.

| Platform | Path for an application named `my-project` |
| --- | --- |
| Linux / Unix | `$XDG_CACHE_HOME/my-project`, or `~/.cache/my-project` when unset |
| macOS | `~/Library/Caches/my-project` |
| Windows | `%LOCALAPPDATA%\my-project\my-project\Cache` |

Paths follow `appdirs.user_cache_dir`, including Windows' repeated application
name. The function's return value is authoritative for the current platform.
If `envkey` names a nonempty environment variable, use that variable's value as
the root and append `subdir` if supplied. `envkey` is opt-in for this helper;
there is no implicit `DATACACHE_*` environment override for all APIs. Pass the
returned path as `cache_root` to use that selection elsewhere.

**Returns:** directory path string. **Raises:** platform directory lookup
errors from appdirs if it cannot determine the location; invalid argument
types can raise `TypeError`. Does not require the directory to exist.

```python
print(dc.get_data_dir())  # Platform cache directory for datacache.
print(dc.get_data_dir("my-project"))
# For an application-specific override:
selected_root = dc.get_data_dir("my-project", envkey="MY_PROJECT_CACHE_ROOT")
```

### `resolve_path`

```text
resolve_path(filename, subdir=None, *, cache_root=None)
```

Join a filename/path with `cache_root`, or with `get_data_dir(subdir)` if no
root is supplied. Accepts string or `Path` values for `filename` and
`cache_root`. Relative roots remain relative. Unlike `expected_path`, does not
normalize filenames; nested components and absolute filenames are supported.
An absolute filename overrides the root. This is ordinary path joining, not
confinement to a cache directory. Performs no filesystem access.

**Returns:** path string. **Raises:** path argument `TypeError` for unsupported
types, or platform lookup errors from `get_data_dir`.

```python
nested_path = dc.resolve_path("nested/records.csv", cache_root=cache_root)
assert nested_path == str(cache_root / "nested" / "records.csv")
```

### `build_path`

```text
build_path(filename, subdir=None, *, cache_root=None)
```

Resolve a path with the same parameters as [resolve_path](#resolve_path), and
create its parent directories if needed. Does not create or validate the file.
Use `resolve_path` for lookup without writes.

**Returns:** path string. **Raises:** `resolve_path` errors and `OSError`
subclasses from directory creation, including permission or path-type errors.

```python
writable_path = dc.build_path("nested/output.txt", cache_root=cache_root)
assert Path(writable_path).parent.is_dir()
```

### `ensure_dir`

```text
ensure_dir(path)
```

Create `path` and its parents when the path does not exist. Accepts a string
or `Path`. An existing path is left alone; this helper does not verify that an
existing path is a directory.

**Returns:** `None`. **Raises:** `OSError` subclasses for directory creation
failures, including `PermissionError` and races resulting in `FileExistsError`.

```python
dc.ensure_dir(root / "extra" / "nested")
assert (root / "extra" / "nested").is_dir()
```

### `clear_cache`

```text
clear_cache(subdir=None)
```

Recursively delete the **entire directory** selected by `get_data_dir(subdir)`,
including its root. `subdir=None` selects the default `datacache` cache. This
function has no `cache_root` argument. For an explicit root, use
[`Cache.delete_all`](#cachedelete_all), which preserves the root directory.
Deletion is explicit, is not transactional, and may partially complete on error.

**Returns:** `None`. **Raises:** `OSError` subclasses such as
`FileNotFoundError` when the root is missing, `PermissionError` on denied access,
or a path-type error if the root is not a directory. A root symlink is rejected
by `shutil.rmtree`; it is not followed.

This example is commented out so running this page does not delete a real cache:

```python
# dc.clear_cache("my-project")  # Delete this application's platform cache and root.
```

## Integrity, inspection, and permissions

### `validate_file`

```text
validate_file(path, expected_sha256=None, expected_size=None, *, show_progress=False)
```

Read-only validation of a string or `Path` pointing to a readable regular file;
symlinks are followed. `expected_sha256` and `expected_size` have the same
types and installed-byte meanings as in [fetch_file](#fetch_file). Without
expectations, checks only readability and file type. `show_progress=True`
enables a tqdm bar during SHA-256 hashing, including for an existing file.
No download, directory creation, lock, permission change, or repair occurs.

**Returns:** the supplied path as a string. **Raises:** `ValueError` for invalid
expectations, `FileValidationError` for non-regular files or mismatched size/hash,
`FileNotFoundError` for a missing file, other `OSError` subclasses for access
failures, and `ImportError` if a requested hash bar requires missing tqdm.

```python
assert dc.validate_file(path, expected_sha256=sha256, expected_size=len(contents)) == path
```

### `inspect_file`

```text
inspect_file(path, expected_sha256=None, expected_size=None)
```

Inspect a string or `Path` using the same optional expectations as
`validate_file`, entirely offline and without writes. Filesystem and validation
failures become result statuses instead of being raised.

**Returns:** [`FileInspection`](#fileinspection). **Raises:** `ValueError` for
invalid expectation arguments. Unsupported path types can raise `TypeError`.

```python
inspection = dc.inspect_file(path, expected_sha256=sha256)
assert inspection.status == "available" and inspection.verified
assert dc.inspect_file(root / "absent.csv").status == "missing"
```

### `inspect_files`

```text
inspect_files(cache_root, files)
```

Inspect a required-file inventory under `cache_root` (string or `Path`), with no
writes or network access. `files` must be a nonempty mapping of normalized
relative names to dictionaries containing `expected_sha256` and/or
`expected_size`. Use `{}` or `None` when expectations are unavailable. Nested
names use `/`; absolute paths, `..`, redundant separators, backslashes, and
colons (including drive-qualified names) are rejected. Symlinks are followed
during inspection.
This checks a caller-supplied inventory; it does not parse a manifest or take
a snapshot across concurrent changes.

**Returns:** [`CacheInspection`](#cacheinspection), including individual file
results when the root is an accessible directory. A missing root is `missing`;
an existing root missing a required file is `corrupt`. Inaccessibility takes
precedence over corrupt or missing files. `verified=True` requires a matching
SHA-256 for every required file.

**Raises:** `ValueError` for an empty mapping, invalid names, unexpected metadata
keys, or invalid expectations. Malformed mapping/metadata types can raise
`TypeError` or `AttributeError`. Filesystem failures are returned in the result.

```python
inventory = dc.inspect_files(cache_root, {
    "records.csv": {"expected_sha256": sha256, "expected_size": len(contents)},
})
assert inventory.status == "available" and inventory.verified
assert inventory.files["records.csv"].path == path
```

### `FileInspection`

```text
FileInspection(
    path: str, status: str, verified: bool = False, error: Optional[Exception] = None
)
```

Frozen dataclass returned by `inspect_file` and `Cache.inspect`.

| Field | Meaning |
| --- | --- |
| `path` | Inspected path string. |
| `status` | `available` (readable regular file matching supplied expectations), `missing` (absent), `corrupt` (wrong type or failed expectations), or `inaccessible` (other filesystem error). |
| `verified` | `True` only when a supplied SHA-256 matched. Readability or matching size alone leaves it `False`. |
| `error` | Original exception for an unavailable file, or `None` on success. |

**Construction:** returns a `FileInspection` instance; fields are assigned as
given, with no validation of manually supplied status/type combinations.
Incorrect constructor arguments raise `TypeError`. Assigning a field afterward
raises `dataclasses.FrozenInstanceError`.

```python
assert isinstance(inspection, dc.FileInspection)
manual_result = dc.FileInspection(path=path, status="available")
assert not manual_result.verified
```

### `CacheInspection`

```text
CacheInspection(
    path: str, status: str, files: Dict[str, FileInspection], verified: bool = False,
    error: Optional[Exception] = None
)
```

Frozen dataclass returned by `inspect_files`.

| Field | Meaning |
| --- | --- |
| `path` | Inspected root path string. |
| `status` | `available` (all required files available), `missing` (root absent), `corrupt` (wrong root type or incomplete/corrupt files), or `inaccessible` (root or required file access error). |
| `files` | Dictionary from required relative names to `FileInspection` results. Empty if the root could not be inspected as a directory. |
| `verified` | `True` only when every required file has a matching SHA-256. |
| `error` | Root failure or a representative failing file's exception; `None` on success. Inspect `files` for all individual failures. |

**Construction:** returns a `CacheInspection` instance; manually supplied fields
are not validated. Incorrect constructor arguments raise `TypeError`. Field
assignment raises `dataclasses.FrozenInstanceError`; the contained `files`
dictionary itself is not frozen.

```python
assert isinstance(inventory, dc.CacheInspection)
manual_inventory = dc.CacheInspection(
    path=str(cache_root), status="available", files={"records.csv": manual_result},
)
assert not manual_inventory.verified
```

### `FileValidationError`

```text
FileValidationError(path, reason)
```

`ValueError` subclass for a non-regular file, an integrity mismatch, or another
file validation failure. `path` accepts a string or `Path`; `reason` describes
the failure. Attributes are `path` (converted with `os.fspath`) and `reason`.
The message is `"<path>: <reason>"`. No filesystem access occurs in construction.

**Construction:** returns an exception instance; an invalid path type raises
`TypeError`. It is raised by validation/download helpers and retained in
inspection results when applicable.

```python
try:
    dc.validate_file(path, expected_size=0)
except dc.FileValidationError as error:
    assert error.path == path
    assert "size mismatch" in error.reason
```

### `make_file_readable`

```text
make_file_readable(path, *, group=True, others=False)
```

Explicitly add group read permission (`group=True`) and/or other-user read
permission (`others=True`) to one existing regular file, identified by a string
or `Path`. Both flags must be booleans. Adds no write/execute bits, removes no
permissions, and changes no contents. A `False` flag leaves that class's
existing access unchanged. Requires POSIX file-descriptor support. The caller
must be able to open the file and change its mode; parent directories and group
ownership must already allow the intended readers to reach it.

**Returns:** path string. **Raises:** `ValueError` for non-boolean flags,
`NotImplementedError` on unsupported platforms, `FileValidationError` for a
symlink or non-regular file, and `OSError` subclasses for missing files or
open/chmod failures. This is never called automatically on cache reuse.

```python
if hasattr(os, "fchmod") and hasattr(os, "O_NOFOLLOW"):
    dc.make_file_readable(path, group=True, others=False)
```

## SQLite

### Shared database behavior

All database builders return an open `sqlite3.Connection` owned by the caller.
Use `contextlib.closing` or call `.close()` explicitly: `with connection:`
commits/rolls back transactions but does not close the connection. Connections
are opened with `check_same_thread=False`, so Python does not reject cross-thread
use: applications must serialize simultaneous use of one connection and should
give independent concurrent workers their own.

A matching version and all requested tables permit reuse without parsing or
validating new DataFrames, changing old constraints, or rewriting the file.
Table names match as SQLite compares them, ignoring the case of ASCII letters.
Data changes are not detected automatically. Change `version` (an integer) or
use `overwrite=True` where available to rebuild. A rebuild replaces the
database's tables, not just the named table, and rolls back on failure.
Explicit overwrites also remove views; version-only rebuilds retain views.
New databases are staged before publication. See [reuse and replacement](data.md#reuse-and-replacement)
for locking, symlink, and filesystem requirements.

For a build, table names must be nonempty strings, distinct ignoring case,
and must not use the reserved metadata name `_datacache_metadata` or names starting with `sqlite_`.
DataFrame column names must be nonempty strings; spaces become underscores
and names must remain distinct ignoring case. Primary keys name one column;
index specifications are sequences of nonempty column-name sequences, such
as `[("id",), ("id", "value")]`, not bare strings. Original and normalized
column spellings are accepted. The pandas row index is not stored.

`show_progress=True` enables optional tqdm row-insertion bars; reuse is quiet.
Signed integers preserve precision, missing values become SQL `NULL`, and
dates/datetimes become ISO text. See [types and constraints](data.md) for details.

**Shared errors:** `ValueError` for invalid names, constraints, versions, or
unsupported dtypes; `TypeError` for unsupported argument types;
`sqlite3.IntegrityError` for duplicate or null primary keys; `OverflowError` for
integers outside SQLite's signed 64-bit range; other `sqlite3.Error` subclasses
for binding, database, schema, or locking failures; `OSError` subclasses for
filesystem/publication failures; and `ImportError` for missing tqdm when a
progress bar is needed. Existing malformed databases are not silently repaired.

### `db_from_dataframe`

```text
db_from_dataframe(
    db_filename, table_name, df, primary_key=None, subdir=None, overwrite=False,
    indices=(), version=1, *, cache_root=None, show_progress=False
)
```

Build or reuse one table named `table_name` from pandas DataFrame `df`.
`db_filename`, `subdir`, and `cache_root` are resolved with `resolve_path`;
parents are created if a build is needed. `primary_key=None` means no primary
key; `indices=()` means no extra indexes. `overwrite`, `version`, column naming,
and progress follow [shared database behavior](#shared-database-behavior).

**Returns:** open `sqlite3.Connection`. **Raises:** the shared database errors.

```python
with closing(dc.db_from_dataframe(
    "records.db", "records", frame, primary_key="id", indices=[("value",)],
    cache_root=cache_root, version=1,
)) as connection:
    assert connection.execute("SELECT value FROM records ORDER BY id").fetchall() == [(10,), (20,)]
```

### `db_from_dataframes`

```text
db_from_dataframes(
    db_filename, dataframes, primary_keys=None, indices=None, subdir=None,
    overwrite=False, version=1, *, cache_root=None, show_progress=False
)
```

Build or reuse a database containing the tables in the nonempty mapping
`dataframes`, from table names to pandas DataFrames. `primary_keys` optionally
maps table names to a primary-key column; omitted entries have no primary key.
`indices` optionally maps table names to index specifications; omitted entries
have no extra indexes. `db_filename`, `subdir`, and `cache_root` select the path
as in `db_from_dataframe`, creating parents when needed. `overwrite`, `version`,
and `show_progress` follow the shared behavior above.

**Returns:** open `sqlite3.Connection`. **Raises:** the shared database errors.

```python
with closing(dc.db_from_dataframes(
    "multiple.db", {"records": frame, "other_records": frame},
    primary_keys={"records": "id"}, indices={"records": [("value",)]},
    cache_root=cache_root,
)) as connection:
    assert connection.execute("SELECT COUNT(*) FROM other_records").fetchone() == (2,)
```

### `db_from_dataframes_with_absolute_path`

```text
db_from_dataframes_with_absolute_path(
    db_path, table_names_to_dataframes, table_names_to_primary_keys=None,
    table_names_to_indices=None, overwrite=False, version=1, *, show_progress=False
)
```

Use `db_path` directly (string or `Path`), without selecting a platform cache
directory. Despite the name, relative paths also work relative to the current
directory. The parent directory must already exist. For dangling symlinks,
the target's parent must exist; publication preserves the symlink.

`table_names_to_dataframes`, `table_names_to_primary_keys`, and
`table_names_to_indices` have the same mapping meanings as `dataframes`,
`primary_keys`, and `indices` in `db_from_dataframes`. `overwrite`, `version`,
and `show_progress` follow the shared database behavior.

**Returns:** open `sqlite3.Connection`. **Raises:** the shared database errors,
including filesystem errors if the parent is absent.

```python
with closing(dc.db_from_dataframes_with_absolute_path(
    root / "direct.db", {"records": frame},
    table_names_to_primary_keys={"records": "id"},
)) as connection:
    assert connection.execute("SELECT COUNT(*) FROM records").fetchone() == (2,)
```

### `fetch_csv_db`

```text
fetch_csv_db(
    table_name, download_url, csv_filename=None, db_filename=None, subdir=None,
    version=1, *, download_options=None, show_progress=False, **pandas_kwargs
)
```

Download/parse a CSV and build or reuse `table_name` in SQLite. `download_url`,
`csv_filename`, `subdir`, `download_options`, and `**pandas_kwargs` follow
`fetch_csv_dataframe`. `csv_filename=None` infers a download key from the URL;
`db_filename=None` infers a database name from the CSV name, row count, column
names, and dtypes. A schema whose column names contain `/` or `\`, or that would
make a name longer than 255 bytes, is named by a digest instead, so the database
is always a file directly in the cache directory. A database an older release
nested inside the cache under such a name is still reused in place when its
tables and version match. Other inferred names keep their historical spelling.
An explicit `db_filename` works independently of `csv_filename`. A `cache_root`
inside `download_options` applies to both files. Parser options must produce a
DataFrame with non-empty string column names: do not pass `chunksize` or
`iterator`, and pair `header=None` with `names=`.

With an explicit database filename, matching tables/version can be reused
without downloading or parsing the source. Supplying `force`, `expected_sha256`,
or `expected_size` in `download_options` requests source work even on such a
hit. Refreshing a source alone does not replace a matching database: increment
`version` when its data or schema changes. There is no `overwrite` parameter.
`show_progress` enables both download and insertion displays; an inner setting
overrides only the download display.

**Returns:** open `sqlite3.Connection`. **Raises:** `fetch_csv_dataframe` errors
and the shared database errors. Incompatible parser return types can raise
`AttributeError` or `TypeError` during database construction.

```python
with closing(dc.fetch_csv_db(
    "records", url, csv_filename="records.csv", db_filename="from-csv.db",
    download_options={"cache_root": cache_root}, version=1,
)) as connection:
    assert connection.execute("SELECT SUM(value) FROM records").fetchone() == (30,)
```

### `connect_if_correct_version`

```text
connect_if_correct_version(db_path, version, *, read_only=False)
```

Open an existing `db_path` (string or `Path`) when its DataCache metadata matches
the requested integer `version`. `read_only=True` uses SQLite's read-only open
mode. Never creates a missing database; closes connections it rejects. This
checks metadata only, not application tables, data correctness, or a checksum.

**Returns:** an open caller-owned `sqlite3.Connection` on a match, or `None`
when the path is missing, metadata is absent, or the version differs.

**Raises:** `OSError` subclasses for filesystem access failures,
`sqlite3.Error` subclasses for invalid/unreadable databases or malformed
metadata tables, and conversion errors such as `ValueError` for malformed
stored versions.

```python
connection = dc.connect_if_correct_version(cache_root / "records.db", 1, read_only=True)
assert connection is not None
with closing(connection):
    assert connection.execute("SELECT COUNT(*) FROM records").fetchone() == (2,)
assert dc.connect_if_correct_version(root / "absent.db", 1) is None
```

## Cache object

### `Cache`

```text
Cache(subdir='datacache', *, cache_root=None)
```

Select a root once for file, inspection, database, and deletion operations.
`subdir` must be a nonempty application name; `cache_root` optionally supplies
the exact string/`Path` root, overriding the platform location selected by
`subdir`. Construction performs no directory creation or download.

**Returns:** a `Cache` instance. Public attributes are `subdir` (the supplied
name) and `cache_directory_path` (the selected root as a string). Relative roots
remain relative to the working directory; use an absolute root if that directory
may change. **Raises:** `ValueError` for an empty `subdir`, invalid path-type
errors, or platform directory lookup errors from `get_data_dir`.

```python
cache = dc.Cache("reference-example", cache_root=root / "object-cache")
assert cache.cache_directory_path == str(root / "object-cache")
```

### `Cache.fetch`

```text
Cache.fetch(
    url, filename=None, decompress=False, force=False, timeout=None,
    use_wget_if_available=None, *, chunk_size=1048576, progress_callback=None,
    expected_sha256=None, expected_size=None, max_retries=2, retry_backoff=1.0,
    retry_max_delay=30.0, show_progress=False
)
```

Instance form of [fetch_file](#fetch_file), with `url` as the source URL and
the instance's root as `cache_root`. Every other parameter has the same meaning
and default as its `fetch_file` counterpart. Explicit names are remembered for
this instance's `delete_url` calls; this association is not written to disk.

**Returns:** local path string. **Raises:** the errors documented for `fetch_file`.

```python
cached_path = cache.fetch(url, filename="records.csv", expected_sha256=sha256)
assert Path(cached_path).read_bytes() == contents
```

### `Cache.local_filename`

```text
Cache.local_filename(url=None, filename=None, decompress=False)
```

Instance form of [build_local_filename](#build_local_filename). `url` is its
`download_url`; `filename` and `decompress` have identical meanings. Does not
access the filesystem or include the root directory in the result.

**Returns:** filename string. **Raises:** `ValueError` if neither a URL nor
a filename is supplied.

```python
assert cache.local_filename(filename="records.csv.gz", decompress=True) == "records.csv"
```

### `Cache.local_path`

```text
Cache.local_path(url=None, filename=None, decompress=False, download=False)
```

Instance form of [expected_path](#expected_path). `url`, `filename`, and
`decompress` select a cache key under this instance's root. With the default
`download=False`, only computes the path. With `download=True`, delegates to
`Cache.fetch` with its defaults; use `Cache.fetch` directly for timeouts,
integrity expectations, or other download options.

**Returns:** path string. **Raises:** naming/path argument errors, plus
`Cache.fetch` errors when `download=True`.

```python
assert cache.local_path(filename="records.csv") == cached_path
```

### `Cache.exists`

```text
Cache.exists(url=None, filename=None, decompress=False)
```

Check presence at `Cache.local_path(url, filename, decompress)`, using the
[file_exists](#file_exists) contract: no download, repair, or directory creation;
directories count as present and dangling symlinks as absent.

**Returns:** `bool`. **Raises:** naming/path argument errors and filesystem
errors other than missing files, such as `PermissionError`.

```python
assert cache.exists(filename="records.csv")
```

### `Cache.inspect`

```text
Cache.inspect(
    url=None, filename=None, decompress=False, *, expected_sha256=None,
    expected_size=None
)
```

Call [inspect_file](#inspect_file) on the path selected by `url`, `filename`,
and `decompress`. Both expectations have the same meanings as in `inspect_file`.
No network access or writes occur.

**Returns:** `FileInspection`. **Raises:** naming/path argument errors and
`ValueError` for invalid expectations; filesystem failures are in the result.

```python
assert cache.inspect(filename="records.csv", expected_sha256=sha256).verified
```

### `Cache.make_readable`

```text
Cache.make_readable(
    url=None, filename=None, decompress=False, *, group=True, others=False
)
```

Call [make_file_readable](#make_file_readable) on the path selected by `url`,
`filename`, and `decompress`. `group` and `others` have that function's meanings.
Explicit POSIX maintenance only: does not download or recurse into directories.

**Returns:** path string. **Raises:** naming/path argument errors and the
errors documented for `make_file_readable`.

```python
if hasattr(os, "fchmod") and hasattr(os, "O_NOFOLLOW"):
    cache.make_readable(filename="records.csv", group=True)
```

### `Cache.db_from_dataframe`

```text
Cache.db_from_dataframe(
    db_filename, table_name, df, key_column_name=None, *, overwrite=False, version=1,
    show_progress=False
)
```

Build or reuse `table_name` from `df` at `db_filename` under this cache's root.
An absolute filename overrides the root. `key_column_name` is the primary-key
column, equivalent to `primary_key` in the top-level builder. `overwrite`,
`version`, and `show_progress` follow [db_from_dataframe](#db_from_dataframe).
This method has no index specification argument; use the top-level function
with `cache_root=cache.cache_directory_path` to request indexes.

**Returns:** open caller-owned `sqlite3.Connection`. **Raises:** the
[shared database errors](#shared-database-behavior).

```python
with closing(cache.db_from_dataframe(
    "records.db", "records", frame, key_column_name="id",
)) as connection:
    assert connection.execute("SELECT COUNT(*) FROM records").fetchone() == (2,)
```

### `Cache.delete_url`

```text
Cache.delete_url(url)
```

Remove local files for a URL string. Includes paths successfully fetched by
this `Cache` instance (including explicit filenames), plus the URL-derived
raw/decompressed paths that may have been created by other instances.
Missing files are ignored. Explicit filenames used by earlier instances are
not recorded persistently and cannot be rediscovered by URL alone. Files
unrelated to those paths are left alone. A symlink at a selected file path is
unlinked without deleting its target.

**Returns:** `None`. **Raises:** naming errors and `OSError` subclasses such as
`PermissionError` or a directory-removal error. Deletion is not transactional:
some files may already have been removed when another removal fails.

```python
cache.delete_url(url)
assert not cache.exists(filename="records.csv")
```

### `Cache.delete_all`

```text
Cache.delete_all()
```

Remove **all contents** of this cache root recursively, including files not
created by DataCache. Use only a dedicated directory. Preserves the root and
its permissions. A root symlink is followed to the selected directory, while
symlinks inside the cache are unlinked without clearing their external targets.
The instance's remembered download paths are cleared after success.

**Returns:** `None`. **Raises:** `FileNotFoundError` if the root is absent,
`NotADirectoryError` for a non-directory root, and other `OSError` subclasses
on access/removal failure. Deletion is not transactional and may partially
complete on error. Unlike `clear_cache`, this method preserves the root and
works with an explicit `cache_root`.

```python
cache.delete_all()  # Safe here: object-cache is inside our temporary example directory.
assert Path(cache.cache_directory_path).is_dir()
assert not list(Path(cache.cache_directory_path).iterdir())
```

## Package version

### `__version__`

String containing the installed DataCache package version. This is a constant,
not a callable; it has no parameters, defaults, or API-specific exceptions.
It is independent of the integer `version` used to identify a SQLite cache.

```python
print(dc.__version__)
```

After finishing the examples, remove their temporary files:

```python
temporary.cleanup()
```
