# DataCache

[![Tests](https://github.com/openvax/datacache/actions/workflows/tests.yml/badge.svg)](https://github.com/openvax/datacache/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/datacache.svg)](https://pypi.org/project/datacache/)

Download, verify, transform, and cache datasets for Python applications,
including OpenVax libraries such as pyensembl. DataCache provides streaming
downloads, gzip/ZIP decompression, reusable local paths, offline inspection,
and SQLite caches built from pandas DataFrames.

## Install

Python 3.9 or newer is required.

```sh
python -m pip install datacache
# Optional progress bars and HTML-table conversion:
python -m pip install "datacache[progress,html]"
```

Progress is opt-in with `show_progress=True`. Normal use does not import tqdm
or configure your application's logging.

The existing pandas dependency range is unchanged; upgrading DataCache does
not introduce a pandas 1.5 requirement. CI covers pandas 1.4.4, 1.5.3, and
current releases on supported Python versions.

## Quickstart

Download a file once, then reuse its local path on later calls:

```python
from datacache import Cache

cache = Cache("my-project")
url = "https://raw.githubusercontent.com/openvax/datacache/master/LICENSE"
path = cache.fetch(url, filename="LICENSE", timeout=30)
print(path)

# Reuses the cached file without a network request, even in a later process.
assert cache.fetch(url, filename="LICENSE") == path
```

Replace the URL and filename with your dataset. Existing files are reused until
you explicitly refresh them with `force=True`; DataCache does not check whether
the remote file has changed. Add `show_progress=True` to display a download bar
after installing `datacache[progress]`.

### Find, inspect, or clear your cache

`Cache("my-project")` selects a platform cache directory without creating it.
`Cache()` and top-level helpers without a `subdir` use the name `datacache`.

| Platform | Default directory for `Cache("my-project")` |
| --- | --- |
| Linux / Unix | `$XDG_CACHE_HOME/my-project`, or `~/.cache/my-project` when unset |
| macOS | `~/Library/Caches/my-project` |
| Windows | `%LOCALAPPDATA%\my-project\my-project\Cache` |

A cache name selects its own application directory; it is not nested inside
the `datacache` directory. To choose the exact root instead, use
`Cache("my-project", cache_root="/data/my-project")`. A relative root stays
relative and is re-resolved against the working directory on every call, so use
an absolute root if your program may change directories.

Using the cache from the quickstart:

```python
print(cache.cache_directory_path)  # Directory containing cached files.
print(cache.local_path(filename="LICENSE"))  # Computes a path without creating it.
print(cache.inspect(filename="LICENSE").status)  # available, missing, corrupt, or inaccessible

# Explicit cleanup; uncomment only when you want to remove these cached files:
# cache.delete_url(url)  # Removes this root's downloads for this URL.
# cache.delete_all()  # Removes ALL contents, keeping the root directory.
```

Inspection works offline and never repairs files. Without an expected SHA-256,
`available` means readable and regular; it does not prove the bytes are correct.
`delete_url` also finds the URL-derived filenames under this root, whichever
instance created them, but explicit filenames from earlier instances are not
recorded persistently. Keep a dedicated
cache directory: `delete_all()` removes every file and subdirectory in it and
raises `FileNotFoundError` if the root does not exist. For the default platform
location, `clear_cache("my-project")` removes the root too. See the
[cleanup API](https://github.com/openvax/datacache/blob/master/docs/api.md#cachedelete_all)
for details.

### Offline example with integrity checking

This example runs entirely offline and cleans up after itself:

```python
import gzip
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory

from datacache import Cache

with TemporaryDirectory() as directory:
    root = Path(directory)
    contents = b">reference\nACGT\n"
    source = root / "reference.fa.gz"
    source.write_bytes(gzip.compress(contents))

    cache = Cache("references", cache_root=root / "cache")
    path = cache.fetch(
        source.as_uri(),  # HTTP, HTTPS, and FTP URLs also work.
        filename="reference.fa",
        expected_sha256=hashlib.sha256(contents).hexdigest(),
    )
    assert Path(path).read_bytes() == contents
    source.unlink()
    assert cache.fetch(source.as_uri(), filename="reference.fa") == path
    print(cache.inspect(filename="reference.fa").status)  # available
```

For real releases, get the expected hash from trusted release metadata. Hashes
describe the installed bytes **after** decompression or conversion. A hash
computed from an untrusted download does not establish authenticity.

To install at an exact path instead of using a cache key:

```python
from datacache import fetch_file

path = fetch_file(
    "https://example.org/releases/v1/records.tsv.gz",
    destination="references/v1/records.tsv",
    decompress=True,
    timeout=30,
    show_progress=True,  # Requires datacache[progress].
)
```

Replace the example URL with your dataset URL. Existing files are reused;
`force=True` explicitly replaces them. When integrity expectations are supplied,
an invalid cache hit raises `FileValidationError` instead of silently replacing
the file. Failed downloads leave the previous file intact.

## Choose the right API

The [complete API reference](https://github.com/openvax/datacache/blob/master/docs/api.md)
lists every public signature, default, return value, exception, and example.

| Task | API | Result |
| --- | --- | --- |
| Download or reuse one file | `fetch_file(...)`, `Cache.fetch(...)` | Local path string |
| Compute a path without filesystem access | `expected_path(...)`, `Cache.local_path(...)` | Path string |
| Check presence | `file_exists(...)`, `Cache.exists(...)` | Boolean; does not establish integrity |
| Validate bytes, raising on failure | `validate_file(...)` | Path string |
| Inspect without repair or network access | `inspect_file(...)`, `Cache.inspect(...)` | `FileInspection` |
| Inspect a set of required files | `inspect_files(root, files)` | `CacheInspection` |
| Explicitly share an existing private file | `make_file_readable(...)`, `Cache.make_readable(...)` | Path string; POSIX only |
| Download and parse CSV/TSV | `fetch_csv_dataframe(...)` | pandas DataFrame |
| Cache a custom file transformation | `fetch_and_transform(...)` | Transformer/loader result |
| Create a SQLite cache | `db_from_dataframe(...)`, `db_from_dataframes(...)` | Open SQLite connection |
| Download CSV into SQLite | `fetch_csv_db(...)` | Open SQLite connection |
| Reopen a database with matching metadata | `connect_if_correct_version(...)` | Connection or `None` |

The library does not export `fetch_fasta_dict` or `fetch_fasta_db`. Download
FASTA files with `fetch_file`, then parse them in the consuming library.

## Guides

- [API reference](https://github.com/openvax/datacache/blob/master/docs/api.md):
  all public functions, `Cache` methods, inspection results, and exceptions.
- [Downloads and cache inspection](https://github.com/openvax/datacache/blob/master/docs/downloads.md): destinations, naming,
  decompression, integrity, retries, concurrency, and downstream compatibility.
- [Progress and logging](https://github.com/openvax/datacache/blob/master/docs/progress.md): tqdm, callbacks, retries, and
  independent download options for CSV helpers.
- [SQLite and transformations](https://github.com/openvax/datacache/blob/master/docs/data.md): numeric fidelity, column names,
  versioning, rollback, connection ownership, and custom transformations.
- [Shared caches](https://github.com/openvax/datacache/blob/master/docs/shared-caches.md): permissions, read-only use, and
  troubleshooting existing installations.
- [Downstream integration](https://github.com/openvax/datacache/blob/master/docs/integration.md): contracts for consuming libraries.
- [Release notes](https://github.com/openvax/datacache/blob/master/CHANGELOG.md) and
  [release procedure](https://github.com/openvax/datacache/blob/master/RELEASING.md).

## Guarantees and limits

Downloads are staged privately and published atomically after validation.
New files respect the process umask; replacements preserve existing access
permissions. This includes pyensembl's private download helpers.

Custom single-file transformations publish only successful output. Existing
SQLite caches rebuild in a transaction: failure rolls back both schema and
rows. New databases are built privately before publication. Cached data is
reused by path or database version; DataCache does not automatically discover
remote changes or repair previously corrupted caches.

Upgrades keep existing cache names and database metadata compatible. Valid
cache hits do not rewrite files, change permissions, or apply new schema
constraints. See [upgrading existing caches](https://github.com/openvax/datacache/blob/master/docs/data.md#upgrading-existing-caches)
and [sharing old private files](https://github.com/openvax/datacache/blob/master/docs/shared-caches.md#files-already-downloaded-as-0600).

File publication requires local filesystem support for atomic replacement;
new SQLite database publication also requires hard links. SQLite locking and
transactions govern database rebuilds. These are single-file guarantees, not
a multi-file release installer or a distributed lock service.

## Development

```sh
python -m pip install -e ".[test]"
./lint-and-test.sh
python -m examples.basic_usage
```

Tests use local files, mocked responses, and local HTTP servers. They do not
depend on external dataset servers. See CI for the Python, dependency, and
operating-system combinations exercised.
