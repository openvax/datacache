# Choose an interface and understand guarantees

## Choose the right API

The [complete API reference](https://github.com/openvax/datacache/blob/master/docs/api.md)
lists every public signature, default, return value, exception, and example.

| Task | API | Result |
| --- | --- | --- |
| Install or reuse a versioned dataset | `VersionedDatasetRegistry`, `install_bundle(...)` | Mapping of asset names to snapshot paths |
| Install versioned archive trees | `VersionedArchiveRegistry`, `install_archive(...)` | Immutable extracted-generation `Path` |
| Reuse an established fixed-path versioned file cache | `VersionedFileRegistry` | One Path and a legacy-compatible root receipt |
| Inspect a complete dataset generation | `inspect_bundle(...)` | `BundleInspection` |
| Inspect an installed archive tree | `inspect_archive(...)` | `ArchiveInspection` |
| Discard retained partial download bytes | `discard_partial(destination)` | Installed file unchanged |
| Download or reuse one file | `fetch_file(...)`, `Cache.fetch(...)` | Local path string |
| Compute a path without filesystem access | `expected_path(...)`, `Cache.local_path(...)` | Path string |
| Choose a root shared by several packages | `get_cache_root(name, *envkeys)` | Path string |
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

## Guarantees and limits

Downloads and complete archive trees are staged privately and published
atomically after validation.
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
transactions govern database rebuilds. Use `install_bundle` for atomic multi-file
publication. Bundle installation and resumable transfers require POSIX local
filesystems; distributed coordination is outside their scope.
