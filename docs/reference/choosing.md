# Choose an interface and understand guarantees

## Choose the right API

The [complete API reference](../api.md)
lists every public signature, default, return value, exception, and example.

| Task | API | Result |
| --- | --- | --- |
| Download or reuse one file | `fetch_file(...)`, `Cache.fetch(...)` | Local path string |
| Refresh a cached file periodically, keeping it when the source is unreachable | `fetch_file(..., expire_after=..., return_stale_on_error=True, validator=...)` | Local path string |
| Read a small resource into memory | `fetch_bytes(...)` | `bytes` |
| Install or reuse a versioned dataset | `VersionedDatasetRegistry`, `install_bundle(...)` | Mapping of asset names to paths in the current bundle |
| Build derived files from versioned dependencies | `materialize(...)` | Mapping of output names to paths in the current bundle |
| Install versioned archive trees | `VersionedArchiveRegistry`, `install_archive(...)` | `Path` of the extracted tree, the current bundle |
| List or delete old bundles in any store | `list_bundles(...)`, `prune_bundles(...)` | Bundle paths, oldest first |
| Reuse an established fixed-path versioned file cache | `VersionedFileRegistry` | One Path and a legacy-compatible root receipt |
| Inspect a dataset's current bundle | `inspect_bundle(...)` | `BundleInspection` |
| Inspect an installed archive tree | `inspect_archive(...)` | `ArchiveInspection` |
| Discard retained partial download bytes | `discard_partial(destination)` | Installed file unchanged |
| Compute a path without filesystem access | `expected_path(...)`, `Cache.local_path(...)` | Path string |
| Choose where a package keeps its data, shared or not | `get_cache_root(name, *envkeys, override=..., legacy=...)` | Path string |
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
reused by path or database version. DataCache doesn't ask servers whether files
changed; `expire_after` refreshes files after a set time and also replaces
cached files that no longer validate. Otherwise invalid caches are reported,
not repaired.

Upgrades keep existing cache names and database metadata compatible. Valid
cache hits do not rewrite files, change permissions, or apply new schema
constraints. See [upgrading existing caches](../data.md#upgrading-existing-caches)
and [sharing old private files](../shared-caches.md#files-already-downloaded-as-0600).

File publication requires local filesystem support for atomic replacement;
new SQLite database publication also requires hard links. SQLite locking and
transactions govern database rebuilds. Use `install_bundle` for atomic multi-file
publication. Bundle, archive and materialization installs work on local filesystems on
Linux, macOS and Windows; resumable transfers need POSIX. Distributed
coordination is outside their scope.
