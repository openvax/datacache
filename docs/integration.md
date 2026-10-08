# Building downstream libraries on DataCache

See the [API reference](api.md) for signatures, defaults, return values, and errors.

Use the public `Cache`, `fetch_file`, inspection, and database APIs for new
integrations. Existing pyensembl calls to
`_download_and_decompress_if_necessary` retain their legacy URL inference and
receive the corrected permissions and HTTP retry behavior. Explicit private
helper transform flags retain the public parsed-URL behavior.

## Recommended contracts

1. Let callers choose a cache root. Do not create it merely to check whether a
   dataset is installed: use `expected_path`, `Cache.local_path`, or inspection.
2. Keep presence separate from integrity. `exists` answers whether a path is
   present; `inspect` and `validate_file` check readability and supplied trusted
   hash/size. Inspect every required member of a release.
3. Pass trusted metadata for installed bytes, after transformation. Reuse the
   same expectations across concurrent writers to a destination.
4. Make installation and repair explicit. Do not automatically force a refresh
   merely because a cache is corrupt, inaccessible, or unavailable offline.
5. Forward `timeout`, retry settings, `progress_callback`, and `show_progress`
   where your library exposes downloads. Keep logging configuration in the
   top-level application.
6. Use database versions to identify data/schema changes. Close connections
   when finished; use separate connections for independent concurrent work.
7. Let original network and filesystem exceptions propagate, or retain them
   as causes when adding domain-specific context. `FileValidationError.path`
   and `.reason` provide structured validation details.
8. Use `install_archive` when a released tar file owns a directory tree. Treat
   only `inspect_archive(...).status == "available"` as installed and resolve
   member paths from the returned bundle directory, never from the store.

## Reusable downstream toolkit

Downstream libraries should keep domain catalogues, CLI wording and generated
indexes, while delegating acquisition and source-store lifecycle to DataCache:

| Downstream shape | DataCache API | Shared behavior |
| --- | --- | --- |
| One established file path, as in PyEnsembl annotation/FASTA sources | `fetch_file` or `VersionedFileRegistry` | Atomic download, decompression, retries, resume, progress, integrity and provenance |
| A released tar tree, as in MHCflurry weights/data | `VersionedArchiveRegistry` or `install_archive` | Ordered parts, safe extraction, consumer receipts, all-or-nothing publication and status |
| Several separately published files that belong together | `VersionedDatasetRegistry` or `install_bundle` | Pinned versions, all-or-nothing publication and inspection |

All explicit download methods forward `show_progress`, callback, timeout and
bounded retry options to the shared transport. Registry construction, status,
inspection and local-path resolution are offline. Applications can therefore
offer consistent `list`/`info`/`download` behavior without maintaining private
network or extraction implementations.

PyEnsembl can preserve its fixed paths and derived SQLite/FASTA indexes while
continuing to use `fetch_file`; direct callers can set `record_provenance=True`
and expose `inspect_file` results for download visibility. Those derived
artifacts remain application-owned.

MHCflurry can preserve release selection, exact URL receipts and public model
paths through the archive registry's `store_path` callback and the returned bundle path.

## Shared OpenVax cache

OpenVax packages share one cache so that published data, such as test datasets,
is downloaded once for all of them. Select its root with `get_cache_root`,
listing the package's own variable before the shared one:

```python
root = datacache.get_cache_root("openvax", "OSTEOSARC_CACHE", "OPENVAX_DATA_CACHE")
```

`OPENVAX_DATA_CACHE`, when set, is the root itself; otherwise the platform cache
directory named `openvax` is used. Store content-addressed files under
`<root>/objects/sha256/<sha256><original suffixes>`, as osteosarc and Vaxrank
do, so identical files are shared. Keep each package's own receipts and derived
files in a subdirectory named after the package. Importing datacache does not
import pandas, numpy, or requests, so command-line tools can resolve the root
cheaply; those are loaded only by the functions that need them.

## Compatibility and scope

Existing positional download and database arguments remain supported. New
settings are keyword-only. Filename normalization and archive-selection rules
remain compatible; see the [download reference](downloads.md).

Cache hits preserve existing bytes, names, schemas, versions, and permissions.
New-build validation runs only when a database actually needs rebuilding.
The [upgrade guide](data.md#upgrading-existing-caches) explains legacy source
reuse and deliberate repair of previously corrupted data. Optional
`make_file_readable` maintenance lets file owners share selected old `0600`
files without changing them during normal cache access.

The existing pandas dependency range remains unchanged. CI exercises pandas
1.4.4, 1.5.3, and current versions; older versions in the historical dependency
range are not all tested on modern Python. Nullable dtype support depends on
the installed pandas version.

DataCache caches by path or database version, not by tracking remote content
changes. Refreshing a URL does not invalidate arbitrary downstream artifacts.
Libraries own dataset versions and their dependency relationships.

tqdm is installed by default; enable displays with `show_progress=True`. HTML
table conversion requires the `html` extra. The library does not install
logging handlers or change the process umask. Download retry counts and waits
are bounded, while `timeout` limits connection/read inactivity per attempt,
not total elapsed time.

Use [versioned bundles](bundles.md) when named files must be installed together,
or [archive installation](archives.md) when a tar archive defines the complete
tree. Archive stores use cross-platform locks; `install_bundle` currently
requires POSIX `flock`.
Individual atomic downloads do not provide multi-file transactions or distributed
locking. Unhandled termination can leave staging files behind. SQLite operations depend on
SQLite's filesystem locking and journaling. New database publication needs
hard-link support. Validate deployment-specific network filesystems and ACL
requirements in the consuming application.

## Regression coverage

The test suite includes raw/gzip/ZIP downloads, private downstream helper
contracts, shared-file modes, immutable inspection, HTTP retries, callback
failure, concurrent publication, integer precision, nullable dtypes, constraint
normalization, failed database rebuilds, and transformation failure cleanup.
Released-code fixtures also check legacy cache names and SQLite schemas,
read-only reuse, and unchanged contents and permissions across upgrades.
The runnable [offline example](https://github.com/openvax/datacache/blob/master/examples/basic_usage.py) exercises the public
download, transform, inspection, CSV, and database APIs together.
