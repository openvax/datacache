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

Progress is optional; install the `progress` extra when enabling tqdm. HTML
table conversion requires the `html` extra. The library does not install
logging handlers or change the process umask. Download retry counts and waits
are bounded, while `timeout` limits connection/read inactivity per attempt,
not total elapsed time.

Atomic downloads do not provide multi-file transactions, distributed locking,
or crash cleanup for abandoned staging files. SQLite operations depend on
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
The runnable [offline example](../examples/basic_usage.py) exercises the public
download, transform, inspection, CSV, and database APIs together.
