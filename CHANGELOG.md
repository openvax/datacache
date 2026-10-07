# Changelog

## 1.18.1

- Reorganize the documentation into a site (`./docs.sh`, published from
  master): installation and an offline verified-cache example on the home
  page, guides ordered by common tasks, and an interface and guarantees
  reference. The README links to the guides by absolute URL, so they work on
  PyPI, and old README section links land on the guides that replaced them.
- Run the documentation's examples and check internal links in the
  documentation build.

## 1.18.0

- Rename `fetch_file` and `Cache.fetch`'s `stale_if_error`, added in 1.17.0, to
  `return_stale_on_error`, which says what it does: return the cached copy when
  a refresh fails. The old name is no longer accepted.

## 1.17.0

- Add `fetch_bytes` for small resources such as directory listings: the body
  in memory, transferred and retried exactly like `fetch_file` downloads,
  with nothing written to disk. Downloads and `fetch_bytes` share one retry
  loop.
- Add `expire_after` to `fetch_file` and `Cache.fetch`, named as in
  requests-cache: a cached file older than the given seconds or `timedelta`,
  or one that no longer validates, is downloaded again (also with
  `resume=True`). Age comes from the provenance record's fetch time, or else
  the file's modification time.
- Add `stale_if_error`: when a refresh fails, return the valid cached file
  with a redacted warning instead of raising, like HTTP `stale-if-error`.
  Progress-callback cancellations still propagate.
- Add `validator`: a callable that rejects wrong content, such as an HTTP 200
  error page, by raising or returning `False`, before it replaces the cached
  file and on cache hits. Generalizes the empty-response check (#74).

## 1.16.1

- Always install `tqdm>=4.64` as a required dependency. Progress displays remain
  opt-in with `show_progress=True` and tqdm stays lazily imported. Keep the
  `progress` extra as a compatibility alias for existing installation commands.

## 1.16.0

- Add `install_archive` and `inspect_archive` for safely installing a complete
  tar archive tree as one immutable generation. Ordered split archives are
  concatenated byte-for-byte; URL and local-path sources share the same API.
  Extraction rejects traversal, links, special files, path collisions and
  reserved metadata names, and ignores archive ownership and permission bits.
  Standard `.` / `./` tar paths are normalized safely; resource limits stop
  header scanning at the first violation, and local FIFOs cannot block a writer.
- Add `VersionedArchiveRegistry`, a catalogue facade with pinned defaults,
  explicit versions, reusable status rows, fast local path resolution, consumer
  layout callbacks, local predownloaded-part overrides, and the same progress,
  retry, verification and recovery behavior as `install_archive`.
- Include the recorded URL and observed SHA-256 in `VersionedFileRegistry`
  status rows so fixed-file consumers can report the same download provenance.
- Publish consumer metadata such as MHCflurry's `DOWNLOAD_INFO.csv` inside the
  same transaction with `extra_files`. Archive receipts record the ordered
  source identities, assembled and per-part observed hashes, and every extracted
  file. Trusted expectations remain distinct from observed consistency checks.
  Reuse checks the complete consumer metadata inventory, including removals.
  Large manifests are read without the small control-record size cap and
  validated in staging before the generation is published.
- Serialize archive writers with a cross-platform file lock, retain immutable
  generations across refreshes, and recover a complete generation after an
  interrupted pointer publication. Legacy or otherwise foreign directories are
  never claimed, including with `force=True` (#85).

## 1.15.0

- Add `VersionedFileRegistry` for established single-file caches with fixed
  version paths and root provenance manifests. Legacy files are reused offline
  without relocation; transfers use the shared downloader, and new receipts
  use bounded-memory hashing and atomic JSON publication (#83). Writers serialize
  per root to prevent the legacy registry’s lost manifest-update race.

## 1.14.0

- Allow `resume=True` with `expected_size` alone when the server supplies a
  strong ETag. Resume uses `If-Range` and validates the returned range and
  validator; servers without a strong ETag still require `expected_sha256`.
  Complete size-only partials restart instead of being published without a
  fresh response. Size-only validation does not verify content checksums (#80).
- Add `raw=True` to `fetch_file` and `Cache.fetch` to disable archive
  decompression and HTML conversion at arbitrary output names. It composes
  with integrity checks, atomic replacement, progress, provenance, and resume;
  existing suffix inference is unchanged when raw mode is omitted (#81).

## 1.13.0

- Add integrity-pinned resumable raw HTTP downloads with `resume=True`, private
  persistent partials, validated ranges, bounded retries, and explicit
  `discard_partial` cleanup (#64).
- Add `VersionedDatasetRegistry`, `install_bundle`, and `inspect_bundle` for
  single-file and multi-file datasets. Immutable generations and an atomic
  pointer preserve readers and old installations through refreshes; completed
  local generations can be recovered offline. Supports hitlist's registry
  mapping and independently versioned source bundles (#59).
- Check full source URL fingerprints and decompression settings before reusing
  bundle assets without trusted hashes. Hash-pinned generations remain shareable
  across libraries using different mirrors or compression settings.
- Accept precreated empty bundle directories without forcing an install, keep
  their access modes, and reject asset/provenance-sidecar collisions before writes.
  Publish generations with their final sharing permissions while keeping failed
  or interrupted resumable work inside a private staging directory.
- Remove the redundant test-local FASTA downloader and unused private `ext`
  option; use urllib3's public exception import. Keep the published 2.0
  deprecation deadline for `DatabaseTable.from_fasta_dict` (#67).

- Report `size` and `mtime` for an available file from `inspect_file`,
  `Cache.inspect`, and `inspect_files`, taken from the file that was validated,
  so a cache listing needs no extra `stat` (#75).
- Add `record_provenance=True` to `fetch_file` and `Cache.fetch`. After
  publishing, a hidden `.<name>.datacache.json` records the source URL (without
  user name, password, query string, or fragment), fetch time, size, and the
  SHA-256 when `expected_sha256` verified it, with the file's permissions.
  Inspection reports `source_url`, `fetched_at`, and `recorded_sha256` while the
  file is unchanged; `verified` still means a supplied digest matched just now.
  Any new download removes a stale record. Recording is off by default (#75).
- Reject empty downloads. A complete but empty response, such as a withdrawn
  upstream record, was published and then reused as a valid cache entry. An
  empty HTTP response is now retried like a transient failure, then raises
  `FileValidationError` without publishing; an empty decompressed archive
  member is rejected too. An empty file already in a cache is an invalid hit
  that `force=True` replaces. Pass `allow_empty=True` to `fetch_file` or
  `Cache.fetch`, or `expected_size=0`, when an empty file is expected (#74).

## 1.12.0

- Add `get_cache_root(name, *envkeys)`, which selects a cache root shared by
  several packages. The first environment variable that is set is the root
  itself; otherwise the platform cache directory for `name` is used. Unlike
  `get_data_dir(subdir, envkey)`, nothing is appended to the variable's value.
  OpenVax packages use `get_cache_root("openvax", "<PACKAGE>_CACHE",
  "OPENVAX_DATA_CACHE")` so that shared data is downloaded once.
- Import pandas, numpy, and requests only in the functions that use them.
  `import datacache` now takes tens of milliseconds instead of hundreds, so
  command-line tools can resolve cache paths cheaply. `download.pd` and
  `download.requests` still refer to those modules.
- Map numpy's fixed-width types in `db_type` on Python 3: `str_` (`dtype('U…')`)
  becomes `TEXT`, and `bytes_` (`dtype('S…')`) and `bytes` become `BLOB`,
  instead of raising `ValueError`. Databases built from DataFrames are
  unchanged, since pandas stores such arrays with the `object` dtype.
- Remove leftover Travis, pylint, and Python 2 configuration and syntax.

## 1.11.1

- Shorten cache names longer than 255 UTF-8 bytes. Names were shortened by
  character count, so a long non-ASCII filename (for example 100 CJK
  characters) exceeded Linux's 255-byte limit, and `fetch_file`,
  `fetch_csv_dataframe`, and `fetch_csv_db` failed with `OSError`. Such names
  now become a digest followed by as much of their end, including the
  extension, as fits. Every name within the limit is unchanged. On macOS and
  Windows, which limit characters rather than bytes, a file already cached
  under such a name is downloaded once more under the shorter name.

## 1.11.0

- Keep `fetch_csv_db` database names safe. An inferred name spells out each
  column, so a header containing `/` created subdirectories (and `..` could
  place the database outside the cache), characters such as `:` and `?` from
  headers or the CSV filename are invalid on Windows, and wide CSVs exceeded the
  255-byte file name limit and failed with `OSError`. Names are now spelled out
  only when every character is valid on all supported platforms and SQLite's
  journal still fits, measured the same way everywhere; otherwise the schema is
  named by a digest. A matching database stored under the historical name is
  still reused in place. A new `version` rebuilds it under the new name and
  leaves the older copy alone, since another process may still have it open, and
  logs a warning naming it.
- Explain why a database cannot be rebuilt when the filesystem has no room for
  its SQLite `-journal` file name: a `version` change or `overwrite=True` now
  raises a clear `ValueError` instead of `unable to open database file`.
  Creating and reusing such a database still work.
- Reject the reserved `_datacache_metadata` and `sqlite_` table names before
  reusing a database. Requesting `_datacache_metadata` for an existing database
  returned the version table instead of storing the DataFrame.
- Compare table and column names the way SQLite does, ignoring the case of ASCII
  letters only: `records` now reuses an existing `Records` table instead of
  rebuilding the database, and `Éclair` and `éclair` are accepted as distinct.
- Report non-string column labels (for example `header=None` without `names=`)
  with the same `ValueError` whether or not `db_filename` is given, instead of
  an `AttributeError`.
- Choose ZIP members more carefully: a member with the output's name inside a
  folder, or differing only in letter case, is installed instead of the largest
  member, preferring the copy nearest the archive root, then an exact-case
  match. When nothing matches, the largest member is still installed, now with a
  logged warning if the output was named explicitly and the archive has several.
  New downloads of such archives install different bytes, so an
  `expected_sha256` pinned to the previously installed member fails validation;
  files already in a cache are unchanged.
- Suggest `force=True` only when a regular file is present; it cannot replace a
  directory at the cache path.
- Store `float16` DataFrame columns as `FLOAT`.
- Let `ensure_dir` accept a directory that another process creates at the same
  moment.
- Declare MD5 cache-key digests as not used for security, so naming works on
  FIPS-mode Python. Existing cache keys are unchanged.
- Deprecate `DatabaseTable.from_fasta_dict`; it will be removed in datacache
  2.0.
- Correct `fetch_file`'s `subdir` and `timeout` documentation, and document
  `Cache`, `ensure_dir`, `get_data_dir`, and `clear_cache`.

## 1.10.2

- Add a complete [public API reference](docs/api.md) covering every name in
  `datacache.__all__` and every public `Cache` method, with signatures,
  defaults, return values, exceptions, and runnable offline examples.
- Lead the README with a copy-pasteable download quickstart and explain how to
  locate, inspect, and clear a cache directory on each platform.
- Use absolute documentation links so the PyPI project page resolves them.
- Check the reference in CI: its version, its coverage of `datacache.__all__` and
  the public `Cache` methods, its signatures, and its examples are all tested.

No API, behavior, or cache-format changes.

## 1.10.1

- Rebuild databases containing SQLite full-text-search tables without trying
  to drop already-removed shadow tables. Failed rebuilds still restore the
  original tables, full-text indexes, data, and version.
- Create every requested SQLite index when generated names collide, including
  across tables. Keep historical names where available and reuse equivalent
  non-partial indexes instead of duplicating them.
- Keep cache hits unchanged. Existing databases with missing indexes can be
  rebuilt explicitly with `overwrite=True` or a new database version.

## 1.10.0

- Publish new downloads and decompressed files with normal creation permissions
  for shared caches; keep staging private and preserve existing modes (#68).
- Roll back failed SQLite rebuilds, including explicit overwrites, and publish
  new databases only after successful construction. Close rejected version
  lookup connections and support explicit read-only lookup.
- Remove existing views during explicit SQLite overwrites in the same
  transaction, restoring views and their triggers if rebuilding fails.
- Create databases through dangling symlinks by staging and publishing at
  their targets, preserving the links and cleaning up failed builds.
- Preserve integer types and precision; support nullable pandas scalars and
  empty tables. Normalize column constraints consistently and reject ambiguous
  names. Insert rows incrementally in bounded batches.
- Publish custom transformations only after success, honor their source cache
  directory, and add explicit rebuild/download options.
- Fix CSV database filename inference and separate download settings from
  pandas parsing options.
- Keep legacy filenames, database versions, schemas, and cached file modes
  unchanged on reuse. Open matching databases before validating rebuild
  inputs, and reuse explicitly named CSV databases without their source file.
  Honor source integrity and refresh requests when supplied.
- Reuse transform sources stored in the old default directory without moving
  them, while placing new sources in the requested directory.
- Keep the existing pandas dependency range and cover pandas 1.4.4 in CI.
- Add explicit, single-file `make_file_readable` / `Cache.make_readable`
  maintenance for owners who want to share older private cache files.
- Add optional tqdm progress for downloads, decompression, hash verification,
  and database insertion, with exception-safe cleanup and per-attempt counts.
- Replace stale API documentation with an offline quickstart and guides for
  shared caches, databases, progress, and downstream integration.

There is no cache migration or required rebuild on upgrade. Existing private
files remain usable by their owner; use the explicit permission helper to
share selected files. Databases already containing corrupted numbers require
a deliberate rebuild from source to recover those values. Existing caches
are not silently modified.
