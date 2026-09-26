# Changelog

## Unreleased

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
