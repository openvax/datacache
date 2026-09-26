# Changelog

## Unreleased

- Keep `fetch_csv_db` databases inside their cache directory. An inferred
  database name is built from the CSV's column headers; a header could add `..`
  path components, and wide CSVs exceeded the 255-byte filename limit and
  failed with `OSError`. Such schemas are now named by a digest. Every other
  inferred name is unchanged, so existing databases are still reused.
- Report non-string column labels (for example `header=None` without `names=`)
  with the same `ValueError` whether or not `db_filename` is given, instead of
  an `AttributeError` when the database name is inferred.
- Select a ZIP member stored inside a folder when its name matches the output,
  instead of silently installing the largest member under the requested name.
- Suggest `force=True` only when a regular file is present; it cannot replace
  a directory at the cache path.
- Store `float16` DataFrame columns as `FLOAT`.
- Correct `fetch_file`'s `subdir` and `timeout` documentation, and document
  `Cache`, `ensure_dir`, `get_data_dir`, and `clear_cache`.
- Remove the unused internal `DatabaseTable.from_fasta_dict`.

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
