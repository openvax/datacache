# Changelog

## Unreleased

- Publish new downloads and decompressed files with normal creation permissions
  for shared caches; keep staging private and preserve existing modes (#68).
- Roll back failed SQLite rebuilds, including explicit overwrites, and publish
  new databases only after successful construction. Close rejected version
  lookup connections and support explicit read-only lookup.
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
