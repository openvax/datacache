# Legacy cache fixtures

These fixtures were generated using the unmodified DataCache 1.9.1 source at
commit `ee20b5a`, imported from a separate extraction of `git archive`.

- `legacy_v191.sql` is SQLite's `iterdump()` of a database built with
  `db_from_dataframe`, table `records`, version `7`, and the DataFrame
  `{"sample id": [1, 2], "label": ["case", "control"]}`. The original
  `primary_key="sample id"` was ignored after column normalization; retaining
  that exact old schema tests reuse without applying new constraints.
- `legacy_v191_names.json` records `build_local_filename` for raw, gzip, ZIP,
  query-string, and fragment URLs, plus the database filename used by
  `fetch_csv_db` for `records.csv.gz` with integer `id` and text `label`
  columns and two rows.

The tests create databases from the SQL dump to avoid committing opaque binary
files. Do not regenerate these fixtures with the new implementation: they
represent existing user caches, not the expected output of new builds.
