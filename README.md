[![Tests](https://github.com/openvax/datacache/actions/workflows/tests.yml/badge.svg)](https://github.com/openvax/datacache/actions/workflows/tests.yml)
<a href="https://coveralls.io/github/openvax/datacache?branch=master">
<img src="https://coveralls.io/repos/openvax/datacache/badge.svg?branch=master&service=github" alt="Coverage Status" />
</a>
<a href="https://pypi.python.org/pypi/datacache/">
<img src="https://img.shields.io/pypi/v/datacache.svg?maxAge=1000" alt="PyPI" />
</a>

# DataCache

Helpers for transparently downloading datasets

## API

- **fetch_file**(download\_url, filename=_None_, decompress=_False_, subdir=_None_)
- **fetch_and_transform**(transformed\_filename, transformer, loader, source_filename, source_url, subdir=_None_)
- **fetch_fasta_dict**(download\_url, filename=_None_, subdir=_None_)
- **fetch_fasta_db**(table\_name, download_url, fasta_filename=_None_, key\_column = _'id'_, value\_column=_'seq'_, subdir=_None_)
- **fetch_csv_db**(table\_name, download\_url, csv\_filename=_None_, subdir=_None_, \*\*pandas_kwargs)

## Verified downloads

Use `destination` to install a single file at an exact path, including its
filename. Supply trusted integrity metadata to check both cached files and new
downloads:

```python
from datacache import fetch_file, validate_file, FileValidationError

path = fetch_file(
    "https://example.org/releases/v1/records.tsv.gz",
    destination="/data/references/v1/records.tsv",
    decompress=True,
    expected_sha256=release_metadata["installed_sha256"],
    expected_size=release_metadata["installed_size"],
    timeout=30,
)

# Read-only validation: no requests, directory creation, locks, or repair.
validate_file(path, expected_sha256=release_metadata["installed_sha256"])
```

`destination` accepts a string or `pathlib.Path` and cannot be combined with
`filename` or `subdir`. Existing calls using the default cache continue to work
and can also supply `expected_sha256` and `expected_size`. Parent directories are
created only when fetching a missing file or explicitly refreshing it. A valid
cached file can be reused offline in a readable, non-writable installation.

Both expectations always describe **installed bytes**, after decompression or
HTML-to-CSV conversion. They do not describe HTTP wire bytes or a compressed
archive when its contents are being installed. To verify and retain an archive,
keep its `.gz` or `.zip` suffix at the destination and leave `decompress=False`.
As with existing callers, a compressed URL is decompressed automatically when
the destination lacks its compression suffix. `decompress=True` explicitly
requests decompression while preserving an explicit destination's exact name.
For ZIP files, the member matching the output filename is selected, falling
back to the largest non-directory member. No archive paths are extracted.

A size or SHA-256 mismatch raises `FileValidationError`, with the path and
expected/actual values in its message. A corrupt cache hit does not trigger a
download: call `fetch_file(..., force=True)` to explicitly attempt replacement.
`validate_file` raises `FileNotFoundError` for missing files and propagates
permission errors; it rejects non-regular files. Fetching propagates transport,
decompression, and filesystem errors so applications can translate them.
Expectations are optional; omitting them provides no integrity guarantee.

Downloads and transformed output use unique staging files in the destination
directory. Only a complete, validated file is published, using `os.replace`.
Transfer, transformation, validation, or publication failure leaves an existing
destination unchanged and cleans up staging files, including on a handled
keyboard interruption. This avoids `shutil.move`'s cross-filesystem copy and
metadata fallback (related to [#39](https://github.com/openvax/datacache/issues/39));
SELinux policy compatibility still needs testing on the target installation.

The publication guarantee assumes a local filesystem supporting atomic
replacement of sibling files. Concurrent fetches use separate staging files;
the last successful replacement wins and readers opening the destination see
complete files. Callers sharing a destination should use the same expectations.
A returned path is not a permanent snapshot: later fetches may replace its
contents. Platforms that deny replacing an open file may reject publication;
the old file is preserved. This does not provide multi-file transactions,
distributed coordination, or durability/recovery after power loss or an
unhandled process termination, which may leave staging files behind.
