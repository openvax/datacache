# Download and cache reference

See the [API reference](api.md) for signatures, defaults, return values, and errors.

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
`filename`, `subdir`, or `cache_root`. Existing calls using the default cache continue to work
and can also supply `expected_sha256` and `expected_size`. Parent directories are
created only when fetching a missing file or explicitly refreshing it. A valid
cached file can be reused offline in a readable, non-writable installation.

Both expectations always describe **installed bytes**, after decompression or
HTML-to-CSV conversion. They do not describe HTTP wire bytes or a compressed
archive when its contents are being installed. To verify and retain an archive,
keep its `.gz` or `.zip` suffix at the destination and leave `decompress=False`.
With an inferred filename, archives are retained by default, including URLs
with query strings or fragments; their existing cache keys are preserved.
`decompress=True` uses a distinct key for the decompressed contents, keeping the
full URL in the key's digest. When a download endpoint's inferred filename
has no removable archive suffix, `.decompressed` distinguishes its output.
With an explicit `filename` or `destination`, a
missing compression suffix still implies decompression for compatibility.
`decompress=True` explicitly requests decompression while preserving an explicit
destination's exact name. Format detection prefers a supported extension on
the URL path (case-insensitively). For download endpoints without a supported
path extension, a filename in the final query parameter is also recognized
for compatibility, such as IEDB's `downloader.php?file_name=doc/data.zip`.
Bare format hints such as `?format=.gz` and fragments do not select a format.
For ZIP files, the member whose stored path matches the output filename is
selected, then a member in a folder with that name (such as `release/data.csv`
for `data.csv`), falling back to the largest non-directory member. No archive
paths are extracted.
HTML-to-CSV conversion requires an explicit `filename` or `destination` ending
in `.csv`. Query strings and fragments in inferred cache keys never request
conversion; those downloads retain their original HTML bytes.

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

Download and conversion staging files remain owner-only throughout writing and
validation. On replacement, existing files' read/write/execute permission bits
are applied to the validated staging file immediately before publication.
All new outputs, including raw downloads, decompressed files, and HTML-to-CSV
conversions, use normal file-creation permissions (`0666` filtered by the
process umask). For example, umask `022` produces `0644`, `002` produces `0664`,
and `077` keeps files private at `0600`. This also applies to the private download
and decompression helpers used by downstream packages such as pyensembl, fixing
new shared-cache files being unreadable by other users
([#68](https://github.com/openvax/datacache/issues/68)). Existing cache files are
not automatically made more permissive, even on a forced refresh; owners must
explicitly adjust permissions on files previously downloaded as `0600` if those
files should be shared. An empty, disposable file measures normal creation
permissions without reading or changing umask globally; that file never
contains downloaded or converted data.
Permission-setting failure preserves the old file and cleans up
staging files. Atomic replacement creates a new inode: ownership, ACLs, and
extended attributes of an existing destination are not copied, and special
setuid/setgid/sticky bits are not preserved.

The publication guarantee assumes a local filesystem supporting atomic
replacement of sibling files. Concurrent fetches use separate staging files;
the last successful replacement wins and readers opening the destination see
complete files. Callers sharing a destination should use the same expectations.
A returned path is not a permanent snapshot: later fetches may replace its
contents. Platforms that deny replacing an open file may reject publication;
the old file is preserved. This does not provide multi-file transactions,
distributed coordination, or durability/recovery after power loss or an
unhandled process termination, which may leave staging files behind.

## Transient HTTP failures

HTTP and HTTPS downloads retry temporary failures by default, with at most
three attempts (the initial request plus two retries). This includes HTTP
408, 429, 500, 502, 503, and 504 responses, connection failures, timeouts, and
interrupted response streams. Configure the policy on `fetch_file` or
`Cache.fetch`:

```python
path = fetch_file(
    "https://example.org/releases/v1/records.tsv.gz",
    destination="/data/references/v1/records.tsv",
    decompress=True,
    expected_sha256=release_metadata["installed_sha256"],
    timeout=30,
    max_retries=2,
    retry_backoff=1.0,
    retry_max_delay=30.0,
)
```

`max_retries` counts additional attempts; set it to `0` to disable retries.
`retry_backoff` is the first delay in seconds and doubles after each retry,
capped by `retry_max_delay`. Defaults therefore wait one second and then two
seconds. `Retry-After` supports both integer seconds and HTTP dates. When its
wait fits within `retry_max_delay`, the larger of that wait and the backoff is
used. If the server requests a longer wait, the original error is raised
immediately instead of retrying sooner than requested. Malformed header values
are ignored. Both delay settings must be finite non-negative real numbers;
accepted values are normalized to Python floats before use.

Each attempt downloads from the beginning into a fresh private staging file.
Failed responses are closed and partial files are removed before waiting or
retrying. An existing destination remains intact until a successful transfer
has been transformed, validated, and published. Retry warnings report the
attempt count, failure category, and delay; exhaustion raises the original
Requests exception, preserving its response and cause.

Permanent HTTP failures such as 404, TLS errors (including proxy-wrapped TLS
failures), local filesystem errors,
progress callback exceptions, and transformation/integrity/publication errors
are not retried. Local file and FTP transfers remain single attempts. Cache
reuse and read-only inspection make no requests and do not wait. Existing
pyensembl calls through `_download_and_decompress_if_necessary` receive the same
default policy without changes to downstream code.

Progress byte counts describe the current attempt and may decrease after a
restart. The same `timeout` is passed to each request; Requests timeouts govern
connection/read inactivity, not an overall elapsed-time deadline. Retry count
and waiting time are bounded independently of the transfer duration.

## Read-only cache inspection

Path lookup, presence, and integrity checks have different contracts:

```python
from datacache import Cache, expected_path, file_exists, inspect_file, inspect_files

root = "/data/references/v1"
path = expected_path(filename="records.tsv", cache_root=root)
present = file_exists(filename="records.tsv", cache_root=root)
result = inspect_file(path, expected_sha256=release_metadata["installed_sha256"])
if result.status == "available" and result.verified:
    process_records(result.path)

# The object API uses the same paths and validation contract.
cache = Cache("references", cache_root=root)
result = cache.inspect(filename="records.tsv",
                       expected_sha256=release_metadata["installed_sha256"])

# Inventory required files using trusted, caller-supplied metadata.
installation = inspect_files(root, {
    "records.tsv": {"expected_sha256": release_metadata["installed_sha256"]},
    "manifest.json": {"expected_sha256": release_metadata["manifest_sha256"]},
})
```

`expected_path`, `resolve_path`, and `Cache.local_path(download=False)` only
compute paths, with no filesystem access. `file_exists` and `Cache.exists`
check presence without requiring a readable regular file: directories count as
present, broken symlinks count as absent, and permission errors propagate.
None of these operations creates a directory or a lock file.

`inspect_file`, `inspect_files`, and `Cache.inspect` return results with `path`,
`status`, `verified`, and `error` attributes:

| Status | File inspection | Required-file inventory |
| --- | --- | --- |
| `available` | Readable regular file matching supplied metadata | Every required file is available |
| `missing` | File absent | Cache root absent |
| `corrupt` | Wrong file type or size/hash mismatch | Wrong root type, corrupt file, or incomplete installation |
| `inaccessible` | Permission or other filesystem error | Root or required file cannot be inspected |

`verified` is true only when a supplied SHA-256 digest matched; presence,
readability, or a size check alone does not establish verified integrity.
`error` retains the original filesystem or validation exception when unavailable.
Invalid integrity arguments raise `ValueError` rather than reporting a cache
problem. `inspect_files` also returns a `files` mapping of individual results;
for example, a missing manifest makes the installation `corrupt` while that
manifest's individual status is `missing`. Inaccessible files take precedence
over corrupt or missing files in the aggregate result. Required names must be
normalized relative paths; nested names are supported. Use `{}` or `None` for
an entry without integrity metadata. The required mapping must be nonempty.

Inspection only reads local files. It never downloads, writes manifests,
creates locks, or attempts recovery, so a valid read-only version and a missing
sibling version can be inspected independently. The caller supplies required
files and trusted integrity metadata; datacache does not parse manifests or
discover versions. Symlinks are followed as in ordinary file access. Inventory
is not a snapshot across concurrent external changes or a multi-file
installation mechanism; versioned bundle installation is tracked in
[#59](https://github.com/openvax/datacache/issues/59).

`cache_root` accepts a string or `pathlib.Path` and names the actual directory
containing cached files, overriding the platform location selected by `subdir`.
Relative roots remain relative to the current working directory. It is
supported by `fetch_file`, `expected_path`, `file_exists`, `resolve_path`,
`build_path`, and `Cache`. An exact `destination` is mutually exclusive with
`cache_root`. Default cache locations and filename normalization remain the
same. `build_path` still creates parents; use `resolve_path` for pure lookup.
Creation and repair remain explicit operations: use `fetch_file` or
`Cache.fetch`, supplying integrity metadata and `force=True` for replacement.
`Cache.fetch` validates every reuse and respects different filenames for the
same URL. Its database and deletion methods also use the selected cache root.
Database paths preserve the filesystem meaning of symlinks followed by `..`.
`Cache.delete_all()` clears the root's contents while preserving the directory
and its permissions, including when the root is `.` or a symlink. Symlinks
inside the cache are removed without clearing their external targets.

## Downstream compatibility

The private `_download_and_decompress_if_necessary` entry point, used by
pyensembl, retains its pre-1.8 literal-URL format inference when transform flags
are omitted. In particular, query/fragment-bearing archive URLs retain the
same bytes under pyensembl's existing cache keys. Explicit transform flags and
the public `fetch_file` API retain the parsed-URL behavior documented above.
The IEDB download endpoints used by pepdata continue to decompress archives
named at the end of the URL query into the requested CSV filenames.
The `_decompress_to_file` helper remains available and uses failure-safe atomic
publication. New integrations should use the public download and inspection
APIs.
