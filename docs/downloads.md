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
archive when its contents are being installed. To verify and retain an archive
at any output name, pass `raw=True`. This also disables HTML-to-CSV conversion:

```python
path = fetch_file(
    archive_url,
    destination="reference.bin",
    raw=True,
    expected_sha256=release_metadata["archive_sha256"],
    expected_size=release_metadata["archive_size"],
    record_provenance=True,
)
```

`Cache.fetch` accepts the same `raw=True` option with `filename`. Raw mode is
incompatible with `decompress=True`. It preserves the downloaded payload after
normal HTTP transfer decoding, not encoded HTTP wire bytes. Hashes and sizes
refer to that payload. A cache hit still uses the supplied local validation
checks; changing transformation options does not replace an existing output.
Use distinct names, trusted expectations, or `force=True` when switching modes.

Without `raw=True`, keeping the source's `.gz` or `.zip` suffix at the destination
and leaving `decompress=False` retains an archive as before.
With an inferred filename, archives are retained by default, including URLs
with query strings or fragments; their existing cache keys are preserved.
`decompress=True` uses a distinct key for the decompressed contents, keeping the
full URL in the key's digest. When a download endpoint's inferred filename
has no removable archive suffix, `.decompressed` distinguishes its output.
With an explicit `filename` or `destination`, a
missing compression suffix still implies decompression for compatibility unless
`raw=True` is supplied.
`decompress=True` explicitly requests decompression while preserving an explicit
destination's exact name. Format detection prefers a supported extension on
the URL path (case-insensitively). For download endpoints without a supported
path extension, a filename in the final query parameter is also recognized
for compatibility, such as IEDB's `downloader.php?file_name=doc/data.zip`.
Bare format hints such as `?format=.gz` and fragments do not select a format.
For ZIP files, the member stored at the output filename is selected; otherwise a
member with that name in any folder, ignoring letter case (such as
`release/data.csv` or `Data.CSV` for `data.csv`), preferring the one nearest the
archive root and then an exact-case match. If no member matches, the largest
non-directory member is installed, and a warning is logged when the output was
named explicitly and that was a guess among several. No archive paths are
extracted.
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

An empty installed file is rejected even without expectations, because a
complete but empty response, such as a withdrawn upstream record, would
otherwise be cached as a valid file and parse to zero records much later. An
empty HTTP response is retried like a transient failure, then raises
`FileValidationError`; nothing is published and an existing file stays in
place. After decompression, an empty archive member is rejected the same way.
An empty file already in the cache is an invalid hit, which `force=True`
replaces. Pass `allow_empty=True`, or `expected_size=0`, when an empty file is
the correct result.

## Provenance

Pass `record_provenance=True` to `fetch_file` or `Cache.fetch` to record where a
download came from. After publishing, datacache writes a hidden record,
`.<name>.datacache.json`, beside the file. It holds the source URL, the fetch
time, the size, and the SHA-256 when `expected_sha256` verified it. The URL is
stored without its user name, password, query string, or fragment, since signed
URLs carry secrets there; its path is kept as is, so do not record URLs that
embed a secret in the path, as some share links do. The record has the file's
permissions, and sharing the file with `make_file_readable` shares it too.

`inspect_file`, `Cache.inspect`, and `inspect_files` report `source_url`,
`fetched_at`, and `recorded_sha256`, but only while the file's size and
modification time match the record. `recorded_sha256` is a record, not a check
of the current bytes: an in-place edit that keeps the size and modification
time, or a forged record in a shared cache, would still report it. `verified`
therefore keeps its meaning, a supplied SHA-256 matching the bytes now; pass
`recorded_sha256` back as `expected_sha256` to check them. A missing,
unreadable, or malformed record reads as unrecorded, and inspection never
writes.

Recording is off by default because a caller that downloads to a temporary
name and then moves the file would leave the record behind. A record is
written only by a download, never by a cache hit, and any later replacement of
the file removes the previous record first, whether or not it records a new
one. `Cache.delete_url` removes records with their files. Recording is best
effort: failing to write a record never fails the download.
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

Every HTTP and HTTPS request, including resumed range requests and
`fetch_bytes`, sends `User-Agent: datacache/<version>
(+https://github.com/openvax/datacache)`. Some hosts, IEDB among them, refuse
Requests' default `python-requests` User-Agent.

Progress byte counts describe the current attempt and may decrease after a
restart. The same `timeout` is passed to each request; Requests timeouts govern
connection/read inactivity, not an overall elapsed-time deadline. Retry count
and waiting time are bounded independently of the transfer duration.

## Small metadata and freshness

`fetch_bytes` returns a resource's bytes in memory with the same transport,
TLS trust and retry policy as `fetch_file`, without writing to disk. It suits
small resources such as directory listings, and caches that are read-only.

Metadata that changes upstream, such as a list of releases, can be cached and
refreshed periodically. `expire_after` is named as in requests-cache, and
`return_stale_on_error` behaves like the HTTP `stale-if-error` directive:

```python
path = fetch_file(
    "https://example.org/releases/",
    destination="/data/references/releases.html",
    raw=True,
    timeout=10,
    expire_after=86400,  # seconds or a datetime.timedelta
    return_stale_on_error=True,
    validator=require_release_links,
)
```

- `expire_after` downloads a cached copy again once it is older than the
  given time, or once it no longer validates. Age comes from the provenance
  record's fetch time when there is one (`record_provenance=True`), otherwise
  from the file's modification time, which publication sets when the download
  is written.
- `return_stale_on_error` returns the cached copy, with a logged warning, when a
  refresh fails, including a rejected validation. Without a valid cached copy
  the error propagates; so does a progress callback's exception, which cancels
  the fetch.
- `validator(path)` rejects content that a successful transfer can still get
  wrong, such as an HTTP 200 error page from a proxy, by raising or returning
  `False`. It runs on the staged bytes before publication, so a rejected
  download never replaces the cached copy, and on cache hits.

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
files and trusted integrity metadata; these individual-file helpers do not
discover versions. Symlinks are followed as in ordinary file access. Inventory
is not a snapshot across concurrent external changes or a multi-file
installation mechanism. Use [versioned bundles](bundles.md) for atomic
multi-file installation and inspection of one complete generation.

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
New integrations should use the public download and inspection APIs.

## Resumable HTTP downloads

Use `fetch_file(..., resume=True, expected_sha256=sha256, expected_size=size)`
(or the same options on `Cache.fetch`) for large immutable raw HTTP/HTTPS files.
`expected_size` is required. Prefer a trusted `expected_sha256` when available.
For sources such as Ensembl that do not publish SHA-256 digests, omit the hash:

```python
path = fetch_file(
    ensembl_dna_url,
    destination="reference.download",
    raw=True,
    resume=True,
    expected_size=ensembl_dna_size,
)
```

Without `expected_sha256`, every accepted response must supply a syntactically
valid strong ETag. Missing or weak ETags (including a Last-Modified value alone)
raise `FileValidationError` before response bytes are retained or published.
Strong ETags prevent joining bytes from different server representations; they
are not independently verified checksums. Callers can still apply their source's
checksum to the completed archive. Size-only cache hits remain offline and check
only the local byte count, not remote freshness or same-size corruption. Optional
provenance records no verified SHA-256 unless the caller supplied one.

Use `raw=True` for an arbitrary destination name, or keep the source archive's
suffix. Resumable decompression and HTML conversion are not supported. Resume is
off by default and requires a POSIX local filesystem.

Each destination gets a private, owner-only working directory beside it, with
one bounded partial file, metadata and a permanent advisory lock. Threads and
processes serialize on that destination for the same user; other users retain
separate private state. A valid destination remains intact throughout transfer,
verification, and publication. With an expected SHA-256, complete partials are
verified and reused without network; oversized or corrupt ones restart. Without
one, a complete-size partial restarts to obtain a fresh validated response, and
an incomplete partial without a saved strong ETag is discarded. Interruptions and exhausted transport
retries keep safely written partial bytes for the next call.

Range requests use `Accept-Encoding: identity`. DataCache checks the exact
`Content-Range` offset and total, carries `If-Range` for a strong ETag, and only
appends bytes from a matching representation. A 200 response replaces the
partial; incompatible ranges, changed validators, or 416 responses restart it.
When a trusted SHA-256 is supplied, downloads can also resume without strong
ETags, with Last-Modified changes triggering restarts and the final digest
authorizing publication. The final byte count is always checked. HTTP range behavior follows
[RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-range-requests).

Progress reports bytes present in the current transfer, including the retained
prefix, and the trusted total size. A resumed retry continues its byte count;
a server-required restart resets it. Callback failures propagate without being
retried. The existing bounded HTTP retry/backoff options apply.

Call `discard_partial(destination)` to explicitly discard this user's partial
bytes and metadata. It waits for any active transfer, preserves the installed
file, and retains the small lock directory for coordination. Missing state is a
no-op. `force=True` refreshes the installed file but can reuse an independently
verified complete partial; call `discard_partial` first to require a new transfer.

Allow space for the old destination, up to `expected_size` partial bytes, and a
second file of that size during publication. Copying verified bytes into ordinary
staging keeps persistent partials private even when publication fails after
setting shared file permissions. No credentials or raw signed URLs are stored in
the private metadata; a URL hash identifies the source. Changing the URL or
integrity expectations starts a new transfer.
