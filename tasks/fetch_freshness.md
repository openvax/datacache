# Small metadata fetches: fetch_bytes, freshness, validation (1.17.0)

Driver: pyensembl lists annotation dates from 1.5 KB HTML directory listings.
It currently imports `is_retryable_http_error`/`retry_delay` and hand-rolls a
retry loop, a JSON cache with a fetch timestamp, "refresh daily, use the
cache offline", and "never cache a page without dates". Those are generic.

## 1. `fetch_bytes(url, *, timeout=None, max_retries=2, retry_backoff=1.0, retry_max_delay=30.0, allow_empty=False)`

Return the response body in memory with exactly fetch_file's transport:
Requests for HTTP(S) (certifi / REQUESTS_CA_BUNDLE), urllib for file/FTP,
retries for connection errors, 408/429/5xx and empty bodies, Retry-After.
For small resources; also usable when the cache is read-only.
Implementation: move the retry loop out of `_download_to_temp_file` into one
helper used by both, so they cannot drift.

## 2. `fetch_file(..., max_age=None, return_stale_on_error=False)` (and `Cache.fetch`)

- `max_age`: non-negative seconds. A valid cached file older than this is
  downloaded again. Age comes from the provenance record's `fetched_at` when
  a valid record describes the file, else from its modification time
  (publication by os.replace keeps the download time). `None` keeps today's
  behaviour: a valid cached file is reused however old.
- `return_stale_on_error`: when a refresh (force=True or max_age expiry) fails with
  an `Exception` and a valid cached file exists, log a warning and return the
  cached path. Without a cached file the error propagates. Off by default.

## 3. `fetch_file(..., validate=None)` (and `Cache.fetch`)

Callable `validate(path)` that raises (normally ValueError) when the content
is wrong, e.g. an HTTP 200 proxy error page. Applied to the staged installed
bytes before publication, so a rejected download never replaces the cached
file, and to cache hits, like expected_sha256. Failures raise
FileValidationError (path, "failed validation: ..."), chained to the cause.
Not combinable with resume=True (that path publishes partials itself).

## Not included

Selective retry of timeouts: callers wanting fast failure pass a short
timeout or max_retries=0.

## Docs and tests

docs/api.md: version, fetch_file/Cache.fetch signatures and parameter rows,
new fetch_bytes section with a runnable example. downloads.md: a short
"Small metadata and freshness" section. CHANGELOG 1.17.0.
Tests: fetch_bytes retries/empty/file URLs; shared loop unchanged for files;
max_age by provenance and by mtime; return_stale_on_error on network error and on
validation failure, and propagation without a cache; validate on publish and
on cache hit; Cache.fetch forwarding.
