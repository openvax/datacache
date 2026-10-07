# DataCache

Download a dataset once, verify its contents, and reuse the local file on later
runs. DataCache also manages pinned bundles, archive trees and SQLite caches
for applications such as PyEnsembl.

## Install

```sh
python -m pip install datacache
```

Python 3.9 or later is required. Add `datacache[html]` if you need HTML-table
conversion. Progress bars are available with `show_progress=True`.

## Cache and verify a file

This complete example uses a local file and a temporary cache, so it runs
offline and cleans up after itself. HTTP, HTTPS and FTP URLs work through the
same `fetch()` interface.

```python
import gzip
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from datacache import Cache

with TemporaryDirectory() as directory:
    root = Path(directory)
    contents = b">reference\nACGT\n"
    source = root / "reference.fa.gz"
    source.write_bytes(gzip.compress(contents))

    cache = Cache("references", cache_root=root / "cache")
    path = cache.fetch(
        source.as_uri(),
        filename="reference.fa",
        expected_sha256=hashlib.sha256(contents).hexdigest(),
    )
    print(Path(path).read_text().strip())
    print(cache.inspect(filename="reference.fa").status)
    source.unlink()
    print(cache.fetch(source.as_uri(), filename="reference.fa") == path)
```

```text
>reference
ACGT
available
True
```

The output is decompressed because the explicit filename omits the gzip suffix.
The hash describes the installed, decompressed bytes. The final call reuses
the cached file after the source has been removed.

For real data, obtain the expected hash from trusted release metadata. Computing
a hash from an untrusted download does not establish its authenticity.

## Use your dataset

This template requires your own dataset URL and destination name:

```python
from datacache import Cache

cache = Cache("my-project")
path = cache.fetch(
    "https://example.org/releases/v1/records.tsv",
    filename="records.tsv",
    timeout=30,
    show_progress=True,
)
```

Existing files are reused until you request `force=True`, or once they are
older than `expire_after`. DataCache doesn't ask the server whether a file has
changed. Supply trusted integrity expectations or a `validator` when you need
to validate a cached hit; invalid hits raise an error rather than silently
downloading replacements, unless `expire_after` asks for periodic refreshes.

Read [download and inspection options](downloads.md) for exact destinations,
decompression, retries and resumable transfers. Use [pinned bundles](bundles.md)
when a dataset contains several assets that must be installed together.

## Choose an interface

| Task | Start here |
| --- | --- |
| Download and reuse one file | `Cache.fetch()` or `fetch_file()` |
| Refresh changing metadata periodically | `fetch_file(expire_after=..., return_stale_on_error=True)` |
| Read a small resource into memory | `fetch_bytes()` |
| Check installed files without network access or repair | `Cache.inspect()` or `inspect_file()` |
| Read tables or build SQLite caches | [Tables and transformations](data.md) |
| Install a pinned set of assets | `VersionedDatasetRegistry` |
| Install a versioned archive directory | `VersionedArchiveRegistry` |

The [interface selection reference](reference/choosing.md) covers the main
entry points and the publication guarantees. The [API reference](api.md) gives
every signature, default, result and error.
