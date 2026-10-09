# Fixed-path versioned files

`VersionedFileRegistry` supports applications whose single-file datasets already
live at `<root>/<name>/<version>/<filename>` with a root `manifest.json`.
It reuses those files in place. Use `VersionedDatasetRegistry` and bundles when
you need several files installed together as one bundle.

```python
from pathlib import Path
from datacache import VersionedFileRegistry

registry = VersionedFileRegistry({
    "reference": {
        "filename": "records.tsv",
        "default_version": "2026-09",
        "urls": {"2026-09": "https://data.example.org/2026-09/records.tsv.gz"},
        "description": "Reference records",
    },
}, cache_dir=lambda: Path("existing-cache"))

expected = registry.local_path("reference")  # works before installation; no writes
status = registry.status()                  # presence, URL/hash receipt, offline
path = registry.ensure("reference", timeout=60, record_provenance=True)
```

The dataset mapping and cache-root callable remain application-owned. The root
is resolved for each operation, so environment-backed callables may change it.
Use trusted definitions. Writers serialize per root using a cross-platform
file lock; cache hits and inspection never acquire or create that lock. Dataset names,
version labels and filenames must describe paths within the selected root.

Ordinary cache hits check presence and reuse the old path without reading its
bytes, networking, hashing or rewriting receipts. This deliberately preserves
legacy behavior and is not integrity verification. Explicit `expected_sha256`
or `expected_size` options validate reused files through `fetch_file`; invalid
files require `force=True`. `ensure` accepts the same options. New acquisition
forwards `fetch_file` options, including raw/decompression, timeout, bounded
retries, progress, provenance and resume. Integrity expectations describe the
installed bytes. Human cache-status messages belong in the calling application.

After downloading, the registry hashes the installed file in bounded chunks
and atomically updates its root JSON receipt. Entries are keyed by dataset name
and retain `version`, `url`, `path`, `bytes`, `sha256` and `downloaded_at` (UTC).
The URL is the caller's original URL for legacy compatibility; avoid credentials
or signed URLs in these definitions. Local receipts describe observed bytes,
not independently trusted checksums. Missing, unreadable or malformed JSON
receipts are treated as empty, matching the legacy registry.

`status()` returns `name`, `description`, `default_version`, `available_versions`,
`cached`, `cached_version`, `url`, `bytes`, `sha256`, `downloaded_at` and `path`. Presence and path
refer to the pinned default. Receipt fields describe the most recent download
for that dataset, which may name another version. This compatibility view does
not certify freshness or integrity; use `inspect_file` for the selected path.

Unknown dataset/version errors and acquisition failures use `error_cls`
(`RuntimeError` by default). Acquisition failures retain their original cause.
Filesystem failures while hashing or writing the receipt propagate directly.
Failed acquisition leaves the previous file and receipt intact. The file and
receipt are separate publications: if receipt publication fails after a valid
download, the new file remains installed and the prior receipt remains intact.
Concurrent registry writers serialize acquisition and receipt updates, so
downloading different datasets cannot discard each other’s receipts. These are the
established single-file semantics, not the guarantees of bundles.
