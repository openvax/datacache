# Versioned datasets and bundles

`VersionedDatasetRegistry` gives libraries a common way to resolve pinned data
versions, install their files together, and inspect them offline. Dataset names,
versions, URLs, and biological interpretation belong to the caller. DataCache
owns the transfer, verification, and publication. Existing `Cache` and database
APIs work independently; adopting a registry is optional.

A dataset can contain one file, a records/manifest pair, or many source files:

```python
from pathlib import Path
from datacache import VersionedDatasetRegistry, get_cache_root

registry = VersionedDatasetRegistry(
    {
        "reference": {
            "default_version": "2026-09",
            "versions": {
                "2026-09": {
                    "records.fa": {
                        "url": "https://data.example.org/2026-09/records.fa.gz",
                        "decompress": True,
                        "sha256": trusted_records_sha256,
                        "size": trusted_records_size,
                    },
                    "manifest.json": {
                        "url": "https://data.example.org/2026-09/manifest.json",
                        "sha256": trusted_manifest_sha256,
                        "size": trusted_manifest_size,
                    },
                },
            },
        },
    },
    cache_root=Path(get_cache_root("openvax", "OPENVAX_DATA_CACHE")) / "datasets",
)

# No directory creation, locks, network requests, or repairs:
store = registry.bundle_path("reference")
state = registry.inspect("reference")

# Download if missing; verify and reuse if already installed:
paths = registry.download("reference", timeout=60)
records = paths["records.fa"]

# Explicit repair or refresh; old returned paths remain usable:
paths = registry.download("reference", force=True, timeout=60)
```

`get_cache_root` returns a string; wrap it in `Path` when joining paths with `/`. SHA-256 and size describe the installed bytes,
after decompression. Use opaque, concrete data-version labels independent of
Python package versions. A default must name a version in the supplied mapping;
DataCache never resolves a remote `latest` alias.

For a caller that already resolves versions, the lower-level operations are:

```python
from datacache import install_bundle, inspect_bundle

paths = install_bundle(source_directory, assets, download_options={"timeout": 60})
state = inspect_bundle(source_directory, assets)
```

`assets` is the same mapping of relative names to metadata. Names cannot escape
the bundle, collide as files/directories or by letter case, or occupy DataCache's
reserved metadata names. A file or directory at another asset's automatic
provenance sidecar path is also rejected before installation starts. Source
directories are managed stores: an existing empty directory is reported as
`missing` and can be initialized without `force=True`, preserving its access
mode. Installation refuses to take over a nonempty directory without its
ownership marker, even with `force=True`. Store generated indices and other derived
outputs under a separate application-owned directory. Reinstalling sources does
not visit those outputs.

## Status and trust

`BundleInspection.status` is `available`, `missing`, `invalid`, `inaccessible`, or
`recovery-required`. `files` maps asset names to `FileInspection` objects from one
generation, and `generation` is that generation's directory. The `error` field
retains the cause of a failed inspection.

Inspection hashes every asset. With trusted registry metadata, `verified=True`
means every file matched the supplied SHA-256 just now. Every completed generation
also has its own manifest recording sizes, observed SHA-256 digests, source URLs,
and fetch time. `inspect_bundle(directory)` can check those recorded hashes
without a registry or network, but returns `verified=False`: a local receipt is
not an independent authority. Display URLs omit credentials, queries and
fragments; a separate SHA-256 fingerprint identifies the full source URL without
storing that omitted text. Paths are retained, so do not use secret-bearing URL
paths.

`verified=False` on installation or registry construction explicitly permits
acquiring assets without trusted hashes/sizes. Their observed hashes are still
recorded and checked on reuse, but cannot authenticate the original download.
Use this mode only for upstream data without immutable integrity metadata.
Without a trusted hash, reuse requires the same full URL fingerprint and
decompression setting. Changing either reports an invalid installation; use
`force=True` to acquire the newly requested data. Older receipts without source
fingerprints still support offline consistency checks and trusted-hash reuse,
but need an explicit refresh before reuse without trusted hashes.

Trusted SHA-256 expectations identify the installed bytes. Libraries using the
same root, dataset name, version and asset names can therefore share a verified
generation even when they use different mirrors, signed URLs or compression
settings. DataCache checks the supplied hashes and sizes without downloading or
rewriting a matching generation.

Cache reuse and inspection are read-only. Corrupt entries require `force=True`.
If publication was interrupted after creating a complete generation, inspection
can report `recovery-required`. Explicit installation validates and publishes a
matching local generation before attempting downloads. No recovery happens
merely by listing or inspecting a cache.

## Publication and path lifetime

The layout is `<root>/<dataset>/<version>/`, containing an ownership marker,
`current.json`, and `generations/<id>/`. Each generation contains all assets and
its own `.datacache-manifest.json`. Writers serialize per store. A writer stages
and verifies every asset, then atomically replaces `current.json`. Readers read
that pointer once and validate that immutable generation without locks. They
never combine members from different refreshes or require write permission.

Published generations are retained. Paths returned from one installation remain
usable through later installations, until the caller explicitly removes the
store or its generations. This avoids reader lock files and a missing-directory
window. It also means forced refreshes consume additional disk space; DataCache
does not implement garbage collection. Never modify files within a generation.
Resolve once and use that result for a multi-file operation; separate resolution
calls can legitimately select different generations.

Installation is supported on POSIX local filesystems providing `flock` and atomic
sibling `os.replace` (Linux and macOS). Shared caches use normal umask-derived
permissions; readers need only read/search access. A private outer staging
directory protects unfinished files while the inner generation already has its
final sharing permissions. Renaming that generation makes it recoverable with
the correct permissions immediately, including after an interruption. A failed
rename leaves resumable work protected by the private outer directory.
Atomic publication covers
process interruption, not guaranteed durability after power loss. Distributed
coordination and arbitrary network filesystem semantics are outside this API.
An unhandled termination can leave staging directories; successful generations
are the only paths returned to callers.

## Large raw assets

For integrity-pinned HTTP assets that need no decompression or conversion, use
`registry.download(name, resume=True)` or
`install_bundle(..., download_options={"resume": True})`. An interrupted bundle
keeps a private working directory per user and asset mapping. A later call with
the same mapping reuses completed assets and resumes partial ones; the installed
generation remains intact. Persistent partials require both hash and size.
Changing the mapping chooses a different working directory. The normal
non-resumable mode cleans its staging directory on handled failures.

See [resumable downloads](downloads.md#resumable-http-downloads) for protocol,
progress, disk-space and partial-discard details.

## Adopting from downstream libraries

- **MHCflurry:** use [`install_archive`](archives.md) for released `.tar.bz2`
  trees and ordered historical parts. Resolve member paths from the returned
  generation; preserve `DOWNLOAD_INFO.csv` with `extra_files`. Do not enumerate
  model files as bundle assets or treat the managed store's existence as a
  completed download.
- **hitlist / tsarina:** use [VersionedFileRegistry](file_registry.md) to retain
  fixed paths, single-Path returns and legacy root manifests without moving old
  caches. For a deliberate migration to generation bundles, the existing
  `{filename, urls, default_version}` mapping
  is accepted with `verified=False`, as is the `cache_dir` root callable. This is
  mapping compatibility, not a drop-in filesystem or return-value migration:
  `download` returns asset paths, `local_path` requires an installed bundle, and
  status rows contain a `BundleInspection`. A downstream adapter can preserve
  its public return types and errors. Keep old paths readable during migration;
  install into a new managed root instead of overwriting legacy directories.
- **mhcseqs:** express each records/manifest pair or multi-file source bundle as
  one version's assets. Keep schema checks and biological validation in mhcseqs.
  Generated outputs stay outside the source store. The library no longer needs
  to compose lock, backup, rename and rollback helpers.
- **Vaxrank / Isovar / Varcode / Topiary:** share a root, dataset name and data
  revision to share a complete installation. Existing digest-addressed individual
  assets can remain under `<root>/objects/sha256/`; these APIs do not relocate
  them or change prediction-cache identities.
- **pyensembl:** single-file download and SQLite APIs remain supported. Bundles
  are optional for references with trustworthy multi-file metadata.

The [runnable offline example](https://github.com/openvax/datacache/blob/master/examples/versioned_datasets.py) demonstrates
single-file, paired-file and multi-file registries, generated SQLite indices,
shared reuse, and inspection with all upstream sources removed.
