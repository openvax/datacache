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
store = registry.store_path("reference")
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
directories are stores: an existing empty directory is reported as `missing`
and becomes the store on the first install, in place, so it keeps its owner,
group and permissions. Installation never takes over a directory with files in
it unless it is already a bundle store, even with `force=True`. Store generated indices and other derived
outputs under a separate application-owned directory. Reinstalling sources does
not visit those outputs.

## Consumer-selected store paths

Select exactly one of `cache_root`, a zero-argument `cache_dir` root callable,
or an exact `store_path(name, version)` callback. Root strategies retain
`<root>/<name>/<version>`; the callback can keep an application's own layout,
for example a version directory per release:

```python
registry = VersionedDatasetRegistry(
    datasets,
    store_path=lambda name, version: application_root / version / "sources" / name,
)
store = registry.store_path("reference")
```

The callback receives a validated concrete version, including when the caller
omits it and selects the pinned default. Construction never calls it. The first
lookup calls it once for every dataset version, without filesystem inspection
or mutation, and the paths are reused afterwards, so callbacks should only
compute paths. Each version needs its own store: a callback that gives two
versions the same path raises `ValueError`, as does one that returns something
other than a path. Inspection, installation and refresh all use the chosen
store; DataCache owns everything inside it.

The store's parent belongs to the application and may be a link, for example to
another disk; the store itself is never a link. Installation creates missing
parent directories and keeps a lock file in the parent, beside the store, so
give the stores a parent of their own rather than a directory the application
lists or cleans. Unfinished downloads, including resumable ones, stay in hidden
`.staging-*` directories inside the store.

Do not point this callback at a populated legacy data/index directory: even
`force=True` cannot adopt a foreign directory, and installation raises
`FileValidationError` saying so. Keep those old caches readable
and deliberately install into a new managed source subdirectory instead. Model,
biological naming and migration policy remain the application's responsibility.

## Status and trust

`BundleInspection.status` is `available`, `missing`, `invalid` or `inaccessible`.
`bundle` is the current bundle's directory, and `files` maps asset names to
`FileInspection` objects from it. The `error` field retains the cause of a
failed inspection.

Explicit inspection hashes every asset by default. With trusted registry
metadata, `verified=True` means every file matched the supplied SHA-256 just now. Every bundle
also has its own manifest recording sizes, observed SHA-256 digests, source URLs,
and fetch time. `inspect_bundle(directory)` can check those recorded hashes
without a registry or network, but returns `verified=False`, for the bundle and
for each file: a local receipt is not an independent authority. Display URLs omit credentials, queries and
fragments; a separate SHA-256 fingerprint identifies the full source URL without
storing that omitted text. Paths are retained, so do not use secret-bearing URL
paths.

Use `inspect_bundle(directory, assets, verify_files=False)` or
`registry.inspect(name, verify_files=False)` for metadata-only checks. This
checks ownership, the bundle's manifest, registry expectations, the
required inventory, regular file types, readability and recorded sizes without
reading payloads. Both bundle and per-file results have `verified=False`.
Same-size corruption requires full verification to detect.

Every registry method hashes by default, so `inspect`, `local_path`,
`is_cached`, `status`, `download` and `ensure` agree about a corrupted bundle.
Pass `verify_files=False` to any of them for metadata-only checks where speed
matters more than detecting same-size corruption. On `install_bundle`,
`registry.download` or `ensure` it skips payload reads on reuse only: new
downloads are always hashed before they are published. All these
checks remain offline and read-only. Archive registries differ: their
`local_path`, `is_cached` and `status` have been metadata-only by default since
they were added, and take `verify_files=True` for full checks.

`verified=False` on installation or registry construction explicitly permits
acquiring assets without trusted hashes/sizes. Their observed hashes are still
recorded and checked on full verification, but cannot authenticate the original download.
Use this mode only for upstream data without immutable integrity metadata.
Without a trusted hash, reuse requires the same full URL fingerprint and
decompression setting. Changing either reports an invalid installation; use
`force=True` to acquire the newly requested data. Older receipts without source
fingerprints still support offline consistency checks and trusted-hash reuse,
but need an explicit refresh before reuse without trusted hashes.

Trusted SHA-256 expectations identify the installed bytes. Libraries using the
same root, dataset name, version and asset names can therefore share a verified
bundle even when they use different mirrors, signed URLs or compression
settings. DataCache checks the supplied hashes and sizes without downloading or
rewriting a matching bundle.

Cache reuse and inspection are read-only. A corrupt bundle requires
`force=True`, which installs a new one.

## How bundles are stored

```text
<root>/<dataset>/<version>/                 the store for one dataset version
    .datacache-store.json                   marks the directory as a DataCache store
    bundles/
        2026-09-30T14-11-05Z/               an older bundle
        2026-10-08T17-02-42Z/               the current bundle: always the newest
            records.fa
            manifest.json
            .datacache-manifest.json        what DataCache installed, and from where
```

A bundle is a complete set of files that belong together. Each bundle is named
for the UTC time it was installed, and the newest is always the current one.
Installing downloads and checks every file in a hidden staging directory, then
renames that directory into `bundles/`. A rename happens completely or not at
all, so readers see the old bundle or the new one, never a mix, and need no
locks or write permission. Installs into one store take turns, using a lock
file beside the store.

Old bundles are kept, so paths returned by one install keep working after later
installs. Forced refreshes therefore use more disk space until you
[remove old bundles](#removing-old-bundles); DataCache never deletes them on its
own. Never modify files in a bundle. Resolve paths once for a multi-file
operation: a later lookup can return a newer bundle.

Installation works on local filesystems with atomic sibling renames, on Linux,
macOS and Windows; resumable downloads (`resume=True`) need POSIX. Shared caches use normal umask-derived
permissions; readers need only read/search access. The staging directory stays
private while files download, but the bundle inside it already has its final
sharing permissions, so others can read it the moment it is renamed into place.
A failed rename leaves resumable work in the private staging directory.
Atomic publication covers
process interruption, not guaranteed durability after power loss. Distributed
coordination and arbitrary network filesystem semantics are outside this API.
An unhandled termination can leave staging directories; only complete bundles
are ever returned to callers.

## Removing old bundles

Every forced refresh adds a complete bundle and keeps the old ones, so a
multi-GB reference refreshed three times takes four times the space. Delete the
old ones explicitly when nothing still uses them:

```python
from datacache import list_bundles, prune_bundles

list_bundles(store)                 # Every bundle, oldest first; the last is current.
prune_bundles(store)                # Keep only the current bundle.
prune_bundles(store, keep=2)        # Keep the current bundle and the one before it.
registry.prune("reference", "110")  # The same, through a registry.
```

`prune_bundles` returns the paths it deleted. The current bundle is always
kept. Paths into deleted bundles stop working, so prune when no running program
still uses them, for example from a cleanup command rather than during an
analysis. Pruning takes the store's lock, so it never races an install. It works
the same for archive and materialization stores.

## Local and predownloaded files

An asset can name a local file with `path` instead of `url`; its identity is the
file's `file://` URL:

```python
assets = {"genes.gtf": {"path": "/data/custom/genes.gtf", "sha256": genes_sha256, "size": genes_size}}
```

To install from files someone already downloaded, while keeping the declared
URLs as the assets' identity, pass `source_paths`:

```python
paths = registry.download(
    "reference", "110",
    source_paths={"genes.gtf": "/downloads/Homo_sapiens.GRCh38.110.gtf"},
)
```

Each local file is checked against the asset's trusted hash and size exactly as
a download would be, and copied in; the original stays where it is. The
manifest records the declared URL, so later installs and inspections treat the
bundle as if it had been downloaded, including assets without trusted hashes.
For an asset with `decompress=True`, the local file must keep its `.gz` or
`.zip` suffix.

## Large raw assets

For integrity-pinned HTTP assets that need no decompression or conversion, use
`registry.download(name, resume=True)` or
`install_bundle(..., download_options={"resume": True})`. An interrupted bundle
keeps a private working directory per user and asset mapping. A later call with
the same mapping reuses completed assets and resumes partial ones; the current
bundle stays in place. Persistent partials require both hash and size.
Changing the mapping chooses a different working directory. The normal
non-resumable mode cleans its staging directory on handled failures.

See [resumable downloads](downloads.md#resumable-http-downloads) for protocol,
progress, disk-space and partial-discard details.

Notes for specific OpenVax libraries are in [integrating a consuming library](integration.md#notes-for-specific-libraries).
