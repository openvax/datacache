# Archive directory installation

`install_archive` safely publishes an entire tar archive tree. It is intended
for applications such as MHCflurry whose released weights and datasets already
have nested paths inside `.tar.bz2` files. DataCache owns transfer, extraction,
integrity receipts, concurrency and publication; the caller continues to own
catalogue versions and domain-specific validation.

For a reusable catalogue, wrap it in `VersionedArchiveRegistry`; the lower-level
functions remain useful when a downstream library already owns version lookup.

## One archive

```python
from datacache import install_archive, inspect_archive

tree = install_archive(
    cache_root / "models_class1_presentation",
    "https://example.org/models.tar.bz2",
    expected_sha256=trusted_archive_sha256,
    expected_size=trusted_archive_size,
    download_options={"timeout": 60, "max_retries": 3, "show_progress": True},
)
models = tree / "models"
state = inspect_archive(
    cache_root / "models_class1_presentation",
    "https://example.org/models.tar.bz2",
    expected_sha256=trusted_archive_sha256,
    expected_size=trusted_archive_size,
)
assert state.status == "available" and state.verified
```

The destination is a store, not the extracted directory. Installation returns
the current bundle: the directory holding the extracted tree. Resolve it once
and use that returned path for an operation; never construct
`<destination>/<archive member>` directly.

## Ordered split archives and local inputs

An ordered list is concatenated byte-for-byte before tar parsing:

```python
urls = [
    "https://example.org/data.tar.bz2.part.aa",
    "https://example.org/data.tar.bz2.part.ab",
]
tree = install_archive(
    cache_root / "data_evaluation",
    urls,
    expected_sha256=trusted_concatenated_sha256,
    expected_size=trusted_concatenated_size,
)
```

Each item can instead be `{url, sha256, size}` to verify parts independently.
If a command already has downloaded files, supply `path` as well as the logical
catalogue URL. The local bytes are not fetched again, while reuse still compares
the catalogue identities:

```python
sources = [
    {"url": url, "path": already_downloaded / url.rsplit("/", 1)[-1]}
    for url in urls
]
```

Strings are URLs; use a `Path` object for a local-only source. The full logical
URL is hashed for identity, while receipts omit credentials, query strings and
fragments. Avoid secrets in URL paths because paths are retained for display.

## MHCflurry compatibility

Historical MHCflurry catalogues lack published hashes and record the exact,
ordered URL list in `DOWNLOAD_INFO.csv`. An adapter can preserve that contract:

```python
import csv
import io

buffer = io.StringIO(newline="")
writer = csv.writer(buffer)
writer.writerow(["url"])
writer.writerows([url] for url in urls)

tree = install_archive(
    release_directory / download_name,
    sources,
    verified=False,
    extra_files={"DOWNLOAD_INFO.csv": buffer.getvalue()},
    download_options={"timeout": timeout, "max_retries": max_retries},
)
```

MHCflurry should treat a fast
`inspect_archive(..., verify_files=False).status == "available"` using the same
sources and `extra_files` as installed, not mere destination existence. Its
`get_path` adapter should append member paths to `state.bundle`. The fast
check validates the manifest and requested source identity without reading
large model files on every path lookup. Explicit diagnostics can
use the default `verify_files=True` for complete tree verification. This prevents
an initialized store or interrupted install from being mistaken for a complete
bundle.

For a catalogue-wide adapter, `VersionedArchiveRegistry` supplies consistent
version resolution, download, path, inspection and status methods while allowing
MHCflurry's existing directory order:

```python
from datacache import VersionedArchiveRegistry

registry = VersionedArchiveRegistry(
    archive_definitions,
    store_path=lambda name, release: release_directory(release) / name,
    verified=False,  # Historical catalogue entries have no published hashes.
)
tree = registry.download(
    "models_class1_presentation", "2.3.0",
    show_progress=True, timeout=60, max_retries=3,
)
rows = registry.status("models_class1_presentation")
```

`source_paths=` on `download` accepts one local file per ordered catalogue URL,
covering MHCflurry's `--already-downloaded-dir` mode without changing source
identity or bypassing transactional extraction.

Existing MHCflurry directories with files in them are never adopted or
overwritten, whether complete, partial, or unreceipted. A migration can continue to
read a legacy directory and use archive stores only for new installations, or
explicitly validate/import legacy content in application code. DataCache never
claims it silently, including with `force=True`.

## Extraction policy

DataCache parses tar, gzip-compressed tar, bzip2-compressed tar and xz-compressed
tar archives using format detection. Leading `./` components are normalized and
the `.` root directory entry is ignored, so archives created with
`tar -cf archive.tar -C tree .` work. Before writing members it rejects:

- absolute, parent-traversing, non-portable, and reserved DataCache paths;
- symbolic links, hard links, devices, FIFOs and every other special member;
- duplicate names, file/directory conflicts, and collisions ignoring case;
- archives without a regular file; and
- consumer `extra_files` that collide with archive content.

Extraction writes regular files itself rather than calling `extractall`. Archive
ownership, timestamps and permission bits—including executable and set-ID bits—
are not applied. New files and directories use ordinary umask-derived modes.
`max_members` and `max_extracted_size` can impose application-specific resource
limits. They are enforced as each header arrives, before traversing the payload
of a member that exceeds the limit; scanning stops at the first violation.
The member count includes root directory entries even though they are not
extracted. Local sources must be regular files; links and special inputs such
as FIFOs are rejected without waiting for a writer.

After extraction, the bundle's manifest records the assembled archive's
observed hash and size, every part's observed hash, ordered source fingerprints,
and a sorted hash/size manifest of every installed file. The complete consumer
`extra_files` inventory is recorded separately. Manifest reading supports the
same sizes as writing, including inventories larger than 1 MiB. Inspection rejects
missing, changed, additional, linked or special files and directory changes.
`ArchiveInspection` also exposes the redacted source URLs, fetch time, assembled
size and recorded digest for downstream `list` and `info` commands.

## Trust and reuse

Trusted verification requires either an assembled archive hash and size or a
hash and size for every part. Hash-pinned content can be reused across different
mirrors. `verified=False` permits historical unpinned sources; it verifies local
consistency against observed receipts and requires the same ordered full source
identities, but `ArchiveInspection.verified` remains false.

A valid cache hit is entirely read-only. `inspect_archive` hashes the complete
tree by default; `verify_files=False` checks only the manifest and source
identity, reading none of the extracted tree, and never reports
`verified=True`. Invalid installations require an
explicit `force=True`; inaccessible installations propagate their permission
error. Consumer metadata is part of the requested archive's identity, so a
different, added, or removed metadata entry requires an explicit refresh.

## Publication and platforms

Archive stores use the same layout as [bundles](bundles.md#how-bundles-are-stored):
each install is a bundle in `bundles/`, named for its UTC install time, and the
newest is the current one. Installs into one store take turns on a lock file
beside the store. Downloads, concatenation, extraction, consumer metadata and
the manifest are completed in private staging and read back and checked there.
Only then is the tree renamed into `bundles/`, which publishes it in one step.
A failed or interrupted install leaves the previous bundle current.

Old bundles are kept so paths already returned to readers remain valid, until
you delete them with `prune_bundles` or `registry.prune`; see
[removing old bundles](bundles.md#removing-old-bundles). Installs lock a file beside the store
(`flock` on POSIX, the `filelock` package on Windows) and publish with
same-filesystem atomic renames, supporting ordinary local filesystems on Linux,
macOS and Windows. Distributed coordination, arbitrary
network-filesystem semantics, and durability after sudden power loss are outside
the contract.

With `download_options={"resume": True}`, remote part files persist privately
across attempts and each remote part must have a trusted hash and size. A single
part may use the assembled archive expectations. Retry, timeout and progress
options otherwise have the same meanings as `fetch_file`; progress covers
network transfer, not concatenation or extraction.
