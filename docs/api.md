# Public API reference

This reference covers every name exported in `datacache.__all__` and every
public `Cache` method. Import these names from `datacache`. Signatures and
parameter descriptions are generated directly from the source docstrings.

Start with the [quickstart](index.md#cache-and-verify-a-file) for a first download,
or run through the self-contained [API examples](reference/examples.md).
The [download guide](downloads.md), [archive guide](archives.md), [data guide](data.md),
[progress guide](progress.md), and [shared-cache guide](shared-caches.md)
explain the longer workflows and compatibility guarantees.

## Index

| Area | APIs |
| --- | --- |
| Downloading | [fetch_file](#datacache.fetch_file), [fetch_bytes](#datacache.fetch_bytes), [fetch_and_transform](#datacache.fetch_and_transform), [discard_partial](#datacache.discard_partial) |
| Paths and presence | [expected_path](#datacache.expected_path), [build_local_filename](#datacache.build_local_filename), [get_data_dir](#datacache.get_data_dir), [get_cache_root](#datacache.get_cache_root), [resolve_cache_dir](#datacache.resolve_cache_dir), [CacheLocation](#datacache.CacheLocation), [SHARED_CACHE_ENV_KEY](#datacache.SHARED_CACHE_ENV_KEY), [format_bytes](#datacache.format_bytes), [build_path](#datacache.build_path), [ensure_dir](#datacache.ensure_dir), [clear_cache](#datacache.clear_cache) |
| Offline and progress policy | [OfflineError](#datacache.OfflineError), [OFFLINE_ENV_KEYS](#datacache.OFFLINE_ENV_KEYS), [is_offline](#datacache.is_offline), [offline_source](#datacache.offline_source), [require_online](#datacache.require_online), [NO_PROGRESS_ENV_KEYS](#datacache.NO_PROGRESS_ENV_KEYS), [should_show_progress](#datacache.should_show_progress) |
| Integrity and inspection | [validate_file](#datacache.validate_file), [inspect_file](#datacache.inspect_file), [inspect_files](#datacache.inspect_files), [plan_fetch](#datacache.plan_fetch) |
| Results and exceptions | [FileInspection](#datacache.FileInspection), [CacheInspection](#datacache.CacheInspection), [FetchPlan](#datacache.FetchPlan), [FileValidationError](#datacache.FileValidationError), [InsufficientSpaceError](#datacache.InsufficientSpaceError), [CannotResume](#datacache.CannotResume) |
| SQLite | [db_from_dataframes_with_absolute_path](#datacache.db_from_dataframes_with_absolute_path), [connect_if_correct_version](#datacache.connect_if_correct_version) |
| Cache object | [Cache](#datacache.Cache), [fetch](#datacache.Cache.fetch), [local_filename](#datacache.Cache.local_filename), [local_path](#datacache.Cache.local_path), [exists](#datacache.Cache.exists), [inspect](#datacache.Cache.inspect), [plan_fetch](#datacache.Cache.plan_fetch), [make_readable](#datacache.Cache.make_readable), [delete_url](#datacache.Cache.delete_url), [delete_all](#datacache.Cache.delete_all) |
| Complete archive trees | [install_archive](#datacache.install_archive), [inspect_archive](#datacache.inspect_archive), [ArchiveInspection](#datacache.ArchiveInspection) |
| Versioned file bundles | [install_bundle](#datacache.install_bundle), [inspect_bundle](#datacache.inspect_bundle), [BundleInspection](#datacache.BundleInspection), [VersionedDatasetRegistry](#datacache.VersionedDatasetRegistry) |
| Derived artifacts | [materialize](#datacache.materialize), [inspect_materialization](#datacache.inspect_materialization), [MaterializationInspection](#datacache.MaterializationInspection) |
| Fixed-path file registry | [VersionedFileRegistry](#datacache.VersionedFileRegistry) |
| Individual HTTP ZIP members | [install_zip_member](#datacache.install_zip_member) |
| Package version | [`__version__`](#datacache.__version__) |

## Downloading

::: datacache.fetch_file

::: datacache.fetch_bytes

::: datacache.fetch_and_transform

::: datacache.discard_partial

## Paths and presence

::: datacache.expected_path

::: datacache.build_local_filename

::: datacache.get_data_dir

::: datacache.get_cache_root

::: datacache.resolve_cache_dir

::: datacache.CacheLocation

::: datacache.SHARED_CACHE_ENV_KEY

::: datacache.format_bytes

::: datacache.build_path

::: datacache.ensure_dir

::: datacache.clear_cache

## Offline and progress policy

::: datacache.OfflineError

::: datacache.OFFLINE_ENV_KEYS

::: datacache.is_offline

::: datacache.offline_source

::: datacache.require_online

::: datacache.NO_PROGRESS_ENV_KEYS

::: datacache.should_show_progress

## Integrity, inspection, and results

::: datacache.validate_file

::: datacache.inspect_file

::: datacache.inspect_files

::: datacache.plan_fetch

::: datacache.FileInspection

::: datacache.CacheInspection

::: datacache.FetchPlan

::: datacache.FileValidationError

::: datacache.InsufficientSpaceError

::: datacache.CannotResume

## SQLite

All database builders return an open `sqlite3.Connection` owned by the caller
(opened with `check_same_thread=False`). Use `contextlib.closing` or call
`.close()` explicitly. See the [data guide](data.md#reuse-and-replacement) for
locking, symlink, type mapping, and filesystem requirements.

::: datacache.db_from_dataframes_with_absolute_path

::: datacache.connect_if_correct_version

## Cache object

::: datacache.Cache

## Complete archive trees

See the [archive installation guide](archives.md) for split sources, safe-member
rules, publication semantics, and an MHCflurry adapter.

::: datacache.install_archive

::: datacache.inspect_archive

::: datacache.ArchiveInspection

## Versioned file bundles

See the [bundle guide](bundles.md) and [integration guide](integration.md#notes-for-specific-libraries)
for complete multi-file dataset examples.

::: datacache.install_bundle

::: datacache.inspect_bundle

::: datacache.BundleInspection

::: datacache.VersionedDatasetRegistry

## Derived artifacts

See the [materialization guide](materialization.md) for receipts, resume,
callback contracts, retention, and disk usage.

::: datacache.materialize

::: datacache.inspect_materialization

::: datacache.MaterializationInspection

## Fixed-path file registry

See the [fixed-path guide](file_registry.md) for legacy receipt semantics,
trust boundaries, and the distinction from bundles.

::: datacache.VersionedFileRegistry

## Individual HTTP ZIP members

See the [member guide](zip-members.md) for a complete example and storage costs.

::: datacache.install_zip_member

## Package version

::: datacache.__version__
