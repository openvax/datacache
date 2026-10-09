"""Versioned datasets installed as bundles of separately downloaded files."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import unicodedata
from uuid import uuid4

from ._filesystem import path_present, read_json, write_json
from .bundle_store import (
    INVALID_STORE_ERRORS, MANIFEST, BundleStore, discard_private_directory, hash_file, local_file_identity,
    private_directory, prune_bundles, require_directory, source_fingerprint, user_key, validate_distinct_paths, validate_no_sidecar_collisions,
    validate_path_component, validate_relative_name,
)
from .download import normal_creation_mode, validate_download_options, validate_size_within_limit
from .integrity import FileValidationError, _validate_expectations
from .provenance import redact_url

FORMAT = 1


def _assets(assets, verified=True):
    if not isinstance(assets, dict) or not assets:
        raise ValueError('assets must be a nonempty mapping of relative names to metadata')
    result = {}
    for name, spec in assets.items():
        validate_relative_name(name)
        if isinstance(spec, str):
            spec = {'url': spec}
        if not isinstance(spec, dict):
            raise ValueError('asset metadata must be a mapping')
        unknown = set(spec) - {'url', 'path', 'sha256', 'size', 'decompress'}
        if unknown:
            raise ValueError('unknown asset options: %s' % sorted(unknown))
        if ('url' in spec) == ('path' in spec):
            raise ValueError('each asset needs exactly one of url or path')
        if 'path' in spec and (not isinstance(spec['path'], (str, os.PathLike)) or not os.fspath(spec['path'])):
            raise ValueError('asset path must be a nonempty path')
        # A local file is identified by its file:// URL, like any other source.
        url = spec['url'] if 'url' in spec else local_file_identity(spec['path'])
        if not isinstance(url, str) or not url:
            raise ValueError('each asset requires a nonempty URL')
        digest, size = spec.get('sha256'), spec.get('size')
        _validate_expectations(digest, size)
        if verified and (digest is None or size is None):
            raise ValueError('verified bundles require sha256 and size for every asset')
        decompress = spec.get('decompress', False)
        if not isinstance(decompress, bool):
            raise ValueError('decompress must be a boolean')
        result[name] = dict(url=url, sha256=digest.lower() if digest else None,
                            size=size, decompress=decompress)
    validate_distinct_paths(result)
    validate_no_sidecar_collisions(result)
    return result


def _receipt_assets(receipt):
    if not isinstance(receipt, dict) or receipt.get('format') != FORMAT:
        raise ValueError('unrecognized bundle manifest')
    assets = receipt.get('assets')
    # Every local receipt pins the bytes, even in unverified acquisition mode.
    return _assets(assets, verified=True)


@dataclass(frozen=True)
class BundleInspection:
    """Read-only status of a store's current bundle; paths belong to that bundle.

    status is available, missing, invalid or inaccessible. bundle is the
    current bundle's directory. verified means every file matched caller-supplied trusted hashes now;
    receipt-only full inspection checks recorded hashes without asserting trust.
    Metadata-only inspection checks readability and recorded sizes, not hashes.
    """
    path: str
    status: str
    verified: bool = False
    bundle: object = None
    files: dict = field(default_factory=dict)
    error: object = None


def _check_bundle(store, directory, assets, *, verify_files=True):
    # Manifests grow with the number of assets; the writer has no size cap.
    receipt = read_json(directory / MANIFEST, limit=None)
    recorded = _receipt_assets(receipt)
    if assets is not None:
        if set(recorded) != set(assets):
            raise FileValidationError(directory, 'manifest asset names disagree with registry')
        for name, spec in assets.items():
            if spec['sha256'] is None:
                fingerprints = receipt.get('source_fingerprints')
                if (not isinstance(fingerprints, dict) or
                        fingerprints.get(name) != source_fingerprint(spec['url'])):
                    raise FileValidationError(directory, 'manifest source identity is missing or disagrees with registry')
                if spec['decompress'] != recorded[name]['decompress']:
                    raise FileValidationError(directory, 'manifest decompression setting disagrees with registry')
            for key in ('sha256', 'size'):
                if spec[key] is not None and recorded[name][key] != spec[key]:
                    raise FileValidationError(directory, 'manifest expectations disagree with registry')
    files = {}
    for name, spec in recorded.items():
        target = directory / name
        for parent in target.relative_to(directory).parents:
            require_directory(directory / parent)
        hashed = hash_file(target, spec, hash_contents=verify_files)
        # Only a hash the caller supplied verifies a file; the manifest's own doesn't.
        supplied = assets is not None and assets[name]['sha256'] is not None
        files[name] = hashed.inspection(verified=verify_files and supplied)
    trusted = verify_files and assets is not None and all(spec['sha256'] for spec in assets.values())
    return BundleInspection(str(store.path), 'available', bool(trusted), str(directory), files)


def inspect_bundle(destination, assets=None, *, verify_files=True):
    """Check the current bundle offline, without writes, locks or repair.

    Optional assets is the caller's trusted mapping (url, sha256, size). Without
    it, check consistency with the bundle's own manifest, verified=False.
    verify_files=False checks the receipt, source expectations, required file
    types, readability and sizes without reading payloads; verified stays False.
    """
    if not isinstance(verify_files, bool):
        raise ValueError('verify_files must be a boolean')
    path = Path(destination)
    store = BundleStore(path, 'bundle')
    expected = _assets(assets, verified=False) if assets is not None else None
    try:
        bundle = store.current_bundle()
        if bundle is None:
            return BundleInspection(str(path), 'missing')
        return _check_bundle(store, bundle, expected, verify_files=verify_files)
    except PermissionError as error:
        return BundleInspection(str(path), 'inaccessible', error=error)
    except INVALID_STORE_ERRORS as error:
        return BundleInspection(str(path), 'invalid', error=error)


def _local_sources(source_paths, assets):
    """source_paths as absolute Paths, checked against the assets they replace."""
    if source_paths is None:
        return {}
    if not isinstance(source_paths, dict):
        raise ValueError('source_paths must map asset names to local files')
    unknown = set(source_paths) - set(assets)
    if unknown:
        raise ValueError('source_paths names unknown assets: %s' % sorted(unknown))
    result = {}
    for name, path in source_paths.items():
        if not isinstance(path, (str, os.PathLike)) or not os.fspath(path):
            raise ValueError('source_paths[%r] must be a path' % name)
        path = Path(path).absolute()
        if assets[name]['decompress'] and path.suffix.lower() not in ('.gz', '.zip'):
            raise ValueError('source_paths[%r] must end in .gz or .zip to be decompressed' % name)
        result[name] = path
    return result


def _paths(inspection):
    return {name: value.path for name, value in inspection.files.items()}


def install_bundle(destination, assets, *, force=False, verified=True, verify_files=True, download_options=None,
                   source_paths=None):
    """Download every asset, then publish them together as a new bundle.

    assets maps relative names to {url, sha256, size, decompress?}; path in
    place of url names a local file. Trusted sha256 and size are mandatory
    unless verified=False is explicit. source_paths maps asset names to local
    files to read instead of downloading, such as files already downloaded by
    hand; each asset keeps its declared URL as its identity, so later
    installs treat the bundle exactly as if it had been downloaded. On a
    valid cache hit this is read-only, including on a read-only filesystem.
    An invalid current bundle requires force=True, which installs a new one.

    Returns a dict of asset names to paths in the current bundle. Old bundles
    are kept, so these paths survive later force installs. Generated outputs
    belong outside this store. Installation works on local filesystems with
    atomic sibling renames, on Linux, macOS and Windows. verify_files controls
    checks of an existing bundle only; new downloads are always checked.
    """
    from .download import fetch_file
    if not all(isinstance(value, bool) for value in (verified, force, verify_files)):
        raise ValueError('verified, force and verify_files must be booleans')
    expected = _assets(assets, verified)
    options = validate_download_options(download_options, 'bundle')
    local_sources = _local_sources(source_paths, expected)
    for name, spec in expected.items():
        validate_size_within_limit(spec['size'], options.get('max_bytes'), '%s size' % name)
    if options.get('resume'):
        # Check before creating anything: a resumable install keeps its staging.
        from .resume import validate_resume
        for name, spec in expected.items():
            if name in local_sources:
                continue  # Copied, not downloaded.
            validate_resume(spec['url'], spec['sha256'], spec['size'])
            if spec['decompress']:
                raise ValueError('resume=True supports raw downloads only; %s has decompress=True' % name)
    path = Path(destination)
    store = BundleStore(path, 'bundle')
    # A forced install replaces the bundle whatever it holds: don't hash it.
    inspection = inspect_bundle(path, expected, verify_files=verify_files and not force)
    if store.can_reuse(inspection, force=force):
        return _paths(inspection)
    path.parent.mkdir(parents=True, exist_ok=True)
    with store.lock():
        store.create()
        inspection = inspect_bundle(path, expected, verify_files=verify_files and not force)
        if store.can_reuse(inspection, force=force):
            return _paths(inspection)
        # A resumable bundle keeps its private working directory across calls,
        # including completed assets. The registry identity selects it, and
        # different users never inherit each other's private partials.
        resumable = options.get('resume', False)
        staging_key = (hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()
                       + '-' + user_key()) if resumable else uuid4().hex
        working = private_directory(path / ('.staging-' + staging_key))
        # The private parent protects unfinished bytes. The inner directory
        # already has its final sharing mode, so the bundle is readable by
        # others the moment it is renamed into place.
        staged = working / 'files'
        staged.mkdir(exist_ok=resumable)
        require_directory(staged)
        published = False
        try:
            recorded = {}
            for name, spec in expected.items():
                target = staged / name
                fetch_options = dict(options, destination=target, decompress=spec['decompress'],
                                     expected_sha256=spec['sha256'], expected_size=spec['size'])
                source = spec['url']
                if name in local_sources:
                    # Read the local copy; the manifest keeps the declared URL.
                    source = local_sources[name].as_uri()
                    fetch_options.pop('resume', None)
                    fetch_options['raw'] = not spec['decompress']
                try:
                    fetch_file(source, **fetch_options)
                except FileValidationError:
                    if not resumable or not target.is_file():
                        raise
                    # This is private, unpublished working state, not a user's
                    # installed bundle. Explicit installation may repair it.
                    fetch_file(source, force=True, **fetch_options)
                # fetch_file already checked a trusted hash; hash only the rest.
                hashed = hash_file(target, hash_contents=spec['sha256'] is None)
                recorded[name] = dict(url=redact_url(spec['url']), sha256=hashed.sha256 or spec['sha256'],
                                      size=hashed.size, decompress=spec['decompress'])
            receipt = dict(format=FORMAT, fetched_at=datetime.now(timezone.utc).isoformat(), assets=recorded,
                           source_fingerprints={name: source_fingerprint(spec['url'])
                                                for name, spec in expected.items()})
            # Remove DataCache's own temporary files: resume state and anything
            # an interrupted earlier attempt left. No asset is named .datacache-*.
            for leftover in sorted(staged.rglob('.datacache-*'), reverse=True):
                if leftover.is_dir() and not leftover.is_symlink():
                    shutil.rmtree(leftover)
                else:
                    leftover.unlink(missing_ok=True)
            write_json(staged / MANIFEST, receipt, mode=normal_creation_mode(staged))
            # Check the complete bundle, hashes included, before publishing it.
            _check_bundle(store, staged, expected)
            bundle = store.publish(staged)
            published = True
            return {name: str(bundle / name) for name in expected}
        finally:
            if published or not resumable:
                discard_private_directory(working)


class VersionedDatasetRegistry:
    """A plain mapping of dataset names, pinned defaults, versions and assets.

    Each dataset is {default_version, versions: {version: {asset: metadata}}}.
    The single-file shape {filename, urls: {version: url}, default_version} is
    also accepted with verified=False. cache_root is a path; cache_dir
    optionally accepts a zero-argument root callable. Alternatively, store_path is
    a two-argument (name, version) callback selecting an exact managed store.
    Construction never writes or invokes either callback.
    """

    def __init__(self, datasets, *, cache_root=None, cache_dir=None, store_path=None, verified=True):
        if sum(value is not None for value in (cache_root, cache_dir, store_path)) != 1:
            raise ValueError('provide exactly one of cache_root, cache_dir or store_path')
        if not isinstance(verified, bool):
            raise ValueError('verified must be a boolean')
        if cache_dir is not None and not callable(cache_dir):
            raise ValueError('cache_dir must be callable')
        if store_path is not None and not callable(store_path):
            raise ValueError('store_path must be callable')
        if store_path is None:
            # A root callable is consulted on every lookup, so it can change.
            root = cache_dir if cache_dir is not None else lambda: cache_root
            self._store_path = lambda name, version: Path(root()) / name / version
        else:
            self._store_path = store_path
        # Custom store paths, resolved and checked once on first use.
        self._custom_stores = None if store_path is None else {}
        self.verified = verified
        self._datasets = {}
        for name, spec in datasets.items():
            validate_path_component(name)
            versions = spec.get('versions')
            if versions is None:
                versions = {version: {spec['filename']: {'url': url, 'decompress': True}}
                            for version, url in spec['urls'].items()}
            normalized = {validate_path_component(version): _assets(assets, verified) for version, assets in versions.items()}
            # Versions are directories: case-insensitive filesystems would give
            # two that differ only by case one store.
            validate_distinct_paths(list(normalized))
            default = spec['default_version']
            if default not in normalized:
                raise ValueError('default_version must name a pinned version')
            self._datasets[name] = dict(default_version=default, versions=normalized,
                                        description=spec.get('description', ''))
        validate_distinct_paths(list(self._datasets))

    def resolve_version(self, name, version=None):
        if name not in self._datasets:
            raise ValueError('unknown dataset %r; known: %s' % (name, ', '.join(sorted(self._datasets))))
        spec = self._datasets[name]
        version = spec['default_version'] if version is None else version
        if version not in spec['versions']:
            raise ValueError('unknown version %r for %s' % (version, name))
        return version

    def store_path(self, name, version=None):
        """Resolve the store path without checking it or creating directories."""
        version = self.resolve_version(name, version)
        if self._custom_stores is None:
            path = self._store_path(name, version)
            # <root>/<name> is DataCache's own directory: never follow a link there.
            if path_present(path.parent):
                require_directory(path.parent)
            return path
        # A custom store's parent belongs to the application and may be a link
        # (e.g. to another disk); the store itself is still never one.
        if not self._custom_stores:
            self._custom_stores = self._resolve_custom_stores()
        return self._custom_stores[(name, version)]

    def _resolve_custom_stores(self):
        """Every (name, version)'s store_path result: one store per version."""
        stores, owners = {}, {}
        for name in sorted(self._datasets):
            for version in sorted(self._datasets[name]['versions']):
                path = self._store_path(name, version)
                if not isinstance(path, (str, os.PathLike)) or not os.fspath(path):
                    raise ValueError('store_path(%r, %r) returned %r, not a path' % (name, version, path))
                shown = os.path.normpath(os.path.abspath(path))
                # Case-folded: case-insensitive filesystems store these as one.
                key = unicodedata.normalize('NFC', shown.casefold())
                if key in owners:
                    raise ValueError('store_path gives %s for both %s %s and %s %s; '
                                     'each version needs its own store' % ((shown,) + owners[key] + (name, version)))
                owners[key] = (name, version)
                stores[(name, version)] = Path(path)
        return stores

    def inspect(self, name, version=None, *, verify_files=True):
        version = self.resolve_version(name, version)
        return inspect_bundle(self.store_path(name, version), self._datasets[name]['versions'][version],
                              verify_files=verify_files)

    def download(self, name, version=None, *, force=False, verify_files=True, source_paths=None,
                 **download_options):
        """Install if needed (or always, with force=True); return asset paths in the current bundle.

        source_paths maps asset names to local files to read instead of
        downloading; the declared URLs remain the assets' identity.
        """
        version = self.resolve_version(name, version)
        return install_bundle(self.store_path(name, version), self._datasets[name]['versions'][version],
                              force=force, verified=self.verified, verify_files=verify_files,
                              download_options=download_options, source_paths=source_paths)

    def prune(self, name, version=None, *, keep=1):
        """Delete all but the newest keep bundles of one version; see prune_bundles."""
        return prune_bundles(self.store_path(name, version), keep=keep)

    def local_path(self, name, version=None, *, asset=None, verify_files=True):
        """Resolve the current bundle; no writes or network. Missing raises.

        For one asset, return its Path; for multiple assets return the bundle
        directory, or select an individual asset with asset=. Assets are hashed
        by default; verify_files=False checks metadata and sizes only, which
        cannot detect same-size corruption.
        """
        inspected = self.inspect(name, version, verify_files=verify_files)
        if inspected.status == 'missing':
            raise FileNotFoundError(inspected.path)
        if inspected.status != 'available':
            raise FileValidationError(inspected.path, inspected.status) from inspected.error
        if asset is not None:
            return Path(inspected.files[asset].path)
        if len(inspected.files) == 1:
            return Path(next(iter(inspected.files.values())).path)
        return Path(inspected.bundle)

    def ensure(self, name, version=None, **download_options):
        """Download/reuse, then return what local_path would: the single asset's
        Path, or the bundle directory of several assets."""
        paths = self.download(name, version, **download_options)
        if len(paths) == 1:
            return Path(next(iter(paths.values())))
        # The paths download validated: no second inspection, so a concurrent
        # refresh cannot swap the bundle between the two.
        asset_name, asset_path = next(iter(paths.items()))
        return Path(asset_path).parents[len(Path(asset_name).parts) - 1]

    def is_cached(self, name, version=None, *, verify_files=True):
        """Whether inspection reports available; verify_files=False skips hashing."""
        return self.inspect(name, version, verify_files=verify_files).status == 'available'

    def status(self, *, verify_files=True):
        """One read-only status row per pinned default; verify_files=False skips hashing."""
        return [dict(name=name, version=self.resolve_version(name),
                     description=self._datasets[name]['description'],
                     available_versions=sorted(self._datasets[name]['versions']),
                     inspection=self.inspect(name, verify_files=verify_files)) for name in sorted(self._datasets)]
