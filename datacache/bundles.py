"""Versioned datasets published as complete, immutable local generations."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from uuid import uuid4

from ._filesystem import file_lock, open_regular, path_present, read_json, write_json
from .integrity import FileValidationError, _validate_expectations
from .inspection import FileInspection, inspect_file
from .provenance import redact_url, sidecar_path

STORE = '.datacache-bundle.json'
MANIFEST = '.datacache-manifest.json'
CURRENT = 'current.json'
FORMAT = 1


def _relative_name(value):
    if not isinstance(value, str) or not value or '\\' in value:
        raise ValueError('asset names must be nonempty relative POSIX paths')
    for part in value.split('/'):
        if (part in ('', '.', '..') or part.endswith((' ', '.')) or
                re.search(r'[\x00-\x1f<>:"|?*]', part) or
                part.lower().startswith('.datacache-') or
                re.fullmatch(r'(?i:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?', part)):
            raise ValueError('unsafe asset path: %r' % value)
    return value


def _component(value):
    _relative_name(value)
    if '/' in value:
        raise ValueError('dataset names and versions must be single path components')
    return value


def _assets(assets, verified=True):
    if not isinstance(assets, dict) or not assets:
        raise ValueError('assets must be a nonempty mapping of relative names to metadata')
    result = {}
    for name, spec in assets.items():
        _relative_name(name)
        if isinstance(spec, str):
            spec = {'url': spec}
        if not isinstance(spec, dict):
            raise ValueError('asset metadata must be a mapping')
        unknown = set(spec) - {'url', 'sha256', 'size', 'decompress'}
        if unknown:
            raise ValueError('unknown asset options: %s' % sorted(unknown))
        url = spec.get('url')
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
    folded = {name.casefold() for name in result}
    parents = {'/'.join(name.split('/')[:i]).casefold()
               for name in result for i in range(1, len(name.split('/')))}
    if len(folded) != len(result) or folded & parents:
        raise ValueError('asset paths collide as files/directories or ignoring case')
    sidecars = {Path(sidecar_path(name)).as_posix().casefold() for name in result}
    if sidecars & (folded | parents):
        raise ValueError('asset paths collide with automatic provenance sidecars')
    return result


def _source_fingerprint(url):
    """Identify the complete source without storing credentials or query text."""
    return hashlib.sha256(url.encode('utf-8')).hexdigest()


def _directory(path):
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise FileValidationError(path, 'expected a directory, not a link')


def _store(path):
    _directory(path)
    if read_json(path / STORE) != {'format': FORMAT}:
        raise FileValidationError(path, 'unrecognized bundle store')
    _directory(path / 'generations')


def _generation(path, generation):
    if not isinstance(generation, str) or re.fullmatch('[0-9a-f]{32}', generation) is None:
        raise FileValidationError(path, 'invalid generation pointer')
    result = path / 'generations' / generation
    _directory(result)
    return result


def _receipt_assets(receipt):
    if not isinstance(receipt, dict) or receipt.get('format') != FORMAT:
        raise ValueError('unrecognized bundle manifest')
    assets = receipt.get('assets')
    # Every local receipt pins the bytes, even in unverified acquisition mode.
    return _assets(assets, verified=True)


@dataclass(frozen=True)
class BundleInspection:
    """Read-only bundle status; paths belong to one immutable generation.

    status is available, missing, invalid, inaccessible, or recovery-required.
    verified means every file matched caller-supplied trusted hashes now;
    receipt-only full inspection checks recorded hashes without asserting trust.
    Metadata-only inspection checks readability and recorded sizes, not hashes.
    """
    path: str
    status: str
    verified: bool = False
    generation: object = None
    files: dict = field(default_factory=dict)
    error: object = None


def _inspect_generation(store, generation, assets, *, verify_files=True):
    directory = _generation(store, generation)
    receipt = read_json(directory / MANIFEST)
    recorded = _receipt_assets(receipt)
    if assets is not None:
        if set(recorded) != set(assets):
            raise FileValidationError(directory, 'manifest asset names disagree with registry')
        for name, spec in assets.items():
            if spec['sha256'] is None:
                fingerprints = receipt.get('source_fingerprints')
                if (not isinstance(fingerprints, dict) or
                        fingerprints.get(name) != _source_fingerprint(spec['url'])):
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
            _directory(directory / parent)
        # Reject symlinks and special files before the ordinary inspection API.
        fd = open_regular(target)
        try:
            info = None if verify_files else os.fstat(fd)
        finally:
            os.close(fd)
        if verify_files:
            inspected = inspect_file(target, expected_sha256=spec['sha256'], expected_size=spec['size'])
            if inspected.status != 'available':
                raise inspected.error or FileValidationError(target, 'unavailable bundle asset')
        else:
            # Metadata only: an open, readable regular file of the recorded size.
            if info.st_size != spec['size']:
                raise FileValidationError(target, 'bundle asset size disagrees with manifest')
            inspected = FileInspection(str(target), 'available', size=info.st_size, mtime=info.st_mtime)
        files[name] = inspected
    trusted = verify_files and assets is not None and all(spec['sha256'] for spec in assets.values())
    return BundleInspection(str(store), 'available', bool(trusted), str(directory), files)


def inspect_bundle(destination, assets=None, *, verify_files=True):
    """Validate one installed snapshot offline, without writes, locks or repair.

    Optional assets is the caller's trusted mapping (url, sha256, size). Without
    it, verify consistency with the per-generation receipt, verified=False.
    An interrupted install with completed local generations but no pointer is
    recovery-required; explicit install_bundle can recover it without network.
    verify_files=False checks the receipt, source expectations, required file
    types, readability and sizes without reading payloads; verified stays False.
    """
    if not isinstance(verify_files, bool):
        raise ValueError('verify_files must be a boolean')
    path = Path(destination)
    expected = _assets(assets, verified=False) if assets is not None else None
    try:
        if not path_present(path):
            return BundleInspection(str(path), 'missing')
        try:
            _store(path)
        except FileNotFoundError:
            # A caller may create the destination before installing. Existing
            # recognized stores need no directory listing merely to inspect.
            if not any(path.iterdir()):
                return BundleInspection(str(path), 'missing')
            # Another installer may have initialized an empty directory after
            # our missing-marker read. Recheck before calling it invalid.
            _store(path)
        try:
            pointer = read_json(path / CURRENT)
        except FileNotFoundError:
            if any((path / 'generations').iterdir()):
                return BundleInspection(str(path), 'recovery-required')
            return BundleInspection(str(path), 'missing')
        return _inspect_generation(path, pointer['generation'], expected, verify_files=verify_files)
    except PermissionError as error:
        return BundleInspection(str(path), 'inaccessible', error=error)
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as error:
        return BundleInspection(str(path), 'invalid', error=error)


def _paths(inspection):
    return {name: value.path for name, value in inspection.files.items()}


def _initialize(path):
    existing_mode = None
    if path_present(path):
        _directory(path)
        if any(path.iterdir()):
            _store(path)  # Never take over an arbitrary nonempty directory.
            return
        existing_mode = stat.S_IMODE(path.lstat().st_mode)
    # Publish a complete store skeleton at once: concurrent first-time readers
    # see absence or a recognized store, never a half-written ownership marker.
    staging = Path(tempfile.mkdtemp(prefix='.datacache-store-', dir=path.parent))
    try:
        write_json(staging / STORE, {'format': FORMAT}, mode=_file_mode(path.parent))
        (staging / 'generations').mkdir()
        os.chmod(staging, existing_mode if existing_mode is not None else
                 stat.S_IMODE((staging / 'generations').stat().st_mode))
        os.replace(staging, path)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _file_mode(path):
    from .download import _normal_creation_mode
    return _normal_creation_mode(path)


def _recover(path, assets):
    generations = path / 'generations'
    for entry in sorted(generations.iterdir(), reverse=True):
        try:
            candidate = _inspect_generation(path, entry.name, assets)
        except (OSError, ValueError, KeyError, TypeError, RecursionError):
            continue
        write_json(path / CURRENT, {'generation': entry.name}, mode=_file_mode(path))
        return candidate
    return None


def install_bundle(destination, assets, *, force=False, verified=True, verify_files=True, download_options=None):
    """Install all assets before publishing one atomic generation pointer.

    assets maps relative names to {url, sha256, size, decompress?}. Trusted
    sha256 and size are mandatory unless verified=False is explicit. On a
    valid cache hit this is read-only, including on a read-only filesystem.
    Invalid installations require force=True. Recoverable complete local
    generations are preferred to network acquisition when the pointer is lost.

    Returns a dict of asset names to snapshot paths. Published generations are
    retained, so these paths survive later force installs. Generated outputs
    belong outside this managed source store. Installation requires a POSIX
    local filesystem with flock and atomic sibling os.replace.
    verify_files controls existing-generation checks only. New generations and
    explicit recovery always validate payloads before publication.
    """
    from .download import fetch_file
    if not all(isinstance(value, bool) for value in (verified, force, verify_files)):
        raise ValueError('verified, force and verify_files must be booleans')
    expected = _assets(assets, verified)
    options = dict(download_options or {})
    allowed = {'timeout', 'chunk_size', 'progress_callback', 'show_progress',
               'max_retries', 'retry_backoff', 'retry_max_delay', 'resume'}
    if set(options) - allowed:
        raise ValueError('unsupported bundle download options: %s' % sorted(set(options) - allowed))
    path = Path(destination)
    inspection = inspect_bundle(path, expected, verify_files=verify_files)
    if not force and inspection.status == 'available':
        return _paths(inspection)
    if inspection.status == 'inaccessible':
        raise inspection.error
    if not force and inspection.status == 'invalid':
        raise FileValidationError(path, 'invalid bundle; use force=True to explicitly repair') from inspection.error
    if os.name != 'posix':
        raise NotImplementedError('Bundle installation requires a POSIX local filesystem')
    path.parent.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(os.fsencode(path.name)).hexdigest()[:32]
    with file_lock(path.parent / ('.datacache-bundle-lock-' + key)):
        _initialize(path)
        inspection = inspect_bundle(path, expected, verify_files=verify_files)
        if not force and inspection.status == 'available':
            return _paths(inspection)
        if inspection.status == 'invalid' and not force:
            raise FileValidationError(path, 'invalid bundle; use force=True to explicitly repair') from inspection.error
        if inspection.status in ('missing', 'recovery-required', 'invalid'):
            # No automatic rollback from an invalid current generation unless
            # explicitly repairing. A missing pointer is explicit recovery.
            recovered = _recover(path, expected)
            if recovered is not None:
                return _paths(recovered)
        generation = uuid4().hex
        # A resumable bundle keeps its private working directory across calls,
        # including completed assets. The registry identity selects it, and
        # different users never inherit each other's private partials.
        resumable = options.get('resume', False)
        staging_key = (hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()
                       + '-%d' % os.getuid()) if resumable else generation
        working = path / ('.staging-' + staging_key)
        working.mkdir(mode=0o700, exist_ok=resumable)
        info = working.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) & 0o077):
            raise FileValidationError(working, 'bundle staging must be a private directory')
        # The private parent protects unfinished bytes. The inner directory
        # already has its final sharing mode, including during the atomic
        # rename, so an interrupted publication is recoverable by other readers.
        staged = working / 'files'
        staged.mkdir(exist_ok=resumable)
        _directory(staged)
        published = False
        try:
            recorded = {}
            for name, spec in expected.items():
                target = staged / name
                fetch_options = dict(options, destination=target, decompress=spec['decompress'],
                                     expected_sha256=spec['sha256'], expected_size=spec['size'])
                try:
                    fetch_file(spec['url'], **fetch_options)
                except FileValidationError:
                    if not resumable or not target.is_file():
                        raise
                    # This is private, unpublished working state, not a user's
                    # installed bundle. Explicit installation may repair it.
                    fetch_file(spec['url'], force=True, **fetch_options)
                observed_sha256 = spec['sha256']
                if observed_sha256 is None:
                    digest = hashlib.sha256()
                    with target.open('rb') as source:
                        for chunk in iter(lambda: source.read(2 ** 20), b''):
                            digest.update(chunk)
                    observed_sha256 = digest.hexdigest()
                recorded[name] = dict(url=redact_url(spec['url']), sha256=observed_sha256,
                                      size=target.stat().st_size, decompress=spec['decompress'])
            receipt = dict(format=FORMAT, fetched_at=datetime.now(timezone.utc).isoformat(), assets=recorded,
                           source_fingerprints={name: _source_fingerprint(spec['url'])
                                                for name, spec in expected.items()})
            write_json(staged / MANIFEST, receipt, mode=_file_mode(staged))
            final = path / 'generations' / generation
            os.replace(staged, final)
            published = True
            candidate = _inspect_generation(path, generation, expected)
            write_json(path / CURRENT, {'generation': generation}, mode=_file_mode(path))
            return _paths(candidate)
        finally:
            if published or not resumable:
                shutil.rmtree(working)


class VersionedDatasetRegistry:
    """A plain mapping of dataset names, pinned defaults, versions and assets.

    Each dataset is {default_version, versions: {version: {asset: metadata}}}.
    The hitlist shape {filename, urls: {version: url}, default_version} is also
    accepted with verified=False. cache_root is a path; cache_dir optionally
    accepts hitlist's zero-argument root callable. Construction never writes.
    """

    def __init__(self, datasets, *, cache_root=None, cache_dir=None, verified=True):
        if (cache_root is None) == (cache_dir is None):
            raise ValueError('provide exactly one of cache_root or cache_dir')
        if not isinstance(verified, bool):
            raise ValueError('verified must be a boolean')
        self._root = cache_dir if cache_dir is not None else lambda: cache_root
        if not callable(self._root):
            raise ValueError('cache_dir must be callable')
        self.verified = verified
        self._datasets = {}
        for name, spec in datasets.items():
            _component(name)
            versions = spec.get('versions')
            if versions is None:
                versions = {version: {spec['filename']: {'url': url, 'decompress': True}}
                            for version, url in spec['urls'].items()}
            normalized = {_component(version): _assets(assets, verified) for version, assets in versions.items()}
            default = spec['default_version']
            if default not in normalized:
                raise ValueError('default_version must name a pinned version')
            self._datasets[name] = dict(default_version=default, versions=normalized,
                                        description=spec.get('description', ''))

    def resolve_version(self, name, version=None):
        if name not in self._datasets:
            raise ValueError('unknown dataset %r; known: %s' % (name, ', '.join(sorted(self._datasets))))
        spec = self._datasets[name]
        version = spec['default_version'] if version is None else version
        if version not in spec['versions']:
            raise ValueError('unknown version %r for %s' % (version, name))
        return version

    def bundle_path(self, name, version=None):
        """Resolve the store path without checking it or creating directories."""
        version = self.resolve_version(name, version)
        parent = Path(self._root()) / name
        if path_present(parent):
            _directory(parent)
        return parent / version

    def inspect(self, name, version=None, *, verify_files=True):
        version = self.resolve_version(name, version)
        return inspect_bundle(self.bundle_path(name, version), self._datasets[name]['versions'][version],
                              verify_files=verify_files)

    def download(self, name, version=None, *, force=False, verify_files=True, **download_options):
        """Explicitly install/repair and return a mapping of asset snapshot paths."""
        version = self.resolve_version(name, version)
        return install_bundle(self.bundle_path(name, version), self._datasets[name]['versions'][version],
                              force=force, verified=self.verified, verify_files=verify_files,
                              download_options=download_options)

    def local_path(self, name, version=None, *, asset=None, verify_files=True):
        """Resolve an installed snapshot; no writes/network. Missing raises.

        For one asset, return its Path; for multiple assets return the generation
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
        return Path(inspected.generation)

    def ensure(self, name, version=None, **download_options):
        """Download/reuse, then return what local_path would: the single asset's
        Path, or the generation directory of several assets."""
        paths = self.download(name, version, **download_options)
        if len(paths) == 1:
            return Path(next(iter(paths.values())))
        # The paths download validated: no second inspection, so a concurrent
        # refresh cannot swap the generation between the two.
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
