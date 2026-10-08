"""Fast bundle reads retain snapshot, ownership and explicit trust boundaries."""

import builtins
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import threading

import pytest

from datacache import FileValidationError, VersionedDatasetRegistry, inspect_bundle, install_bundle
from datacache import bundle_store, bundles, download

pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX bundle installation')


@pytest.fixture
def installed(tmp_path):
    upstream = tmp_path / 'upstream'
    upstream.mkdir()
    assets = {}
    for name, payload in [('records.txt', b'records'), ('nested/metadata.txt', b'metadata')]:
        source = upstream / name
        source.parent.mkdir(exist_ok=True)
        source.write_bytes(payload)
        assets[name] = dict(url=source.as_uri(), sha256=sha256(payload).hexdigest(), size=len(payload))
    registry = VersionedDatasetRegistry(
        {'reference': dict(default_version='v1', versions={'v1': assets})}, cache_root=tmp_path / 'cache')
    paths = registry.download('reference')
    return registry, assets, paths


@pytest.mark.parametrize('trusted', [True, False])
def test_fast_resolution_and_cache_hits_read_no_payloads_or_write(installed, monkeypatch, trusted):
    registry, assets, paths = installed
    if not trusted:
        assets = {name: {'url': spec['url']} for name, spec in assets.items()}
        registry = VersionedDatasetRegistry(
            {'reference': dict(default_version='v1', versions={'v1': assets})},
            cache_root=registry.store_path('reference').parents[1], verified=False)
    payloads = set(map(Path, paths.values()))
    original_open, original_os_open, original_read = builtins.open, os.open, os.read
    payload_fds = set()

    def guarded_open(path, *args, **kwargs):
        if isinstance(path, (str, os.PathLike)) and Path(path) in payloads:
            raise AssertionError('fast lookup opened a payload for content reading')
        return original_open(path, *args, **kwargs)

    def tracked_os_open(path, *args, **kwargs):
        # Opening a payload to check its type and readability is allowed;
        # reading from it is not.
        fd = original_os_open(path, *args, **kwargs)
        if isinstance(path, (str, os.PathLike)) and Path(path) in payloads:
            payload_fds.add(fd)
        return fd

    def guarded_read(fd, *args, **kwargs):
        if fd in payload_fds:
            raise AssertionError('fast lookup read payload bytes')
        return original_read(fd, *args, **kwargs)

    def forbidden(*args, **kwargs):
        raise AssertionError('fast lookup tried full inspection, mutation or acquisition')

    monkeypatch.setattr(builtins, 'open', guarded_open)
    monkeypatch.setattr(io, 'open', guarded_open)
    monkeypatch.setattr(os, 'open', tracked_os_open)
    monkeypatch.setattr(os, 'read', guarded_read)
    hash_file = bundles.hash_file

    def sizes_only(*args, **kwargs):
        if kwargs.get('hash_contents', True):
            raise AssertionError('fast lookup tried full inspection')
        return hash_file(*args, **kwargs)

    monkeypatch.setattr(bundles, 'hash_file', sizes_only)
    monkeypatch.setattr(bundle_store.BundleStore, 'lock', forbidden)
    monkeypatch.setattr(bundles, 'write_json', forbidden)
    monkeypatch.setattr(bundle_store, 'write_json', forbidden)
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    monkeypatch.setattr(Path, 'mkdir', forbidden)
    state = registry.inspect('reference', verify_files=False)
    assert state.status == 'available' and not state.verified
    assert {name: item.path for name, item in state.files.items()} == paths
    assert all(not item.verified and item.size is not None for item in state.files.values())
    assert registry.local_path('reference', verify_files=False) == Path(state.bundle)
    assert registry.local_path(
        'reference', asset='records.txt', verify_files=False) == Path(paths['records.txt'])
    assert registry.is_cached('reference', verify_files=False)
    assert not registry.status(verify_files=False)[0]['inspection'].verified
    assert registry.download('reference', verify_files=False) == paths
    assert registry.ensure('reference', verify_files=False) == Path(state.bundle)
    assert inspect_bundle(registry.store_path('reference'), verify_files=False).status == 'available'
    with pytest.raises(AssertionError, match='full inspection'):
        registry.inspect('reference')


def test_defaults_agree_on_same_size_corruption_that_only_fast_checks_miss(installed):
    registry, assets, paths = installed
    path = Path(paths['records.txt'])
    path.write_bytes(b'changed')
    assert len(b'changed') == assets['records.txt']['size']
    # Every lookup verifies by default, so they agree the bundle is invalid.
    assert registry.inspect('reference').status == 'invalid'
    assert not registry.is_cached('reference')
    assert registry.status()[0]['inspection'].status == 'invalid'
    with pytest.raises(FileValidationError):
        registry.local_path('reference')
    with pytest.raises(FileValidationError, match='force=True'):
        registry.download('reference')
    with pytest.raises(FileValidationError, match='force=True'):
        registry.ensure('reference')
    # Opting into metadata-only checks trades that detection for speed.
    assert registry.inspect('reference', verify_files=False).status == 'available'
    assert registry.is_cached('reference', verify_files=False)
    assert registry.download('reference', verify_files=False) == paths


def test_fast_resolution_on_a_read_only_store(installed):
    registry, assets, paths = installed
    store = registry.store_path('reference')
    entries = [store, *store.rglob('*')]
    modes = {path: path.stat().st_mode & 0o777 for path in entries}
    try:
        for path in entries:
            path.chmod(0o555 if path.is_dir() else 0o444)
        state = registry.inspect('reference', verify_files=False)
        assert state.status == 'available' and not state.verified
        assert registry.local_path('reference', verify_files=False) == Path(state.bundle)
        assert registry.is_cached('reference', verify_files=False)
        assert registry.status(verify_files=False)[0]['inspection'].status == 'available'
        assert registry.download('reference', verify_files=False) == paths
    finally:
        for path, mode in modes.items():
            path.chmod(mode)


@pytest.mark.parametrize('damage', ['missing', 'size', 'symlink', 'hardlink', 'fifo',
                                  'parent-link', 'newest-link', 'manifest', 'inventory', 'expectations'])
def test_fast_lookup_rejects_invalid_metadata_and_unsafe_files(installed, tmp_path, damage):
    registry, assets, paths = installed
    store = registry.store_path('reference')
    target = Path(paths['records.txt'])
    bundle = target.parent
    manifest = bundle / bundles.MANIFEST
    if damage == 'missing':
        target.unlink()
    elif damage == 'size':
        target.write_bytes(b'short')
    elif damage == 'symlink':
        target.unlink()
        target.symlink_to(tmp_path / 'upstream/records.txt')
    elif damage == 'hardlink':
        os.link(target, tmp_path / 'hardlink')
    elif damage == 'fifo':
        target.unlink()
        os.mkfifo(target)
    elif damage == 'parent-link':
        nested = bundle / 'nested'
        nested.rename(bundle / 'original-nested')
        nested.symlink_to(bundle / 'original-nested', target_is_directory=True)
    elif damage == 'newest-link':
        (store / 'bundles' / '2999-01-01T00-00-00Z').symlink_to(tmp_path / 'upstream', target_is_directory=True)
    elif damage == 'manifest':
        manifest.write_text('{')
    elif damage == 'inventory':
        receipt = json.loads(manifest.read_text())
        del receipt['assets']['records.txt']
        manifest.write_text(json.dumps(receipt))
    else:
        assets['records.txt']['sha256'] = '0' * 64
    assert inspect_bundle(store, assets, verify_files=False).status == 'invalid'


def test_fast_lookup_retains_unverified_source_identity_checks(installed):
    registry, assets, paths = installed
    untrusted = {name: dict(url=spec['url']) for name, spec in assets.items()}
    untrusted['records.txt']['url'] = 'https://different.invalid/records.txt'
    assert inspect_bundle(registry.store_path('reference'), untrusted, verify_files=False).status == 'invalid'


def test_fast_lookup_reports_permission_errors(installed, monkeypatch):
    registry, assets, paths = installed
    original = bundle_store.open_regular

    def denied(path, *args, **kwargs):
        if str(path) == paths['records.txt']:
            raise PermissionError('payload is not readable')
        return original(path, *args, **kwargs)

    monkeypatch.setattr(bundle_store, 'open_regular', denied)
    state = registry.inspect('reference', verify_files=False)
    assert state.status == 'inaccessible'
    assert isinstance(state.error, PermissionError)


def test_fast_install_flag_never_weakens_new_bundle_validation(installed, monkeypatch):
    registry, assets, paths = installed
    store = registry.store_path('reference')
    old_bundle = inspect_bundle(store).bundle

    def bad_download(url, *, destination, **kwargs):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b'x' * kwargs['expected_size'])
        return str(destination)

    monkeypatch.setattr(download, 'fetch_file', bad_download)
    with pytest.raises(FileValidationError, match='SHA-256'):
        registry.download('reference', force=True, verify_files=False)
    assert inspect_bundle(store).bundle == old_bundle
    assert registry.local_path('reference', asset='records.txt') == Path(paths['records.txt'])


def test_fast_readers_resolve_one_bundle_during_refresh(installed, monkeypatch):
    registry, assets, paths = installed
    entered, release = threading.Event(), threading.Event()
    replace = os.replace
    bundles_directory = registry.store_path('reference') / 'bundles'

    def paused(source, target):
        if Path(target).parent == bundles_directory:
            entered.set()
            assert release.wait(5)
        return replace(source, target)

    monkeypatch.setattr(os, 'replace', paused)
    with ThreadPoolExecutor() as pool:
        future = pool.submit(registry.download, 'reference', force=True)
        assert entered.wait(5)
        try:
            state = registry.inspect('reference', verify_files=False)
            assert {name: item.path for name, item in state.files.items()} == paths
        finally:
            release.set()
        refreshed = future.result()
    state = registry.inspect('reference', verify_files=False)
    assert {name: item.path for name, item in state.files.items()} == refreshed
    assert all(Path(path).exists() for path in paths.values())


@pytest.mark.parametrize('flag', [None, 0, 'false'])
def test_nonboolean_verification_modes_fail_without_creating_paths(tmp_path, flag):
    dest = tmp_path / 'missing'
    with pytest.raises(ValueError, match='verify_files'):
        inspect_bundle(dest, verify_files=flag)
    with pytest.raises(ValueError, match='verify_files'):
        install_bundle(dest, {'file': {'url': 'https://example.invalid/data'}},
                       verified=False, verify_files=flag)
    assert not dest.exists()


def test_ensure_returns_the_downloaded_snapshot_without_inspecting_again(installed, monkeypatch):
    registry, assets, paths = installed
    bundle = Path(paths['records.txt']).parent
    monkeypatch.setattr(registry, 'inspect', lambda *args, **kwargs: pytest.fail('inspected again'))
    monkeypatch.setattr(bundles, 'install_bundle', lambda *args, **kwargs: paths)
    assert registry.ensure('reference') == bundle
    single = {'records.txt': paths['records.txt']}
    monkeypatch.setattr(bundles, 'install_bundle', lambda *args, **kwargs: single)
    assert registry.ensure('reference') == Path(paths['records.txt'])
