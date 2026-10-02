"""Fast bundle reads retain snapshot, ownership and explicit trust boundaries."""

import builtins
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import json
import os
from pathlib import Path
import threading

import pytest

from datacache import FileValidationError, VersionedDatasetRegistry, inspect_bundle, install_bundle
from datacache import bundles, download

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
            cache_root=registry.bundle_path('reference').parents[1], verified=False)
    payloads = set(map(Path, paths.values()))
    original_open = builtins.open

    def guarded_open(path, *args, **kwargs):
        if isinstance(path, (str, os.PathLike)) and Path(path) in payloads:
            raise AssertionError('fast lookup opened a payload for content reading')
        return original_open(path, *args, **kwargs)

    def forbidden(*args, **kwargs):
        raise AssertionError('fast lookup tried full inspection, mutation or acquisition')

    monkeypatch.setattr(builtins, 'open', guarded_open)
    monkeypatch.setattr(bundles, 'inspect_file', forbidden)
    monkeypatch.setattr(bundles, 'file_lock', forbidden)
    monkeypatch.setattr(bundles, 'write_json', forbidden)
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    monkeypatch.setattr(Path, 'mkdir', forbidden)
    state = registry.inspect('reference', verify_files=False)
    assert state.status == 'available' and not state.verified
    assert {name: item.path for name, item in state.files.items()} == paths
    assert all(not item.verified and item.size is not None for item in state.files.values())
    assert registry.local_path('reference') == Path(state.generation)
    assert registry.local_path('reference', asset='records.txt') == Path(paths['records.txt'])
    assert registry.is_cached('reference')
    assert not registry.status()[0]['inspection'].verified
    assert registry.download('reference', verify_files=False) == paths
    assert registry.ensure('reference', verify_files=False) == Path(state.generation)
    assert inspect_bundle(registry.bundle_path('reference'), verify_files=False).status == 'available'
    with pytest.raises(AssertionError, match='full inspection'):
        registry.inspect('reference')


def test_fast_lookup_accepts_same_size_corruption_but_full_checks_reject(installed):
    registry, assets, paths = installed
    path = Path(paths['records.txt'])
    path.write_bytes(b'changed')
    assert len(b'changed') == assets['records.txt']['size']
    assert registry.is_cached('reference')
    assert not registry.is_cached('reference', verify_files=True)
    assert registry.inspect('reference', verify_files=False).status == 'available'
    assert registry.inspect('reference').status == 'invalid'
    assert registry.download('reference', verify_files=False) == paths
    with pytest.raises(FileValidationError, match='force=True'):
        registry.download('reference')
    with pytest.raises(FileValidationError):
        registry.local_path('reference', verify_files=True)


def test_fast_resolution_on_a_read_only_store(installed):
    registry, assets, paths = installed
    store = registry.bundle_path('reference')
    entries = [store, *store.rglob('*')]
    modes = {path: path.stat().st_mode & 0o777 for path in entries}
    try:
        for path in entries:
            path.chmod(0o555 if path.is_dir() else 0o444)
        state = registry.inspect('reference', verify_files=False)
        assert state.status == 'available' and not state.verified
        assert registry.local_path('reference') == Path(state.generation)
        assert registry.is_cached('reference')
        assert registry.status()[0]['inspection'].status == 'available'
        assert registry.download('reference', verify_files=False) == paths
    finally:
        for path, mode in modes.items():
            path.chmod(mode)


@pytest.mark.parametrize('damage', ['missing', 'size', 'symlink', 'hardlink', 'fifo',
                                  'parent-link', 'pointer', 'manifest', 'inventory', 'expectations'])
def test_fast_lookup_rejects_invalid_metadata_and_unsafe_files(installed, tmp_path, damage):
    registry, assets, paths = installed
    store = registry.bundle_path('reference')
    target = Path(paths['records.txt'])
    generation = target.parent
    manifest = generation / bundles.MANIFEST
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
        nested = generation / 'nested'
        nested.rename(generation / 'original-nested')
        nested.symlink_to(generation / 'original-nested', target_is_directory=True)
    elif damage == 'pointer':
        (store / bundles.CURRENT).write_text(json.dumps({'generation': '../outside'}))
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
    assert inspect_bundle(registry.bundle_path('reference'), untrusted, verify_files=False).status == 'invalid'


def test_fast_lookup_reports_permission_errors(installed, monkeypatch):
    registry, assets, paths = installed
    original = bundles.open_regular

    def denied(path, *args, **kwargs):
        if str(path) == paths['records.txt']:
            raise PermissionError('payload is not readable')
        return original(path, *args, **kwargs)

    monkeypatch.setattr(bundles, 'open_regular', denied)
    state = registry.inspect('reference', verify_files=False)
    assert state.status == 'inaccessible'
    assert isinstance(state.error, PermissionError)


def test_fast_install_flag_never_weakens_new_generation_validation(installed, monkeypatch):
    registry, assets, paths = installed
    store = registry.bundle_path('reference')
    pointer = (store / bundles.CURRENT).read_bytes()

    def bad_download(url, *, destination, **kwargs):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b'x' * kwargs['expected_size'])
        return str(destination)

    monkeypatch.setattr(download, 'fetch_file', bad_download)
    with pytest.raises(FileValidationError, match='SHA-256'):
        registry.download('reference', force=True, verify_files=False)
    assert (store / bundles.CURRENT).read_bytes() == pointer
    assert registry.local_path('reference', asset='records.txt') == Path(paths['records.txt'])


def test_fast_install_flag_never_weakens_recovery_validation(installed, monkeypatch):
    registry, assets, paths = installed
    store = registry.bundle_path('reference')
    (store / bundles.CURRENT).unlink()
    Path(paths['records.txt']).write_bytes(b'changed')
    assert registry.inspect('reference', verify_files=False).status == 'recovery-required'

    def forbidden(*args, **kwargs):
        raise RuntimeError('recovery rejected corrupted bytes and attempted acquisition')

    monkeypatch.setattr(download, 'fetch_file', forbidden)
    with pytest.raises(RuntimeError, match='recovery rejected'):
        registry.download('reference', verify_files=False)
    assert not (store / bundles.CURRENT).exists()


def test_fast_readers_resolve_one_generation_during_refresh(installed, monkeypatch):
    registry, assets, paths = installed
    entered, release = threading.Event(), threading.Event()
    original = bundles.write_json

    def paused(path, value, **kwargs):
        if Path(path).name == bundles.CURRENT:
            entered.set()
            assert release.wait(5)
        return original(path, value, **kwargs)

    monkeypatch.setattr(bundles, 'write_json', paused)
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
