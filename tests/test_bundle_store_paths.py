"""Consumer-selected paths use the ordinary owned bundle transaction."""

from hashlib import sha256
import os
from pathlib import Path

import pytest

from datacache import FileValidationError, VersionedDatasetRegistry
from datacache import bundles, download

pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX bundle installation')


@pytest.fixture
def datasets(tmp_path):
    source = tmp_path / 'source.fa'
    payload = b'>reference\nACGT\n'
    source.write_bytes(payload)
    assets = {'records.fa': dict(url=source.as_uri(), sha256=sha256(payload).hexdigest(), size=len(payload))}
    return {'reference': dict(default_version='110', versions={'110': assets, '109': assets})}


def test_custom_path_resolution_is_pure_and_uses_concrete_versions(tmp_path, datasets, monkeypatch):
    calls = []

    def store_path(name, version):
        calls.append((name, version))
        return str(tmp_path / 'GRCh38' / ('ensembl-' + version) / 'sources' / name)

    registry = VersionedDatasetRegistry(datasets, store_path=store_path)
    assert calls == []

    def forbidden(*args, **kwargs):
        raise AssertionError('path resolution inspected or mutated the filesystem')

    monkeypatch.setattr(bundles, 'path_present', forbidden)
    monkeypatch.setattr(Path, 'mkdir', forbidden)
    assert registry.bundle_path('reference') == tmp_path / 'GRCh38/ensembl-110/sources/reference'
    # Every version's store is resolved once, on first use, and then reused.
    assert sorted(calls) == [('reference', '109'), ('reference', '110')]
    assert registry.bundle_path('reference', '109') == tmp_path / 'GRCh38/ensembl-109/sources/reference'
    with pytest.raises(ValueError):
        registry.bundle_path('reference', 'latest')
    assert len(calls) == 2
    assert not (tmp_path / 'GRCh38').exists()


def test_custom_layout_install_versions_refresh_and_read_only_reuse(tmp_path, datasets, monkeypatch):
    root = tmp_path / 'application'
    index = root / 'GRCh38' / 'ensembl-110' / 'index.sqlite'
    index.parent.mkdir(parents=True)
    index.write_bytes(b'application-owned derived index')
    registry = VersionedDatasetRegistry(
        datasets, store_path=lambda name, version: root / 'GRCh38' / ('ensembl-' + version) / 'sources' / name)
    assert registry.inspect('reference').status == 'missing'
    assert not registry.bundle_path('reference').exists()
    current = registry.download('reference')
    older = registry.download('reference', '109')
    assert current != older
    assert registry.local_path('reference') == Path(current['records.fa'])
    assert registry.local_path('reference', '109') == Path(older['records.fa'])
    refreshed = registry.download('reference', force=True)
    assert refreshed != current
    assert Path(current['records.fa']).read_bytes() == Path(refreshed['records.fa']).read_bytes()
    assert index.read_bytes() == b'application-owned derived index'
    store = registry.bundle_path('reference')
    entries = [store, *store.rglob('*')]
    modes = {path: path.stat().st_mode & 0o777 for path in entries}

    def forbidden(*args, **kwargs):
        raise AssertionError('offline reuse attempted a write or acquisition')

    monkeypatch.setattr(download, 'fetch_file', forbidden)
    monkeypatch.setattr(bundles, 'file_lock', forbidden)
    monkeypatch.setattr(bundles, 'write_json', forbidden)
    try:
        for path in entries:
            path.chmod(0o555 if path.is_dir() else 0o444)
        assert registry.download('reference') == refreshed
        assert registry.local_path('reference') == Path(refreshed['records.fa'])
        assert registry.is_cached('reference')
        assert registry.inspect('reference').verified
    finally:
        for path, mode in modes.items():
            path.chmod(mode)


def test_custom_layout_recovers_completed_generation_without_network(tmp_path, datasets, monkeypatch):
    registry = VersionedDatasetRegistry(
        datasets, store_path=lambda name, version: tmp_path / ('sources-' + version) / name)
    original = bundles.write_json

    def interrupted(path, value, **kwargs):
        if Path(path).name == bundles.CURRENT:
            raise KeyboardInterrupt('pointer publication interrupted')
        return original(path, value, **kwargs)

    monkeypatch.setattr(bundles, 'write_json', interrupted)
    with pytest.raises(KeyboardInterrupt):
        registry.download('reference')
    state = registry.inspect('reference')
    assert state.status == 'recovery-required'
    monkeypatch.setattr(bundles, 'write_json', original)

    def forbidden(*args, **kwargs):
        raise AssertionError('recovery attempted acquisition')

    monkeypatch.setattr(download, 'fetch_file', forbidden)
    paths = registry.download('reference')
    assert registry.inspect('reference').verified
    assert registry.local_path('reference') == Path(paths['records.fa'])


@pytest.mark.parametrize('force', [False, True])
def test_custom_paths_never_take_over_foreign_directories(tmp_path, datasets, force):
    foreign = tmp_path / 'foreign'
    foreign.mkdir()
    precious = foreign / 'index.sqlite'
    precious.write_bytes(b'keep me')
    registry = VersionedDatasetRegistry(
        datasets, store_path=lambda name, version: foreign if version == '110' else tmp_path / version)
    with pytest.raises(FileValidationError, match='never taken over'):
        registry.download('reference', force=force)
    assert list(foreign.iterdir()) == [precious]
    assert precious.read_bytes() == b'keep me'


def test_custom_paths_reject_symlinked_stores(tmp_path, datasets):
    foreign = tmp_path / 'foreign'
    foreign.mkdir()
    store = tmp_path / 'link'
    store.symlink_to(foreign, target_is_directory=True)
    registry = VersionedDatasetRegistry(
        datasets, store_path=lambda name, version: store if version == '110' else tmp_path / version)
    assert registry.inspect('reference').status == 'invalid'
    with pytest.raises(FileValidationError):
        registry.download('reference', force=True)
    assert list(foreign.iterdir()) == []


@pytest.mark.parametrize('options', [{}, {'cache_root': '/cache', 'cache_dir': lambda: '/cache'},
    {'cache_root': '/cache', 'store_path': lambda name, version: '/store'},
    {'cache_dir': lambda: '/cache', 'store_path': lambda name, version: '/store'},
    {'cache_root': '/cache', 'cache_dir': lambda: '/cache', 'store_path': lambda name, version: '/store'}])
def test_destination_strategies_are_mutually_exclusive(datasets, options):
    with pytest.raises(ValueError, match='exactly one'):
        VersionedDatasetRegistry(datasets, **options)


@pytest.mark.parametrize('key', ['cache_dir', 'store_path'])
def test_path_callbacks_must_be_callable(datasets, key):
    with pytest.raises(ValueError, match=key + ' must be callable'):
        VersionedDatasetRegistry(datasets, **{key: '/not-a-callback'})


def test_existing_root_strategies_remain_compatible(tmp_path, datasets):
    explicit = VersionedDatasetRegistry(datasets, cache_root=tmp_path / 'root')
    selected = [tmp_path / 'root']
    dynamic = VersionedDatasetRegistry(datasets, cache_dir=lambda: selected[0])
    assert explicit.bundle_path('reference') == dynamic.bundle_path('reference')
    selected[0] = tmp_path / 'different-root'
    assert dynamic.bundle_path('reference') == tmp_path / 'different-root/reference/110'
    assert not selected[0].exists()


def test_versions_sharing_a_custom_store_are_rejected(tmp_path, datasets):
    registry = VersionedDatasetRegistry(datasets, store_path=lambda name, version: tmp_path / name)
    with pytest.raises(ValueError, match='each version needs its own store'):
        registry.bundle_path('reference')
    assert not (tmp_path / 'reference').exists()


@pytest.mark.parametrize('result', [None, '', 7, b'/bytes'])
def test_custom_store_paths_must_be_paths(datasets, result):
    registry = VersionedDatasetRegistry(datasets, store_path=lambda name, version: result)
    with pytest.raises(ValueError, match='not a path'):
        registry.bundle_path('reference')


def test_custom_store_parent_may_be_a_link(tmp_path, datasets):
    disk = tmp_path / 'disk'
    disk.mkdir()
    (tmp_path / 'data').symlink_to(disk, target_is_directory=True)
    registry = VersionedDatasetRegistry(
        datasets, store_path=lambda name, version: tmp_path / 'data' / version / name)
    paths = registry.download('reference')
    assert Path(paths['records.fa']).resolve().is_relative_to(disk)
