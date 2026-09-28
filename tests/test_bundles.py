"""Atomic multi-file installs with tiny offline fixtures and failure injection."""

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import json
import os
from pathlib import Path
import threading

import pytest

from datacache import (
    FileValidationError, VersionedDatasetRegistry, inspect_bundle, install_bundle,
)
from datacache import bundles, download

pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX bundle locks')


@pytest.fixture
def assets(tmp_path):
    root = tmp_path / 'upstream'
    root.mkdir()
    result = {}
    for name, data in [('records.json', b'{"sequence":"ACGT"}\n'),
                       ('release/manifest.json', b'{"release":"2026-09"}\n')]:
        path = root / name
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(data)
        result[name] = dict(url=path.as_uri(), size=len(data), sha256=sha256(data).hexdigest())
    return result


def test_install_inspect_offline_and_generated_outputs_survive(tmp_path, assets, monkeypatch):
    destination = tmp_path / 'source' / '2026-09'
    outputs = tmp_path / 'generated'
    outputs.mkdir()
    (outputs / 'index.sqlite').write_bytes(b'derived data')
    paths = install_bundle(destination, assets)
    inspected = inspect_bundle(destination, assets)
    assert inspected.status == 'available' and inspected.verified
    assert set(paths) == set(assets)
    assert all(Path(path).is_file() for path in paths.values())
    assert inspect_bundle(destination).status == 'available'
    assert not inspect_bundle(destination).verified
    old = {name: Path(path).read_bytes() for name, path in paths.items()}
    refreshed = install_bundle(destination, assets, force=True)
    assert paths != refreshed
    assert {name: Path(path).read_bytes() for name, path in paths.items()} == old
    assert (outputs / 'index.sqlite').read_bytes() == b'derived data'

    def forbidden(*args, **kwargs):
        raise AssertionError('read-only reuse attempted a mutation or download')
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    monkeypatch.setattr(bundles, 'file_lock', forbidden)
    monkeypatch.setattr(bundles, 'write_json', forbidden)
    monkeypatch.setattr(Path, 'mkdir', forbidden)
    assert install_bundle(destination, assets) == refreshed
    assert inspect_bundle(destination, assets).verified
    assert inspect_bundle(destination.parent / 'absent', assets).status == 'missing'


@pytest.mark.parametrize('failure', ['last-download', 'hash', 'publication'])
def test_failed_refresh_preserves_old_generation(tmp_path, assets, monkeypatch, failure):
    destination = tmp_path / 'data'
    paths = install_bundle(destination, assets)
    old_pointer = (destination / bundles.CURRENT).read_bytes()
    replacement = {k: dict(v) for k, v in assets.items()}
    if failure == 'last-download':
        replacement['release/manifest.json']['url'] = (tmp_path / 'missing').as_uri()
    elif failure == 'hash':
        replacement['release/manifest.json']['sha256'] = '0' * 64
    else:
        original = bundles.write_json
        def fail(path, value, **kwargs):
            if Path(path).name == bundles.CURRENT:
                raise OSError('publication failed')
            return original(path, value, **kwargs)
        monkeypatch.setattr(bundles, 'write_json', fail)
    with pytest.raises((OSError, FileValidationError)):
        install_bundle(destination, replacement, force=True)
    assert (destination / bundles.CURRENT).read_bytes() == old_pointer
    assert inspect_bundle(destination, assets).verified
    assert all(Path(path).exists() for path in paths.values())


def test_interrupted_first_publish_recovers_without_network(tmp_path, assets, monkeypatch):
    destination = tmp_path / 'data'
    original = bundles.write_json
    def fail(path, value, **kwargs):
        if Path(path).name == bundles.CURRENT:
            raise KeyboardInterrupt
        return original(path, value, **kwargs)
    monkeypatch.setattr(bundles, 'write_json', fail)
    with pytest.raises(KeyboardInterrupt):
        install_bundle(destination, assets)
    assert inspect_bundle(destination, assets).status == 'recovery-required'
    monkeypatch.setattr(bundles, 'write_json', original)
    def forbidden(*args, **kwargs):
        raise AssertionError('recovery attempted network')
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    paths = install_bundle(destination, assets)
    assert inspect_bundle(destination, assets).verified
    assert all(Path(path).exists() for path in paths.values())


def test_concurrent_first_and_forced_installs(tmp_path, assets):
    destination = tmp_path / 'data'
    barrier = threading.Barrier(4)
    def install(_):
        barrier.wait(timeout=5)
        return install_bundle(destination, assets)
    with ThreadPoolExecutor(max_workers=4) as pool:
        initial = list(pool.map(install, range(4)))
    assert all(value == initial[0] for value in initial)
    with ThreadPoolExecutor(max_workers=3) as pool:
        forced = list(pool.map(lambda _: install_bundle(destination, assets, force=True), range(3)))
    assert len({tuple(p.values()) for p in forced}) == 3
    assert all(Path(path).exists() for paths in forced + initial for path in paths.values())
    assert inspect_bundle(destination, assets).verified


def test_managed_reader_sees_complete_old_generation_during_publish(tmp_path, assets, monkeypatch):
    destination = tmp_path / 'data'
    old = install_bundle(destination, assets)
    entered, release = threading.Event(), threading.Event()
    original = bundles.write_json
    def paused(path, value, **kwargs):
        if Path(path).name == bundles.CURRENT:
            entered.set()
            assert release.wait(5)
        return original(path, value, **kwargs)
    monkeypatch.setattr(bundles, 'write_json', paused)
    with ThreadPoolExecutor() as pool:
        future = pool.submit(install_bundle, destination, assets, force=True)
        assert entered.wait(5)
        try:
            inspected = inspect_bundle(destination, assets)
            assert inspected.verified
            assert {name: value.path for name, value in inspected.files.items()} == old
        finally:
            release.set()
        assert future.result() != old
    assert all(Path(path).exists() for path in old.values())


@pytest.mark.parametrize('name', ['/absolute', '../escape', 'a/../escape', 'a//b', 'C:/file',
                                  'a\\b', '.datacache-manifest.json', 'CON', 'a/', 'a\x00b'])
def test_unsafe_asset_names_rejected_without_creating_root(tmp_path, assets, name):
    dest = tmp_path / 'missing' / 'bundle'
    with pytest.raises(ValueError):
        install_bundle(dest, {name: next(iter(assets.values()))})
    assert not dest.parent.exists()


@pytest.mark.parametrize('names', [('a', 'a/b'), ('A', 'a'), ('A', 'a/b')])
def test_colliding_asset_paths_are_rejected(tmp_path, assets, names):
    with pytest.raises(ValueError):
        install_bundle(tmp_path / 'bundle', {name: next(iter(assets.values())) for name in names})


def test_foreign_directory_is_never_replaced_even_with_force(tmp_path, assets):
    dest = tmp_path / 'foreign'
    dest.mkdir()
    precious = dest / 'precious.txt'
    precious.write_text('keep me')
    with pytest.raises((FileValidationError, FileNotFoundError)):
        install_bundle(dest, assets, force=True)
    assert precious.read_text() == 'keep me'


def test_integrity_mismatch_requires_explicit_repair(tmp_path, assets):
    dest = tmp_path / 'data'
    paths = install_bundle(dest, assets)
    Path(paths['records.json']).write_bytes(b'corrupt')
    assert inspect_bundle(dest, assets).status == 'invalid'
    with pytest.raises(FileValidationError, match='force=True'):
        install_bundle(dest, assets)
    assert install_bundle(dest, assets, force=True) != paths
    assert inspect_bundle(dest, assets).verified


def test_missing_manifest_and_symlinks_cannot_escape_store(tmp_path, assets):
    dest = tmp_path / 'data'
    paths = install_bundle(dest, assets)
    inspected = inspect_bundle(dest, assets)
    target = Path(paths['records.json'])
    target.unlink()
    target.symlink_to(Path(assets['records.json']['url'][7:]))
    assert inspect_bundle(dest, assets).status == 'invalid'
    manifest = Path(inspected.generation) / bundles.MANIFEST
    manifest.unlink()
    assert inspect_bundle(dest).status == 'invalid'
    (dest / bundles.CURRENT).write_text(json.dumps({'generation': '../../upstream'}))
    assert inspect_bundle(dest).status == 'invalid'


def test_permission_error_is_inaccessible_without_writes(tmp_path, assets, monkeypatch):
    dest = tmp_path / 'data'
    install_bundle(dest, assets)
    def forbidden(*args, **kwargs):
        raise PermissionError('unreadable')
    monkeypatch.setattr(bundles, 'read_json', forbidden)
    assert inspect_bundle(dest, assets).status == 'inaccessible'
    with pytest.raises(PermissionError):
        install_bundle(dest, assets)


def test_registry_versions_are_independent_and_pinned(tmp_path, assets):
    reg = VersionedDatasetRegistry({'example': dict(default_version='2026-09',
        versions={'2026-09': assets, '2025-01': {'records.json': assets['records.json']}})},
        cache_root=tmp_path / 'cache')
    assert reg.resolve_version('example') == '2026-09'
    assert reg.inspect('example').status == 'missing'
    assert not reg.bundle_path('example').parent.exists()
    reg.download('example')
    assert reg.is_cached('example')
    assert reg.inspect('example', '2025-01').status == 'missing'
    assert reg.local_path('example', asset='records.json').read_bytes().startswith(b'{')
    reg.download('example', '2025-01')
    assert reg.local_path('example', '2025-01').is_file()
    assert reg.status()[0]['inspection'].verified
    with pytest.raises(ValueError):
        reg.resolve_version('example', 'latest')


def test_hitlist_registry_shape_and_explicit_unverified_mode(tmp_path, assets):
    mapping = {'example': {'filename': 'data.json', 'default_version': 'v1',
                          'urls': {'v1': assets['records.json']['url']}}}
    with pytest.raises(ValueError, match='sha256'):
        VersionedDatasetRegistry(mapping, cache_root=tmp_path / 'cache')
    reg = VersionedDatasetRegistry(mapping, cache_dir=lambda: tmp_path / 'cache', verified=False)
    path = reg.ensure('example')
    assert path.is_file()
    assert reg.inspect('example').status == 'available'
    assert not reg.inspect('example').verified
    assert not (tmp_path / 'cache' / 'manifest.json').exists()


def test_parent_permission_error_is_not_missing(tmp_path, assets, monkeypatch):
    dest = tmp_path / 'unreadable' / 'data'
    original = os.lstat
    def denied(path, *args, **kwargs):
        if Path(path) == dest:
            raise PermissionError('parent not searchable')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(os, 'lstat', denied)
    assert inspect_bundle(dest, assets).status == 'inaccessible'
    with pytest.raises(PermissionError):
        install_bundle(dest, assets)


def test_separate_processes_converge_on_one_generation(tmp_path, assets):
    import subprocess
    import sys
    dest = tmp_path / 'data'
    script = ('import json, sys; from datacache import install_bundle; '
              'print(json.dumps(install_bundle(sys.argv[1], json.loads(sys.argv[2])), sort_keys=True))')
    processes = [subprocess.Popen([sys.executable, '-c', script, str(dest), json.dumps(assets)],
                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(3)]
    results = [process.communicate(timeout=10) for process in processes]
    assert all(process.returncode == 0 for process in processes), results
    assert len({stdout for stdout, stderr in results}) == 1
    assert len(list((dest / 'generations').iterdir())) == 1


def test_single_manifest_tampering_does_not_override_trusted_registry(tmp_path, assets):
    dest = tmp_path / 'data'
    paths = install_bundle(dest, assets)
    generation = Path(inspect_bundle(dest).generation)
    Path(paths['records.json']).write_bytes(b'forged')
    manifest = generation / bundles.MANIFEST
    receipt = json.loads(manifest.read_text())
    receipt['assets']['records.json'].update(sha256=sha256(b'forged').hexdigest(), size=6)
    manifest.write_text(json.dumps(receipt))
    assert inspect_bundle(dest).status == 'available'
    assert not inspect_bundle(dest).verified
    assert inspect_bundle(dest, assets).status == 'invalid'


def test_registry_rejects_symlinked_dataset_directory(tmp_path, assets):
    outside = tmp_path / 'outside'
    outside.mkdir()
    root = tmp_path / 'cache'
    root.mkdir()
    (root / 'example').symlink_to(outside)
    reg = VersionedDatasetRegistry({'example': dict(default_version='v1', versions={'v1': assets})}, cache_root=root)
    with pytest.raises(FileValidationError):
        reg.download('example')
    assert list(outside.iterdir()) == []
