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
from datacache import bundle_store, bundles, download

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
    monkeypatch.setattr(bundle_store.BundleStore, 'lock', forbidden)
    monkeypatch.setattr(bundles, 'write_json', forbidden)
    monkeypatch.setattr(bundle_store, 'write_json', forbidden)
    monkeypatch.setattr(Path, 'mkdir', forbidden)
    assert install_bundle(destination, assets) == refreshed
    assert inspect_bundle(destination, assets).verified
    assert inspect_bundle(destination.parent / 'absent', assets).status == 'missing'


@pytest.mark.parametrize('failure', ['last-download', 'hash', 'publication'])
def test_failed_refresh_preserves_old_bundle(tmp_path, assets, monkeypatch, failure):
    destination = tmp_path / 'data'
    paths = install_bundle(destination, assets)
    old_bundle = inspect_bundle(destination).bundle
    replacement = {k: dict(v) for k, v in assets.items()}
    if failure == 'last-download':
        replacement['release/manifest.json']['url'] = (tmp_path / 'missing').as_uri()
    elif failure == 'hash':
        replacement['release/manifest.json']['sha256'] = '0' * 64
    else:
        replace = os.replace
        def fail(source, target):
            if Path(target).parent == destination / 'bundles':
                raise OSError('publication failed')
            return replace(source, target)
        monkeypatch.setattr(os, 'replace', fail)
    with pytest.raises((OSError, FileValidationError)):
        install_bundle(destination, replacement, force=True)
    assert inspect_bundle(destination).bundle == old_bundle
    assert inspect_bundle(destination, assets).verified
    assert all(Path(path).exists() for path in paths.values())
    assert not list(destination.glob('.staging-*'))


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


def test_reader_sees_complete_old_bundle_during_publish(tmp_path, assets, monkeypatch):
    destination = tmp_path / 'data'
    old = install_bundle(destination, assets)
    entered, release = threading.Event(), threading.Event()
    replace = os.replace
    def paused(source, target):
        if Path(target).parent == destination / 'bundles':
            entered.set()
            assert release.wait(5)
        return replace(source, target)
    monkeypatch.setattr(os, 'replace', paused)
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
    manifest = Path(inspected.bundle) / bundles.MANIFEST
    manifest.unlink()
    assert inspect_bundle(dest).status == 'invalid'
    # A newest "bundle" that links elsewhere is never followed.
    (dest / 'bundles' / '2999-01-01T00-00-00Z').symlink_to(tmp_path / 'upstream', target_is_directory=True)
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
    assert not reg.store_path('example').parent.exists()
    reg.download('example')
    assert reg.is_cached('example')
    assert reg.inspect('example', '2025-01').status == 'missing'
    assert reg.local_path('example', asset='records.json').read_bytes().startswith(b'{')
    reg.download('example', '2025-01')
    assert reg.local_path('example', '2025-01').is_file()
    assert reg.status()[0]['inspection'].verified
    assert not reg.status(verify_files=False)[0]['inspection'].verified
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


def test_separate_processes_converge_on_one_bundle(tmp_path, assets):
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
    assert len(list((dest / 'bundles').iterdir())) == 1


def test_single_manifest_tampering_does_not_override_trusted_registry(tmp_path, assets):
    dest = tmp_path / 'data'
    paths = install_bundle(dest, assets)
    bundle = Path(inspect_bundle(dest).bundle)
    Path(paths['records.json']).write_bytes(b'forged')
    manifest = bundle / bundles.MANIFEST
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


@pytest.mark.parametrize('changed_url', [
    'https://reader:old-secret@example.test/download?id=new#original',
    'https://reader:new-secret@example.test/download?id=old#original',
    'https://reader:old-secret@example.test/download?id=old#changed',
])
def test_unverified_reuse_checks_full_source_identity(tmp_path, monkeypatch, changed_url):
    original_url = 'https://reader:old-secret@example.test/download?id=old#original'
    bodies = {original_url: b'old data', changed_url: b'new data'}
    requests = []

    def stream(url, handle, **kwargs):
        requests.append(url)
        return handle.write(bodies[url])

    monkeypatch.setattr(download, '_stream_to_file', stream)
    dest = tmp_path / 'data'
    original = {'data': {'url': original_url, 'size': 8}}
    changed = {'data': {'url': changed_url, 'size': 8}}
    old_paths = install_bundle(dest, original, verified=False)
    assert install_bundle(dest, original, verified=False) == old_paths
    assert requests == [original_url]
    assert inspect_bundle(dest, changed).status == 'invalid'
    with pytest.raises(FileValidationError, match='force=True'):
        install_bundle(dest, changed, verified=False)
    assert requests == [original_url]
    new_paths = install_bundle(dest, changed, verified=False, force=True)
    assert Path(new_paths['data']).read_bytes() == b'new data'
    assert Path(old_paths['data']).read_bytes() == b'old data'
    assert requests == [original_url, changed_url]
    receipt = json.loads((Path(inspect_bundle(dest).bundle) / bundles.MANIFEST).read_text())
    assert receipt['source_fingerprints']['data'] == sha256(changed_url.encode()).hexdigest()
    assert receipt['assets']['data']['url'] == 'https://example.test/download'
    serialized = json.dumps(receipt)
    assert 'secret' not in serialized and 'id=' not in serialized and 'reader' not in serialized


@pytest.mark.parametrize('decompress', [False, True])
def test_unverified_reuse_checks_requested_decompression(tmp_path, decompress):
    import gzip
    payload = b'plain dataset bytes\n'
    compressed = gzip.compress(payload)
    source = tmp_path / 'source.gz'
    source.write_bytes(compressed)
    dest = tmp_path / 'bundle'
    original = {'data.gz': dict(url=source.as_uri(), decompress=decompress)}
    changed = {'data.gz': dict(url=source.as_uri(), decompress=not decompress)}
    old_paths = install_bundle(dest, original, verified=False)
    assert Path(old_paths['data.gz']).read_bytes() == (payload if decompress else compressed)
    assert inspect_bundle(dest, changed).status == 'invalid'
    with pytest.raises(FileValidationError, match='force=True'):
        install_bundle(dest, changed, verified=False)
    new_paths = install_bundle(dest, changed, verified=False, force=True)
    assert Path(new_paths['data.gz']).read_bytes() == (compressed if decompress else payload)


def test_trusted_hashes_allow_cross_library_reuse_across_mirrors(tmp_path, assets, monkeypatch):
    root = tmp_path / 'shared'
    mapping = {'reference': dict(default_version='v1', versions={'v1': assets})}
    first = VersionedDatasetRegistry(mapping, cache_root=root)
    paths = first.download('reference')
    mirrored = {name: dict(spec, url='https://mirror.example.test/' + name + '.gz?token=renewed',
                           decompress=True) for name, spec in assets.items()}
    second = VersionedDatasetRegistry(
        {'reference': dict(default_version='v1', versions={'v1': mirrored})}, cache_root=root)

    def forbidden(*args, **kwargs):
        raise AssertionError('trusted shared reuse must not download or write')

    monkeypatch.setattr(download, 'fetch_file', forbidden)
    monkeypatch.setattr(bundle_store.BundleStore, 'lock', forbidden)
    monkeypatch.setattr(bundles, 'write_json', forbidden)
    monkeypatch.setattr(bundle_store, 'write_json', forbidden)
    assert second.download('reference') == paths
    assert second.inspect('reference').verified


def test_first_install_into_empty_directory_is_missing_and_preserves_mode(tmp_path, assets):
    dest = tmp_path / 'precreated'
    dest.mkdir(mode=0o700)
    assert inspect_bundle(dest, assets).status == 'missing'
    assert list(dest.iterdir()) == []
    paths = install_bundle(dest, assets)
    assert inspect_bundle(dest, assets).verified
    assert all(Path(path).is_file() for path in paths.values())
    assert dest.stat().st_mode & 0o777 == 0o700


def test_empty_symlink_is_not_an_uninitialized_bundle(tmp_path, assets):
    target = tmp_path / 'outside'
    target.mkdir()
    dest = tmp_path / 'symlink'
    dest.symlink_to(target)
    assert inspect_bundle(dest, assets).status == 'invalid'
    with pytest.raises(FileValidationError):
        install_bundle(dest, assets)
    assert list(target.iterdir()) == []


@pytest.mark.parametrize('creation_mask', [0o022, 0o002, 0o077])
def test_bundle_is_shared_and_current_once_renamed(tmp_path, assets, monkeypatch, creation_mask):
    dest = tmp_path / 'bundle'
    replace = os.replace
    installed_modes = []

    def interrupt_after_rename(source, target):
        replace(source, target)
        if Path(target).parent == dest / 'bundles':
            installed_modes.append(Path(target).stat().st_mode & 0o777)
            raise KeyboardInterrupt('interrupted immediately after the bundle rename')

    monkeypatch.setattr(os, 'replace', interrupt_after_rename)
    previous_mask = os.umask(creation_mask)
    try:
        with pytest.raises(KeyboardInterrupt):
            install_bundle(dest, assets)
        assert installed_modes == [0o777 & ~creation_mask]
        monkeypatch.setattr(os, 'replace', replace)

        def forbidden(*args, **kwargs):
            raise AssertionError('an installed bundle must not be downloaded again')

        monkeypatch.setattr(download, 'fetch_file', forbidden)
        assert inspect_bundle(dest, assets).verified
        paths = install_bundle(dest, assets)
        assert Path(inspect_bundle(dest).bundle).stat().st_mode & 0o777 == 0o777 & ~creation_mask
        assert all(Path(path).is_file() for path in paths.values())
    finally:
        os.umask(previous_mask)


@pytest.mark.parametrize('names', [
    ('data', '.data.datacache.json'),
    ('dir/data', 'dir/.data.datacache.json'),
    ('dir/DATA', 'dir/.data.datacache.JSON'),
    ('data', '.data.datacache.json/nested'),
])
@pytest.mark.parametrize('reverse', [False, True])
def test_provenance_sidecar_collisions_fail_before_any_writes(tmp_path, assets, monkeypatch, names, reverse):
    if reverse:
        names = names[::-1]
    dest = tmp_path / 'missing' / 'bundle'

    def forbidden(*args, **kwargs):
        raise AssertionError('invalid asset mapping must not download or create files')

    monkeypatch.setattr(download, 'fetch_file', forbidden)
    monkeypatch.setattr(Path, 'mkdir', forbidden)
    with pytest.raises(ValueError, match='provenance'):
        install_bundle(dest, {name: next(iter(assets.values())) for name in names})
    assert not dest.parent.exists()


def test_existing_bundle_inspection_lists_only_the_bundles_directory(tmp_path, assets, monkeypatch):
    dest = tmp_path / 'bundle'
    paths = install_bundle(dest, assets)
    iterdir = Path.iterdir

    def only_bundles(path):
        # Finding the newest bundle lists bundles/; known files need no listing.
        assert path == dest / 'bundles', 'listed %s' % path
        return iterdir(path)

    monkeypatch.setattr(Path, 'iterdir', only_bundles)
    assert inspect_bundle(dest, assets).verified
    assert install_bundle(dest, assets) == paths


def test_legacy_receipts_still_verify_but_cannot_prove_unpinned_source_identity(tmp_path, assets):
    dest = tmp_path / 'bundle'
    paths = install_bundle(dest, assets)
    manifest = Path(inspect_bundle(dest).bundle) / bundles.MANIFEST
    receipt = json.loads(manifest.read_text())
    del receipt['source_fingerprints']
    manifest.write_text(json.dumps(receipt))
    assert inspect_bundle(dest).status == 'available'
    assert install_bundle(dest, assets) == paths
    unpinned = {name: {'url': spec['url']} for name, spec in assets.items()}
    assert inspect_bundle(dest, unpinned).status == 'invalid'
    with pytest.raises(FileValidationError, match='force=True'):
        install_bundle(dest, unpinned, verified=False)
    refreshed = install_bundle(dest, unpinned, verified=False, force=True)
    assert refreshed != paths
    assert inspect_bundle(dest, unpinned).status == 'available'


def test_sidecar_like_asset_without_collision_remains_supported(tmp_path, assets):
    mapping = {'.data.datacache.json': assets['records.json'], 'other/data': assets['release/manifest.json']}
    dest = tmp_path / 'bundle'
    paths = install_bundle(dest, mapping)
    assert set(paths) == set(mapping)
    assert inspect_bundle(dest, mapping).verified


@pytest.mark.parametrize('force', [False, True])
def test_hidden_foreign_files_are_never_treated_as_an_empty_destination(tmp_path, assets, force):
    dest = tmp_path / 'bundle'
    dest.mkdir()
    precious = dest / '.user-owned'
    precious.write_text('keep me')
    assert inspect_bundle(dest, assets).status == 'invalid'
    with pytest.raises((FileNotFoundError, FileValidationError)):
        install_bundle(dest, assets, force=force)
    assert list(dest.iterdir()) == [precious]
    assert precious.read_text() == 'keep me'


def test_concurrent_initialization_of_precreated_directory_reuses_winner(tmp_path, assets, monkeypatch):
    dest = tmp_path / 'bundle'
    dest.mkdir()
    missing_marker, resume_inspection = threading.Event(), threading.Event()
    read_json = bundle_store.read_json

    def pause_after_missing_marker(path, *args, **kwargs):
        try:
            return read_json(path, *args, **kwargs)
        except FileNotFoundError:
            if Path(path) == dest / bundle_store.MARKER and not missing_marker.is_set():
                missing_marker.set()
                assert resume_inspection.wait(5)
            raise

    monkeypatch.setattr(bundle_store, 'read_json', pause_after_missing_marker)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(install_bundle, dest, assets)
        assert missing_marker.wait(5)
        try:
            winner = install_bundle(dest, assets)
        finally:
            resume_inspection.set()
        assert future.result() == winner
    assert len(list((dest / 'bundles').iterdir())) == 1


def test_large_manifests_are_read(tmp_path, assets):
    dest = tmp_path / 'bundle'
    paths = install_bundle(dest, assets)
    manifest = Path(inspect_bundle(dest).bundle) / bundles.MANIFEST
    receipt = json.loads(manifest.read_text())
    # Manifests grow with the number of assets; this one is over 1 MiB.
    receipt['assets']['records.json']['url'] += '/' + 'x' * (2 ** 21)
    manifest.write_text(json.dumps(receipt))
    assert inspect_bundle(dest).status == 'available'
    assert all(Path(path).is_file() for path in paths.values())


def test_files_are_verified_only_by_hashes_the_caller_supplied(tmp_path, assets):
    dest = tmp_path / 'bundle'
    install_bundle(dest, assets)
    assert all(item.verified for item in inspect_bundle(dest, assets).files.values())
    # The bundle's own manifest checks the bytes but vouches for nothing.
    receipt_only = inspect_bundle(dest)
    assert receipt_only.status == 'available'
    assert not any(item.verified for item in receipt_only.files.values())


def test_cleanup_failure_after_publishing_is_logged_not_raised(tmp_path, assets, monkeypatch, caplog):
    import shutil
    rmtree = shutil.rmtree

    def failing_rmtree(path, *args, **kwargs):
        if Path(path).name.startswith('.staging-'):
            raise OSError('busy')
        return rmtree(path, *args, **kwargs)

    monkeypatch.setattr(shutil, 'rmtree', failing_rmtree)
    paths = install_bundle(tmp_path / 'bundle', assets)
    assert all(Path(path).is_file() for path in paths.values())
    assert 'Could not remove the working directory' in caplog.text


def test_the_newest_bundle_is_current(tmp_path, assets):
    dest = tmp_path / 'bundle'
    first = install_bundle(dest, assets)
    second = install_bundle(dest, assets, force=True)
    names = sorted(entry.name for entry in (dest / 'bundles').iterdir())
    assert len(names) == 2
    assert Path(inspect_bundle(dest).bundle).name == names[-1]
    assert install_bundle(dest, assets) == second != first
    assert all(Path(path).is_file() for path in first.values())
