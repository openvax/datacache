"""Released hitlist contracts for the shared fixed-path registry."""

import gzip
import hashlib
import json
import multiprocessing
from pathlib import Path
import threading

import pytest

from datacache import VersionedFileRegistry
from datacache import file_registry


@pytest.fixture
def registry(tmp_path):
    source = tmp_path / 'source'
    source.write_bytes(b'original')
    mapping = {'thing': {
        'filename': 'thing.tsv', 'urls': {'v1': source.as_uri(), 'v2': source.as_uri()},
        'default_version': 'v2', 'description': 'reference',
    }}
    return VersionedFileRegistry(mapping, cache_dir=lambda: tmp_path / 'cache'), source


def test_missing_lookup_and_status_are_read_only(registry, tmp_path):
    reg, _ = registry
    assert reg.resolve_version('thing') == 'v2'
    assert reg.resolve_version('thing', 'v1') == 'v1'
    assert reg.local_path('thing') == tmp_path / 'cache/thing/v2/thing.tsv'
    assert not reg.is_cached('thing')
    assert reg.status() == [{
        'name': 'thing', 'description': 'reference', 'default_version': 'v2',
        'available_versions': ['v1', 'v2'], 'cached': False, 'cached_version': None,
        'url': None, 'bytes': None, 'sha256': None, 'downloaded_at': None,
        'path': str(reg.local_path('thing')),
    }]
    assert not (tmp_path / 'cache').exists()


def test_download_receipt_and_multiple_versions(registry, tmp_path):
    reg, source = registry
    first = reg.download('thing', 'v1', timeout=2, record_provenance=True)
    assert isinstance(first, Path)
    assert first == tmp_path / 'cache/thing/v1/thing.tsv'
    assert first.read_bytes() == b'original'
    manifest_path = tmp_path / 'cache/manifest.json'
    receipt = json.loads(manifest_path.read_text())['thing']
    assert receipt['version'] == 'v1'
    assert receipt['path'] == str(first)
    assert receipt['url'] == source.as_uri()
    assert receipt['bytes'] == 8
    assert receipt['sha256'] == hashlib.sha256(b'original').hexdigest()
    assert receipt['downloaded_at'].endswith('+00:00')
    second = reg.ensure('thing')
    assert first.exists() and second.exists()
    assert first != second
    status = reg.status()[0]
    assert status['cached_version'] == 'v2'
    assert status['url'] == source.as_uri()
    assert status['sha256'] == hashlib.sha256(b'original').hexdigest()
    assert not list(manifest_path.parent.glob('.datacache-json-*'))


def test_legacy_reuse_is_offline_without_hash_or_manifest_rewrite(registry, tmp_path, monkeypatch):
    reg, source = registry
    path = reg.local_path('thing')
    path.parent.mkdir(parents=True)
    path.write_bytes(b'legacy file')
    manifest = tmp_path / 'cache/manifest.json'
    manifest.write_text(json.dumps({'thing': {
        'version': 'v2', 'path': str(path), 'bytes': 11, 'downloaded_at': '2020-01-01',
    }}))
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in (path, manifest)}
    source.unlink()
    monkeypatch.setattr(file_registry, 'fetch_file', lambda *a, **k: pytest.fail('fetch'))
    monkeypatch.setattr(file_registry.hashlib, 'sha256', lambda *a: pytest.fail('hash'))
    assert reg.download('thing') == path
    assert reg.ensure('thing') == path
    assert reg.is_cached('thing')
    assert reg.status()[0]['downloaded_at'] == '2020-01-01'
    assert before == {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in (path, manifest)}


def test_forced_failure_preserves_legacy_file_and_receipt(registry, tmp_path):
    reg, source = registry
    path = reg.download('thing')
    manifest = tmp_path / 'cache/manifest.json'
    before = manifest.read_bytes()
    source.unlink()
    with pytest.raises(RuntimeError, match='failed to download') as raised:
        reg.download('thing', force=True)
    assert raised.value.__cause__ is not None
    assert path.read_bytes() == b'original'
    assert manifest.read_bytes() == before


def test_refresh_and_raw_archive_override(registry):
    reg, source = registry
    archive = source.with_suffix('.gz')
    archive.write_bytes(gzip.compress(b'expanded'))
    reg._datasets['thing']['urls']['v2'] = archive.as_uri()
    path = reg.download('thing')
    assert path.read_bytes() == b'expanded'
    assert reg.download('thing', force=True, raw=True).read_bytes() == archive.read_bytes()


def test_callable_root_remains_dynamic(registry, tmp_path):
    reg, _ = registry
    selected = [tmp_path / 'first']
    reg._cache_dir = lambda: str(selected[0])
    first = reg.download('thing')
    selected[0] = tmp_path / 'second'
    second = reg.ensure('thing')
    assert first != second
    assert first.read_bytes() == second.read_bytes()


def test_custom_errors_and_cause(registry, monkeypatch):
    reg, _ = registry
    class DomainError(RuntimeError):
        pass
    reg._error_cls = DomainError
    with pytest.raises(DomainError, match='unknown dataset'):
        reg.resolve_version('absent')
    with pytest.raises(DomainError, match='available: v1, v2'):
        reg.resolve_version('thing', 'v3')
    error = OSError('offline')
    def fail(*a, **k):
        raise error
    monkeypatch.setattr(file_registry, 'fetch_file', fail)
    with pytest.raises(DomainError) as raised:
        reg.download('thing')
    assert raised.value.__cause__ is error


@pytest.mark.parametrize('method', ['download', 'ensure'])
def test_explicit_integrity_expectations_are_not_ignored_on_reuse(registry, method):
    reg, _ = registry
    path = reg.download('thing')
    with pytest.raises(RuntimeError, match='size'):
        getattr(reg, method)('thing', expected_size=99)
    assert path.read_bytes() == b'original'


def test_failed_manifest_publication_preserves_prior_receipt(registry, tmp_path, monkeypatch):
    reg, source = registry
    reg.download('thing')
    manifest = tmp_path / 'cache/manifest.json'
    before = manifest.read_bytes()
    source.write_bytes(b'new file')
    import datacache._filesystem as filesystem
    replace = filesystem.os.replace
    def fail_manifest(src, dest):
        if Path(dest) == manifest:
            raise OSError('receipt publication failed')
        return replace(src, dest)
    monkeypatch.setattr(filesystem.os, 'replace', fail_manifest)
    with pytest.raises(OSError, match='receipt publication failed'):
        reg.download('thing', force=True)
    assert manifest.read_bytes() == before
    # Legacy single-file contract: file publication and receipt are separate.
    assert reg.local_path('thing').read_bytes() == b'new file'
    assert not list(manifest.parent.glob('.datacache-json-*'))


def _download_in_process(root, source, name, start, read_barrier):
    """Widen the old manifest race deterministically, in separate interpreters."""
    registry = VersionedFileRegistry({name: {
        'filename': 'data', 'urls': {'v1': Path(source).as_uri()}, 'default_version': 'v1',
    }}, cache_dir=lambda: Path(root))
    read = registry._read_manifest_at
    def overlapping_read(path):
        result = read(path)
        try:
            read_barrier.wait(timeout=0.5)
        except threading.BrokenBarrierError:
            pass  # a correct writer lock prevents the other reader entering
        return result
    registry._read_manifest_at = overlapping_read
    start.wait(timeout=20)
    registry.download(name)


def test_concurrent_processes_preserve_both_manifest_entries(tmp_path):
    ctx = multiprocessing.get_context('spawn')
    source = tmp_path / 'source'
    source.write_bytes(b'data')
    root = tmp_path / 'cache'
    start, read_barrier = ctx.Barrier(2), ctx.Barrier(2)
    processes = [ctx.Process(target=_download_in_process,
                            args=(str(root), str(source), name, start, read_barrier))
                 for name in ('first', 'second')]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=30)
        assert [process.exitcode for process in processes] == [0, 0]
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
    manifest = json.loads((root / 'manifest.json').read_text())
    assert set(manifest) == {'first', 'second'}
    assert all(Path(record['path']).read_bytes() == b'data' for record in manifest.values())


def test_download_resolves_dynamic_root_once(registry, tmp_path):
    reg, _ = registry
    calls = []
    def changing_root():
        calls.append(len(calls))
        return tmp_path / str(len(calls))
    reg._cache_dir = changing_root
    path = reg.download('thing')
    assert len(calls) == 1
    receipt = json.loads((tmp_path / '1/manifest.json').read_text())
    assert receipt['thing']['path'] == str(path)


def test_atomic_receipt_supports_platform_without_fchmod(tmp_path, monkeypatch):
    import datacache._filesystem as filesystem
    monkeypatch.delattr(filesystem.os, 'fchmod')
    path = tmp_path / 'manifest.json'
    filesystem.write_json(path, {'data': 'receipt'})
    assert json.loads(path.read_text()) == {'data': 'receipt'}
