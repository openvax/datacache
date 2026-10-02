"""Caller-built generations, private retained inputs, and raw gzip dependencies."""

from concurrent.futures import ThreadPoolExecutor
import gzip
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time

import pytest

from datacache import FileValidationError, inspect_bundle, inspect_materialization, materialize
from datacache import download, materialization as module
from datacache.resume import _state_directory

pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX materialization locks')
DATA = b'>chr1\nACGTACGT\n'
COMPRESSED = gzip.compress(DATA, mtime=0)
TRANSFORM = {'version': 'fasta-1', 'options': {'validate': True}}


def metadata(data):
    return {'sha256': sha256(data).hexdigest(), 'size': len(data)}


@pytest.fixture
def sources(tmp_path):
    source = tmp_path / 'caller-owned.fa.gz'
    source.write_bytes(COMPRESSED)
    return {'dna.fa.gz': dict(path=source, **metadata(COMPRESSED))}


@pytest.fixture
def outputs():
    return {'dna.fa': metadata(DATA), 'index/info.json': metadata(b'{"length":8}\n')}


def build(sources, outputs):
    assert all(Path(path).parent.stat().st_mode & 0o077 == 0 for path in sources.values())
    with gzip.open(sources['dna.fa.gz'], 'rb') as source, open(outputs['dna.fa'], 'wb') as target:
        while chunk := source.read(4):
            target.write(chunk)
    Path(outputs['index/info.json']).write_bytes(b'{"length":8}\n')


def install(store, sources, outputs, **options):
    defaults = dict(transform=TRANSFORM, outputs=outputs, builder=build,
                    download_options={'show_progress': False})
    defaults.update(options)
    return materialize(store, sources, **defaults)


def inspected(store, sources, outputs, **options):
    return inspect_materialization(store, sources, transform=TRANSFORM, outputs=outputs, **options)


def input_dir(store):
    return next(store.glob('.inputs-*'))


def test_transaction_receipt_cleanup_and_immutable_paths(tmp_path, sources, outputs):
    store = tmp_path / 'artifacts' / 'dna-110'
    paths = install(store, sources, outputs)
    assert Path(paths['dna.fa']).read_bytes() == DATA
    assert Path(paths['index/info.json']).read_bytes() == b'{"length":8}\n'
    state = inspected(store, sources, outputs)
    assert state.status == 'available' and state.verified
    assert all(file.verified for file in state.files.values())
    assert state.transform == TRANSFORM
    record = state.sources['dna.fa.gz']
    assert record['verified'] is True
    assert {key: record[key] for key in ('sha256', 'size')} == metadata(COMPRESSED)
    receipt = json.loads((Path(state.generation) / module.MANIFEST).read_text())
    assert receipt['definition']['transform'] == TRANSFORM
    assert set(receipt['outputs']) == set(outputs)
    assert Path(sources['dna.fa.gz']['path']).read_bytes() == COMPRESSED
    assert not list(store.glob('.inputs-*'))
    assert not list(store.glob('.staging-*'))
    assert inspect_bundle(store).status == 'invalid'  # Distinct store ownership.
    assert not inspect_materialization(store).verified
    new_paths = install(store, sources, outputs, force=True)
    assert paths != new_paths
    assert all(Path(path).exists() for path in paths.values())


def forbidden(*args, **kwargs):
    raise AssertionError('offline cache hit tried to write, build, or acquire')


def test_readonly_offline_hit_never_touches_sources(tmp_path, sources, outputs, monkeypatch):
    store = tmp_path / 'dna'
    paths = install(store, sources, outputs)
    Path(sources['dna.fa.gz']['path']).unlink()
    changed_modes = []
    for path in [*store.rglob('*'), store]:
        changed_modes.append((path, path.stat().st_mode & 0o777))
        os.chmod(path, 0o555 if path.is_dir() else 0o444)
    try:
        monkeypatch.setattr(download, 'fetch_file', forbidden)
        monkeypatch.setattr(module, 'file_lock', forbidden)
        monkeypatch.setattr(module, 'write_json', forbidden)
        monkeypatch.setattr(Path, 'mkdir', forbidden)
        assert install(store, sources, outputs, builder=forbidden) == paths
        assert inspected(store, sources, outputs).verified
        fast = inspected(store, sources, outputs, verify_files=False)
        assert fast.status == 'available' and not fast.verified
        assert not any(file.verified for file in fast.files.values())
    finally:
        for path, mode in reversed(changed_modes):
            os.chmod(path, mode)


@pytest.mark.parametrize('failure', ['builder', 'keyboard', 'last-output', 'validation', 'rename', 'pointer'])
def test_failed_refresh_preserves_old_and_keeps_complete_input(tmp_path, sources, outputs, monkeypatch, failure):
    store = tmp_path / 'dna'
    old_paths = install(store, sources, outputs)
    old_pointer = (store / module.CURRENT).read_bytes()

    def failing_build(inputs, targets):
        build(inputs, targets)
        assert (store / module.CURRENT).read_bytes() == old_pointer
        if failure == 'builder':
            raise RuntimeError('biological validation failed')
        if failure == 'keyboard':
            raise KeyboardInterrupt
        if failure == 'last-output':
            Path(targets['index/info.json']).unlink()
        if failure == 'validation':
            Path(targets['index/info.json']).write_bytes(b'wrong')

    original_replace, original_write = module.os.replace, module.write_json
    if failure == 'rename':
        def fail_replace(source, target):
            if Path(target).parent.name == 'generations':
                raise OSError('generation rename failed')
            return original_replace(source, target)
        monkeypatch.setattr(module.os, 'replace', fail_replace)
    if failure == 'pointer':
        def fail_write(path, value, **options):
            if Path(path).name == module.CURRENT:
                raise OSError('pointer failed')
            return original_write(path, value, **options)
        monkeypatch.setattr(module, 'write_json', fail_write)
    with pytest.raises((RuntimeError, KeyboardInterrupt, FileValidationError, OSError)):
        install(store, sources, outputs, builder=failing_build, force=True)
    assert (store / module.CURRENT).read_bytes() == old_pointer
    assert inspected(store, sources, outputs).verified
    assert all(Path(path).exists() for path in old_paths.values())
    assert (input_dir(store) / 'dna.fa.gz').read_bytes() == COMPRESSED
    assert not list(store.glob('.staging-*'))
    Path(sources['dna.fa.gz']['path']).unlink()
    monkeypatch.setattr(module.os, 'replace', original_replace)
    monkeypatch.setattr(module, 'write_json', original_write)
    assert install(store, sources, outputs, force=True) != old_paths
    assert not list(store.glob('.inputs-*'))


def test_missing_pointer_recovers_locally_and_only_then_discards_inputs(tmp_path, sources, outputs, monkeypatch):
    store = tmp_path / 'dna'
    original = module.write_json
    def interrupted(path, value, **options):
        if Path(path).name == module.CURRENT:
            raise KeyboardInterrupt
        return original(path, value, **options)
    monkeypatch.setattr(module, 'write_json', interrupted)
    with pytest.raises(KeyboardInterrupt):
        install(store, sources, outputs)
    assert inspected(store, sources, outputs).status == 'recovery-required'
    assert (input_dir(store) / 'dna.fa.gz').exists()
    Path(sources['dna.fa.gz']['path']).unlink()
    monkeypatch.setattr(module, 'write_json', original)
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    paths = install(store, sources, outputs, builder=forbidden)
    assert Path(paths['dna.fa']).read_bytes() == DATA
    assert inspected(store, sources, outputs).verified
    assert not list(store.glob('.inputs-*'))


def test_recovery_always_hashes_outputs_even_with_fast_hit_flag(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    paths = install(store, sources, outputs, retain_sources=True)
    (store / module.CURRENT).unlink()
    Path(paths['dna.fa']).write_bytes(b'x' * len(DATA))
    repaired = install(store, sources, outputs, verify_files=False)
    assert repaired != paths
    assert Path(repaired['dna.fa']).read_bytes() == DATA


@pytest.mark.parametrize('change', ['source', 'transform-version', 'transform-options', 'output-inventory'])
def test_dependency_mismatch_is_explicit_and_does_not_touch_old(tmp_path, sources, outputs, change):
    store = tmp_path / 'dna'
    paths = install(store, sources, outputs, retain_sources=True)
    new_sources = {name: dict(spec) for name, spec in sources.items()}
    options = {}
    new_outputs = outputs
    if change == 'source':
        replacement = tmp_path / 'another.fa.gz'
        replacement.write_bytes(COMPRESSED)
        new_sources['dna.fa.gz']['path'] = replacement
    elif change == 'transform-version':
        options['transform'] = {'version': 'fasta-2', 'options': TRANSFORM['options']}
    elif change == 'transform-options':
        options['transform'] = {'version': TRANSFORM['version'], 'options': {'validate': False}}
    else:
        new_outputs = {'dna.fa': outputs['dna.fa']}
        options['builder'] = lambda inputs, targets: Path(targets['dna.fa']).write_bytes(DATA)
    with pytest.raises(FileValidationError, match='force=True'):
        install(store, new_sources, new_outputs, **options)
    assert inspected(store, sources, outputs).verified
    assert install(store, new_sources, new_outputs, force=True, **options) != paths


def test_transform_change_can_reuse_retained_inputs_after_upstream_removed(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    install(store, sources, outputs, retain_sources=True)
    owned = input_dir(store)
    Path(sources['dna.fa.gz']['path']).unlink()
    paths = install(store, sources, outputs, force=True, transform={'version': 'fasta-2'})
    assert Path(paths['dna.fa']).read_bytes() == DATA
    assert not owned.exists()


def test_untrusted_observation_is_not_trusted_verification(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    untrusted_sources = {'dna.fa.gz': {'path': sources['dna.fa.gz']['path']}}
    untrusted_outputs = {name: {} for name in outputs}
    paths = install(store, untrusted_sources, untrusted_outputs)
    state = inspected(store, untrusted_sources, untrusted_outputs)
    assert state.status == 'available' and not state.verified
    assert state.sources['dna.fa.gz']['verified'] is False
    assert state.sources['dna.fa.gz']['sha256'] == sha256(COMPRESSED).hexdigest()
    Path(paths['dna.fa']).write_bytes(b'x' * len(DATA))
    assert inspected(store, untrusted_sources, untrusted_outputs, verify_files=False).status == 'available'
    assert inspected(store, untrusted_sources, untrusted_outputs).status == 'invalid'
    with pytest.raises(FileValidationError):
        install(store, untrusted_sources, untrusted_outputs)
    assert install(store, untrusted_sources, untrusted_outputs, force=True) != paths


@pytest.mark.parametrize('kind', ['symlink', 'hardlink', 'fifo', 'extra', 'extra-directory', 'input-mutation'])
def test_unsafe_or_undeclared_builder_results_are_never_published(tmp_path, sources, outputs, kind):
    store = tmp_path / 'dna'
    def unsafe(inputs, targets):
        build(inputs, targets)
        target = Path(targets['dna.fa'])
        if kind in ('symlink', 'hardlink', 'fifo'):
            target.unlink()
            if kind == 'symlink':
                target.symlink_to(sources['dna.fa.gz']['path'])
            elif kind == 'hardlink':
                os.link(inputs['dna.fa.gz'], target)
            else:
                os.mkfifo(target)
        elif kind == 'extra':
            (target.parent / 'undeclared').write_text('extra')
        elif kind == 'extra-directory':
            (target.parent / 'unused').mkdir()
        else:
            Path(inputs['dna.fa.gz']).write_bytes(b'changed')
    with pytest.raises(FileValidationError):
        install(store, sources, outputs, builder=unsafe)
    assert inspected(store, sources, outputs).status == 'missing'
    assert list((store / 'generations').iterdir()) == []
    assert Path(sources['dna.fa.gz']['path']).read_bytes() == COMPRESSED


@pytest.mark.parametrize('kind', ['symlink', 'hardlink', 'fifo', 'directory'])
def test_nonregular_local_sources_rejected_without_blocking(tmp_path, sources, outputs, kind):
    target = tmp_path / 'unsafe'
    if kind == 'symlink':
        target.symlink_to(sources['dna.fa.gz']['path'])
    elif kind == 'hardlink':
        os.link(sources['dna.fa.gz']['path'], target)
    elif kind == 'fifo':
        os.mkfifo(target)
    else:
        target.mkdir()
    sources['dna.fa.gz']['path'] = target
    with pytest.raises(FileValidationError):
        install(tmp_path / 'dna', sources, outputs)


@pytest.mark.parametrize('force', [False, True])
def test_foreign_directory_never_adopted(tmp_path, sources, outputs, force):
    store = tmp_path / 'foreign'
    store.mkdir()
    (store / 'precious').write_text('keep')
    with pytest.raises((FileValidationError, OSError)):
        install(store, sources, outputs, force=force)
    assert (store / 'precious').read_text() == 'keep'


def test_complete_gzip_but_failed_decompression_reuses_input(tmp_path, sources, outputs):
    invalid_gzip = b'not-gzip'
    source = sources['dna.fa.gz']['path']
    source.write_bytes(invalid_gzip)
    sources['dna.fa.gz'].update(metadata(invalid_gzip))
    store = tmp_path / 'dna'
    with pytest.raises(gzip.BadGzipFile):
        install(store, sources, outputs)
    source.unlink()
    owned = input_dir(store) / 'dna.fa.gz'
    assert owned.read_bytes() == invalid_gzip
    calls = []
    def failure(inputs, targets):
        calls.append(Path(inputs['dna.fa.gz']).read_bytes())
        raise RuntimeError('retry reached builder with complete input')
    with pytest.raises(RuntimeError, match='retry reached'):
        install(store, sources, outputs, builder=failure)
    assert calls == [invalid_gzip] and owned.exists()


def test_same_artifact_serializes_but_unrelated_builders_can_run(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    entered, release = threading.Event(), threading.Event()
    calls = []
    def paused(inputs, targets):
        calls.append(1)
        entered.set()
        assert release.wait(8)
        build(inputs, targets)
    with ThreadPoolExecutor(max_workers=3) as pool:
        first = pool.submit(install, store, sources, outputs, builder=paused)
        assert entered.wait(8)
        second = pool.submit(install, store, sources, outputs, builder=paused)
        unrelated = pool.submit(install, tmp_path / 'other', sources, outputs)
        try:
            assert unrelated.result(timeout=8)
            assert not second.done()
        finally:
            release.set()
        assert first.result(timeout=8) == second.result(timeout=8)
    assert calls == [1]


def test_reader_sees_whole_old_pair_until_pointer_publication(tmp_path, sources, outputs, monkeypatch):
    store = tmp_path / 'dna'
    old = install(store, sources, outputs)
    entered, release = threading.Event(), threading.Event()
    original = module.write_json
    def paused(path, value, **options):
        if Path(path).name == module.CURRENT:
            entered.set()
            assert release.wait(8)
        return original(path, value, **options)
    monkeypatch.setattr(module, 'write_json', paused)
    with ThreadPoolExecutor() as pool:
        future = pool.submit(install, store, sources, outputs, force=True)
        assert entered.wait(8)
        try:
            state = inspected(store, sources, outputs)
            assert state.verified
            assert {name: file.path for name, file in state.files.items()} == old
            assert (input_dir(store) / 'dna.fa.gz').exists()
        finally:
            release.set()
        new = future.result(timeout=8)
    assert new != old
    assert Path(new['index/info.json']).parents[1] == Path(new['dna.fa']).parent


def test_large_dependency_receipts_are_readable(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    transform = {'version': '1', 'options': 'x' * (1024 * 1024 + 1)}
    paths = install(store, sources, outputs, transform=transform)
    receipt = Path(paths['dna.fa']).parent / module.MANIFEST
    assert receipt.stat().st_size > 1024 * 1024
    assert inspect_materialization(store, sources, transform=transform, outputs=outputs).verified


@pytest.fixture
def server():
    requests = []
    actions = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(dict(self.headers))
            action = actions.pop(0) if actions else {}
            offset = int(self.headers.get('Range', 'bytes=0-')[6:-1])
            body = COMPRESSED[offset:]
            self.send_response(206 if offset else 200)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('ETag', '"dna-110"')
            if offset:
                self.send_header('Content-Range', 'bytes %d-%d/%d' % (offset, len(COMPRESSED) - 1, len(COMPRESSED)))
            self.end_headers()
            try:
                self.wfile.write(body[:action.get('drop', len(body))])
                self.wfile.flush()
                if 'drop' in action:
                    self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        def log_message(self, *args):
            pass
    srv = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield 'http://127.0.0.1:%d/dna.gz?token=secret' % srv.server_port, requests, actions
    srv.shutdown()
    srv.server_close()
    thread.join()


@pytest.mark.parametrize('trusted', [True, False])
def test_resume_gzip_and_record_validated_transport(server, tmp_path, outputs, trusted):
    url, requests, actions = server
    actions.append({'drop': 12})
    sources = {'dna.fa.gz': dict(url=url, size=len(COMPRESSED))}
    if trusted:
        sources['dna.fa.gz']['sha256'] = sha256(COMPRESSED).hexdigest()
    store = tmp_path / 'dna'
    paths = install(store, sources, outputs, download_options={'resume': True, 'chunk_size': 4,
                    'retry_backoff': 0, 'show_progress': False})
    assert Path(paths['dna.fa']).read_bytes() == DATA
    assert requests[1]['Range'] == 'bytes=12-'
    assert requests[1]['If-Range'] == '"dna-110"'
    state = inspected(store, sources, outputs)
    record = state.sources['dna.fa.gz']
    assert record['verified'] is trusted
    assert record['transport'] == {'header': 'ETag', 'value': '"dna-110"'}
    assert record['origin'] == url.split('?')[0]
    assert 'secret' not in (Path(state.generation) / module.MANIFEST).read_text()
    assert not list(store.glob('.inputs-*'))


def test_interrupted_download_and_builder_retry_without_network(server, tmp_path, outputs, monkeypatch):
    url, requests, actions = server
    sources = {'dna.fa.gz': dict(url=url, **metadata(COMPRESSED))}
    store = tmp_path / 'dna'
    def interrupt(done, total):
        if done >= 8:
            raise KeyboardInterrupt
    options = {'resume': True, 'chunk_size': 4, 'progress_callback': interrupt, 'show_progress': False}
    with pytest.raises(KeyboardInterrupt):
        install(store, sources, outputs, download_options=options)
    owned = input_dir(store) / 'dna.fa.gz'
    assert (_state_directory(owned) / 'partial').read_bytes() == COMPRESSED[:8]
    assert inspected(store, sources, outputs).status == 'missing'
    del options['progress_callback']
    def interrupted_build(inputs, targets):
        with gzip.open(inputs['dna.fa.gz'], 'rb') as source, open(targets['dna.fa'], 'wb') as target:
            target.write(source.read(4))
            raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        install(store, sources, outputs, builder=interrupted_build, download_options=options)
    assert requests[1]['Range'] == 'bytes=8-'
    assert owned.read_bytes() == COMPRESSED
    assert not (_state_directory(owned) / 'partial').exists()
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    assert install(store, sources, outputs, download_options=options)
    assert len(requests) == 2
    assert not owned.exists()


def test_killed_builder_retries_with_completed_local_input(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    ready = tmp_path / 'builder-ready'
    code = '''
import json, sys, time
from pathlib import Path
from datacache import materialize
store, source, ready, outputs = sys.argv[1:]
def builder(inputs, targets):
    Path(targets['dna.fa']).write_bytes(b'partial')
    Path(ready).write_text('ready')
    time.sleep(30)
materialize(store, {'dna.fa.gz': {'path': source}}, transform={'version':'fasta-1','options':{'validate':True}},
            outputs=json.loads(outputs), builder=builder, download_options={'show_progress':False})
'''
    process = subprocess.Popen([sys.executable, '-c', code, str(store), str(sources['dna.fa.gz']['path']),
                                str(ready), json.dumps(outputs)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        deadline = time.monotonic() + 8
        while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists(), process.communicate(timeout=2)
        process.kill()
        process.communicate(timeout=5)
        assert list(store.glob('.staging-*'))
        Path(sources['dna.fa.gz']['path']).unlink()
        untrusted = {'dna.fa.gz': {'path': sources['dna.fa.gz']['path']}}
        paths = install(store, untrusted, outputs)
        assert Path(paths['dna.fa']).read_bytes() == DATA
        assert not list(store.glob('.staging-*'))
        assert not list(store.glob('.inputs-*'))
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)


@pytest.mark.parametrize('options', [
    {'force': 1}, {'retain_sources': 'yes'}, {'verify_files': None}, {'builder': None},
    {'transform': {'version': ''}}, {'transform': {'version': '1', 'options': float('nan')}},
    {'outputs': {'../escape': {}}}, {'outputs': {'A': {}, 'a/b': {}}},
    {'download_options': {'raw': False}}, {'download_options': {'show_progress': 1}},
    {'download_options': {'resume': 'yes'}},
    {'download_options': {'chunk_size': 0}}, {'download_options': {'progress_callback': 'yes'}},
    {'download_options': {'max_retries': -1}},
])
def test_invalid_options_fail_before_creating_store(tmp_path, sources, outputs, options):
    store = tmp_path / 'missing' / 'dna'
    with pytest.raises((ValueError, TypeError)):
        install(store, sources, outputs, **options)
    assert not store.parent.exists()


def test_fast_reuse_does_not_hash_payloads(tmp_path, sources, outputs, monkeypatch):
    store = tmp_path / 'dna'
    paths = install(store, sources, outputs)
    original = module._observe
    def no_hash(*args, **options):
        assert options.get('verify_files') is False
        return original(*args, **options)
    monkeypatch.setattr(module, '_observe', no_hash)
    assert install(store, sources, outputs, verify_files=False, builder=forbidden) == paths


def test_input_hash_failure_and_bad_final_output_never_publish(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    sources['dna.fa.gz']['sha256'] = '0' * 64
    with pytest.raises(FileValidationError):
        install(store, sources, outputs, verify_files=False)
    assert not list((store / 'generations').iterdir())
    sources['dna.fa.gz'].update(metadata(COMPRESSED))
    outputs['dna.fa']['sha256'] = '0' * 64
    with pytest.raises(FileValidationError):
        install(store, sources, outputs, verify_files=False)
    assert not list((store / 'generations').iterdir())


def test_source_file_url_is_private_copy_and_not_deleted(tmp_path, sources, outputs):
    source = sources['dna.fa.gz']['path']
    sources['dna.fa.gz'] = dict(url=source.as_uri(), **metadata(COMPRESSED))
    assert install(tmp_path / 'dna', sources, outputs)
    assert source.read_bytes() == COMPRESSED


def test_private_input_corruption_requires_explicit_repair(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    def interrupted(inputs, targets):
        raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        install(store, sources, outputs, builder=interrupted)
    owned = input_dir(store) / 'dna.fa.gz'
    owned.write_bytes(b'x' * len(COMPRESSED))
    with pytest.raises(FileValidationError, match='force=True'):
        install(store, sources, outputs)
    assert install(store, sources, outputs, force=True)
    assert not owned.exists()


def test_bare_complete_input_without_receipt_can_be_explicitly_repaired(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    def interrupted(inputs, targets):
        raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        install(store, sources, outputs, builder=interrupted)
    directory = input_dir(store)
    (directory / module.INPUTS).unlink()
    (directory / 'dna.fa.gz').write_bytes(b'x' * len(COMPRESSED))
    with pytest.raises(FileValidationError, match='force=True'):
        install(store, sources, outputs)
    assert install(store, sources, outputs, force=True)


def test_untrusted_bare_input_requires_new_acquisition(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    sources = {'dna.fa.gz': {'path': sources['dna.fa.gz']['path']}}
    def interrupted(inputs, targets):
        raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        install(store, sources, outputs, builder=interrupted)
    directory = input_dir(store)
    (directory / module.INPUTS).unlink()
    (directory / 'dna.fa.gz').write_bytes(b'unverified unknown')
    assert install(store, sources, outputs)


def test_inspection_requires_complete_definition_or_none(tmp_path, sources, outputs):
    with pytest.raises(ValueError):
        inspect_materialization(tmp_path / 'dna', sources, outputs=outputs)
    with pytest.raises(ValueError):
        inspect_materialization(tmp_path / 'dna', verify_files='yes')


def test_precreated_empty_store_preserves_mode(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    store.mkdir(mode=0o750)
    assert inspected(store, sources, outputs).status == 'missing'
    install(store, sources, outputs)
    assert store.stat().st_mode & 0o777 == 0o750


def test_retention_toggle_on_hit_does_not_cleanup(tmp_path, sources, outputs):
    store = tmp_path / 'dna'
    paths = install(store, sources, outputs, retain_sources=True)
    owned = input_dir(store) / 'dna.fa.gz'
    assert install(store, sources, outputs) == paths
    assert owned.exists()
    assert install(store, sources, outputs, force=True) != paths
    assert not owned.exists()


@pytest.mark.parametrize('kind', ['symlink', 'fifo', 'shared-directory'])
def test_planted_private_state_is_not_followed_or_adopted(tmp_path, sources, outputs, kind):
    store = tmp_path / 'dna'
    def interrupted(inputs, targets):
        raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        install(store, sources, outputs, builder=interrupted)
    owned_directory = input_dir(store)
    owned = owned_directory / 'dna.fa.gz'
    if kind == 'shared-directory':
        os.chmod(owned_directory, 0o755)
    else:
        owned.unlink()
        if kind == 'symlink':
            owned.symlink_to(sources['dna.fa.gz']['path'])
        else:
            os.mkfifo(owned)
    with pytest.raises(FileValidationError):
        install(store, sources, outputs, force=True)
    assert Path(sources['dna.fa.gz']['path']).read_bytes() == COMPRESSED


@pytest.mark.parametrize('malformation', [
    'missing-output', 'extra-output', 'source-size', 'source-identity', 'source-trust',
    'source-inventory', 'transform', 'output-digest', 'transport', 'definition',
])
def test_malformed_manifest_is_invalid_even_for_fast_inspection(tmp_path, sources, outputs, malformation):
    store = tmp_path / 'dna'
    paths = install(store, sources, outputs)
    manifest = Path(paths['dna.fa']).parent / module.MANIFEST
    receipt = json.loads(manifest.read_text())
    if malformation == 'missing-output':
        del receipt['outputs']['dna.fa']
    elif malformation == 'extra-output':
        receipt['outputs']['extra'] = metadata(b'')
    elif malformation == 'source-size':
        receipt['sources']['dna.fa.gz']['size'] += 1
    elif malformation == 'source-identity':
        receipt['sources']['dna.fa.gz']['identity'] = '0' * 64
    elif malformation == 'source-trust':
        receipt['sources']['dna.fa.gz']['verified'] = False
    elif malformation == 'source-inventory':
        receipt['sources'] = {}
    elif malformation == 'transform':
        receipt['definition']['transform'] = {'version': ''}
    elif malformation == 'output-digest':
        receipt['outputs']['dna.fa']['sha256'] = None
    elif malformation == 'transport':
        receipt['sources']['dna.fa.gz']['transport'] = {'header': 'ETag', 'value': 'W/"weak"'}
    else:
        receipt['definition'] = None
    manifest.write_text(json.dumps(receipt))
    assert inspected(store, sources, outputs, verify_files=False).status == 'invalid'
    assert inspect_materialization(store, verify_files=False).status == 'invalid'


def test_inspection_permission_error_is_inaccessible(tmp_path, sources, outputs, monkeypatch):
    store = tmp_path / 'dna'
    install(store, sources, outputs)
    def permission_denied(*args, **kwargs):
        raise PermissionError('cannot read receipt')
    monkeypatch.setattr(module, 'read_json', permission_denied)
    assert inspected(store, sources, outputs).status == 'inaccessible'
    with pytest.raises(PermissionError):
        install(store, sources, outputs)


@pytest.mark.parametrize('bad_sources', [
    {}, {'a': {}}, {'a': {'url': 'https://example.test', 'path': 'local'}},
    {'a': {'url': ''}}, {'a': {'url': 'unsupported://example.test'}},
    {'a': {'url': 'file://remote/file'}}, {'a': {'url': 'file:///file?query'}},
    {'a': {'url': 'https://example.test', 'decompress': True}},
    {'a': {'path': 'local'}, '.a.datacache.json': {'path': 'local'}},
    {'a': {'path': 'local'}, '.a.datacache.json/extra': {'path': 'local'}},
])
def test_invalid_source_definitions_fail_without_writes(tmp_path, outputs, bad_sources):
    store = tmp_path / 'missing' / 'dna'
    with pytest.raises(ValueError):
        install(store, bad_sources, outputs)
    assert not store.parent.exists()


def test_multiple_inputs_keep_completed_first_source_on_later_failure(tmp_path, sources, outputs, monkeypatch):
    store = tmp_path / 'dna'
    original = Path(sources['dna.fa.gz']['path'])
    sources['auxiliary'] = {'path': tmp_path / 'missing'}
    with pytest.raises(FileNotFoundError):
        install(store, sources, outputs)
    completed = input_dir(store) / 'dna.fa.gz'
    assert completed.read_bytes() == COMPRESSED
    original.unlink()
    Path(sources['auxiliary']['path']).write_bytes(b'auxiliary')
    paths = install(store, sources, outputs)
    assert Path(paths['dna.fa']).read_bytes() == DATA
    assert set(inspected(store, sources, outputs).sources) == {'dna.fa.gz', 'auxiliary'}


def test_explicitly_empty_input_and_output(tmp_path):
    source = tmp_path / 'empty'
    source.touch()
    sources = {'empty': dict(path=source, **metadata(b''))}
    outputs = {'empty-result': metadata(b'')}
    def empty(inputs, targets):
        Path(targets['empty-result']).touch()
    store = tmp_path / 'artifact'
    materialize(store, sources, outputs=outputs, transform={'version': '1'}, builder=empty,
                download_options={'show_progress': False})
    assert inspect_materialization(store, sources, transform={'version': '1'}, outputs=outputs).verified
    assert source.exists()


def test_progress_defaults_on_and_existing_hits_stay_quiet(tmp_path, sources, outputs, monkeypatch):
    enabled = []
    original = module.Progress
    def progress(enabled_flag, *args, **options):
        enabled.append(enabled_flag)
        return original(False, *args, **options)
    monkeypatch.setattr(module, 'Progress', progress)
    store = tmp_path / 'dna'
    paths = install(store, sources, outputs, download_options={})
    assert any(enabled)
    enabled.clear()
    assert install(store, sources, outputs, download_options={}) == paths
    assert not any(enabled)


def test_builder_paths_stay_absolute_after_working_directory_change(tmp_path, sources, outputs, monkeypatch):
    monkeypatch.chdir(tmp_path)
    other = tmp_path / 'other'
    other.mkdir()
    def changing_directory(inputs, targets):
        assert all(Path(path).is_absolute() for path in [*inputs.values(), *targets.values()])
        monkeypatch.chdir(other)
        build(inputs, targets)
    paths = install(Path('relative') / 'dna', sources, outputs, builder=changing_directory)
    assert Path(paths['dna.fa']).read_bytes() == DATA
