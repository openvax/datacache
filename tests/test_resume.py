"""Real local HTTP transfers, interrupted on deterministic byte boundaries."""

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import os
import socket
import threading

import pytest

from datacache import Cache, FileValidationError, discard_partial, fetch_file, inspect_file
from datacache.resume import _state_directory

PAYLOAD = b'0123456789abcdef' * 8
DIGEST = sha256(PAYLOAD).hexdigest()
pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX resume locks')


@pytest.fixture
def server():
    servers = []

    def start(*actions):
        actions = list(actions)
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(dict(self.headers))
                action = actions.pop(0) if actions else {}
                offset = int(self.headers.get('Range', 'bytes=0-')[6:-1])
                status = action.get('status', 206 if offset else 200)
                if status == 200:
                    offset = 0
                body = action.get('body', PAYLOAD)[offset:]
                self.send_response(status)
                self.send_header('Content-Length', str(len(body)))
                if action.get('etag', '"v1"') is not None:
                    self.send_header('ETag', action.get('etag', '"v1"'))
                if action.get('modified'):
                    self.send_header('Last-Modified', action['modified'])
                if status == 206:
                    self.send_header('Content-Range', action.get(
                        'range', 'bytes %d-%d/%d' % (offset, len(PAYLOAD) - 1, len(PAYLOAD))))
                if action.get('encoding'):
                    self.send_header('Content-Encoding', action['encoding'])
                self.end_headers()
                try:
                    self.wfile.write(body[:action.get('drop', len(body))])
                    self.wfile.flush()
                    if 'drop' in action:
                        self.connection.shutdown(socket.SHUT_RDWR)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass

            def log_message(self, *args):
                pass

        srv = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        servers.append((srv, thread))
        return 'http://127.0.0.1:%d/data' % srv.server_port, requests

    yield start
    for srv, thread in servers:
        srv.shutdown()
        srv.server_close()
        thread.join()


def fetch(url, dest, **options):
    defaults = dict(resume=True, expected_sha256=DIGEST, expected_size=len(PAYLOAD),
                    chunk_size=8, retry_backoff=0)
    defaults.update(options)
    return fetch_file(url, destination=dest, **defaults)


def interrupt(url, dest):
    def callback(done, total):
        if done >= 16:
            raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        fetch(url, dest, progress_callback=callback)


def test_dropped_connection_resumes_and_records(server, tmp_path):
    url, requests = server({'drop': 32})
    events = []
    dest = tmp_path / 'data'
    fetch(url, dest, progress_callback=lambda *args: events.append(args), record_provenance=True)
    assert requests[1]['Range'] == 'bytes=32-'
    assert requests[1]['If-Range'] == '"v1"'
    assert all(r['Accept-Encoding'] == 'identity' for r in requests)
    assert events[-1] == (len(PAYLOAD), len(PAYLOAD))
    assert [e[0] for e in events] == list(range(8, len(PAYLOAD) + 1, 8))
    assert dest.read_bytes() == PAYLOAD
    assert inspect_file(dest).recorded_sha256 == DIGEST
    assert not (_state_directory(dest) / 'partial').exists()


@pytest.mark.parametrize('action', [
    {'status': 200}, {'range': 'bytes 0-127/128'}, {'range': 'nonsense'},
    {'status': 416}, {'etag': '"v2"'},
])
def test_protocol_changes_restart_safely(server, tmp_path, action):
    url, requests = server({}, action)
    dest = tmp_path / 'data'
    interrupt(url, dest)
    fetch(url, dest, max_retries=0)
    assert requests[1]['Range'] == 'bytes=16-'
    assert dest.read_bytes() == PAYLOAD
    if action.get('status') != 200:
        assert 'Range' not in requests[2]


def test_interruption_preserves_private_partial_and_old_destination(server, tmp_path):
    url, requests = server()
    dest = tmp_path / 'data'
    dest.write_bytes(b'old')
    def callback(done, total):
        raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        fetch(url, dest, force=True, progress_callback=callback)
    assert dest.read_bytes() == b'old'
    partial = _state_directory(dest) / 'partial'
    assert partial.read_bytes() == PAYLOAD[:8]
    assert partial.stat().st_mode & 0o077 == 0
    fetch(url, dest, force=True)
    assert requests[1]['Range'] == 'bytes=8-'
    assert dest.read_bytes() == PAYLOAD


def test_discard_is_explicit_and_does_not_touch_destination(server, tmp_path):
    dest = tmp_path / 'data'
    discard_partial(dest)
    assert list(tmp_path.iterdir()) == []
    url, requests = server()
    interrupt(url, dest)
    discard_partial(dest)
    fetch(url, dest)
    assert 'Range' not in requests[1]
    discard_partial(dest)
    assert dest.read_bytes() == PAYLOAD


@pytest.mark.parametrize('content', [PAYLOAD, b'x' * len(PAYLOAD), PAYLOAD + b'oversized'])
def test_complete_partials_validate_or_restart_without_range(server, tmp_path, content):
    url, requests = server()
    dest = tmp_path / 'data'
    interrupt(url, dest)
    (_state_directory(dest) / 'partial').write_bytes(content)
    fetch(url, dest)
    assert len(requests) == (1 if content == PAYLOAD else 2)
    assert all('Range' not in request for request in requests)
    assert dest.read_bytes() == PAYLOAD


def test_integrity_failure_never_replaces_old_file(server, tmp_path):
    url, requests = server({'body': b'x' * len(PAYLOAD)})
    dest = tmp_path / 'data'
    dest.write_bytes(b'old')
    with pytest.raises(FileValidationError, match='SHA-256'):
        fetch(url, dest, force=True)
    assert dest.read_bytes() == b'old'
    assert (_state_directory(dest) / 'partial').stat().st_size == 0
    assert len(requests) == 1


def test_partial_persists_after_exhausted_retries(server, tmp_path):
    url, requests = server({'drop': 24})
    dest = tmp_path / 'data'
    import requests as http
    with pytest.raises(http.exceptions.ChunkedEncodingError):
        fetch(url, dest, max_retries=0)
    assert not dest.exists()
    fetch(url, dest)
    assert requests[1]['Range'] == 'bytes=24-'


def test_concurrent_first_calls_download_once(server, tmp_path):
    url, requests = server()
    dest = tmp_path / 'data'
    with ThreadPoolExecutor(max_workers=4) as pool:
        paths = list(pool.map(lambda _: fetch(url, dest), range(4)))
    assert paths == [str(dest)] * 4
    assert dest.read_bytes() == PAYLOAD
    assert len(requests) == 1


@pytest.mark.parametrize('kind', ['symlink', 'fifo', 'hardlink'])
def test_unsafe_partial_is_rejected(server, tmp_path, kind):
    url, requests = server()
    dest = tmp_path / 'data'
    interrupt(url, dest)
    partial = _state_directory(dest) / 'partial'
    partial.unlink()
    target = tmp_path / 'precious'
    target.write_bytes(b'precious')
    if kind == 'symlink':
        partial.symlink_to(target)
    elif kind == 'fifo':
        os.mkfifo(partial)
    else:
        os.link(target, partial)
    with pytest.raises(FileValidationError):
        fetch(url, dest)
    assert target.read_bytes() == b'precious'
    assert len(requests) == 1


@pytest.mark.parametrize('options', [
    {'expected_sha256': None}, {'expected_size': None}, {'decompress': True},
    {'resume': 'yes'},
])
def test_invalid_resume_options_have_no_side_effects(tmp_path, options):
    dest = tmp_path / 'missing' / 'file'
    with pytest.raises(ValueError):
        fetch('https://example.test/file', dest, **options)
    assert not dest.parent.exists()


def test_cache_forwards_resume(server, tmp_path):
    url, requests = server({'drop': 16})
    path = Cache(cache_root=tmp_path).fetch(url, filename='data', resume=True,
          expected_sha256=DIGEST, expected_size=len(PAYLOAD), chunk_size=8, retry_backoff=0)
    assert Path(path).read_bytes() == PAYLOAD
    assert requests[1]['Range'] == 'bytes=16-'


def test_resumable_bundle_keeps_work_across_calls(server, tmp_path):
    from datacache import install_bundle, inspect_bundle
    url, requests = server()
    dest = tmp_path / 'bundle'
    assets = {'data': dict(url=url, sha256=DIGEST, size=len(PAYLOAD))}
    def callback(done, total):
        if done >= 16:
            raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        install_bundle(dest, assets, download_options=dict(
            resume=True, chunk_size=8, progress_callback=callback))
    assert inspect_bundle(dest, assets).status == 'missing'
    paths = install_bundle(dest, assets, download_options=dict(resume=True, chunk_size=8))
    assert requests[1]['Range'] == 'bytes=16-'
    assert Path(paths['data']).read_bytes() == PAYLOAD
    assert inspect_bundle(dest, assets).verified


def test_encoded_http_body_is_rejected(server, tmp_path):
    url, _ = server({'encoding': 'gzip'})
    dest = tmp_path / 'data'
    with pytest.raises(FileValidationError, match='identity'):
        fetch(url, dest)
    assert not dest.exists()


def test_publication_failure_keeps_partial_private_and_reusable(server, tmp_path, monkeypatch):
    from datacache import download
    url, requests = server()
    dest = tmp_path / 'data'
    original = download._publish_file
    def fail(*args, **kwargs):
        raise OSError('publication failure')
    monkeypatch.setattr(download, '_publish_file', fail)
    with pytest.raises(OSError):
        fetch(url, dest)
    partial = _state_directory(dest) / 'partial'
    assert partial.read_bytes() == PAYLOAD
    assert partial.stat().st_mode & 0o077 == 0
    monkeypatch.setattr(download, '_publish_file', original)
    fetch(url, dest)
    assert len(requests) == 1
    assert dest.read_bytes() == PAYLOAD


def test_changed_integrity_metadata_discards_partial(server, tmp_path):
    url, requests = server()
    dest = tmp_path / 'data'
    interrupt(url, dest)
    with pytest.raises(FileValidationError):
        fetch(url, dest, expected_sha256='0' * 64)
    assert 'Range' not in requests[1]


def test_server_exceeding_size_keeps_disk_usage_bounded(server, tmp_path):
    url, requests = server({'body': PAYLOAD * 2})
    dest = tmp_path / 'data'
    with pytest.raises(FileValidationError, match='exceeds'):
        fetch(url, dest)
    assert not dest.exists()
    assert (_state_directory(dest) / 'partial').stat().st_size == 0


def test_callback_transport_error_is_not_retried(server, tmp_path):
    import requests as http
    url, requests = server()
    def fail(done, total):
        raise http.ConnectionError('callback failed')
    with pytest.raises(http.ConnectionError, match='callback'):
        fetch(url, tmp_path / 'data', progress_callback=fail)
    assert len(requests) == 1


def test_transient_status_uses_bounded_retry_policy(server, tmp_path, monkeypatch):
    from datacache import resume
    url, requests = server({'status': 429}, {'status': 503})
    waits = []
    monkeypatch.setattr(resume.time, 'sleep', waits.append)
    fetch(url, tmp_path / 'data', retry_backoff=2, retry_max_delay=3)
    assert len(requests) == 3
    assert waits == [2, 3]


def test_repeated_wrong_range_is_rejected(server, tmp_path):
    url, requests = server({}, {'range': 'invalid'}, {'status': 206, 'range': 'invalid'})
    dest = tmp_path / 'data'
    interrupt(url, dest)
    with pytest.raises(FileValidationError, match='Content-Range'):
        fetch(url, dest)
    assert not dest.exists()
    assert len(requests) == 3


def test_changed_last_modified_restarts_without_using_a_weak_if_range(server, tmp_path):
    old = 'Tue, 01 Sep 2026 00:00:00 GMT'
    new = 'Wed, 02 Sep 2026 00:00:00 GMT'
    url, requests = server({'etag': None, 'modified': old},
                           {'etag': None, 'modified': new},
                           {'etag': None, 'modified': new})
    dest = tmp_path / 'data'
    interrupt(url, dest)
    fetch(url, dest)
    assert 'Range' in requests[1] and 'If-Range' not in requests[1]
    assert 'Range' not in requests[2]
    assert dest.read_bytes() == PAYLOAD


def test_resumable_bundle_repairs_corrupt_unpublished_asset(server, tmp_path):
    from datacache import install_bundle
    url, requests = server()
    dest = tmp_path / 'bundle'
    assets = {name: dict(url=url, sha256=DIGEST, size=len(PAYLOAD)) for name in ('first', 'second')}
    completed = []
    def callback(done, total):
        if completed:
            raise KeyboardInterrupt
        if done == total:
            completed.append(done)
    with pytest.raises(KeyboardInterrupt):
        install_bundle(dest, assets, download_options=dict(
            resume=True, chunk_size=8, progress_callback=callback))
    working = next(dest.glob('.staging-*'))
    (working / 'first').write_bytes(b'corrupted private working state')
    paths = install_bundle(dest, assets, download_options=dict(resume=True, chunk_size=8))
    assert all(Path(path).read_bytes() == PAYLOAD for path in paths.values())
    assert len(requests) == 4
    assert requests[-1]['Range'] == 'bytes=8-'
