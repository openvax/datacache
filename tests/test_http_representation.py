"""Keep encoded HTTP source bytes, integrity and partial state in agreement."""

import gzip
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import threading

import pytest

from datacache import Cache, FileValidationError, fetch_file
from datacache import provenance
from datacache.resume import CannotResume, _state_directory

DECODED = b'compressed source data\n' * 200
ENCODED = gzip.compress(DECODED, mtime=0)


@pytest.fixture
def server():
    running = []

    def start(*actions):
        actions = list(actions)
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(dict(self.headers))
                action = actions.pop(0) if actions else {}
                payload = action.get('body', ENCODED)
                offset = int(self.headers.get('Range', 'bytes=0-')[6:-1])
                self.send_response(206 if offset else 200)
                self.send_header('ETag', '"source-v1"')
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Encoding', action.get('encoding', 'x-gzip'))
                body = payload[offset:]
                if offset:
                    self.send_header('Content-Range', 'bytes %d-%d/%d' % (offset, len(payload) - 1, len(payload)))
                if action.get('chunked'):
                    self.send_header('Transfer-Encoding', 'chunked')
                    # Intentionally conflicting: transfer framing takes precedence.
                    self.send_header('Content-Length', '999999')
                else:
                    self.send_header('Content-Length', str(action.get('length', len(body))))
                self.end_headers()
                try:
                    if action.get('chunked'):
                        for index in range(0, len(body), 7):
                            chunk = body[index:index + 7]
                            self.wfile.write(('%x\r\n' % len(chunk)).encode() + chunk + b'\r\n')
                        self.wfile.write(b'0\r\n\r\n')
                    else:
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
        running.append((srv, thread))
        return 'http://127.0.0.1:%d/source.rds' % srv.server_port, requests

    yield start
    for srv, thread in running:
        srv.shutdown()
        srv.server_close()
        thread.join()


def encoded_fetch(url, destination, **kwargs):
    options = dict(raw=True, http_representation='encoded', expected_size=len(ENCODED),
                   expected_sha256=sha256(ENCODED).hexdigest(), max_bytes=len(ENCODED),
                   chunk_size=8, retry_backoff=0, record_provenance=True)
    options.update(kwargs)
    return fetch_file(url, destination=destination, **options)


@pytest.mark.parametrize('chunked', [False, True])
@pytest.mark.parametrize('api', ['fetch_file', 'cache'])
def test_encoded_bytes_hash_size_progress_and_receipt(server, tmp_path, chunked, api):
    url, requests = server({'chunked': chunked})
    dest = tmp_path / 'download.csv'
    events = []
    options = dict(progress_callback=lambda *args: events.append(args))
    if api == 'cache':
        cache = Cache(cache_root=tmp_path)
        result = cache.fetch(url, filename=dest.name, raw=True, http_representation='encoded',
                             expected_size=len(ENCODED), expected_sha256=sha256(ENCODED).hexdigest(),
                             max_bytes=len(ENCODED), record_provenance=True, chunk_size=8, **options)
    else:
        result = encoded_fetch(url, dest, **options)
    assert Path(result).read_bytes() == ENCODED
    assert events[-1] == (len(ENCODED), None if chunked else len(ENCODED))
    assert requests[0]['Accept-Encoding'] == 'identity'
    record = json.loads(Path(provenance.sidecar_path(dest)).read_text())
    assert record['http_representation'] == 'encoded'
    assert record['size'] == len(ENCODED)
    assert record['sha256'] == sha256(ENCODED).hexdigest()
    encoded_fetch(url, dest)
    assert len(requests) == 1


def test_default_raw_still_decodes_http_and_bounds_decoded_bytes(server, tmp_path):
    url, requests = server()
    dest = tmp_path / 'source.rds'
    fetch_file(url, destination=dest, raw=True, expected_sha256=sha256(DECODED).hexdigest(),
               expected_size=len(DECODED), record_provenance=True)
    assert dest.read_bytes() == DECODED
    with pytest.raises(FileValidationError, match='max_bytes'):
        fetch_file(url, destination=dest, raw=True, force=True, max_bytes=len(ENCODED))
    assert dest.read_bytes() == DECODED
    assert len(requests) == 2


@pytest.mark.parametrize('options,match', [
    ({'expected_sha256': '0' * 64}, 'SHA-256'),
    ({'expected_size': len(ENCODED) - 1}, 'size'),
    ({'max_bytes': len(ENCODED) - 1, 'expected_size': None}, 'max_bytes'),
])
@pytest.mark.parametrize('chunked', [False, True])
def test_encoded_validation_preserves_old_file_and_receipt(server, tmp_path, options, match, chunked):
    url, requests = server({}, {'chunked': chunked})
    dest = tmp_path / 'source.rds'
    encoded_fetch(url, dest)
    receipt = Path(provenance.sidecar_path(dest)).read_bytes()
    with pytest.raises(FileValidationError, match=match):
        encoded_fetch(url, dest, force=True, **options)
    assert dest.read_bytes() == ENCODED
    assert Path(provenance.sidecar_path(dest)).read_bytes() == receipt
    assert len(list(tmp_path.iterdir())) == 2
    assert len(requests) == 2


def test_recorded_representation_cannot_be_reused_or_returned_stale(server, tmp_path):
    url, requests = server()
    dest = tmp_path / 'source.rds'
    fetch_file(url, destination=dest, raw=True, record_provenance=True)
    with pytest.raises(FileValidationError, match='representation'):
        fetch_file(url, destination=dest, raw=True, http_representation='encoded')
    assert len(requests) == 1
    with pytest.raises(FileValidationError, match='SHA-256'):
        encoded_fetch(url, dest, expected_sha256='0' * 64, force=True, return_stale_on_error=True)
    assert dest.read_bytes() == DECODED
    encoded_fetch(url, dest, force=True)
    assert dest.read_bytes() == ENCODED


@pytest.mark.parametrize('representation', ['encoded', 'decoded'])
def test_local_override_is_already_in_selected_representation(tmp_path, representation):
    payload = ENCODED if representation == 'encoded' else DECODED
    source = tmp_path / 'mirror.rds'
    source.write_bytes(payload)
    dest = tmp_path / 'source'
    fetch_file(source.as_uri(), destination=dest, raw=True, http_representation=representation,
               expected_sha256=sha256(payload).hexdigest(), expected_size=len(payload),
               max_bytes=len(payload), resume='auto', record_provenance=True)
    assert dest.read_bytes() == payload
    assert json.loads(Path(provenance.sidecar_path(dest)).read_text())['http_representation'] == representation


@pytest.mark.parametrize('options', [
    {'http_representation': None}, {'http_representation': True},
    {'http_representation': 'gzip'}, {'http_representation': 'encoded', 'raw': False},
    {'http_representation': 'encoded', 'decompress': True},
])
def test_invalid_representation_options_fail_before_filesystem_access(tmp_path, options):
    with pytest.raises(ValueError):
        fetch_file('https://invalid.example/source.rds', destination=tmp_path / 'absent' / 'data',
                   **{'raw': True, **options})
    assert list(tmp_path.iterdir()) == []


def test_interrupted_encoded_transfer_retries_from_start(server, tmp_path):
    url, requests = server({'drop': 24})
    encoded_fetch(url, tmp_path / 'source.rds')
    assert len(requests) == 2
    assert all('Range' not in request for request in requests)


def test_encoded_body_with_unusable_length_is_bounded_and_reports_unknown_total(server, tmp_path):
    url, requests = server({'length': 'unknown'})
    events = []
    encoded_fetch(url, tmp_path / 'source.rds', progress_callback=lambda *args: events.append(args))
    assert events[-1] == (len(ENCODED), None)
    assert len(requests) == 1


@pytest.mark.parametrize('failure', ['timeout', 'tls'])
def test_encoded_transport_errors_keep_retry_policy(server, tmp_path, monkeypatch, failure):
    import requests as http
    from urllib3.exceptions import ReadTimeoutError, SSLError

    url, requests = server()
    real_get = http.get
    attempts = []

    def get(*args, **kwargs):
        response = real_get(*args, **kwargs)
        attempts.append(response)
        if len(attempts) == 1:
            def broken_stream(*args, **kwargs):
                if failure == 'timeout':
                    raise ReadTimeoutError(None, url, 'interrupted read')
                raise SSLError('TLS verification failed')
            monkeypatch.setattr(response.raw, 'stream', broken_stream)
        return response

    monkeypatch.setattr(http, 'get', get)
    dest = tmp_path / 'source.rds'
    dest.write_bytes(b'old')
    if failure == 'tls':
        with pytest.raises(http.exceptions.SSLError):
            encoded_fetch(url, dest, force=True)
        assert dest.read_bytes() == b'old'
        assert len(requests) == 1
    else:
        encoded_fetch(url, dest, force=True)
        assert dest.read_bytes() == ENCODED
        assert len(requests) == 2


def test_failed_encoded_provenance_is_optional_and_cannot_leave_old_receipt(server, tmp_path, monkeypatch):
    url, requests = server()
    dest = tmp_path / 'source.rds'
    encoded_fetch(url, dest)
    receipt = Path(provenance.sidecar_path(dest))
    assert receipt.exists()

    def unavailable(*args, **kwargs):
        raise OSError('staging metadata unavailable')

    monkeypatch.setattr(provenance, 'describe', unavailable)
    encoded_fetch(url, dest, force=True)
    assert dest.read_bytes() == ENCODED
    assert not receipt.exists()
    assert len(requests) == 2


@pytest.mark.skipif(os.name != 'posix', reason='POSIX resume locks')
@pytest.mark.parametrize('digest', [None, sha256(ENCODED).hexdigest()])
def test_encoded_resume_counts_encoded_offsets(server, tmp_path, digest):
    url, requests = server({'drop': 24})
    dest = tmp_path / 'source.rds'
    encoded_fetch(url, dest, resume=True, expected_sha256=digest)
    assert requests[1]['Range'] == 'bytes=24-'
    assert requests[1]['If-Range'] == '"source-v1"'
    assert dest.read_bytes() == ENCODED
    assert not (_state_directory(dest) / 'partial').exists()
    assert json.loads(Path(provenance.sidecar_path(dest)).read_text())['http_representation'] == 'encoded'


@pytest.mark.skipif(os.name != 'posix', reason='POSIX resume locks')
def test_resume_restarts_when_content_encoding_changes(server, tmp_path):
    other = b'X' * len(ENCODED)
    url, requests = server({'drop': 24}, {'body': other, 'encoding': 'identity'},
                           {'body': other, 'encoding': 'identity'})
    dest = tmp_path / 'source.rds'
    encoded_fetch(url, dest, resume=True, expected_sha256=None)
    assert requests[1]['Range'] == 'bytes=24-'
    assert 'Range' not in requests[2]
    assert dest.read_bytes() == other


@pytest.mark.skipif(os.name != 'posix', reason='POSIX resume locks')
def test_resume_partials_are_not_shared_between_representations(server, tmp_path):
    other = b'X' * len(ENCODED)
    url, requests = server({'drop': 24}, {'body': other, 'encoding': 'identity'})
    dest = tmp_path / 'source.rds'
    import requests as http
    with pytest.raises(http.exceptions.ChunkedEncodingError):
        encoded_fetch(url, dest, resume=True, max_retries=0, expected_sha256=None)
    fetch_file(url, destination=dest, raw=True, resume=True, expected_size=len(other))
    assert 'Range' not in requests[1]
    assert dest.read_bytes() == other


@pytest.mark.skipif(os.name != 'posix', reason='POSIX resume locks')
def test_default_resume_rejects_encoding_and_auto_decodes_full_response(server, tmp_path):
    url, requests = server()
    dest = tmp_path / 'source.rds'
    with pytest.raises(CannotResume, match='identity Content-Encoding'):
        fetch_file(url, destination=dest, raw=True, resume=True, expected_size=len(DECODED))
    fetch_file(url, destination=dest, raw=True, resume='auto', expected_size=len(DECODED))
    assert dest.read_bytes() == DECODED
