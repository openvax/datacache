"""Raw downloads keep payload bytes at exact destinations without conversion."""

import gzip
from hashlib import sha256
from pathlib import Path
import zipfile

import pytest

from datacache import Cache, FileValidationError, fetch_file, inspect_file
from datacache import download, provenance


@pytest.fixture(params=['gz', 'zip', 'html'])
def source(tmp_path, request):
    path = tmp_path / ('source.' + request.param)
    payload = b'<table><tr><th>name</th></tr><tr><td>gene</td></tr></table>'
    if request.param == 'gz':
        path.write_bytes(gzip.compress(payload))
    elif request.param == 'zip':
        with zipfile.ZipFile(path, 'w') as archive:
            archive.writestr('inside.txt', payload)
    else:
        path.write_bytes(payload)
    return path


@pytest.mark.parametrize('api', ['destination', 'filename', 'cache', 'inferred'])
def test_raw_payload_validation_progress_provenance_and_reuse(source, tmp_path, api):
    payload = source.read_bytes()
    digest = sha256(payload).hexdigest()
    events = []
    options = dict(raw=True, expected_sha256=digest, expected_size=len(payload),
                   record_provenance=True, chunk_size=7,
                   progress_callback=lambda *args: events.append(args))
    if api == 'destination':
        call = lambda: fetch_file(source.as_uri(), destination=tmp_path / 'result.csv', **options)
    elif api == 'filename':
        call = lambda: fetch_file(source.as_uri(), filename='result.csv',
                                 cache_root=tmp_path / 'cache', **options)
    elif api == 'cache':
        cache = Cache(cache_root=tmp_path / 'cache')
        call = lambda: cache.fetch(source.as_uri(), filename='result.csv', **options)
    else:
        call = lambda: fetch_file(source.as_uri(), cache_root=tmp_path / 'cache', **options)
    path = Path(call())
    assert path.read_bytes() == payload
    assert api == 'inferred' or path.name == 'result.csv'
    assert events[-1] == (len(payload), len(payload))
    info = inspect_file(path, expected_sha256=digest)
    assert info.verified
    assert info.recorded_sha256 == digest
    record = Path(provenance.sidecar_path(path)).read_bytes()
    events.clear()
    source.unlink()
    assert call() == str(path)
    assert not events
    assert Path(provenance.sidecar_path(path)).read_bytes() == record
    if api == 'cache':
        cache.delete_url(source.as_uri())
        assert not path.exists()
        assert not Path(provenance.sidecar_path(path)).exists()


def test_raw_validation_failure_preserves_destination_and_provenance(source, tmp_path):
    dest = tmp_path / 'result.csv'
    fetch_file(source.as_uri(), destination=dest, raw=True, record_provenance=True)
    original = dest.read_bytes()
    record = Path(provenance.sidecar_path(dest)).read_bytes()
    source.write_bytes(b'changed source')
    with pytest.raises(FileValidationError, match='SHA-256'):
        fetch_file(source.as_uri(), destination=dest, raw=True, force=True,
                   expected_sha256=sha256(original).hexdigest(), record_provenance=True)
    assert dest.read_bytes() == original
    assert Path(provenance.sidecar_path(dest)).read_bytes() == record
    assert not list(tmp_path.glob('.datacache-*'))


def test_raw_html_does_not_invoke_parser(tmp_path, monkeypatch):
    source = tmp_path / 'not-a-table.html'
    source.write_bytes(b'<!doctype html><p>No table to convert</p>')
    def fail(*args, **kwargs):
        pytest.fail('raw mode attempted HTML conversion')
    monkeypatch.setattr(download.pd, 'read_html', fail)
    path = fetch_file(source.as_uri(), destination=tmp_path / 'result.csv', raw=True)
    assert Path(path).read_bytes() == source.read_bytes()


@pytest.mark.parametrize('options', [dict(raw=True, decompress=True), dict(raw='yes')])
@pytest.mark.parametrize('api', ['fetch_file', 'cache'])
def test_invalid_raw_options_have_no_side_effects(tmp_path, options, api):
    root = tmp_path / 'missing'
    with pytest.raises(ValueError, match='raw'):
        if api == 'fetch_file':
            fetch_file('https://example.invalid/data.gz', destination=root / 'data', **options)
        else:
            Cache(cache_root=root).fetch('https://example.invalid/data.gz', **options)
    assert not root.exists()
