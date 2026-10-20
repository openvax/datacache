"""Explicit source assertions authorize reuse, never trusted verification."""

from copy import deepcopy
import gzip
from hashlib import sha256
import io
import json
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest

from datacache import (
    FileValidationError, VersionedDatasetRegistry, inspect_bundle,
    inspect_materialization, install_bundle, materialize,
)
from datacache import bundle_store, bundles, download, materialization

DATA = b'example dataset\n'
COMPRESSED = gzip.compress(DATA, mtime=0)
IDENTITY = {'provider': 'example', 'revision': '2026.10',
            'unix_sum': '12345 67', 'compressed_size': len(COMPRESSED)}


def forbidden(*args, **kwargs):
    raise AssertionError('cache reuse must not acquire, write, lock, or build')


@pytest.fixture(params=['bundle', 'materialization'])
def artifact(request, tmp_path):
    source = tmp_path / 'source.gz'
    source.write_bytes(COMPRESSED)
    store = tmp_path / 'store'
    spec = {'path': source, 'source_identity': deepcopy(IDENTITY)}

    def builder(inputs, outputs):
        with gzip.open(inputs['data'], 'rb') as compressed:
            Path(outputs['data']).write_bytes(compressed.read())

    if request.param == 'bundle':
        spec['decompress'] = True

        def install(specs=None, **options):
            return install_bundle(store, specs or {'data': spec}, verified=False, **options)

        def inspect(specs=None, **options):
            return inspect_bundle(store, specs or {'data': spec}, **options)
    else:
        def install(specs=None, **options):
            settings = dict(transform={'version': 'decode-1'}, outputs={'data': {}}, builder=builder,
                            download_options={'show_progress': False})
            settings.update(options)
            return materialize(store, specs or {'data': spec}, **settings)

        def inspect(specs=None, **options):
            settings = dict(transform={'version': 'decode-1'}, outputs={'data': {}})
            settings.update(options)
            return inspect_materialization(store, specs or {'data': spec}, **settings)

    return SimpleNamespace(kind=request.param, source=source, store=store, spec=spec,
                           install=install, inspect=inspect)


def mirror(spec):
    result = dict(spec, url='https://mirror.example.test/release/data.gz?token=renewed')
    result.pop('path', None)
    return {'data': result}


def receipt_path(artifact):
    return Path(artifact.inspect().bundle) / bundle_store.MANIFEST


@pytest.mark.parametrize('verify_files', [True, False])
def test_equal_identity_reuses_offline_without_mutating_provenance(artifact, monkeypatch, verify_files):
    paths = artifact.install()
    manifest = receipt_path(artifact)
    original = manifest.read_bytes()
    artifact.source.unlink()
    requested = mirror(artifact.spec)
    requested['data']['source_identity'] = dict(reversed(list(IDENTITY.items())))
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    monkeypatch.setattr(bundle_store.BundleStore, 'lock', forbidden)
    monkeypatch.setattr(bundles, 'write_json', forbidden)
    monkeypatch.setattr(materialization, 'write_json', forbidden)
    monkeypatch.setattr(Path, 'mkdir', forbidden)
    assert artifact.install(requested, verify_files=verify_files) == paths
    state = artifact.inspect(requested, verify_files=verify_files)
    assert state.status == 'available' and not state.verified
    assert not state.files['data'].verified
    assert Path(paths['data']).read_bytes() == DATA
    assert manifest.read_bytes() == original
    if artifact.kind == 'materialization':
        record = state.sources['data']
        assert record['source_identity'] == IDENTITY
        assert record['origin'] == artifact.source.as_uri()
        assert record['size'] == len(COMPRESSED)
        assert record['sha256'] == sha256(COMPRESSED).hexdigest()
        assert record['verified'] is False
    else:
        record = json.loads(original)['assets']['data']
        assert record['source_identity'] == IDENTITY
        assert record['url'] == artifact.source.as_uri()
        assert record['size'] == len(DATA)
        assert record['sha256'] == sha256(DATA).hexdigest()


@pytest.mark.parametrize('pinned', [False, True])
def test_changed_identity_rejects_reuse_even_with_trusted_hashes(artifact, pinned):
    if pinned:
        content = DATA if artifact.kind == 'bundle' else COMPRESSED
        artifact.spec.update(sha256=sha256(content).hexdigest(), size=len(content))
    paths = artifact.install()
    changed = {'data': deepcopy(artifact.spec)}
    changed['data']['source_identity']['unix_sum'] = 'different revision'
    assert artifact.inspect(changed).status == 'invalid'
    with pytest.raises(FileValidationError, match='force=True'):
        artifact.install(changed)
    refreshed = artifact.install(changed, force=True)
    assert refreshed != paths
    assert Path(paths['data']).read_bytes() == DATA
    assert artifact.inspect(changed).status == 'available'


def test_same_identity_never_ignores_changed_integrity_expectations(artifact):
    artifact.install()
    for expectation in ({'sha256': '0' * 64}, {'size': 123456}):
        changed = mirror(dict(artifact.spec, **expectation))
        assert artifact.inspect(changed).status == 'invalid'


def test_same_identity_never_ignores_output_affecting_settings(artifact):
    artifact.install()
    requested = mirror(artifact.spec)
    if artifact.kind == 'bundle':
        requested['data']['decompress'] = False
        assert artifact.inspect(requested).status == 'invalid'
    else:
        assert artifact.inspect(requested, transform={'version': 'decode-2'}).status == 'invalid'
        assert artifact.inspect(requested, transform={'version': 'decode-1', 'options': {'mode': 'other'}}).status == 'invalid'
        assert artifact.inspect(requested, outputs={'another': {}}).status == 'invalid'


@pytest.mark.parametrize('extension', ['gz', 'zip', 'html'])
@pytest.mark.parametrize('decompress', [False, True])
@pytest.mark.parametrize('reverse', [False, True])
def test_bundle_identity_rejects_changed_effective_transform(tmp_path, extension, decompress, reverse):
    decoded = DATA
    if extension == 'gz':
        encoded = COMPRESSED
    elif extension == 'zip':
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            archive.writestr('data.csv', DATA)
        encoded = buffer.getvalue()
    else:
        encoded = b'<table><tr><th>value</th></tr><tr><td>example</td></tr></table>'
        decoded = b'value\nexample\n'
    source, mirror_path = tmp_path / ('source.' + extension), tmp_path / 'download'
    source.write_bytes(encoded)
    mirror_path.write_bytes(encoded)
    original, changed = [
        {'data.csv': {'path': path, 'source_identity': IDENTITY, 'decompress': decompress}}
        for path in ((mirror_path, source) if reverse else (source, mirror_path))]
    store = tmp_path / 'store'
    paths = install_bundle(store, original, verified=False)
    assert Path(paths['data.csv']).read_bytes() == (encoded if reverse else decoded)
    for verify_files in (False, True):
        assert inspect_bundle(store, changed, verify_files=verify_files).status == 'invalid'
    with pytest.raises(FileValidationError, match='force=True'):
        install_bundle(store, changed, verified=False)
    refreshed = install_bundle(store, changed, verified=False, force=True)
    fresh = install_bundle(tmp_path / 'fresh', changed, verified=False)
    assert Path(refreshed['data.csv']).read_bytes() == Path(fresh['data.csv']).read_bytes()
    assert Path(refreshed['data.csv']).read_bytes() == (decoded if reverse else encoded)
    assert Path(paths['data.csv']).read_bytes() == (encoded if reverse else decoded)


@pytest.mark.parametrize('url', ['https://example.test/source.gz?token=secret',
                                 'https://example.test/download?filename=source.gz'])
def test_bundle_identity_tracks_query_selected_transform_without_recording_query(tmp_path, monkeypatch, url):
    monkeypatch.setattr(download, '_stream_to_file', lambda url, handle, **kwargs: handle.write(COMPRESSED))
    store = tmp_path / 'store'
    assets = {'data': {'url': url, 'source_identity': IDENTITY}}
    paths = install_bundle(store, assets, verified=False)
    assert Path(paths['data']).read_bytes() == DATA
    manifest = (Path(paths['data']).parent / bundle_store.MANIFEST).read_text()
    assert '?' not in manifest and 'secret' not in manifest
    assert json.loads(manifest)['source_transforms'] == {'data': 'gunzip'}
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    mirrored = {'data': dict(assets['data'], url='https://mirror.test/get?filename=data.gz')}
    assert install_bundle(store, mirrored, verified=False) == paths
    for filename in ('data', 'data.zip', 'data.gz&token=secret'):
        changed = {'data': dict(assets['data'], url='https://mirror.test/get?filename=' + filename)}
        assert inspect_bundle(store, changed).status == 'invalid'


@pytest.mark.parametrize(('original', 'changed', 'name', 'content'), [
    ('data.txt', 'data.bin', 'data', DATA),
    ('data.html', 'data.htm', 'data.csv', b'<table><tr><th>x</th></tr><tr><td>1</td></tr></table>'),
    ('data.gz', 'download', 'data.gz', COMPRESSED),
])
def test_bundle_identity_allows_different_suffixes_with_same_transform(tmp_path, monkeypatch, original, changed, name, content):
    source = tmp_path / original
    source.write_bytes(content)
    assets = {name: {'path': source, 'source_identity': IDENTITY}}
    paths = install_bundle(tmp_path / 'store', assets, verified=False)
    source.unlink()
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    requested = {name: {'url': 'https://mirror.test/' + changed, 'source_identity': IDENTITY}}
    assert install_bundle(tmp_path / 'store', requested, verified=False) == paths


@pytest.mark.parametrize('transforms', [None, {}, {'data': None}, {'data': 'unknown'}, {'data': ['gunzip']}])
def test_bundle_identity_requires_valid_transform_receipt(tmp_path, transforms):
    source = tmp_path / 'source.gz'
    source.write_bytes(COMPRESSED)
    store = tmp_path / 'store'
    assets = {'data': {'path': source, 'source_identity': IDENTITY}}
    paths = install_bundle(store, assets, verified=False)
    manifest = Path(paths['data']).parent / bundle_store.MANIFEST
    receipt = json.loads(manifest.read_text())
    receipt['source_transforms'] = transforms
    manifest.write_text(json.dumps(receipt))
    assert inspect_bundle(store).status == 'invalid'
    assert inspect_bundle(store, assets, verify_files=False).status == 'invalid'
    with pytest.raises(FileValidationError, match='force=True'):
        install_bundle(store, assets, verified=False)
    install_bundle(store, assets, verified=False, force=True)
    assert inspect_bundle(store, assets).status == 'available'


@pytest.mark.parametrize('pinned', [False, True])
def test_bundle_identity_without_transform_receipt_only_reuses_original_url(tmp_path, monkeypatch, pinned):
    url = 'https://example.test/download?filename=data.gz'
    monkeypatch.setattr(download, '_stream_to_file', lambda url, handle, **kwargs: handle.write(COMPRESSED))
    store = tmp_path / 'store'
    spec = {'url': url, 'source_identity': IDENTITY}
    if pinned:
        spec.update(sha256=sha256(DATA).hexdigest(), size=len(DATA))
    assets = {'data': spec}
    paths = install_bundle(store, assets, verified=pinned)
    manifest = Path(paths['data']).parent / bundle_store.MANIFEST
    receipt = json.loads(manifest.read_text())
    del receipt['source_transforms']
    manifest.write_text(json.dumps(receipt))
    unchanged = manifest.read_bytes()
    with monkeypatch.context() as guard:
        guard.setattr(download, 'fetch_file', forbidden)
        assert inspect_bundle(store).status == 'available'
        assert install_bundle(store, assets, verified=pinned) == paths
        assert inspect_bundle(store, mirror(spec)).status == 'invalid'
        with pytest.raises(FileValidationError, match='force=True'):
            install_bundle(store, mirror(spec), verified=pinned)
    assert manifest.read_bytes() == unchanged
    refreshed = install_bundle(store, assets, verified=pinned, force=True)
    assert refreshed != paths
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    assert install_bundle(store, mirror(spec), verified=pinned) == refreshed


def test_bundle_identity_with_predownloaded_output_keeps_declared_transform(tmp_path, monkeypatch):
    source = tmp_path / 'data'
    source.write_bytes(DATA)
    store = tmp_path / 'store'
    assets = {'data': {'url': 'https://example.test/download?filename=data.gz', 'source_identity': IDENTITY}}
    paths = install_bundle(store, assets, verified=False, source_paths={'data': source})
    assert Path(paths['data']).read_bytes() == DATA
    source.unlink()
    monkeypatch.setattr(download, 'fetch_file', forbidden)
    assert install_bundle(store, mirror(assets['data']), verified=False) == paths


@pytest.mark.parametrize('pinned', [False, True])
def test_legacy_receipt_needs_explicit_refresh_to_add_identity(artifact, pinned):
    legacy = dict(artifact.spec)
    del legacy['source_identity']
    if pinned:
        content = DATA if artifact.kind == 'bundle' else COMPRESSED
        legacy.update(sha256=sha256(content).hexdigest(), size=len(content))
        artifact.spec.update(sha256=legacy['sha256'], size=legacy['size'])
    old = artifact.install({'data': legacy})
    assert artifact.inspect({'data': legacy}).status == 'available'
    assert artifact.inspect().status == 'invalid'
    with pytest.raises(FileValidationError, match='force=True'):
        artifact.install()
    refreshed = artifact.install(force=True)
    assert refreshed != old
    assert artifact.inspect(mirror(artifact.spec)).status == 'available'


def test_without_declared_identity_url_behavior_is_preserved(artifact):
    artifact.install()
    unasserted = dict(artifact.spec)
    del unasserted['source_identity']
    # A caller who did not opt in still needs the original full URL/path.
    assert artifact.inspect({'data': unasserted}).status == 'available'
    assert artifact.inspect(mirror(unasserted)).status == 'invalid'


def test_full_inspection_detects_same_size_corruption_after_identity_reuse(artifact):
    paths = artifact.install()
    Path(paths['data']).write_bytes(b'x' * len(DATA))
    requested = mirror(artifact.spec)
    assert artifact.inspect(requested, verify_files=False).status == 'available'
    assert artifact.inspect(requested).status == 'invalid'
    with pytest.raises(FileValidationError, match='force=True'):
        artifact.install(requested)


@pytest.mark.parametrize('identity', [None, False, 1, 1.0, '', ['revision', 7], {'z': [], 'a': {'b': None}},
                                    {'\U0001f600': 'unicode key', 'revision': '\U0001f600'}])
def test_json_values_roundtrip_without_confusing_absence_with_null(artifact, identity):
    artifact.spec['source_identity'] = identity
    artifact.install()
    assert artifact.inspect(mirror(artifact.spec)).status == 'available'


@pytest.mark.parametrize(('original', 'changed'), [(True, 1), (1, 1.0), (False, 0), (None, 'null'),
                                                 ({'revision': True}, {'revision': 1})])
def test_identity_comparison_preserves_json_types(artifact, original, changed):
    artifact.spec['source_identity'] = original
    artifact.install()
    assert artifact.inspect({'data': dict(artifact.spec, source_identity=changed)}).status == 'invalid'


@pytest.mark.parametrize('identity', [float('nan'), float('inf'), (1, 2), {1: 'integer key'},
                                    {'nested': {None: 'key'}}, {'nested': (1, 2)}, b'bytes',
                                    pytest.param('x' * (64 * 1024), id='oversized'),
                                    pytest.param({'\U0001f600': 'kept', '\ud83d\ude00': 'lost'}, id='colliding-keys'),
                                    pytest.param({'nested': [{'\U0001f600': 'kept', '\ud83d\ude00': 'lost'}]}, id='nested-colliding-keys'),
                                    {'nested': float('-inf')}])
def test_invalid_identity_rejected_before_creating_store(artifact, identity):
    artifact.spec['source_identity'] = identity
    with pytest.raises(ValueError):
        artifact.install()
    with pytest.raises(ValueError):
        artifact.inspect()
    assert not artifact.store.exists()


def test_cyclic_identity_rejected_before_creating_store(artifact):
    identity = []
    identity.append(identity)
    artifact.spec['source_identity'] = identity
    with pytest.raises(ValueError, match='nesting'):
        artifact.install()
    assert not artifact.store.exists()


@pytest.mark.parametrize('malformed', ['nan', 'large', 'duplicate-key', 'missing-fingerprint'])
def test_malformed_receipt_cannot_authorize_reuse(artifact, malformed):
    artifact.install()
    manifest = receipt_path(artifact)
    receipt = json.loads(manifest.read_text())
    spec = (receipt['assets']['data'] if artifact.kind == 'bundle'
            else receipt['definition']['sources']['data'])
    if malformed == 'nan':
        spec['source_identity'] = float('nan')
    elif malformed == 'large':
        spec['source_identity'] = 'x' * (64 * 1024)
    elif malformed == 'missing-fingerprint':
        if artifact.kind == 'bundle':
            del receipt['source_fingerprints']
        else:
            del spec['identity']
    else:
        text = manifest.read_text().replace('"source_identity": {', '"source_identity": {"provider": "conflict",', 1)
        manifest.write_text(text)
    if malformed != 'duplicate-key':
        manifest.write_text(json.dumps(receipt))
    inspect = inspect_bundle if artifact.kind == 'bundle' else inspect_materialization
    assert inspect(artifact.store).status == 'invalid'
    assert artifact.inspect(mirror(artifact.spec), verify_files=False).status == 'invalid'


@pytest.mark.parametrize('change', ['missing', 'different', 'extra'])
def test_materialization_source_record_must_agree_with_definition(tmp_path, change):
    source = tmp_path / 'source'
    source.write_bytes(DATA)
    sources = {'data': {'path': source, 'source_identity': IDENTITY}}
    store = tmp_path / 'store'
    options = dict(transform={'version': 'copy-1'}, outputs={'data': {}},
                   builder=lambda inputs, outputs: Path(outputs['data']).write_bytes(Path(inputs['data']).read_bytes()))
    paths = materialize(store, sources, **options)
    manifest = Path(paths['data']).parent / bundle_store.MANIFEST
    receipt = json.loads(manifest.read_text())
    if change == 'missing':
        del receipt['sources']['data']['source_identity']
    elif change == 'different':
        receipt['sources']['data']['source_identity'] = 'different'
    else:
        del receipt['definition']['sources']['data']['source_identity']
    manifest.write_text(json.dumps(receipt))
    assert inspect_materialization(store).status == 'invalid'


def test_registry_detaches_caller_identity_and_reuses_across_registries(tmp_path):
    source = tmp_path / 'source'
    source.write_bytes(DATA)
    identity = deepcopy(IDENTITY)
    assets = {'data': {'path': source, 'source_identity': identity}}
    mapping = {'example': {'default_version': 'v1', 'versions': {'v1': assets}}}
    registry = VersionedDatasetRegistry(mapping, store_path=lambda name, version: tmp_path / 'shared', verified=False)
    other = VersionedDatasetRegistry(
        {'example': {'default_version': 'v2', 'versions': {'v2': {
            'data': {'url': 'https://mirror.example.test/source', 'source_identity': identity}}}}},
        store_path=lambda name, version: tmp_path / 'shared', verified=False)
    identity['unix_sum'] = 'caller changed its dictionary'
    paths = registry.download('example')
    source.unlink()
    assert other.download('example') == paths
    manifest = json.loads((Path(paths['data']).parent / bundle_store.MANIFEST).read_text())
    assert manifest['assets']['data']['source_identity'] == IDENTITY


def test_source_identity_does_not_relax_resume_requirements(artifact):
    requested = mirror(artifact.spec)
    if artifact.kind == 'bundle':
        requested['data']['decompress'] = False
    with pytest.raises(ValueError, match='expected_size'):
        artifact.install(requested, download_options={'resume': True})
    assert not artifact.store.exists()


def test_trusted_hashes_still_verify_bytes_with_declared_identity(artifact):
    content = DATA if artifact.kind == 'bundle' else COMPRESSED
    artifact.spec.update(sha256=sha256(content).hexdigest(), size=len(content))
    options = {} if artifact.kind == 'bundle' else {'outputs': {'data': {'sha256': sha256(DATA).hexdigest()}}}
    artifact.install(**options)
    state = artifact.inspect(mirror(artifact.spec), **options)
    assert state.status == 'available' and state.verified
    assert state.files['data'].verified
    if artifact.kind == 'materialization':
        assert state.sources['data']['verified'] is True


@pytest.mark.parametrize('kind', ['bundle', 'materialization'])
def test_identity_preserves_redacted_actual_origin_and_fingerprint(tmp_path, monkeypatch, kind):
    url = 'https://user:secret@example.test/source?token=secret#private'
    monkeypatch.setattr(download, '_stream_to_file', lambda url, handle, **kwargs: handle.write(DATA))
    store = tmp_path / 'store'
    sources = {'data': {'url': url, 'source_identity': IDENTITY}}
    if kind == 'bundle':
        paths = install_bundle(store, sources, verified=False)
    else:
        paths = materialize(store, sources, transform={'version': 'copy-1'}, outputs={'data': {}},
                            builder=lambda inputs, outputs: Path(outputs['data']).write_bytes(Path(inputs['data']).read_bytes()))
    text = (Path(paths['data']).parent / bundle_store.MANIFEST).read_text()
    assert 'secret' not in text and 'token=' not in text and 'private' not in text
    receipt = json.loads(text)
    if kind == 'bundle':
        assert receipt['source_fingerprints']['data'] == sha256(url.encode()).hexdigest()
        assert receipt['assets']['data']['url'] == 'https://example.test/source'
    else:
        expected_fingerprint = sha256(json.dumps(url).encode()).hexdigest()
        for record in (receipt['sources']['data'], receipt['definition']['sources']['data']):
            assert record['identity'] == expected_fingerprint
            assert record['origin'] == 'https://example.test/source'
            assert record['source_identity'] == IDENTITY
