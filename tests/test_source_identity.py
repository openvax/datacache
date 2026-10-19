"""Explicit source assertions authorize reuse, never trusted verification."""

from copy import deepcopy
import gzip
from hashlib import sha256
import json
from pathlib import Path
from types import SimpleNamespace

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


@pytest.mark.parametrize('identity', [None, False, 1, 1.0, '', ['revision', 7], {'z': [], 'a': {'b': None}}])
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
                                    {'nested': float('-inf')}])
def test_invalid_identity_rejected_before_creating_store(artifact, identity):
    artifact.spec['source_identity'] = identity
    with pytest.raises(ValueError):
        artifact.install()
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
        {'example': {'default_version': 'v2', 'versions': {'v2': mirror(assets['data'])}}},
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
