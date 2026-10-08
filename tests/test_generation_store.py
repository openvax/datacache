"""The store layout shared by bundles, archive trees and materializations."""

from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path

import pytest

from datacache import FileValidationError, install_archive, install_bundle, materialize
from datacache import generation_store
from datacache.download import validate_download_options
from datacache.generation_store import (
    CURRENT, GenerationStore, StoreKind, check_tree, list_tree, local_file_identity, observe_file,
    validate_distinct_paths, validate_file_record, validate_no_sidecar_collisions, validate_path_component,
    validate_relative_name,
)

pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX permissions and links')

ADOPTING = StoreKind(name='example', marker='.datacache-example.json', marker_contents={'format': 1},
                     receipt='.datacache-receipt.json', created_at='created_at')
REFUSING = StoreKind(name='strict', marker='.datacache-strict.json', marker_contents={'format': 1},
                     receipt='.datacache-receipt.json', created_at='created_at',
                     adopts_empty_directory=False)


def add_generation(store, created_at, payload=b'data'):
    staged = store.path / '.staging'
    staged.mkdir()
    (staged / 'file.txt').write_bytes(payload)
    (staged / store.kind.receipt).write_text(json.dumps({'created_at': created_at.isoformat()}))
    return store.add_generation(staged)


def test_state_follows_creation_generation_and_publication(tmp_path):
    store = GenerationStore(tmp_path / 'store', ADOPTING)
    assert store.read_state() == ('missing', None)
    store.create()
    assert store.read_state() == ('missing', None)
    generation = add_generation(store, datetime.now(timezone.utc))
    # A generation without current.json is what an interrupted install leaves.
    assert store.read_state() == ('recovery-required', None)
    store.publish(generation)
    assert store.read_state() == ('published', generation)
    assert json.loads((store.path / CURRENT).read_text()) == {'generation': generation.name}


def test_empty_directories_are_adopted_only_by_kinds_that_allow_it(tmp_path):
    (tmp_path / 'adopting').mkdir(mode=0o750)
    adopting = GenerationStore(tmp_path / 'adopting', ADOPTING)
    assert adopting.read_state().status == 'missing'
    adopting.create()
    assert (adopting.path.stat().st_mode & 0o777) == 0o750
    adopting.check()

    (tmp_path / 'refusing').mkdir()
    refusing = GenerationStore(tmp_path / 'refusing', REFUSING)
    with pytest.raises(FileNotFoundError):
        refusing.read_state()
    with pytest.raises(FileValidationError, match='never taken over'):
        refusing.refuse_foreign_directory()
    with pytest.raises(FileValidationError, match='never taken over'):
        refusing.create()
    assert list(refusing.path.iterdir()) == []


@pytest.mark.parametrize('kind', [ADOPTING, REFUSING], ids=['adopting', 'refusing'])
def test_populated_directories_are_never_taken_over(tmp_path, kind):
    (tmp_path / 'legacy').mkdir()
    (tmp_path / 'legacy' / 'keep.txt').write_text('keep me')
    store = GenerationStore(tmp_path / 'legacy', kind)
    with pytest.raises(FileValidationError, match='never taken over'):
        store.create()
    assert [path.name for path in store.path.iterdir()] == ['keep.txt']


def test_a_store_of_another_kind_is_unrecognized(tmp_path):
    GenerationStore(tmp_path / 'store', ADOPTING).create()
    other = StoreKind(name='other', marker=ADOPTING.marker, marker_contents={'format': 2},
                      receipt='.datacache-receipt.json', created_at='created_at')
    with pytest.raises(FileValidationError, match='unrecognized other store'):
        GenerationStore(tmp_path / 'store', other).read_state()


def test_pointers_and_publication_stay_inside_generations(tmp_path):
    store = GenerationStore(tmp_path / 'store', ADOPTING)
    store.create()
    (store.path / CURRENT).write_text(json.dumps({'generation': '../../outside'}))
    with pytest.raises(FileValidationError, match='invalid generation pointer'):
        store.read_state()
    elsewhere = tmp_path / 'elsewhere'
    elsewhere.mkdir()
    with pytest.raises(ValueError, match='not a generation'):
        store.publish(elsewhere)


def test_recovery_publishes_the_newest_generation_that_checks_out(tmp_path):
    store = GenerationStore(tmp_path / 'store', ADOPTING)
    store.create()
    now = datetime.now(timezone.utc)
    oldest = add_generation(store, now - timedelta(days=2), b'oldest')
    middle = add_generation(store, now - timedelta(days=1), b'middle')
    newest = add_generation(store, now, b'corrupt')
    assert store.generation_names_newest_first() == [newest.name, middle.name, oldest.name]

    def inspect(directory):
        payload = (directory / 'file.txt').read_bytes()
        if payload == b'corrupt':
            raise FileValidationError(directory, 'corrupt')
        return payload

    assert store.recover(inspect) == b'middle'
    assert store.read_state() == ('published', middle)

    def reject(directory):
        raise FileValidationError(directory, 'corrupt')

    assert store.recover(reject) is None
    assert store.read_state() == ('published', middle)


def test_trees_are_listed_completely_before_any_file_is_read(tmp_path, monkeypatch):
    root = tmp_path / 'tree'
    (root / 'a' / 'b').mkdir(parents=True)
    (root / 'a' / 'b' / 'c.txt').write_bytes(b'abc')
    (root / 'extra.txt').write_bytes(b'x')
    (root / '.datacache-receipt.json').write_text('{}')
    listing = list_tree(root, ignore=('.datacache-receipt.json',))
    assert list(listing.files) == ['a/b/c.txt', 'extra.txt']
    assert listing.directories == ['a', 'a/b']

    def forbidden(*args, **kwargs):
        raise AssertionError('read a file before checking the inventory')

    monkeypatch.setattr(generation_store, 'observe_file', forbidden)
    record = {'sha256': sha256(b'abc').hexdigest(), 'size': 3}
    with pytest.raises(FileValidationError, match=r"missing \[\], unexpected \['extra.txt'\]"):
        check_tree(root, {'a/b/c.txt': record}, ['a', 'a/b'], ignore=('.datacache-receipt.json',))
    with pytest.raises(FileValidationError, match=r"directories differ.*unexpected \['a/b'\]"):
        check_tree(root, {'a/b/c.txt': record, 'extra.txt': record}, ['a'],
                   ignore=('.datacache-receipt.json',))
    monkeypatch.undo()

    (root / 'extra.txt').unlink()
    observed = check_tree(root, {'a/b/c.txt': record}, ['a', 'a/b'], ignore=('.datacache-receipt.json',))
    assert observed['a/b/c.txt'].record == record
    with pytest.raises(FileValidationError, match='SHA-256'):
        check_tree(root, {'a/b/c.txt': dict(record, sha256='0' * 64)}, ['a', 'a/b'],
                   ignore=('.datacache-receipt.json',))


@pytest.mark.parametrize('make', [
    lambda root: (root / 'link').symlink_to(root / 'file.txt'),
    lambda root: (root / 'directory-link').symlink_to(root, target_is_directory=True),
    lambda root: os.mkfifo(root / 'fifo'),
], ids=['file-link', 'directory-link', 'fifo'])
def test_trees_hold_only_regular_files_and_directories(tmp_path, make):
    root = tmp_path / 'tree'
    root.mkdir()
    (root / 'file.txt').write_bytes(b'data')
    make(root)
    with pytest.raises(FileValidationError, match='not a link or special file'):
        list_tree(root)


def test_observing_metadata_only_checks_size_without_reading(tmp_path):
    path = tmp_path / 'file.txt'
    path.write_bytes(b'data')
    observed = observe_file(path, {'sha256': '0' * 64, 'size': 4}, read_contents=False)
    assert observed.sha256 is None and observed.size == 4
    with pytest.raises(FileValidationError, match='size is 4 bytes, expected 5'):
        observe_file(path, {'sha256': None, 'size': 5}, read_contents=False)
    assert observe_file(path).record == {'sha256': sha256(b'data').hexdigest(), 'size': 4}
    linked = tmp_path / 'linked.txt'
    os.link(path, linked)
    with pytest.raises(FileValidationError, match='single link'):
        observe_file(path)


@pytest.mark.parametrize('record', [
    None, {}, {'sha256': None, 'size': 1}, {'sha256': '0' * 64, 'size': None},
    {'sha256': 'xyz', 'size': 1}, {'sha256': '0' * 64, 'size': -1}, {'sha256': '0' * 64, 'size': 1, 'url': ''},
])
def test_file_records_must_identify_observed_bytes(record):
    with pytest.raises(ValueError):
        validate_file_record(record)


@pytest.mark.parametrize('name', [
    '', '/absolute', 'a//b', './a', 'a/..', 'back\\slash', 'trailing.', 'trailing ', 'colon:',
    'CON', 'lpt1.txt', '.datacache-manifest.json', 'nested/.DataCache-x', 7,
])
def test_unsafe_relative_names_are_rejected(name):
    with pytest.raises(ValueError):
        validate_relative_name(name)


def test_name_validation():
    assert validate_relative_name('a/b.c') == 'a/b.c'
    assert validate_path_component('2026-09') == '2026-09'
    with pytest.raises(ValueError, match='single path components'):
        validate_path_component('a/b')
    validate_distinct_paths(['a/b', 'a/c', 'd'])
    for names in (['a', 'A'], ['a', 'a/b'], ['A', 'a/b']):
        with pytest.raises(ValueError, match='collide'):
            validate_distinct_paths(names)
    validate_no_sidecar_collisions(['a.txt', 'dir/b.txt'])
    with pytest.raises(ValueError, match='sidecars'):
        validate_no_sidecar_collisions(['a.txt', '.A.txt.datacache.json'])


def test_local_identity_is_an_absolute_file_url(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert local_file_identity('data/a.fa') == (tmp_path / 'data' / 'a.fa').as_uri()
    assert local_file_identity(Path('/data/a b.fa')) == 'file:///data/a%20b.fa'


def test_download_options_are_checked_like_fetch_file():
    assert validate_download_options(None, 'bundle') == {}
    options = {'timeout': 5, 'resume': True}
    assert validate_download_options(options, 'bundle') == options
    assert validate_download_options(options, 'bundle') is not options
    with pytest.raises(ValueError, match=r"unsupported archive download options: \['force'\]"):
        validate_download_options({'force': True}, 'archive')
    for bad in ({'chunk_size': 0}, {'chunk_size': True}, {'progress_callback': 'print'},
                {'show_progress': 1}, {'resume': 'yes'}, {'max_retries': -1},
                {'retry_backoff': float('inf')}, {'retry_max_delay': -1}):
        with pytest.raises(ValueError):
            validate_download_options(bad, 'bundle')


def test_installs_reject_bad_download_options_before_creating_anything(tmp_path):
    source = tmp_path / 'source.txt'
    source.write_bytes(b'data')
    spec = {'url': source.as_uri(), 'sha256': sha256(b'data').hexdigest(), 'size': 4}
    bad = {'chunk_size': 0}
    with pytest.raises(ValueError, match='chunk_size'):
        install_bundle(tmp_path / 'bundle', {'data.txt': spec}, download_options=bad)
    with pytest.raises(ValueError, match='chunk_size'):
        install_archive(tmp_path / 'archive', dict(spec), download_options=bad)
    with pytest.raises(ValueError, match='chunk_size'):
        materialize(tmp_path / 'derived', {'data.txt': spec}, transform={'version': '1'},
                    outputs={'out.txt': {}}, builder=lambda inputs, outputs: None,
                    download_options=bad)
    assert sorted(path.name for path in tmp_path.iterdir()) == ['source.txt']
