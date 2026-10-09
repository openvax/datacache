"""The store layout shared by bundles, archive trees and materializations."""

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil

import pytest

from datacache import FileValidationError, install_archive, install_bundle, materialize
from datacache import bundle_store
from datacache.bundle_store import (
    MARKER, BundleStore, check_tree, hash_file, is_bundle_name, list_tree, local_file_identity,
    validate_distinct_paths, validate_file_record, validate_no_sidecar_collisions, validate_path_component,
    validate_relative_name,
)
from datacache.download import validate_download_options

pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX permissions and links')


def publish(store, payload=b'data'):
    staged = store.path / '.staging'
    staged.mkdir()
    (staged / 'file.txt').write_bytes(payload)
    return store.publish(staged)


def test_the_newest_bundle_is_current(tmp_path):
    store = BundleStore(tmp_path / 'store', 'bundle')
    assert store.current_bundle() is None
    store.create()
    assert json.loads((store.path / MARKER).read_text()) == {'format': 2, 'kind': 'bundle'}
    assert store.current_bundle() is None
    first = publish(store, b'first')
    second = publish(store, b'second')
    # Two publishes in one second still sort in order.
    assert store.bundle_names() == [first.name, second.name] and first.name < second.name
    assert store.current_bundle() == second
    assert (first / 'file.txt').read_bytes() == b'first'


def test_bundle_names_are_utc_times_that_never_go_backwards(tmp_path, monkeypatch):
    store = BundleStore(tmp_path / 'store', 'bundle')
    store.create()
    now = datetime.now(timezone.utc)
    assert abs(datetime.strptime(publish(store).name, '%Y-%m-%dT%H-%M-%SZ').replace(tzinfo=timezone.utc)
               - now).total_seconds() < 5
    (store.bundles / '2999-01-01T00-00-00Z').mkdir()
    # The clock is behind the newest bundle: the next one still sorts after it.
    assert publish(store).name == '2999-01-01T00-00-01Z'
    assert store.current_bundle().name == '2999-01-01T00-00-01Z'


@pytest.mark.parametrize('name, expected', [
    ('2026-10-08T17-02-42Z', True),
    ('\u0662\u0660\u0662\u0666-10-08T17-02-42Z', False),  # Non-ASCII digits.
    ('2026-1-08T17-02-42Z', False),
    ('2026-13-08T17-02-42Z', False),
    ('2026-10-08T17:02:42Z', False),
    ('notes', False),
])
def test_only_canonical_utc_times_are_bundle_names(name, expected):
    assert is_bundle_name(name) is expected


def test_a_bundle_dated_too_late_to_follow_is_reported(tmp_path):
    store = BundleStore(tmp_path / 'store', 'bundle')
    store.create()
    (store.bundles / '9999-12-31T23-59-59Z').mkdir()
    (store.path / '.staging').mkdir()
    with pytest.raises(FileValidationError, match='no newer name'):
        store.publish(store.path / '.staging')


def test_other_entries_in_bundles_are_ignored(tmp_path):
    store = BundleStore(tmp_path / 'store', 'bundle')
    store.create()
    bundle = publish(store)
    (store.bundles / '.DS_Store').write_text('')
    (store.bundles / 'notes').mkdir()
    assert store.bundle_names() == [bundle.name]
    assert store.current_bundle() == bundle


def test_a_linked_newest_bundle_is_rejected(tmp_path):
    store = BundleStore(tmp_path / 'store', 'bundle')
    store.create()
    publish(store)
    (tmp_path / 'elsewhere').mkdir()
    (store.bundles / '2999-01-01T00-00-00Z').symlink_to(tmp_path / 'elsewhere', target_is_directory=True)
    with pytest.raises(FileValidationError, match='not a link'):
        store.current_bundle()


def test_an_empty_directory_becomes_the_store_in_place(tmp_path):
    (tmp_path / 'store').mkdir(mode=0o750)
    before = (tmp_path / 'store').stat()
    store = BundleStore(tmp_path / 'store', 'archive')
    assert store.current_bundle() is None
    store.create()
    # The same directory, so its owner, group and permissions are kept.
    after = store.path.stat()
    assert (after.st_ino, after.st_dev) == (before.st_ino, before.st_dev)
    assert (after.st_mode & 0o777) == 0o750
    assert sorted(path.name for path in store.path.iterdir()) == [MARKER, 'bundles']
    assert [path.name for path in tmp_path.iterdir()] == ['store']  # No leftover staged marker.


def test_a_store_missing_bundles_has_nothing_installed_and_is_repaired(tmp_path):
    store = BundleStore(tmp_path / 'store', 'bundle')
    store.create()
    publish(store)
    shutil.rmtree(store.bundles)
    # Also what readers see between create() placing the marker and bundles/.
    assert store.current_bundle() is None
    store.create()
    assert store.current_bundle() is None and store.bundles.is_dir()
    assert publish(store) == store.current_bundle()


def test_spellings_of_one_directory_share_a_lock_beside_it(tmp_path, monkeypatch):
    def lock(name):
        return BundleStore(tmp_path / name, 'bundle').lock_path

    assert lock('GRCh38') == lock('grch38')
    assert lock('caf\u00e9') == lock('cafe\u0301')  # NFC and NFD, as macOS may report them.
    assert lock('a') != lock('b') and lock('a').parent == tmp_path
    # A store named "." still locks beside itself, never inside.
    (tmp_path / 'store').mkdir()
    monkeypatch.chdir(tmp_path / 'store')
    assert BundleStore('.', 'archive').lock_path.parent == tmp_path


def test_directories_with_files_are_never_taken_over(tmp_path):
    (tmp_path / 'legacy').mkdir()
    (tmp_path / 'legacy' / 'keep.txt').write_text('keep me')
    store = BundleStore(tmp_path / 'legacy', 'bundle')
    with pytest.raises(FileValidationError, match='never taken over'):
        store.create()
    assert [path.name for path in store.path.iterdir()] == ['keep.txt']


@pytest.mark.parametrize('make', [
    lambda path: path.write_text('a file'),
    lambda path: path.symlink_to(path.parent / 'elsewhere', target_is_directory=True),
], ids=['file', 'link'])
def test_anything_but_a_directory_is_never_taken_over(tmp_path, make):
    (tmp_path / 'elsewhere').mkdir()
    make(tmp_path / 'store')
    store = BundleStore(tmp_path / 'store', 'bundle')
    with pytest.raises(FileValidationError, match='never taken over'):
        store.refuse_takeover()
    with pytest.raises(FileValidationError, match='never taken over'):
        install_bundle(tmp_path / 'store', {'a': {'url': 'https://example.test/a', 'sha256': '0' * 64, 'size': 1}})


def test_datacache_temporary_files_dont_make_a_directory_foreign(tmp_path, monkeypatch):
    (tmp_path / 'store').mkdir()
    (tmp_path / 'store' / '.datacache-json-interrupted').write_text('{')
    store = BundleStore(tmp_path / 'store', 'bundle')
    assert store.current_bundle() is None
    store.create()
    assert store.current_bundle() is None
    # A store named "." is created in place, with nothing left beside it.
    (tmp_path / 'here').mkdir()
    monkeypatch.chdir(tmp_path / 'here')
    BundleStore('.', 'archive').create()
    assert sorted(path.name for path in (tmp_path / 'here').iterdir()) == [MARKER, 'bundles']
    assert sorted(path.name for path in tmp_path.iterdir()) == ['here', 'store']


def test_one_kind_never_uses_another_kinds_store(tmp_path):
    BundleStore(tmp_path / 'store', 'archive').create()
    other = BundleStore(tmp_path / 'store', 'bundle')
    with pytest.raises(FileValidationError, match='not a datacache bundle store'):
        other.current_bundle()
    # Installs check this before suggesting force=True, which can't help.
    with pytest.raises(FileValidationError, match='never taken over'):
        other.refuse_takeover()
    with pytest.raises(FileValidationError, match='never taken over'):
        other.create()


def test_trees_are_listed_completely_before_any_file_is_read(tmp_path, monkeypatch):
    root = tmp_path / 'tree'
    (root / 'a' / 'b').mkdir(parents=True)
    (root / 'a' / 'b' / 'c.txt').write_bytes(b'abc')
    (root / 'extra.txt').write_bytes(b'x')
    (root / '.datacache-manifest.json').write_text('{}')
    listing = list_tree(root, ignore=('.datacache-manifest.json',))
    assert list(listing.files) == ['a/b/c.txt', 'extra.txt']
    assert listing.directories == ['a', 'a/b']

    def forbidden(*args, **kwargs):
        raise AssertionError('read a file before checking the inventory')

    monkeypatch.setattr(bundle_store, 'hash_file', forbidden)
    record = {'sha256': sha256(b'abc').hexdigest(), 'size': 3}
    with pytest.raises(FileValidationError, match=r"missing \[\], unexpected \['extra.txt'\]"):
        check_tree(root, {'a/b/c.txt': record}, ['a', 'a/b'], ignore=('.datacache-manifest.json',))
    with pytest.raises(FileValidationError, match=r"directories differ.*unexpected \['a/b'\]"):
        check_tree(root, {'a/b/c.txt': record, 'extra.txt': record}, ['a'],
                   ignore=('.datacache-manifest.json',))
    monkeypatch.undo()

    (root / 'extra.txt').unlink()
    hashed = check_tree(root, {'a/b/c.txt': record}, ['a', 'a/b'], ignore=('.datacache-manifest.json',))
    assert hashed['a/b/c.txt'].record == record
    with pytest.raises(FileValidationError, match='SHA-256'):
        check_tree(root, {'a/b/c.txt': dict(record, sha256='0' * 64)}, ['a', 'a/b'],
                   ignore=('.datacache-manifest.json',))


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


def test_hashing_can_check_only_the_size(tmp_path):
    path = tmp_path / 'file.txt'
    path.write_bytes(b'data')
    hashed = hash_file(path, {'sha256': '0' * 64, 'size': 4}, hash_contents=False)
    assert hashed.sha256 is None and hashed.size == 4
    with pytest.raises(FileValidationError, match='size is 4 bytes, expected 5'):
        hash_file(path, {'sha256': None, 'size': 5}, hash_contents=False)
    assert hash_file(path).record == {'sha256': sha256(b'data').hexdigest(), 'size': 4}
    os.link(path, tmp_path / 'linked.txt')
    with pytest.raises(FileValidationError, match='single link'):
        hash_file(path)


@pytest.mark.parametrize('record', [
    None, {}, {'sha256': None, 'size': 1}, {'sha256': '0' * 64, 'size': None},
    {'sha256': 'xyz', 'size': 1}, {'sha256': '0' * 64, 'size': -1}, {'sha256': '0' * 64, 'size': 1, 'url': ''},
])
def test_file_records_must_identify_the_bytes(record):
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
    for names in (['a', 'A'], ['a', 'a/b'], ['A', 'a/b'], ['A/x', 'a/y'], ['x', 'x']):
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
    # Resumable installs keep their staging, so unsuitable sources fail first too.
    with pytest.raises(ValueError, match='HTTP'):
        install_bundle(tmp_path / 'bundle', {'data.txt': spec}, download_options={'resume': True})
    with pytest.raises(ValueError, match='raw downloads only'):
        install_bundle(tmp_path / 'bundle', {'data.txt': dict(spec, url='https://example.test/data.gz',
                                                              decompress=True)},
                       download_options={'resume': True})
    with pytest.raises(ValueError, match='HTTP'):
        install_archive(tmp_path / 'archive', dict(spec), download_options={'resume': True})
    assert sorted(path.name for path in tmp_path.iterdir()) == ['source.txt']
