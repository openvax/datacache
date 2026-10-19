"""Atomic first publication where hard links are unsupported."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import errno
from hashlib import sha256
import os
from pathlib import Path
import shutil
import sqlite3
import threading

import pandas as pd
import pytest

from datacache import (connect_if_correct_version, db_from_dataframe,
                       inspect_materialization, materialize)
from datacache import _filesystem
from datacache.database_helpers import _create_cached_db
from datacache.database_table import DatabaseTable


def no_links(*args, **kwargs):
    raise OSError(errno.EPERM, 'hard links unsupported')


@pytest.fixture
def without_links(monkeypatch):
    monkeypatch.setattr(os, 'link', no_links)


@pytest.mark.parametrize('overwrite', [False, True])
def test_new_database_visible_only_after_complete_build(tmp_path, without_links, overwrite):
    dest = tmp_path / 'new.db'

    def rows():
        assert not dest.exists()
        yield (42,)
        assert not dest.exists()

    table = DatabaseTable('records', [('value', 'INT')], rows)
    with closing(_create_cached_db(dest, [table], overwrite=overwrite)) as connection:
        assert connection.execute('SELECT * FROM records').fetchall() == [(42,)]
        assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
    assert list(tmp_path.iterdir()) == [dest]


@pytest.mark.parametrize('symlink', [False, True])
def test_concurrent_creators_reuse_one_complete_database(tmp_path, without_links, symlink):
    path = tmp_path / 'data.db'
    target = tmp_path / 'target.db'
    if symlink:
        path.symlink_to(target.name)
    barrier = threading.Barrier(2, timeout=10)

    def create(value):
        def rows():
            assert not path.exists()
            barrier.wait()
            yield (value,)
        table = DatabaseTable('records', [('value', 'INT')], rows)
        with closing(_create_cached_db(path, [table])) as connection:
            return connection.execute('SELECT value FROM records').fetchone()[0]

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(create, 1)
        second = executor.submit(create, 2)
        assert first.result(timeout=15) == second.result(timeout=15)
    assert set(tmp_path.iterdir()) == ({path, target} if symlink else {path})
    if symlink:
        assert path.is_symlink()


@pytest.mark.parametrize('kind', ['file', 'dangling_link', 'directory'])
def test_native_rename_never_clobbers_existing_entry(tmp_path, kind):
    source = tmp_path / 'staged'
    source.write_bytes(b'candidate')
    dest = tmp_path / 'winner'
    if kind == 'file':
        dest.write_bytes(b'winner')
    elif kind == 'dangling_link':
        dest.symlink_to('missing')
    else:
        dest.mkdir()
    before = dest.lstat()
    with pytest.raises(FileExistsError):
        _filesystem._rename_noreplace(source, dest)
    assert dest.lstat() == before
    assert source.read_bytes() == b'candidate'


@pytest.mark.parametrize('code', [errno.EPERM, errno.ENOTSUP, errno.ENOSYS, errno.EOPNOTSUPP])
def test_unsupported_hardlink_errors_use_native_rename(tmp_path, monkeypatch, code):
    def reject(*args):
        raise OSError(code, 'unsupported')
    monkeypatch.setattr(os, 'link', reject)
    with closing(db_from_dataframe(tmp_path / 'data.db', 'records', pd.DataFrame({'id': [1]}))):
        pass
    assert len(list(tmp_path.iterdir())) == 1


@pytest.mark.parametrize('code', [errno.EACCES, errno.EIO, errno.ENOSPC, errno.EXDEV])
def test_other_link_errors_propagate_without_fallback(tmp_path, monkeypatch, code):
    def reject(*args):
        raise OSError(code, 'failure')
    def forbidden(*args):
        raise AssertionError('must not try rename')
    monkeypatch.setattr(os, 'link', reject)
    monkeypatch.setattr(_filesystem, '_rename_noreplace', forbidden)
    with pytest.raises(OSError) as error:
        db_from_dataframe(tmp_path / 'data.db', 'records', pd.DataFrame({'id': [1]}))
    assert error.value.errno == code
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('code', [errno.ENOTSUP, errno.EINVAL, errno.EPERM, errno.ENOSYS])
def test_no_atomic_primitive_reports_local_build_and_cleans_up(tmp_path, without_links, monkeypatch, code):
    def reject(*args):
        raise OSError(code, 'no exclusive rename')
    monkeypatch.setattr(_filesystem, '_rename_noreplace', reject)
    with pytest.raises(OSError, match='local scratch.*materialize') as error:
        db_from_dataframe(tmp_path / 'data.db', 'records', pd.DataFrame({'id': [1]}))
    assert error.value.errno == errno.ENOTSUP
    assert error.value.__cause__.errno == code
    assert list(tmp_path.iterdir()) == []


def test_failed_build_leaves_no_visible_database(tmp_path, without_links):
    with pytest.raises(sqlite3.IntegrityError):
        db_from_dataframe(tmp_path / 'data.db', 'records', pd.DataFrame({'id': [1, 1]}),
                          primary_key='id')
    assert list(tmp_path.iterdir()) == []


def test_local_build_publishes_portable_bundle_without_volume_links_or_exclusive_rename(tmp_path, monkeypatch):
    local = tmp_path / 'scratch'
    local.mkdir()
    volume = tmp_path / 'volume'
    volume.mkdir()
    real_link = os.link
    real_rename = _filesystem._rename_noreplace

    def local_only(operation):
        def call(source, destination, *args, **kwargs):
            if Path(destination).is_relative_to(volume):
                raise OSError(errno.EPERM, 'unsupported on volume')
            return operation(source, destination, *args, **kwargs)
        return call

    monkeypatch.setattr(os, 'link', local_only(real_link))
    monkeypatch.setattr(_filesystem, '_rename_noreplace', local_only(real_rename))
    source = local / 'records.csv'
    source.write_text('id\n42\n')
    sources = {'records.csv': {'path': source, 'sha256': sha256(source.read_bytes()).hexdigest()}}
    store = volume / 'annotation'
    outputs = {'annotation.db': {}}
    transform = {'version': 'sqlite-index-1'}

    def build(inputs, destinations):
        database = local / 'annotation.db'
        with closing(db_from_dataframe(database, 'records', pd.read_csv(inputs['records.csv']))):
            pass
        shutil.copyfile(database, destinations['annotation.db'])

    paths = materialize(store, sources, outputs=outputs, transform=transform, builder=build)
    state = inspect_materialization(store, sources, outputs=outputs, transform=transform)
    assert state.status == 'available'
    assert not state.verified  # No caller-trusted output hash was supplied.
    assert state.sources['records.csv']['verified']
    with closing(connect_if_correct_version(paths['annotation.db'], 1, read_only=True)) as connection:
        assert connection.execute('SELECT id FROM records').fetchall() == [(42,)]
    def no_rebuild(*args):
        raise AssertionError('verified bundle must be reused')
    assert materialize(store, sources, outputs=outputs, transform=transform, builder=no_rebuild) == paths
    database = Path(paths['annotation.db'])
    content = database.read_bytes()
    database.write_bytes(content[:-1] + bytes([content[-1] ^ 1]))
    assert inspect_materialization(store, sources, outputs=outputs, transform=transform).status == 'invalid'
