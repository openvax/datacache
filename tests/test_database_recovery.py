"""Explicit corruption recovery is staged, locked, and transactional."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import threading

import pandas as pd
import pytest

from datacache import (connect_if_correct_version, db_from_dataframe, db_from_dataframes,
                       db_from_dataframes_with_absolute_path)
from datacache import _sqlite_recovery, database_helpers
from datacache.database import Database
from datacache.database_helpers import _create_cached_db
from datacache.database_table import DatabaseTable


@pytest.fixture(params=['not-a-database', 'schema', 'truncated', 'freelist'])
def corrupt_file(tmp_path, request):
    path = tmp_path / 'cache.db'
    if request.param == 'not-a-database':
        path.write_bytes(b'not a sqlite database')
    else:
        with closing(db_from_dataframe(path, 'old', pd.DataFrame({'id': [1]}))) as connection:
            if request.param == 'freelist':
                connection.execute('CREATE TABLE unused(id)')
                connection.execute('DROP TABLE unused')
                connection.commit()
        contents = bytearray(path.read_bytes())
        if request.param == 'schema':
            contents[100] = 0xff  # Invalid schema b-tree page type.
        elif request.param == 'truncated':
            contents = contents[:1024]
        else:
            page_size = int.from_bytes(contents[16:18], 'big')
            trunk = int.from_bytes(contents[32:36], 'big')
            assert trunk > 0
            offset = (trunk - 1) * page_size
            contents[offset:offset + 4] = (999999).to_bytes(4, 'big')
        path.write_bytes(contents)
    path.chmod(0o640)
    return path


@pytest.mark.parametrize('entry_point', ['absolute', 'single', 'multiple'])
def test_explicit_overwrite_recovers_corruption(corrupt_file, entry_point):
    path = corrupt_file
    before = path.stat()
    frame = pd.DataFrame({'id': ['g']})
    with closing(sqlite3.connect(path)) as reader:
        try:
            assert reader.execute('PRAGMA quick_check').fetchall() != [('ok',)]
        except sqlite3.DatabaseError:
            pass
        if entry_point == 'absolute':
            connection = db_from_dataframes_with_absolute_path(path, {'features': frame}, overwrite=True, version=2)
        elif entry_point == 'single':
            connection = db_from_dataframe(path, 'features', frame, overwrite=True, version=2)
        else:
            connection = db_from_dataframes(path.name, {'features': frame}, cache_root=path.parent,
                                            overwrite=True, version=2)
        with closing(connection):
            assert connection.execute('SELECT * FROM features').fetchall() == [('g',)]
            assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
            assert connection.execute('SELECT version FROM _datacache_metadata').fetchone() == (2,)
        assert reader.execute('SELECT * FROM features').fetchall() == [('g',)]
    after = path.stat()
    assert (after.st_ino, after.st_uid, after.st_gid, stat.S_IMODE(after.st_mode)) == (
        before.st_ino, before.st_uid, before.st_gid, stat.S_IMODE(before.st_mode))
    assert list(path.parent.iterdir()) == [path]


@pytest.mark.parametrize('operation', ['read', 'same-version', 'new-version'])
def test_corruption_without_explicit_overwrite_is_never_repaired(corrupt_file, operation):
    before = corrupt_file.read_bytes()
    # Some corrupt files still have readable metadata. Reading/reusing it is
    # allowed; a new-version rebuild must detect the damage and refuse repair.
    def operation_connection():
        if operation == 'read':
            return connect_if_correct_version(corrupt_file, 1)
        return db_from_dataframe(corrupt_file, 'old', pd.DataFrame({'id': [2]}),
                                 version=2 if operation == 'new-version' else 1)

    if operation == 'new-version':
        with pytest.raises(sqlite3.DatabaseError):
            operation_connection()
    else:
        try:
            with closing(operation_connection()):
                pass
        except sqlite3.DatabaseError:
            pass
    assert corrupt_file.read_bytes() == before
    assert list(corrupt_file.parent.iterdir()) == [corrupt_file]


@pytest.mark.parametrize('failure', ['constraint', 'rows', 'interrupt', 'validation', 'publication'])
def test_failed_recovery_preserves_corrupt_bytes(corrupt_file, monkeypatch, failure):
    path = corrupt_file
    before, info = path.read_bytes(), path.stat()
    expected_error = sqlite3.IntegrityError

    def rows():
        assert path.read_bytes() == before  # New rows only touch private staging.
        yield (1,)
        if failure == 'rows':
            raise RuntimeError('failed rows')
        if failure == 'interrupt':
            raise KeyboardInterrupt()
        yield (1 if failure == 'constraint' else 2,)

    if failure == 'rows':
        expected_error = RuntimeError
    elif failure == 'interrupt':
        expected_error = KeyboardInterrupt
    elif failure == 'validation':
        original = Database.create

        def damage(self, *args, **kwargs):
            original(self, *args, **kwargs)
            self.connection.execute('PRAGMA writable_schema=ON')
            self.connection.execute("UPDATE sqlite_master SET rootpage=999999 WHERE name='records'")
            self.connection.commit()
            self.connection.execute('PRAGMA writable_schema=RESET')

        monkeypatch.setattr(Database, 'create', damage)
        expected_error = sqlite3.DatabaseError
    elif failure == 'publication':
        def fail(*args, **kwargs):
            raise PermissionError('injected publication failure')
        monkeypatch.setattr(_sqlite_recovery, 'restore_database', fail)
        expected_error = PermissionError
    table = DatabaseTable('records', [('id', 'INT')], rows, primary_key='id')
    with pytest.raises(expected_error):
        _create_cached_db(path, [table], overwrite=True)
    assert path.read_bytes() == before
    assert path.stat().st_ino == info.st_ino
    assert stat.S_IMODE(path.stat().st_mode) == stat.S_IMODE(info.st_mode)
    assert list(path.parent.iterdir()) == [path]


@pytest.mark.parametrize('length', [21, 8192, 100005])
@pytest.mark.parametrize('failure', ['io', 'interrupt'])
def test_interrupted_publication_rolls_back_written_pages_and_exact_length(tmp_path, monkeypatch, length, failure):
    path = tmp_path / 'corrupt.db'
    before = b'x' * length
    path.write_bytes(before)
    execute = _sqlite_recovery._execute

    def interrupted(connection, sql):
        execute(connection, sql)
        if sql.startswith('INSERT INTO main."records"'):
            if failure == 'interrupt':
                raise KeyboardInterrupt()
            error = sqlite3.OperationalError('injected I/O error after copying rows')
            error.sqlite_errorcode = sqlite3.SQLITE_IOERR
            raise error

    monkeypatch.setattr(_sqlite_recovery, '_execute', interrupted)
    # Exercise failure inside the publisher, with no other SQLite handles open
    # in this process. Production publication always uses the isolated worker.
    monkeypatch.setattr(_sqlite_recovery, 'restore_database', _sqlite_recovery._restore_database)
    frame = pd.DataFrame({'value': ['x' * 4000] * 1500})
    with pytest.raises(KeyboardInterrupt if failure == 'interrupt' else sqlite3.OperationalError):
        db_from_dataframe(path, 'records', frame, overwrite=True)
    assert path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [path]


def test_error_after_native_commit_never_truncates_completed_replacement(tmp_path, monkeypatch):
    path = tmp_path / 'corrupt.db'
    path.write_bytes(b'broken')
    execute = _sqlite_recovery._execute

    def after_commit(connection, sql):
        execute(connection, sql)
        if sql == 'COMMIT':
            raise RuntimeError('interrupted after commit')

    monkeypatch.setattr(_sqlite_recovery, '_execute', after_commit)
    monkeypatch.setattr(_sqlite_recovery, 'restore_database', _sqlite_recovery._restore_database)
    with pytest.raises(RuntimeError, match='after commit'):
        db_from_dataframe(path, 'records', pd.DataFrame({'id': [42]}), overwrite=True)
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('SELECT * FROM records').fetchall() == [(42,)]
        assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
    assert list(tmp_path.iterdir()) == [path]


def test_incomplete_rollback_snapshot_never_replaces_original(tmp_path, monkeypatch):
    path = tmp_path / 'corrupt.db'
    before = b'broken' * 10000
    path.write_bytes(before)

    def fail_snapshot(source, destination):
        destination.write(source.read(10))
        raise OSError('snapshot failed')

    monkeypatch.setattr(_sqlite_recovery, '_copy_file', fail_snapshot)
    monkeypatch.setattr(_sqlite_recovery, 'restore_database', _sqlite_recovery._restore_database)
    with pytest.raises(OSError, match='snapshot failed'):
        db_from_dataframe(path, 'records', pd.DataFrame({'id': [42]}), overwrite=True)
    assert path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [path]


def test_repeated_parent_interrupt_waits_for_publication(tmp_path, monkeypatch):
    path = tmp_path / 'corrupt.db'
    path.write_bytes(b'broken')

    class InterruptedProcess(subprocess.Popen):
        calls = 0

        def communicate(self, *args, **kwargs):
            self.calls += 1
            if self.calls <= 2:
                raise KeyboardInterrupt()
            return super().communicate(*args, **kwargs)

    monkeypatch.setattr(_sqlite_recovery.subprocess, 'Popen', InterruptedProcess)
    with pytest.raises(KeyboardInterrupt):
        db_from_dataframe(path, 'records', pd.DataFrame({'id': [42]}), overwrite=True)
    with closing(connect_if_correct_version(path, 1)) as connection:
        assert connection.execute('SELECT * FROM records').fetchall() == [(42,)]
        assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
    assert list(tmp_path.iterdir()) == [path]


def test_recovery_copies_all_tables_constraints_indexes_and_values(tmp_path):
    path = tmp_path / "quoted ' # %.db"
    path.write_bytes(b'broken')
    tables = [
        DatabaseTable('sqliteXvalid', [('id', 'INT'), ('quoted"value', 'TEXT')],
                      lambda: [(2**60 + 1, 'text'), (2, None)], primary_key='id',
                      nullable={'quoted"value'}, indices=[('quoted"value',)]),
        DatabaseTable('other " table', [('id', 'INT'), ('blob', 'BLOB')],
                      lambda: [(7, b'\x00\xff')], primary_key='id', indices=[('id',)]),
    ]
    with closing(_create_cached_db(path, tables, version=31, overwrite=True)) as connection:
        assert connection.execute('SELECT * FROM sqliteXvalid ORDER BY id').fetchall() == [
            (2, None), (2**60 + 1, 'text')]
        assert connection.execute('SELECT * FROM "other "" table"').fetchall() == [(7, b'\x00\xff')]
        assert connection.execute('SELECT version FROM _datacache_metadata').fetchall() == [(31,)]
        indexes = connection.execute("SELECT tbl_name FROM sqlite_master WHERE type='index' "
                                     "AND sql IS NOT NULL ORDER BY tbl_name").fetchall()
        assert indexes == [('other " table',), ('sqliteXvalid',)]
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute('INSERT INTO sqliteXvalid VALUES(2, NULL)')
        connection.rollback()
        assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]


def test_corruption_from_new_rows_is_not_mistaken_for_old_cache_damage(tmp_path, monkeypatch):
    path = tmp_path / 'healthy.db'
    with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [1]}))):
        pass
    before = path.read_bytes()

    def forbidden(*args, **kwargs):
        pytest.fail('must not recover an error raised while building new data')

    monkeypatch.setattr(database_helpers, '_recover_cached_db', forbidden)

    def rows():
        error = sqlite3.DatabaseError('injected data source corruption')
        error.sqlite_errorcode = sqlite3.SQLITE_CORRUPT
        raise error

    with pytest.raises(sqlite3.DatabaseError, match='data source'):
        _create_cached_db(path, [DatabaseTable('records', [('id', 'INT')], rows)], overwrite=True)
    assert path.read_bytes() == before


@pytest.mark.parametrize('code', [sqlite3.SQLITE_BUSY, sqlite3.SQLITE_IOERR, sqlite3.SQLITE_READONLY])
def test_other_sqlite_failures_do_not_trigger_recovery(tmp_path, monkeypatch, code):
    path = tmp_path / 'healthy.db'
    with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [1]}))):
        pass
    before = path.read_bytes()

    def fail(*args, **kwargs):
        error = sqlite3.OperationalError('injected failure')
        error.sqlite_errorcode = code
        raise error

    def forbidden(*args, **kwargs):
        pytest.fail('not a corruption recovery case')

    monkeypatch.setattr(Database, 'drop_all_tables', fail)
    monkeypatch.setattr(database_helpers, '_recover_cached_db', forbidden)
    with pytest.raises(sqlite3.OperationalError, match='injected'):
        db_from_dataframe(path, 'records', pd.DataFrame({'id': [2]}), overwrite=True)
    assert path.read_bytes() == before


def test_concurrent_recoverers_publish_whole_transactions(tmp_path):
    path = tmp_path / 'corrupt.db'
    path.write_bytes(b'broken')
    building, release = threading.Event(), threading.Event()

    def rows():
        building.set()
        assert release.wait(10)
        yield (1,)

    def recover():
        with closing(_create_cached_db(path, [DatabaseTable('records', [('id', 'INT')], rows)], overwrite=True)):
            pass

    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(recover)
        assert building.wait(10)
        try:
            with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [2]}), overwrite=True)) as reader:
                assert reader.execute('SELECT * FROM records').fetchall() == [(2,)]
                release.set()
                first.result(timeout=15)
                assert reader.execute('SELECT * FROM records').fetchall() == [(1,)]
                assert reader.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
        finally:
            release.set()
    assert list(tmp_path.iterdir()) == [path]


def test_restore_respects_an_existing_sqlite_writer(tmp_path):
    source, destination = tmp_path / 'source.db', tmp_path / 'destination.db'
    for path in (source, destination):
        with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [1]}))):
            pass
    before = destination.read_bytes()
    with closing(sqlite3.connect(destination)) as writer:
        writer.execute('BEGIN IMMEDIATE')
        writer.execute('UPDATE records SET id=7')
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            _sqlite_recovery.restore_database(source, destination, timeout=0.02)
        assert destination.read_bytes() == before
        writer.commit()
        assert writer.execute('SELECT * FROM records').fetchall() == [(7,)]


@pytest.mark.parametrize('damage', ['schema', 'header', 'lost-header'])
def test_preexisting_reader_discards_its_cached_schema(tmp_path, damage):
    path = tmp_path / 'corrupt.db'
    with closing(sqlite3.connect(path)) as reader:
        reader.execute('CREATE TABLE old(id)')
        reader.execute('INSERT INTO old VALUES(1)')
        reader.commit()
        assert reader.execute('SELECT * FROM old').fetchall() == [(1,)]
        before = reader.execute('PRAGMA schema_version').fetchone()[0]
        contents = bytearray(path.read_bytes())
        if damage == 'lost-header':
            contents = b'not a database'
        elif damage == 'header':
            contents[:16] = b'x' * 16
        else:
            contents[100] = 0xff
        path.write_bytes(contents)
        with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [42]}), overwrite=True)):
            pass
        assert reader.execute('PRAGMA schema_version').fetchone()[0] != before
        assert reader.execute('SELECT * FROM records').fetchall() == [(42,)]
        with pytest.raises(sqlite3.OperationalError, match='no such table'):
            reader.execute('SELECT * FROM old').fetchall()


@pytest.mark.parametrize('link', ['symbolic', 'hard'])
def test_recovery_preserves_links_and_inode(tmp_path, link):
    target, path = tmp_path / 'target.db', tmp_path / 'alias.db'
    target.write_bytes(b'not a database')
    if link == 'symbolic':
        path.symlink_to(target.name)
    else:
        os.link(target, path)
    before = target.stat().st_ino
    with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [1]}), overwrite=True)):
        pass
    with closing(connect_if_correct_version(target, 1)) as connection:
        assert connection.execute('SELECT * FROM records').fetchall() == [(1,)]
    assert target.stat().st_ino == before
    assert path.is_symlink() == (link == 'symbolic')


@pytest.mark.skipif(sys.platform != 'darwin', reason='macOS file ACL commands')
def test_recovery_preserves_access_control_list(tmp_path):
    path = tmp_path / 'corrupt.db'
    path.write_bytes(b'not a database')
    subprocess.run(['chmod', '+a', 'everyone allow read', str(path)], check=True)
    before = subprocess.check_output(['ls', '-le', str(path)], text=True).splitlines()[1:]
    assert before
    with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [1]}), overwrite=True)):
        pass
    after = subprocess.check_output(['ls', '-le', str(path)], text=True).splitlines()[1:]
    assert after == before


@pytest.mark.parametrize('page_size', [4096, 8192])
@pytest.mark.parametrize('damage', ['header', 'schema'])
def test_recovery_handles_wal_sidecars(tmp_path, page_size, damage):
    path = tmp_path / 'corrupt.db'
    with closing(sqlite3.connect(path)) as old:
        old.execute('PRAGMA page_size=%d' % page_size)
        old.execute('CREATE TABLE old(id)')
        old.commit()
        old.execute('PRAGMA journal_mode=WAL')
        old.execute('INSERT INTO old VALUES(1)')
        old.commit()
        wal = Path(str(path) + '-wal').read_bytes()
    contents = bytearray(path.read_bytes())
    if damage == 'header':
        contents[:16] = b'x' * 16
    else:
        contents[100] = 0xff
    path.write_bytes(contents)
    # A crashed writer may leave a WAL beside the damaged main file.
    Path(str(path) + '-wal').write_bytes(wal)
    with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [42]}), overwrite=True)) as connection:
        assert connection.execute('SELECT * FROM records').fetchall() == [(42,)]
        assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
    assert set(tmp_path.iterdir()) <= {path, Path(str(path) + '-wal'), Path(str(path) + '-shm')}
    with closing(connect_if_correct_version(path, 1)) as connection:
        assert connection.execute('SELECT * FROM records').fetchall() == [(42,)]


def test_failed_publication_preserves_a_concurrent_winners_wal_database(tmp_path, monkeypatch):
    source, destination = tmp_path / 'source.db', tmp_path / 'destination.db'
    with closing(db_from_dataframe(source, 'records', pd.DataFrame({'id': [42]}))):
        pass
    with closing(sqlite3.connect(destination)) as winner:
        winner.execute('CREATE TABLE old(id)')
        winner.execute('PRAGMA journal_mode=WAL')
        winner.execute('INSERT INTO old VALUES(7)')
        winner.commit()
        wal = Path(str(destination) + '-wal').read_bytes()
    Path(str(destination) + '-wal').write_bytes(wal)
    execute = _sqlite_recovery._execute

    def fail(connection, sql):
        execute(connection, sql)
        if sql.startswith('INSERT INTO main."records"'):
            raise RuntimeError('publication failed')

    monkeypatch.setattr(_sqlite_recovery, '_execute', fail)
    with pytest.raises(RuntimeError, match='publication failed'):
        _sqlite_recovery._restore_database(source, destination)
    with closing(sqlite3.connect(destination)) as connection:
        assert connection.execute('SELECT * FROM old').fetchall() == [(7,)]
        assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
        assert connection.execute("SELECT name FROM sqlite_master WHERE name='records'").fetchall() == []
