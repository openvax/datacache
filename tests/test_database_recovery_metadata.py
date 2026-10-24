"""Retain SQLite metadata and invalidate both schema and pager caches."""

from contextlib import closing, nullcontext
import os
from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from datacache import db_from_dataframe, restore_database, sqlite_recovery


@pytest.mark.parametrize('journal', ['DELETE', 'WAL'])
def test_lost_header_invalidates_single_transaction_readers(tmp_path, journal):
    path = tmp_path / 'cache.db'
    with closing(db_from_dataframe(path, 'old', pd.DataFrame({'id': [1]}))) as reader:
        # The builder's only commit leaves change counter 1 and three pages.
        before_pages = reader.execute('PRAGMA page_count').fetchone()[0]
        assert int.from_bytes(path.read_bytes()[24:28], 'big') == 1
        reader.execute('PRAGMA journal_mode=' + journal)
        assert reader.execute('SELECT * FROM old').fetchall() == [(1,)]
        path.write_bytes(b'broken')
        reopen = os.name == 'nt' and journal == 'WAL'
        if reopen:
            # Windows retains the WAL reader's locks after raw corruption;
            # POSIX closes above release the process's database file locks.
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                db_from_dataframe(path, 'fresh', pd.DataFrame({'id': [42]}), overwrite=True)
            assert path.read_bytes() == b'broken'
            reader.close()
        with closing(db_from_dataframe(path, 'fresh', pd.DataFrame({'id': [42]}), overwrite=True)) as connection:
            assert connection.execute('PRAGMA page_count').fetchone()[0] == before_pages
            assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
            observer = connection if reopen else reader
            assert observer.execute('SELECT * FROM fresh').fetchall() == [(42,)]
            with pytest.raises(sqlite3.OperationalError, match='no such table'):
                observer.execute('SELECT * FROM old').fetchall()


@pytest.mark.parametrize('auto_vacuum', [0, 1, 2])
def test_recovery_normalizes_vacuum_metadata(tmp_path, auto_vacuum):
    path = tmp_path / 'cache.db'
    with closing(sqlite3.connect(path)) as connection:
        connection.execute('PRAGMA auto_vacuum=%d' % auto_vacuum)
        connection.executescript('CREATE TABLE old(id); CREATE TABLE _datacache_metadata(version INT);'
                                 'INSERT INTO _datacache_metadata VALUES(1);')
    damaged = bytearray(path.read_bytes())
    if auto_vacuum == 0:
        damaged[67] = 1
    else:
        damaged[100] = 0xff
    path.write_bytes(damaged)
    with closing(db_from_dataframe(path, 'fresh', pd.DataFrame({'id': [42]}), overwrite=True)) as connection:
        assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
        assert connection.execute('SELECT * FROM fresh').fetchall() == [(42,)]


@pytest.mark.parametrize('empty', [True, False])
@pytest.mark.parametrize('encoding', ['UTF-8', 'UTF-16le', 'UTF-16be'])
def test_restore_preserves_autoincrement_high_water_mark(tmp_path, empty, encoding):
    source, destination = tmp_path / 'source.db', tmp_path / 'destination.db'
    with closing(sqlite3.connect(source)) as connection:
        connection.execute("PRAGMA encoding='%s'" % encoding)
        connection.executescript('CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT);'
                                 "INSERT INTO records(id,value) VALUES(1,'one'),(10,'ten');"
                                 'DELETE FROM records WHERE id=10;'
                                 'CREATE TABLE _datacache_metadata(version INT);'
                                 'INSERT INTO _datacache_metadata VALUES(7);')
        if empty:
            connection.execute('DELETE FROM records')
            connection.commit()
    # A valid UTF-8 destination also exercises conversion from UTF-16 sources.
    with closing(db_from_dataframe(destination, 'old', pd.DataFrame({'id': [3]}))):
        pass
    restore_database(source, destination)
    with closing(sqlite3.connect(destination)) as connection:
        assert connection.execute('SELECT seq FROM sqlite_sequence WHERE name="records"').fetchone() == (10,)
        assert connection.execute('SELECT id FROM records').fetchall() == ([] if empty else [(1,)])
        connection.execute("INSERT INTO records(value) VALUES('next')")
        assert connection.execute("SELECT id FROM records WHERE value='next'").fetchone() == (11,)
        assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]


@pytest.mark.parametrize('encoding', ['UTF-16le', 'UTF-16be'])
@pytest.mark.parametrize(('operation', 'journal'), [
    ('builder', 'DELETE'), ('restore', 'DELETE'),
    ('lost-destination', 'DELETE'), ('lost-destination', 'WAL'),
])
def test_recovery_preserves_text_encoding_and_unicode(tmp_path, encoding, operation, journal):
    destination, source = tmp_path / 'destination.db', tmp_path / 'source.db'
    text = 'Gène 🧬 日本語'
    with closing(sqlite3.connect(source)) as connection:
        connection.execute("PRAGMA encoding='%s'" % encoding)
        connection.executescript('CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT, payload BLOB);'
                                 'CREATE TABLE _datacache_metadata(version INT);'
                                 'INSERT INTO _datacache_metadata VALUES(7);')
        connection.execute('INSERT INTO records VALUES(1,?,?)', (text, b'\x00\xff'))
        connection.commit()
    with closing(sqlite3.connect(destination)) as reader:
        reader.execute("PRAGMA encoding='%s'" % encoding)
        reader.execute('CREATE TABLE old(id)')
        reader.execute('INSERT INTO old VALUES(3)')
        reader.commit()
        reader.execute('PRAGMA journal_mode=' + journal).fetchall()
        assert reader.execute('SELECT * FROM old').fetchall() == [(3,)]
        raw = bytearray(destination.read_bytes())
        if operation == 'lost-destination':
            destination.write_bytes(b'broken')
        else:
            raw[100] = 0xff
            destination.write_bytes(raw)
        reopen = os.name == 'nt' and journal == 'WAL'
        if reopen:
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                restore_database(source, destination, timeout=0.05)
            assert destination.read_bytes() == b'broken'
            reader.close()
        if operation == 'builder':
            with closing(db_from_dataframe(destination, 'records', pd.DataFrame({'value': [text]}), overwrite=True)):
                pass
        else:
            restore_database(source, destination)
        with closing(sqlite3.connect(destination)) if reopen else nullcontext(reader) as observer:
            assert observer.execute('PRAGMA encoding').fetchone() == (encoding,)
            assert observer.execute('SELECT value FROM records').fetchall() == [(text,)]
            if operation != 'builder':
                assert observer.execute('SELECT payload FROM records').fetchone() == (b'\x00\xff',)
            assert observer.execute('PRAGMA integrity_check').fetchall() == [('ok',)]


def test_restore_respects_existing_wal_reader_locks(tmp_path):
    source, destination = tmp_path / 'source.db', tmp_path / 'destination.db'
    with closing(db_from_dataframe(source, 'fresh', pd.DataFrame({'id': [42]}))):
        pass
    with closing(db_from_dataframe(destination, 'old', pd.DataFrame({'id': [7]}))) as reader:
        reader.execute('PRAGMA journal_mode=WAL').fetchall()
        assert reader.execute('SELECT * FROM old').fetchall() == [(7,)]
        wal = Path(str(destination) + '-wal')
        inode = wal.stat().st_ino
        # An intact WAL header makes SQLite respect the existing connection's
        # shared lock even between read transactions. Recovery needs exclusive
        # access; it must time out without changing this completed cache.
        # Do not open/close another destination handle here: on POSIX that
        # would release this process's SQLite locks and invalidate the test.
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            restore_database(source, destination, timeout=0.05)
        assert wal.stat().st_ino == inode
        assert reader.execute('SELECT * FROM old').fetchall() == [(7,)]
        assert reader.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
    restore_database(source, destination)
    with closing(sqlite3.connect(destination)) as reader:
        assert reader.execute('SELECT * FROM fresh').fetchall() == [(42,)]


def test_wal_notification_preserves_a_completed_concurrent_update(tmp_path):
    path = tmp_path / 'cache.db'
    with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [42]}))) as writer:
        writer.execute('PRAGMA journal_mode=WAL').fetchall()
        writer.execute('UPDATE records SET id=99')
        writer.execute('PRAGMA user_version=73')
        writer.commit()
    sqlite_recovery.notify_wal_readers(path)
    with closing(sqlite3.connect(path)) as reader:
        assert reader.execute('SELECT * FROM records').fetchall() == [(99,)]
        assert reader.execute('PRAGMA user_version').fetchone() == (73,)


def test_encoding_conversion_rejects_invalid_encoding(tmp_path):
    with pytest.raises(ValueError, match='unsupported SQLite text encoding'):
        with sqlite_recovery.database_with_encoding(tmp_path / 'unused.db', 'invalid'):
            pytest.fail('must not yield a source with an invalid requested encoding')


def test_change_counter_requires_committed_data(tmp_path):
    path = tmp_path / 'cache.db'
    with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [42]}))) as connection:
        connection.execute('BEGIN IMMEDIATE')
        with path.open('r+b') as database_file:
            with pytest.raises(ValueError, match='completed transaction'):
                sqlite_recovery.publish_change_counter(connection, database_file)
        connection.rollback()


def test_error_after_commit_preserves_complete_replacement(tmp_path, monkeypatch):
    source, destination = tmp_path / 'source.db', tmp_path / 'destination.db'
    for path, value in [(source, 42), (destination, 7)]:
        with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [value]}))):
            pass

    def fail_sync(descriptor):
        raise OSError('injected header synchronization failure')

    monkeypatch.setattr(sqlite_recovery.os, 'fsync', fail_sync)
    with pytest.raises(OSError, match='header synchronization failure'):
        sqlite_recovery.restore_database_in_process(source, destination)
    with closing(sqlite3.connect(destination)) as connection:
        assert connection.execute('SELECT * FROM records').fetchall() == [(42,)]
        assert connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]


def test_destination_validation_failure_rolls_back_complete_cache(tmp_path, monkeypatch):
    source, destination = tmp_path / 'source.db', tmp_path / 'destination.db'
    for path, value in [(source, 42), (destination, 7)]:
        with closing(db_from_dataframe(path, 'records', pd.DataFrame({'id': [value]}))):
            pass
    before = destination.read_bytes()
    execute = sqlite_recovery.execute_sql

    def damage(connection, sql):
        execute(connection, sql)
        if sql.startswith('PRAGMA main.schema_version='):
            execute(connection, 'PRAGMA writable_schema=ON')
            execute(connection, "UPDATE sqlite_master SET rootpage=999999 WHERE name='records'")
            execute(connection, 'PRAGMA writable_schema=RESET')

    monkeypatch.setattr(sqlite_recovery, 'execute_sql', damage)
    with pytest.raises(sqlite3.DatabaseError):
        sqlite_recovery.restore_database_in_process(source, destination)
    assert destination.read_bytes() == before
    with closing(sqlite3.connect(destination)) as connection:
        assert connection.execute('SELECT * FROM records').fetchall() == [(7,)]
