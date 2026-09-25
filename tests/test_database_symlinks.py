"""Preserve linked cache locations when creating or rebuilding SQLite files."""

from contextlib import closing
import errno
import os
import sqlite3
import stat

import pandas as pd
import pytest

from datacache import connect_if_correct_version, db_from_dataframe


@pytest.mark.parametrize("target_kind", ["relative", "absolute", "chain", "parent"])
def test_dangling_database_symlink_creates_target_and_keeps_link(tmp_path, target_kind):
    directory = tmp_path / "data"
    directory.mkdir()
    target = directory / "records.db"
    path = tmp_path / "cache.db"
    link_text = "data/records.db"
    if target_kind == "absolute":
        link_text = str(target)
    elif target_kind == "chain":
        (tmp_path / "alias.db").symlink_to(link_text)
        link_text = "alias.db"
    elif target_kind == "parent":
        (directory / "child").mkdir()
        (tmp_path / "alias").symlink_to(directory / "child", target_is_directory=True)
        link_text = "alias/../records.db"
    path.symlink_to(link_text)
    before = path.lstat()
    assert connect_if_correct_version(path, 1) is None
    assert not target.exists()

    with closing(db_from_dataframe(path, "records", pd.DataFrame({"value": [42]}))) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(42,)]
    assert target.is_file()
    target.chmod(0o640)
    inode = target.stat().st_ino
    with closing(db_from_dataframe(path, "records", None)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(42,)]
    with closing(db_from_dataframe(path, "records", pd.DataFrame({"value": [99]}),
                                   overwrite=True)) as connection:
        assert connection.execute("SELECT * FROM records").fetchall() == [(99,)]
    assert target.stat().st_ino == inode
    assert stat.S_IMODE(target.stat().st_mode) == 0o640
    assert path.is_symlink()
    assert path.lstat().st_ino == before.st_ino
    assert os.readlink(path) == link_text
    if target_kind == "chain":
        assert (tmp_path / "alias.db").is_symlink()
    assert not (tmp_path / "records.db").exists()
    assert not list(directory.glob(".datacache-*"))


@pytest.mark.parametrize("failure", ["constraint", "publication"])
def test_failed_creation_keeps_dangling_link_without_partial_target(tmp_path, monkeypatch, failure):
    directory = tmp_path / "data"
    directory.mkdir()
    path = tmp_path / "cache.db"
    path.symlink_to("data/records.db")
    before = path.lstat()
    error = sqlite3.IntegrityError
    if failure == "publication":
        def reject(*args, **kwargs):
            raise PermissionError("injected publication failure")

        monkeypatch.setattr(os, "link", reject)
        error = PermissionError
    with pytest.raises(error):
        db_from_dataframe(path, "records", pd.DataFrame({"id": [1, 1]}),
                          primary_key="id" if failure == "constraint" else None)
    assert list(directory.iterdir()) == []
    assert path.is_symlink()
    assert path.lstat().st_ino == before.st_ino
    assert os.readlink(path) == "data/records.db"


def test_dangling_link_does_not_normalize_away_missing_parent(tmp_path):
    path = tmp_path / "cache.db"
    path.symlink_to("missing/../records.db")
    with pytest.raises(FileNotFoundError):
        db_from_dataframe(path, "records", pd.DataFrame({"value": [42]}))
    assert list(tmp_path.iterdir()) == [path]
    assert path.is_symlink()


def test_cyclic_database_symlinks_are_not_replaced(tmp_path):
    path = tmp_path / "cache.db"
    path.symlink_to("./alias.db")
    alias = tmp_path / "alias.db"
    alias.symlink_to("./cache.db")
    with pytest.raises(OSError) as error:
        db_from_dataframe(path, "records", pd.DataFrame({"value": [42]}), overwrite=True)
    assert error.value.errno == errno.ELOOP
    assert path.is_symlink()
    assert alias.is_symlink()
    assert set(tmp_path.iterdir()) == {path, alias}
