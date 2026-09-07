"""Cache mutations must respect the filesystem meaning of selected paths."""

import os
from pathlib import Path
import stat

import pandas as pd
import pytest

from datacache import Cache


@pytest.mark.parametrize("root_kind", ["dot", "path-dot", "dot-slash", "absolute", "parent", "symlink"])
def test_delete_all_preserves_current_directory(tmp_path, monkeypatch, root_kind):
    directory = tmp_path / "cache"
    directory.mkdir(mode=0o750)
    outside = tmp_path / "outside"
    outside.mkdir()
    source = outside / "source"
    source.write_text("keep outside data")
    alias = tmp_path / "alias"
    alias.symlink_to(directory, target_is_directory=True)
    monkeypatch.chdir(directory)
    root = {
        "dot": ".", "path-dot": Path("."), "dot-slash": "./",
        "absolute": directory, "parent": "nested/..", "symlink": alias,
    }[root_kind]
    (directory / "nested").mkdir()
    (directory / "nested" / "records").write_text("cached data")
    (directory / ".hidden").write_text("cached metadata")
    (directory / "linked-directory").symlink_to(outside, target_is_directory=True)
    (directory / "linked-file").symlink_to(source)
    (directory / "dangling").symlink_to(outside / "missing")
    cache = Cache(cache_root=root)
    cache.fetch(source.as_uri(), filename="downloaded")
    previous = directory.stat()

    cache.delete_all()

    assert list(directory.iterdir()) == []
    assert os.path.samefile(".", directory)
    assert directory.stat().st_ino == previous.st_ino
    assert stat.S_IMODE(directory.stat().st_mode) == stat.S_IMODE(previous.st_mode)
    assert source.read_text() == "keep outside data"
    assert alias.is_symlink()
    # A path through nested/.. becomes absent when nested itself is cleared.
    if root_kind != "parent":
        cache.delete_all()
        assert Path(cache.fetch(source.as_uri(), filename="downloaded")).read_text() == "keep outside data"


def test_delete_all_rejects_missing_parent_before_resolving_dotdot(tmp_path):
    sentinel = tmp_path / "keep"
    sentinel.write_text("not a cache")
    with pytest.raises(FileNotFoundError):
        Cache(cache_root=tmp_path / "missing" / "..").delete_all()
    assert sentinel.read_text() == "not a cache"


@pytest.mark.parametrize("location", ["root", "filename", "absolute-filename"])
@pytest.mark.parametrize("absolute_root", [False, True])
def test_database_paths_follow_symlinks_before_parent_components(
        tmp_path, monkeypatch, location, absolute_root):
    monkeypatch.chdir(tmp_path)
    cache_directory = tmp_path / "cache"
    cache_directory.mkdir()
    target = tmp_path / "actual" / "child"
    target.mkdir(parents=True)
    (cache_directory / "link").symlink_to(target, target_is_directory=True)
    root = cache_directory if absolute_root else Path("cache")
    filename = "records.db"
    if location == "root":
        root = root / "link" / ".."
    elif location == "filename":
        filename = "link/../records.db"
    else:
        filename = str(cache_directory / "link" / ".." / "records.db")
    cache = Cache(cache_root=root)
    expected = target.parent / "records.db"
    wrong = cache_directory / "records.db"

    connection = cache.db_from_dataframe(filename, "records", pd.DataFrame({"value": ["correct location"]}))
    try:
        assert connection.execute("select value from records").fetchall() == [("correct location",)]
        assert expected.is_file()
        assert not wrong.exists()
    finally:
        connection.close()

    if location == "root":
        assert cache.inspect(filename="records.db").status == "available"
        cache.delete_all()
        assert not expected.exists()
        assert not wrong.exists()
