"""Cache mutations must respect the filesystem meaning of selected paths."""

import os
from pathlib import Path
import stat

import pandas as pd
import pytest

from datacache import Cache, common, ensure_dir, get_cache_root, get_data_dir


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


def test_ensure_dir_tolerates_a_concurrent_creator(tmp_path, monkeypatch):
    target = tmp_path / "made-elsewhere"
    target.mkdir()
    # Another process creates the directory between the check and makedirs.
    monkeypatch.setattr(common, "exists", lambda path: False)
    ensure_dir(target)
    assert target.is_dir()


def test_ensure_dir_leaves_existing_paths_but_rejects_a_broken_symlink(tmp_path):
    existing = tmp_path / "file.txt"
    existing.write_text("kept")
    ensure_dir(existing)
    assert existing.read_text() == "kept"
    broken = tmp_path / "broken"
    broken.symlink_to(tmp_path / "missing")
    with pytest.raises(FileExistsError):
        ensure_dir(broken)


def test_cache_root_uses_the_first_set_variable_as_the_root(tmp_path, monkeypatch):
    monkeypatch.setenv("SHARED_ROOT", str(tmp_path / "shared"))
    monkeypatch.setenv("OWN_ROOT", "   ")  # blank counts as unset
    root = get_cache_root("openvax", "OWN_ROOT", "SHARED_ROOT")
    # The value is the root itself; get_data_dir would append "openvax".
    assert root == str(tmp_path / "shared")
    assert get_data_dir("openvax", envkey="SHARED_ROOT") == str(tmp_path / "shared" / "openvax")
    monkeypatch.setenv("OWN_ROOT", "  ~/own-root ")
    assert get_cache_root("openvax", "OWN_ROOT", "SHARED_ROOT") == os.path.expanduser("~/own-root")
    assert not (tmp_path / "shared").exists()


def test_cache_root_falls_back_to_the_platform_directory(monkeypatch):
    monkeypatch.delenv("UNSET_ROOT", raising=False)
    assert get_cache_root("openvax", "UNSET_ROOT") == get_data_dir("openvax")
    assert get_cache_root("openvax") == get_data_dir("openvax")


@pytest.mark.parametrize("arguments", [("",), (None,), ("openvax", ""), ("openvax", None)])
def test_cache_root_rejects_empty_names(arguments):
    with pytest.raises(ValueError):
        get_cache_root(*arguments)


def test_cache_root_prefers_an_override_then_variables_then_legacy_data(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("EXAMPLE_ROOT", raising=False)
    legacy = tmp_path / ".example"
    platform = get_cache_root("example")
    assert get_cache_root("example", "EXAMPLE_ROOT", legacy=["~/.example"]) == platform

    # Empty leftover folders don't make a legacy location count.
    (legacy / "proteomes").mkdir(parents=True)
    assert get_cache_root("example", "EXAMPLE_ROOT", legacy=["~/.example"]) == platform
    (legacy / "proteomes" / "human.fa").write_text(">p\n")
    assert get_cache_root("example", "EXAMPLE_ROOT", legacy=["~/.example"]) == str(legacy)
    assert get_cache_root("example", "EXAMPLE_ROOT", legacy="~/.example") == str(legacy)

    monkeypatch.setenv("EXAMPLE_ROOT", "~/chosen")
    assert get_cache_root("example", "EXAMPLE_ROOT", legacy=["~/.example"]) == str(tmp_path / "chosen")
    assert get_cache_root("example", "EXAMPLE_ROOT", override=tmp_path / "flag") == str(tmp_path / "flag")
    assert get_cache_root("example", override="~/flag") == str(tmp_path / "flag")
    with pytest.raises(ValueError, match="override"):
        get_cache_root("example", override="")
