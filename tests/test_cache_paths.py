"""Cache mutations must respect the filesystem meaning of selected paths."""

import os
from pathlib import Path
import stat
import sys

import pandas as pd
import platformdirs
import platformdirs.macos
import platformdirs.unix
import platformdirs.windows
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



@pytest.fixture
def platform_home(tmp_path, monkeypatch):
    """Simulate a platform through platformdirs, with tmp_path as the home directory."""
    for home in ("HOME", "USERPROFILE"):  # expanduser reads USERPROFILE on Windows.
        monkeypatch.setenv(home, str(tmp_path))
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)

    def simulate(kind):
        monkeypatch.setattr(platformdirs, "PlatformDirs", kind)
        monkeypatch.setattr(platformdirs.windows, "get_win_folder",
                            lambda csidl: str(tmp_path / "Local"))
    return simulate


@pytest.mark.skipif(os.name != "posix", reason="POSIX path separators")
@pytest.mark.parametrize("xdg", [None, "/elsewhere", "", "relative/cache"])
def test_macos_caches_stay_in_library_caches(platform_home, tmp_path, monkeypatch, xdg):
    # appdirs ignored XDG_CACHE_HOME on macOS; platformdirs 4.6+ honours it.
    platform_home(platformdirs.macos.MacOS)
    if xdg is not None:
        monkeypatch.setenv("XDG_CACHE_HOME", xdg)
    expected = str(tmp_path / "Library" / "Caches" / "example")
    assert get_data_dir("example") == expected
    assert get_cache_root("example") == expected


@pytest.mark.skipif(os.name != "posix", reason="POSIX path separators")
@pytest.mark.parametrize("xdg, under", [
    (None, ".cache"),
    ("", ".cache"),
    ("relative/cache", ".cache"),  # Never relative to the working directory.
    ("{tmp}/xdg", "xdg"),
])
def test_linux_caches_follow_an_absolute_xdg_cache_home(platform_home, tmp_path, monkeypatch, xdg, under):
    platform_home(platformdirs.unix.Unix)
    if xdg is not None:
        monkeypatch.setenv("XDG_CACHE_HOME", xdg.format(tmp=tmp_path))
    assert get_data_dir("example") == str(tmp_path / under / "example")
    assert get_cache_root("example") == str(tmp_path / under / "example")


@pytest.mark.skipif(os.name != "posix", reason="POSIX path separators")
def test_windows_caches_repeat_the_application_name(platform_home, tmp_path):
    platform_home(platformdirs.windows.Windows)
    local = tmp_path / "Local"
    assert get_data_dir("example") == str(local / "example" / "example" / "Cache")
    # pyensembl's per-genome caches from before get_cache_root nest the name twice.
    assert get_data_dir("pyensembl/GRCh38/ensembl82") == str(
        local / "pyensembl" / "GRCh38" / "ensembl82" / "pyensembl" / "GRCh38" / "ensembl82" / "Cache")


@pytest.mark.skipif(sys.platform != "darwin", reason="real macOS lookup")
def test_this_mac_ignores_xdg_cache_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    assert get_data_dir("example") == str(tmp_path / "Library" / "Caches" / "example")


@pytest.mark.parametrize("arguments", [("",), (None,), ("openvax", ""), ("openvax", None)])
def test_cache_root_rejects_empty_names(arguments):
    with pytest.raises(ValueError):
        get_cache_root(*arguments)


def test_cache_root_prefers_an_override_then_variables_then_legacy_data(tmp_path, monkeypatch):
    for home in ("HOME", "USERPROFILE"):  # expanduser reads USERPROFILE on Windows.
        monkeypatch.setenv(home, str(tmp_path))
    monkeypatch.delenv("EXAMPLE_ROOT", raising=False)
    platform = tmp_path / "platform"
    monkeypatch.setattr(common, "_platform_cache_dir", lambda name: str(platform))
    old, older = tmp_path / ".example", tmp_path / ".older"
    assert get_cache_root("example", "EXAMPLE_ROOT", legacy=["~/.example"]) == str(platform)

    # Empty folders and incidental files don't count; a file at any depth does.
    (old / "proteomes" / "human").mkdir(parents=True)
    (old / ".DS_Store").write_text("")
    (old / ".datacache-file-registry.lock").write_text("")
    assert get_cache_root("example", legacy=["~/.example"]) == str(platform)
    (older / "a" / "b" / "c").mkdir(parents=True)
    (older / "a" / "b" / "c" / "data.fa").write_text(">p\n")
    assert get_cache_root("example", legacy=["~/.example", "~/.older"]) == str(older)
    (old / "proteomes" / "human" / "human.fa").write_text(">p\n")
    assert get_cache_root("example", legacy=["~/.example", "~/.older"]) == str(old)
    assert get_cache_root("example", legacy="~/.example") == str(old)

    # Once the platform directory holds data, it stays the root.
    platform.mkdir()
    (platform / "downloaded.fa").write_text(">p\n")
    assert get_cache_root("example", legacy=["~/.example"]) == str(platform)

    monkeypatch.setenv("EXAMPLE_ROOT", "~/chosen")
    assert get_cache_root("example", "EXAMPLE_ROOT", legacy=["~/.example"]) == str(tmp_path / "chosen")
    assert get_cache_root("example", "EXAMPLE_ROOT", override=tmp_path / "flag") == str(tmp_path / "flag")
    assert get_cache_root("example", override="  ~/flag ") == str(tmp_path / "flag")


@pytest.mark.parametrize("options, message", [
    (dict(override=""), "override"),
    (dict(override="   "), "override"),
    (dict(override=b"/bytes"), "text path"),
    (dict(legacy=[None]), "text path"),
    (dict(legacy=["relative/old"]), "absolute"),
    (dict(legacy=[""]), "non-empty"),
])
def test_cache_root_rejects_bad_locations_wherever_they_are(monkeypatch, options, message):
    monkeypatch.setenv("SET_ROOT", "/somewhere")
    # Checked up front, so a bad argument fails even when a variable is set.
    with pytest.raises(ValueError, match=message):
        get_cache_root("example", "SET_ROOT", **options)
    assert get_cache_root("example", "SET_ROOT", legacy=None) == "/somewhere"


@pytest.mark.skipif(os.name != "posix", reason="POSIX symlinks and permissions")
def test_unreadable_legacy_entries_are_skipped(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "_platform_cache_dir", lambda name: str(tmp_path / "platform"))
    old = tmp_path / "old"
    old.mkdir()
    (old / "loop").symlink_to(old / "loop")
    (old / "locked").mkdir()
    (old / "locked").chmod(0)
    try:
        assert get_cache_root("example", legacy=[str(old)]) == str(tmp_path / "platform")
        (old / "data.fa").write_text(">p\n")
        assert get_cache_root("example", legacy=[str(old)]) == str(old)
    finally:
        (old / "locked").chmod(0o700)
