"""Sharing old caches is explicit and never changes their bytes or identity."""

import os
import stat

import pytest

from datacache import Cache, FileValidationError, make_file_readable


pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX sharing permissions")


@pytest.mark.parametrize("group,others,expected", [
    (True, False, 0o640), (True, True, 0o644), (False, True, 0o604), (False, False, 0o600),
])
def test_explicit_read_access_preserves_cached_file(tmp_path, group, others, expected):
    path = tmp_path / "data"
    path.write_bytes(b"legacy data")
    path.chmod(0o600)
    before = path.stat()
    cache = Cache(cache_root=tmp_path)
    for _ in range(2):
        assert cache.make_readable(filename="data", group=group, others=others) == str(path)
        assert stat.S_IMODE(path.stat().st_mode) == expected
        assert path.read_bytes() == b"legacy data"
        assert path.stat().st_ino == before.st_ino
        assert path.stat().st_mtime_ns == before.st_mtime_ns


@pytest.mark.parametrize("kind", ["directory", "symlink", "fifo"])
def test_sharing_rejects_non_regular_files(tmp_path, kind):
    target = tmp_path / "target"
    target.write_bytes(b"unrelated")
    target.chmod(0o600)
    path = tmp_path / "data"
    if kind == "directory":
        path.mkdir()
    elif kind == "symlink":
        path.symlink_to(target)
    else:
        os.mkfifo(path)
    with pytest.raises(FileValidationError):
        make_file_readable(path)
    assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_sharing_missing_file_does_not_create_it(tmp_path):
    with pytest.raises(FileNotFoundError):
        make_file_readable(tmp_path / "missing")
    assert list(tmp_path.iterdir()) == []


def test_sharing_does_not_remove_existing_write_or_execute_bits(tmp_path):
    path = tmp_path / "data"
    path.write_bytes(b"data")
    path.chmod(0o701)
    make_file_readable(path)
    assert stat.S_IMODE(path.stat().st_mode) == 0o741
