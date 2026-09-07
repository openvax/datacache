"""Inspection must work offline without even attempting a filesystem write."""

import builtins
from contextlib import contextmanager
import hashlib
import io
import os
from pathlib import Path

import pandas as pd
import pytest

from datacache import (
    Cache, FileValidationError, common, download, expected_path, fetch_file,
    file_exists, inspect_file, inspect_files, resolve_path,
)


DATA = b"id,value\n1,example\n"
DIGEST = hashlib.sha256(DATA).hexdigest()
METADATA = {"expected_sha256": DIGEST, "expected_size": len(DATA)}


@contextmanager
def read_only(monkeypatch):
    """Reject writes explicitly, even when chmod would not constrain root."""
    original_open = builtins.open
    original_io_open = io.open
    original_os_open = os.open

    def reject(*args, **kwargs):
        pytest.fail("inspection attempted a write or network operation")

    def guard_opener(original):
        def guarded(path, mode="r", *args, **kwargs):
            if any(flag in mode for flag in "wax+"):
                reject()
            return original(path, mode, *args, **kwargs)
        return guarded

    def guarded_os_open(path, flags, *args, **kwargs):
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            reject()
        return original_os_open(path, flags, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(builtins, "open", guard_opener(original_open))
        patch.setattr(io, "open", guard_opener(original_io_open))
        patch.setattr(os, "open", guarded_os_open)
        for operation in ("mkdir", "makedirs", "remove", "unlink", "rename", "replace", "chmod", "rmdir"):
            patch.setattr(os, operation, reject)
        patch.setattr(download, "_stream_to_file", reject)
        patch.setattr(download.requests, "get", reject)
        patch.setattr(download.urllib.request, "urlopen", reject)
        yield


def test_absent_paths_do_not_create_cache_or_lock(tmp_path, monkeypatch):
    root = tmp_path / "absent" / "v2"
    monkeypatch.setattr(common, "get_data_dir", lambda subdir=None: str(root))
    with read_only(monkeypatch):
        for cache in (Cache(), Cache(cache_root=root)):
            assert cache.local_path(filename="records") == str(root / "records")
            assert not cache.exists(filename="records")
            assert cache.inspect(filename="records", **METADATA).status == "missing"
        assert expected_path(filename="records") == str(root / "records")
        assert not file_exists(filename="records")
        assert not file_exists(destination=root / "records")
        assert not file_exists(filename="records", cache_root=root)
        assert inspect_files(root, {"records": METADATA}).status == "missing"
    assert list(tmp_path.iterdir()) == []


def test_resolution_does_not_access_filesystem(tmp_path, monkeypatch):
    def reject(*args, **kwargs):
        pytest.fail("path resolution inspected the filesystem")

    with monkeypatch.context() as patch:
        patch.setattr(os, "stat", reject)
        patch.setattr(os, "lstat", reject)
        assert resolve_path("file", cache_root=tmp_path) == str(tmp_path / "file")
        assert expected_path(filename="file", cache_root=tmp_path) == str(tmp_path / "file")
        assert expected_path(destination=tmp_path / "file") == str(tmp_path / "file")
        assert Cache(cache_root=tmp_path).local_path(filename="file") == str(tmp_path / "file")


@pytest.mark.parametrize("kw", [{"filename": "file"}, {"subdir": "app"}, {"cache_root": "cache"}])
def test_destination_conflicts_rejected_without_writes(tmp_path, monkeypatch, kw):
    with read_only(monkeypatch):
        with pytest.raises(ValueError, match="cannot be combined"):
            expected_path(destination=tmp_path / "file", **kw)
        with pytest.raises(ValueError, match="cannot be combined"):
            fetch_file("https://host/file", destination=tmp_path / "file", **kw)


def test_read_only_installed_version_and_missing_sibling(tmp_path, monkeypatch):
    installed = tmp_path / "v1"
    installed.mkdir()
    records = installed / "records"
    records.write_bytes(DATA)
    records.chmod(0o444)
    installed.chmod(0o555)
    tmp_path.chmod(0o555)
    try:
        with read_only(monkeypatch):
            cache = Cache(cache_root=installed)
            assert cache.exists(filename="records")
            result = cache.inspect(filename="records", **METADATA)
            assert (result.status, result.verified, result.error) == ("available", True, None)
            assert inspect_files(installed, {"records": METADATA}).verified
            missing = inspect_files(tmp_path / "v2", {"records": METADATA})
            assert (missing.status, missing.verified) == ("missing", False)
            # Ordinary fetch reuses the valid file without needing write access.
            assert cache.fetch("https://host/records", filename="records", **METADATA) == str(records)
            assert cache.local_path("https://host/records", filename="records", download=True) == str(records)
    finally:
        tmp_path.chmod(0o755)
        installed.chmod(0o755)


@pytest.mark.parametrize("condition", ["valid", "truncated", "missing-manifest", "unreadable"])
def test_required_file_inventory(tmp_path, monkeypatch, condition):
    records = tmp_path / "records"
    manifest = tmp_path / "manifest.json"
    records.write_bytes(DATA[:3] if condition == "truncated" else DATA)
    manifest.write_bytes(b"{}")
    if condition == "missing-manifest":
        manifest.unlink()
    elif condition == "unreadable":
        original_open = builtins.open

        def unreadable(path, *args, **kwargs):
            if os.fspath(path) == str(records):
                raise PermissionError("denied by fixture")
            return original_open(path, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", unreadable)
    required = {
        "records": METADATA,
        "manifest.json": {"expected_sha256": hashlib.sha256(b"{}").hexdigest()},
    }
    with read_only(monkeypatch):
        result = inspect_files(tmp_path, required)
        assert result.status == {
            "valid": "available", "truncated": "corrupt",
            "missing-manifest": "corrupt", "unreadable": "inaccessible",
        }[condition]
        assert result.verified == (condition == "valid")
        if condition == "missing-manifest":
            assert result.files["manifest.json"].status == "missing"
            assert result.files["records"].verified
        elif condition == "unreadable":
            assert isinstance(result.error, PermissionError)
            assert file_exists(destination=records)  # Presence does not imply readability.
    assert set(p.name for p in tmp_path.iterdir()) == (
        {"records"} if condition == "missing-manifest" else {"records", "manifest.json"})


def test_presence_readability_and_verified_integrity_are_distinct(tmp_path, monkeypatch):
    path = tmp_path / "file"
    path.write_bytes(DATA)
    with read_only(monkeypatch):
        assert file_exists(destination=tmp_path)  # Directories are present.
        assert inspect_file(tmp_path).status == "corrupt"
        assert inspect_files(path, {"records": METADATA}).status == "corrupt"
        assert inspect_file(path).status == "available"
        assert not inspect_file(path).verified
        assert not inspect_file(path, expected_size=len(DATA)).verified
        assert not inspect_files(tmp_path, {"file": None}).verified
        assert inspect_file(path, expected_sha256=DIGEST.upper()).verified
        result = inspect_file(path, expected_sha256="0" * 64)
        assert result.status == "corrupt"
        assert isinstance(result.error, FileValidationError)


def test_permission_errors_are_never_missing(tmp_path, monkeypatch):
    def denied(*args, **kwargs):
        raise PermissionError("denied by fixture")

    with read_only(monkeypatch), monkeypatch.context() as patch:
        patch.setattr(os, "stat", denied)
        with pytest.raises(PermissionError):
            file_exists(destination=tmp_path / "records")
        with pytest.raises(PermissionError):
            Cache(cache_root=tmp_path).exists(filename="records")
        assert inspect_file(tmp_path / "records").status == "inaccessible"
        assert inspect_files(tmp_path, {"records": METADATA}).status == "inaccessible"


@pytest.mark.parametrize("metadata", [
    {"expected_sha256": "invalid"}, {"expected_size": -1}, {"expected_size": True},
])
def test_invalid_expectations_raise_even_for_absent_cache(tmp_path, monkeypatch, metadata):
    with read_only(monkeypatch):
        with pytest.raises(ValueError):
            inspect_file(tmp_path / "missing", **metadata)
        with pytest.raises(ValueError):
            inspect_files(tmp_path / "missing", {"records": metadata})


@pytest.mark.parametrize("name", ["", ".", "..", "../outside", "a/../b", "/absolute", "C:/file", "a\\b", "a//b", "./a"])
def test_inventory_rejects_ambiguous_asset_paths(tmp_path, monkeypatch, name):
    with read_only(monkeypatch):
        with pytest.raises(ValueError, match="normalized relative"):
            inspect_files(tmp_path / "missing", {name: METADATA})


def test_explicit_root_is_used_for_fetch_inspection_and_deletion(tmp_path, monkeypatch):
    root = tmp_path / "custom"
    default_root = tmp_path / "unused-default"
    monkeypatch.setattr(common, "get_data_dir", lambda subdir=None: str(default_root))
    source = tmp_path / "source"
    source.write_bytes(DATA)
    cache = Cache("app", cache_root=root)
    url = source.as_uri()
    for filename in ("one", "two"):
        path = cache.fetch(url, filename=filename, **METADATA)
        assert path == str(root / filename)
        assert cache.local_path(url, filename=filename) == path
        assert cache.inspect(filename=filename, **METADATA).verified
        assert expected_path(url, filename=filename, subdir="app", cache_root=root) == path
        assert fetch_file(url, filename=filename, subdir="app", cache_root=root, **METADATA) == path
    cache.delete_url(url)
    assert list(root.iterdir()) == []
    cache.delete_url(url)  # Already absent is harmless.
    connection = cache.db_from_dataframe("table.db", "records", pd.DataFrame({"id": ["example"]}))
    try:
        assert connection.execute("select id from records").fetchall() == [("example",)]
        assert (root / "table.db").is_file()
    finally:
        connection.close()
    cache.delete_all()
    assert list(root.iterdir()) == []
    assert not default_root.exists()


def test_cache_fetch_rechecks_integrity_on_every_use(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.write_bytes(DATA)
    cache = Cache(cache_root=tmp_path / "cache")
    path = Path(cache.fetch(source.as_uri(), filename="records", **METADATA))
    path.write_bytes(b"truncated")
    with read_only(monkeypatch):
        with pytest.raises(FileValidationError):
            cache.fetch(source.as_uri(), filename="records", **METADATA)
    assert path.read_bytes() == b"truncated"
    cache.fetch(source.as_uri(), filename="records", force=True, **METADATA)
    assert path.read_bytes() == DATA
