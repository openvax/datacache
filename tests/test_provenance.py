"""Inspection reports size, mtime, and opt-in download provenance (#75)."""

import hashlib
import json
import os
from pathlib import Path
import stat
import threading
from types import SimpleNamespace

import pytest

from datacache import (
    Cache, download, fetch_file, inspect_file, inspect_files, make_file_readable, provenance)
from datacache.integrity import _validate_file


DATA = b"id,value\n1,10\n"
SHA256 = hashlib.sha256(DATA).hexdigest()
posix_permissions = pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0, reason="needs POSIX permissions")


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "source.csv"
    path.write_bytes(DATA)
    return path


def fetch(source, destination, **options):
    return Path(fetch_file(source.as_uri(), destination=destination, **options))


def record_of(path):
    return json.loads(Path(provenance.sidecar_path(path)).read_text())


def test_available_files_report_size_and_mtime(source, tmp_path):
    inspection = inspect_file(source)
    assert inspection.size == len(DATA)
    assert inspection.mtime == os.stat(source).st_mtime
    missing = inspect_file(tmp_path / "absent")
    assert missing.status == "missing" and missing.size is None and missing.mtime is None


def test_size_and_mtime_come_from_the_validated_bytes(source):
    # The stat is taken from the open file that was checked, not a second stat.
    _, info = _validate_file(source, SHA256)
    assert info.st_size == len(DATA)


@posix_permissions
def test_an_unreadable_parent_reports_no_size(tmp_path):
    folder = tmp_path / "locked"
    folder.mkdir()
    (folder / "file").write_bytes(DATA)
    folder.chmod(0)
    try:
        inspection = inspect_file(folder / "file")
    finally:
        folder.chmod(0o755)
    assert inspection.status == "inaccessible" and inspection.size is None


def test_recording_is_opt_in(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256)
    assert sorted(os.listdir(tmp_path)) == ["data.csv", "source.csv"]
    inspection = inspect_file(path)
    assert inspection.recorded_sha256 is None and inspection.source_url is None


def test_a_recorded_digest_is_reported_but_never_counts_as_verified(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    inspection = inspect_file(path)  # no expectation supplied
    # verified keeps its meaning: a trusted digest matched the bytes just now.
    assert inspection.status == "available" and not inspection.verified
    assert inspection.recorded_sha256 == SHA256
    assert inspection.source_url == source.as_uri()
    assert inspection.fetched_at.endswith("+00:00")
    # A caller that trusts the record can check the current bytes against it.
    assert inspect_file(path, expected_sha256=inspection.recorded_sha256).verified


def test_an_unverified_download_records_provenance_but_no_digest(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", record_provenance=True)
    inspection = inspect_file(path)
    assert inspection.source_url == source.as_uri() and inspection.recorded_sha256 is None


@pytest.mark.parametrize("change", ["size", "same-size"])
def test_a_file_changed_after_its_download_is_unrecorded(source, tmp_path, change):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    before = os.stat(path)
    if change == "size":
        path.write_bytes(DATA + b"2,20\n")
    else:
        path.write_bytes(DATA.replace(b"10", b"99"))
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000))
    inspection = inspect_file(path)
    assert inspection.status == "available"
    assert inspection.recorded_sha256 is None and inspection.source_url is None


def test_a_concurrent_replacement_is_not_described_by_the_record(source, tmp_path, monkeypatch):
    # The record describes the bytes this call staged, not whatever another
    # writer publishes at the destination before the record is written.
    destination = tmp_path / "data.csv"
    real_replace = os.replace

    def replace_then_overwrite(source_path, target):
        real_replace(source_path, target)
        if os.fspath(target) == str(destination):
            destination.write_bytes(DATA.replace(b"10", b"77"))  # same size
            info = os.stat(destination)
            os.utime(destination, ns=(info.st_atime_ns, info.st_mtime_ns + 5_000_000_000))

    monkeypatch.setattr(download.os, "replace", replace_then_overwrite)
    fetch(source, destination, expected_sha256=SHA256, record_provenance=True)
    monkeypatch.undo()
    assert inspect_file(destination).recorded_sha256 is None


def test_a_new_download_without_recording_removes_the_old_record(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    fetch(source, path, force=True)  # records nothing this time
    assert not os.path.exists(provenance.sidecar_path(path))
    assert inspect_file(path).recorded_sha256 is None


def test_a_new_recorded_download_replaces_the_record(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    fetch(source, path, force=True, record_provenance=True)  # no expectation this time
    assert record_of(path)["sha256"] is None


@pytest.mark.parametrize("record", ["", "{not json", "[]", '{"format": 99}', "[" * 60000, "directory"])
def test_unusable_records_degrade_to_no_provenance(source, tmp_path, record):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    sidecar = Path(provenance.sidecar_path(path))
    sidecar.unlink()
    if record == "directory":
        sidecar.mkdir()
    else:
        sidecar.write_text(record)
    inspection = inspect_file(path)
    assert inspection.status == "available"
    assert inspection.recorded_sha256 is None and inspection.source_url is None


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs FIFOs")
def test_a_fifo_at_the_record_name_never_blocks_inspection(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv")
    os.mkfifo(provenance.sidecar_path(path))
    results = []
    worker = threading.Thread(target=lambda: results.append(inspect_file(path)), daemon=True)
    worker.start()
    worker.join(timeout=10)
    assert not worker.is_alive(), "inspection blocked on a FIFO"
    assert results[0].status == "available" and results[0].source_url is None


@posix_permissions
def test_an_unreadable_record_degrades_to_no_provenance(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    sidecar = Path(provenance.sidecar_path(path))
    sidecar.chmod(0)
    try:
        inspection = inspect_file(path)
    finally:
        sidecar.chmod(0o644)
    assert inspection.status == "available" and inspection.recorded_sha256 is None


def test_bytes_paths_are_inspected(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    inspection = inspect_file(os.fsencode(path))
    assert inspection.status == "available" and inspection.recorded_sha256 == SHA256


def test_inventories_report_records_but_verify_only_expectations(source, tmp_path):
    root = tmp_path / "cache"
    fetch(source, root / "data.csv", expected_sha256=SHA256, record_provenance=True)
    inventory = inspect_files(root, {"data.csv": None})
    assert list(inventory.files) == ["data.csv"]
    assert inventory.status == "available" and not inventory.verified
    assert inventory.files["data.csv"].recorded_sha256 == SHA256
    assert inspect_files(root, {"data.csv": {"expected_sha256": SHA256}}).verified


@posix_permissions
def test_inspection_writes_nothing_on_a_read_only_cache(source, tmp_path):
    root = tmp_path / "cache"
    path = fetch(source, root / "data.csv", expected_sha256=SHA256, record_provenance=True)
    before = sorted(os.listdir(root))
    root.chmod(0o555)
    try:
        assert inspect_file(path).recorded_sha256 == SHA256
        assert inspect_files(root, {"data.csv": None}).files["data.csv"].recorded_sha256 == SHA256
    finally:
        root.chmod(0o755)
    assert sorted(os.listdir(root)) == before


def test_cache_hits_never_write_a_record(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", record_provenance=True)
    Path(provenance.sidecar_path(path)).unlink()
    fetch(source, path, record_provenance=True)  # cache hit
    assert not os.path.exists(provenance.sidecar_path(path))


@posix_permissions
def test_records_have_their_files_permissions(source, tmp_path):
    previous = os.umask(0o022)
    try:
        path = fetch(source, tmp_path / "data.csv", record_provenance=True)
        assert stat.S_IMODE(os.stat(provenance.sidecar_path(path)).st_mode) == 0o644
        # A file made private keeps a private record when it is refetched.
        path.chmod(0o600)
        fetch(source, path, force=True, record_provenance=True)
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
        assert stat.S_IMODE(os.stat(provenance.sidecar_path(path)).st_mode) == 0o600
        # Sharing the file explicitly shares its record too.
        make_file_readable(path, group=True)
        assert stat.S_IMODE(os.stat(provenance.sidecar_path(path)).st_mode) == 0o640
    finally:
        os.umask(previous)


def test_a_record_name_too_long_for_the_filesystem_counts_as_absent(tmp_path):
    # A 244-character name fits, but its record name (16 more) cannot exist.
    provenance.remove(tmp_path / ("n" * 244))


def test_names_too_long_for_a_record_still_download_and_delete(source, tmp_path):
    # 244 UTF-8 bytes fits ext4's 255-byte limit, but the record name does not
    # (on filesystems that count characters, the record simply fits).
    cache = Cache("provenance", cache_root=tmp_path / "cache")
    name = "é" * 120 + ".csv"
    path = cache.fetch(source.as_uri(), filename=name, record_provenance=True)
    assert Path(path).read_bytes() == DATA
    cache.delete_url(source.as_uri())
    assert not os.path.exists(path) and not os.path.exists(provenance.sidecar_path(path))


def test_secrets_in_the_source_url_are_not_recorded(tmp_path, monkeypatch):
    response = SimpleNamespace(
        headers={"Content-Length": str(len(DATA))}, raise_for_status=lambda: None,
        iter_content=lambda **kw: iter([DATA]), close=lambda: None)
    monkeypatch.setattr(download.requests, "get", lambda *a, **kw: response)
    url = "https://user:secret@example.org:8443/data.csv?X-Amz-Signature=abc#part"
    path = fetch_file(url, destination=tmp_path / "data.csv", record_provenance=True)
    assert inspect_file(path).source_url == "https://example.org:8443/data.csv"
    assert "secret" not in Path(provenance.sidecar_path(path)).read_text()


def test_redaction_keeps_ipv6_hosts_and_ports():
    assert provenance.redact_url("http://[::1]:8000/a?b=c") == "http://[::1]:8000/a"
    assert provenance.redact_url("file:///tmp/data.csv") == "file:///tmp/data.csv"


def test_delete_url_removes_records(source, tmp_path):
    cache = Cache("provenance", cache_root=tmp_path / "cache")
    path = cache.fetch(source.as_uri(), filename="data.csv", record_provenance=True)
    assert os.path.exists(provenance.sidecar_path(path))
    cache.delete_url(source.as_uri())
    assert not os.path.exists(path) and not os.path.exists(provenance.sidecar_path(path))


def test_record_provenance_must_be_a_boolean(source, tmp_path):
    with pytest.raises(ValueError, match="record_provenance"):
        fetch(source, tmp_path / "data.csv", record_provenance="yes")
