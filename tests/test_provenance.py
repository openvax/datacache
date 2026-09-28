"""Inspection reports size, mtime, and opt-in download provenance (#75)."""

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from datacache import Cache, download, fetch_file, inspect_file, inspect_files, provenance


DATA = b"id,value\n1,10\n"
SHA256 = hashlib.sha256(DATA).hexdigest()


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "source.csv"
    path.write_bytes(DATA)
    return path


def fetch(source, destination, **options):
    return Path(fetch_file(source.as_uri(), destination=destination, **options))


def test_available_files_report_size_and_mtime(source, tmp_path):
    inspection = inspect_file(source)
    assert inspection.size == len(DATA)
    assert inspection.mtime == os.stat(source).st_mtime
    missing = inspect_file(tmp_path / "absent")
    assert missing.status == "missing" and missing.size is None and missing.mtime is None


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0, reason="needs POSIX permissions")
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
    assert not inspect_file(path).verified and inspect_file(path).source_url is None


def test_a_verified_download_inspects_as_verified_offline(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    inspection = inspect_file(path)  # no expectation supplied
    assert inspection.status == "available" and inspection.verified
    assert inspection.source_url == source.as_uri()
    assert inspection.fetched_at.endswith("+00:00")
    record = json.loads(Path(provenance.sidecar_path(path)).read_text())
    assert record["sha256"] == SHA256 and record["size"] == len(DATA)


def test_an_unverified_download_records_provenance_but_not_verification(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", record_provenance=True)
    inspection = inspect_file(path)
    assert inspection.source_url == source.as_uri() and not inspection.verified


@pytest.mark.parametrize("change", ["size", "same-size"])
def test_a_file_changed_after_its_download_is_not_verified(source, tmp_path, change):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    before = os.stat(path)
    if change == "size":
        path.write_bytes(DATA + b"2,20\n")
    else:
        path.write_bytes(DATA.replace(b"10", b"99"))
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000))
    inspection = inspect_file(path)
    assert inspection.status == "available"
    assert not inspection.verified and inspection.source_url is None and inspection.fetched_at is None


@pytest.mark.parametrize("record", ["", "{not json", "[]", '{"format": 99}', "directory"])
def test_unusable_records_degrade_to_no_provenance(source, tmp_path, record):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    sidecar = Path(provenance.sidecar_path(path))
    sidecar.unlink()
    if record == "directory":
        sidecar.mkdir()
    else:
        sidecar.write_text(record)
    inspection = inspect_file(path)
    assert inspection.status == "available" and not inspection.verified
    assert inspection.source_url is None


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0, reason="needs POSIX permissions")
def test_an_unreadable_record_degrades_to_no_provenance(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    sidecar = Path(provenance.sidecar_path(path))
    sidecar.chmod(0)
    try:
        inspection = inspect_file(path)
    finally:
        sidecar.chmod(0o644)
    assert inspection.status == "available" and not inspection.verified


def test_inventories_verify_from_records_and_never_list_them(source, tmp_path):
    root = tmp_path / "cache"
    fetch(source, root / "data.csv", expected_sha256=SHA256, record_provenance=True)
    inventory = inspect_files(root, {"data.csv": None})
    assert list(inventory.files) == ["data.csv"]
    assert inventory.status == "available" and inventory.verified


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0, reason="needs POSIX permissions")
def test_inspection_writes_nothing_on_a_read_only_cache(source, tmp_path):
    root = tmp_path / "cache"
    path = fetch(source, root / "data.csv", expected_sha256=SHA256, record_provenance=True)
    before = sorted(os.listdir(root))
    root.chmod(0o555)
    try:
        assert inspect_file(path).verified
        assert inspect_files(root, {"data.csv": None}).verified
    finally:
        root.chmod(0o755)
    assert sorted(os.listdir(root)) == before


def test_cache_hits_never_write_a_record(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", record_provenance=True)
    Path(provenance.sidecar_path(path)).unlink()
    fetch(source, path, record_provenance=True)  # cache hit
    assert not os.path.exists(provenance.sidecar_path(path))


def test_a_new_download_replaces_the_record(source, tmp_path):
    path = fetch(source, tmp_path / "data.csv", expected_sha256=SHA256, record_provenance=True)
    fetch(source, path, force=True, record_provenance=True)  # no expectation this time
    assert json.loads(Path(provenance.sidecar_path(path)).read_text())["sha256"] is None
    assert not inspect_file(path).verified


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
