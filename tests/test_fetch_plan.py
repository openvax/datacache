# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for read-only FetchPlan and plan_fetch preflight (#135)."""

import builtins
from collections import namedtuple
from contextlib import contextmanager
import dataclasses
import hashlib
import io
import os
import shutil
import urllib.parse
import urllib.request

import pytest
import requests

from datacache import (
    Cache,
    FetchPlan,
    FileInspection,
    OfflineError,
    download,
    fetch_file,
    plan_fetch,
)

DATA = b"id,value\n1,example\n"
DIGEST = hashlib.sha256(DATA).hexdigest()
DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


def _fake_disk_usage(free_bytes, seen_paths=None):
    def usage(path):
        if seen_paths is not None:
            seen_paths.append(path)
        return DiskUsage(total=max(free_bytes, 1024), used=0, free=free_bytes)

    return usage


@contextmanager
def read_only(monkeypatch, *, allow_network=False):
    """Fail immediately if plan_fetch attempts any write, mkdir, lock, or unexpected network call."""
    original_open = builtins.open
    original_io_open = io.open
    original_os_open = os.open

    def reject(*args, **kwargs):
        pytest.fail("plan_fetch attempted a forbidden write or network operation")

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
        if not allow_network:
            patch.setattr(requests, "head", reject)
            patch.setattr(requests, "get", reject)
            patch.setattr(urllib.request, "urlopen", reject)
        yield


def test_plan_fetch_reuse_valid_file_is_frozen_and_read_only(tmp_path, monkeypatch):
    dest = tmp_path / "records.csv"
    dest.write_bytes(DATA)
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(0))

    with read_only(monkeypatch):
        plan = plan_fetch(
            "https://example.test/records.csv",
            dest,
            expected_sha256=DIGEST,
            expected_size=len(DATA),
            probe_remote_size=True,  # Must not probe network when reusing.
        )

    assert isinstance(plan, FetchPlan)
    assert plan.url == "https://example.test/records.csv"
    assert plan.destination == str(dest)
    assert plan.action == "reuse"
    assert isinstance(plan.inspection, FileInspection)
    assert plan.inspection.status == "available"
    assert plan.inspection.verified is True
    assert plan.expected_size == len(DATA)
    assert plan.partial_bytes == 0
    assert plan.needed_bytes == 0
    assert plan.free_bytes == 0
    assert plan.sufficient is True

    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.action = "download"


def test_plan_fetch_download_missing_with_nonexistent_parents_and_space_checks(tmp_path, monkeypatch):
    nested = tmp_path / "deep" / "missing" / "subdir" / "iedb.zip"
    probed_dirs = []
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(6 * 2**30, probed_dirs))

    with read_only(monkeypatch):
        insufficient = plan_fetch(
            "https://example.test/iedb.zip",
            nested,
            expected_size=8 * 2**30,
        )
        exact = plan_fetch(
            "https://example.test/iedb.zip",
            nested,
            expected_size=6 * 2**30,
        )
        sufficient = plan_fetch(
            "https://example.test/iedb.zip",
            nested,
            expected_size=2 * 2**30,
        )

    assert not (tmp_path / "deep").exists()
    assert probed_dirs == [str(tmp_path)] * 3

    assert insufficient.action == "download"
    assert insufficient.inspection.status == "missing"
    assert insufficient.expected_size == 8 * 2**30
    assert insufficient.needed_bytes == 8 * 2**30
    assert insufficient.free_bytes == 6 * 2**30
    assert insufficient.sufficient is False

    assert exact.sufficient is True
    assert sufficient.sufficient is True


def test_plan_fetch_replace_for_corrupt_inaccessible_directory_and_force(tmp_path, monkeypatch):
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(100))

    # Corrupt file (wrong size / hash).
    corrupt = tmp_path / "corrupt.dat"
    corrupt.write_bytes(b"short")
    with read_only(monkeypatch):
        plan_corrupt = plan_fetch(
            "https://example.test/corrupt.dat",
            corrupt,
            expected_sha256=DIGEST,
            expected_size=len(DATA),
        )
    assert plan_corrupt.action == "replace"
    assert plan_corrupt.inspection.status == "corrupt"
    assert plan_corrupt.needed_bytes == len(DATA)
    assert plan_corrupt.sufficient is True

    # Directory at destination -> corrupt -> replace.
    dir_dest = tmp_path / "adir"
    dir_dest.mkdir()
    with read_only(monkeypatch):
        plan_dir = plan_fetch("https://example.test/adir", dir_dest, expected_size=200)
    assert plan_dir.action == "replace"
    assert plan_dir.inspection.status == "corrupt"
    assert plan_dir.sufficient is False

    # Valid file with force=True -> replace.
    valid = tmp_path / "valid.dat"
    valid.write_bytes(DATA)
    with read_only(monkeypatch):
        plan_force = plan_fetch(
            "https://example.test/valid.dat",
            valid,
            expected_sha256=DIGEST,
            expected_size=len(DATA),
            force=True,
        )
    assert plan_force.action == "replace"
    assert plan_force.inspection.status == "available"
    assert plan_force.needed_bytes == len(DATA)
    assert plan_force.sufficient is True

    # Inaccessible file -> replace.
    original_open = builtins.open

    def deny_valid(path, *args, **kwargs):
        if os.fspath(path) == str(valid):
            raise PermissionError("denied by fixture")
        return original_open(path, *args, **kwargs)

    with read_only(monkeypatch), monkeypatch.context() as patch:
        patch.setattr(builtins, "open", deny_valid)
        plan_inaccessible = plan_fetch("https://example.test/valid.dat", valid, expected_size=50)
    assert plan_inaccessible.action == "replace"
    assert plan_inaccessible.inspection.status == "inaccessible"
    assert isinstance(plan_inaccessible.inspection.error, PermissionError)
    assert plan_inaccessible.sufficient is True


def test_plan_fetch_unknown_size_returns_none_without_network(tmp_path, monkeypatch):
    dest = tmp_path / "unknown.dat"
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(1024))
    monkeypatch.setenv("DATACACHE_OFFLINE", "1")

    with read_only(monkeypatch):
        plan = plan_fetch("https://example.test/unknown.dat", dest)

    assert plan.action == "download"
    assert plan.expected_size is None
    assert plan.partial_bytes == 0
    assert plan.needed_bytes is None
    assert plan.free_bytes == 1024
    assert plan.sufficient is None


def test_plan_fetch_disk_usage_unavailable_sets_free_and_sufficient_none(tmp_path, monkeypatch):
    dest = tmp_path / "file.dat"

    def fail_usage(path):
        raise OSError("disk_usage unavailable")

    monkeypatch.setattr(shutil, "disk_usage", fail_usage)
    with read_only(monkeypatch):
        plan = plan_fetch("https://example.test/file.dat", dest, expected_size=100)
    assert plan.free_bytes is None
    assert plan.needed_bytes == 100
    assert plan.sufficient is None


class _FakeHeadResponse:
    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = dict(headers or {})

    def close(self):
        pass


class _FakeGetResponse:
    def __init__(self, payload, *, chunk_size=32, headers=None, status_code=200):
        self._payload = payload
        self._chunk_size = chunk_size
        self.headers = dict(headers or {})
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("HTTP %d" % self.status_code, response=self)

    def iter_content(self, chunk_size=None):
        step = chunk_size or self._chunk_size
        for i in range(0, len(self._payload), step):
            yield self._payload[i:i + step]

    def close(self):
        pass


def test_plan_fetch_probe_remote_size_http_head_headers_and_edge_cases(tmp_path, monkeypatch):
    head_requests = []

    def fake_head(url, headers=None, timeout=None, allow_redirects=True):
        path = urllib.parse.urlsplit(url).path
        head_requests.append((path, dict(headers or {}), timeout, allow_redirects))
        if path == "/sized":
            return _FakeHeadResponse(200, {"Content-Length": "4096"})
        if path == "/gzipped":
            return _FakeHeadResponse(200, {"Content-Encoding": "gzip", "Content-Length": "128"})
        if path == "/chunked":
            return _FakeHeadResponse(200, {"Transfer-Encoding": "chunked", "Content-Length": "256"})
        if path == "/no-head":
            return _FakeHeadResponse(405)
        return _FakeHeadResponse(200)

    base = "https://example.test"
    monkeypatch.setattr(requests, "head", fake_head)
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(2048))
    dest = tmp_path / "download.bin"

    with read_only(monkeypatch, allow_network=True):
        # Explicit expected_size skips HEAD even when probe_remote_size=True.
        explicit = plan_fetch(
            base + "/sized", dest, expected_size=1024, probe_remote_size=True
        )
        assert explicit.expected_size == 1024
        assert explicit.sufficient is True
        assert head_requests == []

        # HEAD probe with custom headers reads Content-Length.
        probed = plan_fetch(
            base + "/sized",
            dest,
            probe_remote_size=True,
            timeout=5,
            headers={"X-Custom": "preflight"},
        )
        assert probed.expected_size == 4096
        assert probed.needed_bytes == 4096
        assert probed.free_bytes == 2048
        assert probed.sufficient is False
        assert len(head_requests) == 1
        assert head_requests[0][0] == "/sized"
        assert head_requests[0][1]["X-Custom"] == "preflight"
        assert head_requests[0][1]["Accept-Encoding"] == "identity"
        assert head_requests[0][2] == 5
        assert head_requests[0][3] is True

        # Encoded / chunked / 405 / missing Content-Length yield None safely.
        for suffix in ("/gzipped", "/chunked", "/no-head", "/empty"):
            plan = plan_fetch(base + suffix, dest, probe_remote_size=True)
            assert plan.expected_size is None
            assert plan.needed_bytes is None
            assert plan.sufficient is None

        # Offline mode blocks remote HEAD probing.
        with pytest.raises(OfflineError):
            plan_fetch(base + "/sized", dest, probe_remote_size=True, offline=True)

    # File URL size probing works offline when probe_remote_size=True.
    local_src = tmp_path / "local.bin"
    local_src.write_bytes(DATA)
    with read_only(monkeypatch):
        without_probe = plan_fetch(local_src.as_uri(), dest, probe_remote_size=False)
        with_probe = plan_fetch(local_src.as_uri(), dest, probe_remote_size=True, offline=True)
    assert without_probe.expected_size is None
    assert with_probe.expected_size == len(DATA)
    assert with_probe.sufficient is True


def test_plan_fetch_probe_remote_size_handles_transport_errors_gracefully(tmp_path, monkeypatch):
    def fail_head(*args, **kwargs):
        raise requests.ConnectionError("dns failure")

    monkeypatch.setattr(requests, "head", fail_head)
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(1000))
    plan = plan_fetch("https://example.test/file.dat", tmp_path / "file.dat", probe_remote_size=True)
    assert plan.expected_size is None
    assert plan.needed_bytes is None
    assert plan.sufficient is None


@pytest.mark.skipif(os.name != "posix", reason="POSIX resumable downloads")
def test_plan_fetch_accounts_for_matching_resumable_partial_only(tmp_path, monkeypatch):
    payload = b"0123456789abcdef" * 16  # 256 bytes
    digest = hashlib.sha256(payload).hexdigest()
    url = "https://example.test/big.bin"
    dest = tmp_path / "big.bin"

    monkeypatch.setattr(
        requests,
        "get",
        lambda u, **kwargs: _FakeGetResponse(
            payload,
            chunk_size=32,
            headers={"Content-Length": str(len(payload)), "ETag": '"v1"'},
        ),
    )

    # Create a real private resumable partial of 128 bytes via interrupted download.
    def stop_halfway(done, total):
        if done >= 128:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        fetch_file(
            url,
            destination=dest,
            resume=True,
            expected_sha256=digest,
            expected_size=len(payload),
            chunk_size=32,
            progress_callback=stop_halfway,
        )

    assert not dest.exists()
    # With 160 bytes free: 256 full size wouldn't fit, but 128 remaining bytes DO fit!
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(160))

    with read_only(monkeypatch):
        matching = plan_fetch(
            url,
            dest,
            expected_sha256=digest,
            expected_size=len(payload),
        )
        wrong_url = plan_fetch(
            url + "?other=1",
            dest,
            expected_sha256=digest,
            expected_size=len(payload),
        )
        wrong_hash = plan_fetch(
            url,
            dest,
            expected_sha256="0" * 64,
            expected_size=len(payload),
        )

    assert matching.action == "download"
    assert matching.expected_size == 256
    assert matching.partial_bytes == 128
    assert matching.needed_bytes == 128
    assert matching.free_bytes == 160
    assert matching.sufficient is True

    # Non-matching URL or digest must NOT credit the stale partial.
    assert wrong_url.partial_bytes == 0
    assert wrong_url.needed_bytes == 256
    assert wrong_url.sufficient is False

    assert wrong_hash.partial_bytes == 0
    assert wrong_hash.needed_bytes == 256
    assert wrong_hash.sufficient is False


def test_cache_plan_fetch_is_read_only_and_uses_cache_root(tmp_path, monkeypatch):
    root = tmp_path / "cache-root"
    cache = Cache(cache_root=root)
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(500))

    with read_only(monkeypatch):
        plan = cache.plan_fetch(
            "https://example.test/item.csv",
            filename="item.csv",
            expected_size=1000,
        )

    assert not root.exists()
    assert plan.destination == str(root / "item.csv")
    assert plan.action == "download"
    assert plan.expected_size == 1000
    assert plan.needed_bytes == 1000
    assert plan.free_bytes == 500
    assert plan.sufficient is False


@pytest.mark.parametrize("args,kwargs,exc", [
    (("", "/tmp/x"), {}, ValueError),
    (("https://example.test/x", ""), {}, ValueError),
    (("https://example.test/x", None), {}, TypeError),
    (("https://example.test/x", "/tmp/x"), {"expected_sha256": "bad"}, ValueError),
    (("https://example.test/x", "/tmp/x"), {"expected_size": -1}, ValueError),
    (("https://example.test/x", "/tmp/x"), {"expected_size": True}, ValueError),
    (("https://example.test/x", "/tmp/x"), {"probe_remote_size": "yes"}, ValueError),
    (("https://example.test/x", "/tmp/x"), {"force": 1}, ValueError),
    (("https://example.test/x", "/tmp/x"), {"headers": ["bad"]}, ValueError),
    (("https://example.test/x", "/tmp/x"), {"offline": "yes"}, ValueError),
])
def test_plan_fetch_validates_arguments(args, kwargs, exc):
    with pytest.raises(exc):
        plan_fetch(*args, **kwargs)
