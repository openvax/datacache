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

"""Tests for InsufficientSpaceError and ENOSPC cleanup guarantees (#136)."""

from collections import namedtuple
import errno
import gzip
import hashlib
import os
import shutil
import zipfile

import pytest
import requests

import datacache
from datacache import (
    InsufficientSpaceError,
    download,
    fetch_and_transform,
    fetch_file,
    materialize,
)
from datacache.space import free_disk_bytes, is_enospc, nearest_existing_parent

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


def _fake_disk_usage(free_bytes):
    return lambda path: DiskUsage(total=max(free_bytes, 1024), used=0, free=free_bytes)


class _EnospcWriter:
    """File proxy that raises OSError(ENOSPC) once written bytes pass a threshold."""

    def __init__(self, wrapped, *, fail_after_bytes=0, fail_on_close=False):
        self._wrapped = wrapped
        self.name = wrapped.name
        self._fail_after_bytes = fail_after_bytes
        self._fail_on_close = fail_on_close
        self._written = 0

    def write(self, data):
        if not self._fail_on_close and self._written + len(data) > self._fail_after_bytes:
            raise OSError(errno.ENOSPC, "No space left on device", self.name)
        written = self._wrapped.write(data)
        self._written += len(data)
        return written

    def flush(self):
        return self._wrapped.flush()

    def close(self):
        try:
            if self._fail_on_close:
                raise OSError(errno.ENOSPC, "No space left on device", self.name)
        finally:
            self._wrapped.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __getattr__(self, attr):
        return getattr(self._wrapped, attr)


class _FakeResponse:
    """Minimal streaming HTTP response for offline download tests."""

    def __init__(self, chunks, *, headers=None, status_code=200):
        self._chunks = list(chunks)
        self.headers = dict(headers or {})
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("HTTP %d" % self.status_code, response=self)

    def iter_content(self, chunk_size=1):
        yield from self._chunks

    def close(self):
        pass


def test_insufficient_space_error_attributes_and_formatting():
    err = InsufficientSpaceError("/cache/data.bin", needed_bytes=8192, free_bytes=1024)
    assert isinstance(err, OSError)
    assert not isinstance(err, PermissionError)
    assert not isinstance(err, requests.RequestException)
    assert err.errno == errno.ENOSPC
    assert err.path == "/cache/data.bin"
    assert err.filename == "/cache/data.bin"
    assert err.needed_bytes == 8192
    assert err.free_bytes == 1024
    text = str(err)
    assert "8.0 KiB" in text
    assert "1.0 KiB" in text
    assert "/cache/data.bin" in text

    err_free_only = InsufficientSpaceError("/cache/a", free_bytes=512)
    assert err_free_only.needed_bytes is None
    assert err_free_only.free_bytes == 512
    assert "512 B" in str(err_free_only)

    err_needed_only = InsufficientSpaceError("/cache/b", needed_bytes=2048)
    assert err_needed_only.needed_bytes == 2048
    assert err_needed_only.free_bytes is None
    assert "2.0 KiB" in str(err_needed_only)

    err_minimal = InsufficientSpaceError("/cache/c")
    assert err_minimal.needed_bytes is None
    assert err_minimal.free_bytes is None
    assert "Not enough disk space" in str(err_minimal)

    assert is_enospc(err)
    assert is_enospc(OSError(errno.ENOSPC, "No space left on device"))
    assert not is_enospc(PermissionError(errno.EACCES, "Permission denied"))
    assert not is_enospc(ValueError("nope"))


@pytest.mark.parametrize("kwargs", [
    {"needed_bytes": -1},
    {"needed_bytes": True},
    {"free_bytes": -5},
    {"free_bytes": False},
])
def test_insufficient_space_error_rejects_invalid_byte_counts(kwargs):
    with pytest.raises(ValueError):
        InsufficientSpaceError("/cache/x", **kwargs)


def test_nearest_existing_parent_and_free_disk_bytes_are_read_only(tmp_path):
    nested = tmp_path / "does" / "not" / "exist" / "file.dat"
    assert nearest_existing_parent(nested) == str(tmp_path)
    assert not (tmp_path / "does").exists()
    free = free_disk_bytes(nested)
    assert isinstance(free, int) and free >= 0


def test_enospc_mid_download_removes_staging_and_leaves_no_destination(tmp_path, monkeypatch):
    dest = tmp_path / "dataset.bin"
    payload_chunks = [b"a" * 64, b"b" * 64, b"c" * 64]
    total_size = sum(len(c) for c in payload_chunks)
    attempts = []

    def fake_get(url, **kwargs):
        attempts.append(url)
        return _FakeResponse(payload_chunks, headers={"Content-Length": str(total_size)})

    real_open_staging = download.open_staging_file

    def enospc_staging(*args, **kwargs):
        return _EnospcWriter(real_open_staging(*args, **kwargs), fail_after_bytes=64)

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(download, "open_staging_file", enospc_staging)
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(12))

    with pytest.raises(InsufficientSpaceError) as exc_info:
        fetch_file("https://example.test/dataset.bin", destination=dest, max_retries=2, retry_backoff=0)

    err = exc_info.value
    assert err.errno == errno.ENOSPC
    assert err.path == str(dest)
    assert err.needed_bytes == total_size
    assert err.free_bytes == 12
    assert isinstance(err.__cause__, OSError) and err.__cause__.errno == errno.ENOSPC
    # ENOSPC must never be retried.
    assert len(attempts) == 1
    # Neither final destination nor staging temporary file may remain.
    assert not dest.exists()
    assert list(tmp_path.iterdir()) == []


def test_enospc_mid_download_with_chunked_response_uses_expected_size_or_none(tmp_path, monkeypatch):
    dest = tmp_path / "chunked.bin"
    payload_chunks = [b"x" * 32, b"y" * 32]
    real_open_staging = download.open_staging_file

    monkeypatch.setattr(
        requests,
        "get",
        lambda url, **kwargs: _FakeResponse(payload_chunks, headers={"Transfer-Encoding": "chunked"}),
    )
    monkeypatch.setattr(
        download,
        "open_staging_file",
        lambda *args, **kwargs: _EnospcWriter(real_open_staging(*args, **kwargs), fail_after_bytes=32),
    )
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(8))

    with pytest.raises(InsufficientSpaceError) as with_size:
        fetch_file("https://example.test/chunked.bin", destination=dest, expected_size=64)
    assert with_size.value.needed_bytes == 64
    assert with_size.value.free_bytes == 8
    assert list(tmp_path.iterdir()) == []

    with pytest.raises(InsufficientSpaceError) as without_size:
        fetch_file("https://example.test/chunked.bin", destination=dest)
    assert without_size.value.needed_bytes is None
    assert without_size.value.free_bytes == 8
    assert list(tmp_path.iterdir()) == []


def test_enospc_on_staging_close_removes_staging_file(tmp_path, monkeypatch):
    dest = tmp_path / "flush_fail.bin"
    source = tmp_path / "source.bin"
    source.write_bytes(b"hello world")
    real_open_staging = download.open_staging_file

    monkeypatch.setattr(
        download,
        "open_staging_file",
        lambda *args, **kwargs: _EnospcWriter(real_open_staging(*args, **kwargs), fail_on_close=True),
    )
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(4))

    with pytest.raises(InsufficientSpaceError) as exc_info:
        fetch_file(source.as_uri(), destination=dest, expected_size=11)

    assert exc_info.value.path == str(dest)
    assert exc_info.value.needed_bytes == 11
    assert exc_info.value.free_bytes == 4
    assert not dest.exists()
    assert [p.name for p in tmp_path.iterdir()] == ["source.bin"]


def test_enospc_preserves_existing_valid_file_on_force_refresh(tmp_path, monkeypatch):
    dest = tmp_path / "existing.bin"
    dest.write_bytes(b"original-valid-bytes")
    source = tmp_path / "new.bin"
    source.write_bytes(b"replacement-bytes" * 4)
    real_open_staging = download.open_staging_file

    monkeypatch.setattr(
        download,
        "open_staging_file",
        lambda *args, **kwargs: _EnospcWriter(real_open_staging(*args, **kwargs), fail_after_bytes=8),
    )
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(0))

    with pytest.raises(InsufficientSpaceError):
        fetch_file(source.as_uri(), destination=dest, force=True)

    assert dest.read_bytes() == b"original-valid-bytes"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["existing.bin", "new.bin"]

    # return_stale_on_error=True falls back to the valid cached copy on ENOSPC.
    returned = fetch_file(source.as_uri(), destination=dest, force=True, return_stale_on_error=True)
    assert returned == str(dest)
    assert dest.read_bytes() == b"original-valid-bytes"


def test_enospc_during_gzip_and_zip_decompression_cleans_all_staging(tmp_path, monkeypatch):
    gz_source = tmp_path / "sample.txt.gz"
    with gzip.open(gz_source, "wb") as handle:
        handle.write(b"decompressed payload" * 10)

    zip_source = tmp_path / "sample.zip"
    with zipfile.ZipFile(zip_source, "w") as archive:
        archive.writestr("sample.txt", b"zipped payload" * 10)

    def fail_copy(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(download, "_copy_with_progress", fail_copy)
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(5))

    gz_dest = tmp_path / "out_gz.txt"
    with pytest.raises(InsufficientSpaceError) as gz_err:
        fetch_file(gz_source.as_uri(), destination=gz_dest, decompress=True, expected_size=200)
    assert gz_err.value.path == str(gz_dest)
    assert gz_err.value.needed_bytes == 200
    assert gz_err.value.free_bytes == 5
    assert not gz_dest.exists()

    zip_dest = tmp_path / "out_zip.txt"
    with pytest.raises(InsufficientSpaceError) as zip_err:
        fetch_file(zip_source.as_uri(), destination=zip_dest, decompress=True)
    assert zip_err.value.path == str(zip_dest)
    assert zip_err.value.needed_bytes == len(b"zipped payload" * 10)
    assert zip_err.value.free_bytes == 5
    assert not zip_dest.exists()

    assert sorted(p.name for p in tmp_path.iterdir()) == ["sample.txt.gz", "sample.zip"]


def test_enospc_during_publish_or_makedirs_raises_insufficient_space(tmp_path, monkeypatch):
    source = tmp_path / "src.dat"
    source.write_bytes(b"payload")
    dest = tmp_path / "dst.dat"

    def fail_replace(src, dst):
        raise OSError(errno.ENOSPC, "No space left on device", dst)

    monkeypatch.setattr(os, "replace", fail_replace)
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(0))

    with pytest.raises(InsufficientSpaceError) as err:
        fetch_file(source.as_uri(), destination=dest)
    assert err.value.path == str(dest)
    assert err.value.needed_bytes == 7
    assert not dest.exists()
    assert [p.name for p in tmp_path.iterdir()] == ["src.dat"]

    def fail_makedirs(path, exist_ok=False):
        raise OSError(errno.ENOSPC, "No space left on device", path)

    monkeypatch.setattr(os, "makedirs", fail_makedirs)
    nested_dest = tmp_path / "sub" / "dst.dat"
    with pytest.raises(InsufficientSpaceError) as mk_err:
        fetch_file(source.as_uri(), destination=nested_dest, expected_size=7)
    assert mk_err.value.path == str(nested_dest)
    assert mk_err.value.needed_bytes == 7


@pytest.mark.skipif(os.name != "posix", reason="POSIX resumable downloads")
def test_enospc_mid_resumable_download_keeps_only_private_partial_and_resumes(tmp_path, monkeypatch):
    payload = b"0123456789abcdef" * 8
    digest = hashlib.sha256(payload).hexdigest()
    requests_seen = []

    def fake_get(url, headers=None, **kwargs):
        headers = dict(headers or {})
        requests_seen.append(headers)
        range_header = headers.get("Range")
        offset = int(range_header[6:-1]) if range_header else 0
        body = payload[offset:]
        chunks = [body[i:i + 16] for i in range(0, len(body), 16)]
        resp_headers = {"Content-Length": str(len(body)), "ETag": '"v1"'}
        if offset:
            resp_headers["Content-Range"] = "bytes %d-%d/%d" % (offset, len(payload) - 1, len(payload))
        return _FakeResponse(chunks, headers=resp_headers, status_code=206 if offset else 200)

    monkeypatch.setattr(requests, "get", fake_get)
    url = "https://example.test/data.bin"
    dest = tmp_path / "data.bin"

    real_fdopen = os.fdopen

    def enospc_fdopen(fd, mode="r", *args, **kwargs):
        handle = real_fdopen(fd, mode, *args, **kwargs)
        if mode == "r+b":
            return _EnospcWriter(handle, fail_after_bytes=32)
        return handle

    with monkeypatch.context() as patch:
        patch.setattr(os, "fdopen", enospc_fdopen)
        patch.setattr(shutil, "disk_usage", _fake_disk_usage(10))
        with pytest.raises(InsufficientSpaceError) as exc_info:
            fetch_file(
                url,
                destination=dest,
                resume=True,
                expected_sha256=digest,
                expected_size=len(payload),
                chunk_size=16,
                max_retries=2,
                retry_backoff=0,
            )

    err = exc_info.value
    assert err.errno == errno.ENOSPC
    assert err.path == str(dest)
    assert err.needed_bytes == len(payload) - 32
    assert err.free_bytes == 10
    assert len(requests_seen) == 1
    # Final destination MUST NOT exist after ENOSPC.
    assert not dest.exists()
    # Private resumable partial holds the 32 bytes written before ENOSPC.
    assert datacache.resume.inspect_partial_bytes(
        dest, download_url=url, expected_sha256=digest, expected_size=len(payload)
    ) == 32

    # ENOSPC during the final staging copy removes the sibling staging file,
    # leaves destination absent, and retains the completed private partial.
    real_open_staging = download.open_staging_file
    with monkeypatch.context() as patch:
        patch.setattr(
            download,
            "open_staging_file",
            lambda *args, **kwargs: _EnospcWriter(real_open_staging(*args, **kwargs), fail_after_bytes=16),
        )
        patch.setattr(shutil, "disk_usage", _fake_disk_usage(4))
        with pytest.raises(InsufficientSpaceError) as stage_err:
            fetch_file(
                url,
                destination=dest,
                resume=True,
                expected_sha256=digest,
                expected_size=len(payload),
                chunk_size=16,
            )
    assert stage_err.value.path == str(dest)
    assert stage_err.value.needed_bytes == len(payload)
    assert stage_err.value.free_bytes == 4
    assert not dest.exists()
    assert not list(tmp_path.glob(".datacache-*.tmp"))
    assert requests_seen[1]["Range"] == "bytes=32-"
    assert datacache.resume.inspect_partial_bytes(
        dest, download_url=url, expected_sha256=digest, expected_size=len(payload)
    ) == len(payload)

    # Once space is freed, fetch_file validates the completed partial without
    # another HTTP request and publishes cleanly.
    result = fetch_file(
        url,
        destination=dest,
        resume=True,
        expected_sha256=digest,
        expected_size=len(payload),
        chunk_size=16,
    )
    assert result == str(dest)
    assert dest.read_bytes() == payload
    assert len(requests_seen) == 2


@pytest.mark.skipif(os.name != "posix", reason="POSIX resumable downloads")
def test_enospc_in_resume_lock_is_not_swallowed_into_cannot_resume(tmp_path, monkeypatch):
    from datacache import resume as resume_mod

    dest = tmp_path / "locked.bin"
    monkeypatch.setattr(
        resume_mod,
        "file_lock",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError(errno.ENOSPC, "No space left on device")),
    )
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(0))

    with pytest.raises(InsufficientSpaceError) as err:
        fetch_file(
            "https://example.test/locked.bin",
            destination=dest,
            resume="auto",
            expected_size=1024,
        )
    assert err.value.path == str(dest)
    assert err.value.needed_bytes == 1024
    assert not dest.exists()


def test_enospc_in_fetch_and_transform_and_materialize_cleans_staging(tmp_path, monkeypatch):
    source = tmp_path / "input.txt"
    source.write_bytes(b"hello")
    cache_root = tmp_path / "cache"
    monkeypatch.setattr(shutil, "disk_usage", _fake_disk_usage(0))

    def enospc_transformer(src, out):
        raise OSError(errno.ENOSPC, "No space left on device", out)

    with pytest.raises(InsufficientSpaceError) as transform_err:
        fetch_and_transform(
            "derived.txt",
            enospc_transformer,
            lambda p: open(p, "rb").read(),
            "input.txt",
            source.as_uri(),
            cache_root=cache_root,
        )
    assert transform_err.value.path == str(cache_root / "derived.txt")
    assert not (cache_root / "derived.txt").exists()
    assert not list(cache_root.glob(".datacache-transform-*"))

    mat_store = tmp_path / "mat_store"

    def enospc_builder(src_paths, out_paths):
        raise OSError(errno.ENOSPC, "No space left on device")

    with pytest.raises(InsufficientSpaceError) as mat_err:
        materialize(
            mat_store,
            {"in.txt": {"path": source}},
            transform={"version": "v1", "options": {}},
            outputs={"out.txt": {}},
            builder=enospc_builder,
            download_options={"show_progress": False},
        )
    assert mat_err.value.path == str(mat_store)
    assert not list(mat_store.glob(".staging-*"))


def test_permission_error_is_distinct_from_insufficient_space_and_transport(tmp_path, monkeypatch):
    dest = tmp_path / "denied.bin"
    source = tmp_path / "src.bin"
    source.write_bytes(b"abc")

    def deny(*args, **kwargs):
        raise PermissionError(errno.EACCES, "Permission denied", str(dest))

    monkeypatch.setattr(download, "open_staging_file", deny)

    with pytest.raises(PermissionError) as exc_info:
        fetch_file(source.as_uri(), destination=dest)
    assert not isinstance(exc_info.value, InsufficientSpaceError)
    assert not isinstance(exc_info.value, requests.RequestException)
    assert not dest.exists()
