"""Offline integrity/publication tests, including deterministic concurrent writers."""

import builtins
from concurrent.futures import ThreadPoolExecutor
import errno
import gzip
import hashlib
import os
from pathlib import Path
import tempfile
import threading
import zipfile

import pandas as pd
import pytest
import requests

from datacache import FileValidationError, fetch_file, validate_file
from datacache import common, download


PAYLOAD = b"the complete installed bytes\n" * 100


def sha256(data):
    return hashlib.sha256(data).hexdigest()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def reject(*args, **kwargs):
        pytest.fail("unexpected network request")

    urlopen = download.urllib.request.urlopen

    def local_only(request, *args, **kwargs):
        assert request.full_url.startswith("file://")
        return urlopen(request, *args, **kwargs)

    monkeypatch.setattr(download.requests, "get", reject)
    monkeypatch.setattr(download.urllib.request, "urlopen", local_only)


@pytest.fixture
def source(tmp_path):
    source = tmp_path / "source.bin"
    source.write_bytes(PAYLOAD)
    return source


def reject_mutation(*args, **kwargs):
    pytest.fail("inspection attempted a filesystem mutation")


def test_explicit_destination_and_verified_offline_reuse(source, tmp_path, monkeypatch):
    destination = tmp_path / "custom" / "version-1" / "exact name;with spaces.bin"
    progress = []
    assert fetch_file(
        source.as_uri(), destination=destination,
        expected_sha256=sha256(PAYLOAD).upper(), expected_size=len(PAYLOAD),
        timeout=5, chunk_size=7,
        progress_callback=lambda done, total: progress.append((done, total)),
    ) == str(destination)
    assert destination.read_bytes() == PAYLOAD
    assert progress[-1] == (len(PAYLOAD), len(PAYLOAD))
    assert len(progress) > 1
    source.unlink()
    destination.chmod(0o444)
    destination.parent.chmod(0o555)
    original_open = builtins.open

    def read_only_open(file, mode="r", *args, **kwargs):
        assert not any(flag in mode for flag in "wax+")
        return original_open(file, mode, *args, **kwargs)

    try:
        # Reject write attempts explicitly, even when running as root.
        with monkeypatch.context() as readonly:
            readonly.setattr(builtins, "open", read_only_open)
            for operation in ("makedirs", "mkdir", "replace", "remove"):
                readonly.setattr(download.os, operation, reject_mutation)
            readonly.setattr(download, "_open_staging_file", reject_mutation)
            assert fetch_file(
                "https://unavailable.invalid/file", destination=destination,
                expected_sha256=sha256(PAYLOAD), expected_size=len(PAYLOAD),
            ) == str(destination)
            assert validate_file(destination, sha256(PAYLOAD), len(PAYLOAD)) == str(destination)
            with pytest.raises(FileNotFoundError):
                validate_file(destination.parent / "absent" / "file")
    finally:
        destination.parent.chmod(0o755)
        destination.chmod(0o644)
    assert list(destination.parent.iterdir()) == [destination]


@pytest.mark.parametrize("metadata,match", [
    ({"expected_size": len(PAYLOAD) + 1}, "size mismatch"),
    ({"expected_sha256": "0" * 64}, "SHA-256 mismatch"),
])
def test_corrupt_cache_requires_explicit_repair(source, tmp_path, metadata, match):
    destination = tmp_path / "cache" / "file"
    destination.parent.mkdir()
    destination.write_bytes(PAYLOAD)
    with pytest.raises(FileValidationError, match=match + ".*force=True") as caught:
        fetch_file("https://unavailable.invalid/file", destination=destination, **metadata)
    assert caught.value.path == str(destination)
    assert destination.read_bytes() == PAYLOAD
    destination.write_bytes(b"truncated")
    fetch_file(
        source.as_uri(), destination=destination, force=True,
        expected_sha256=sha256(PAYLOAD), expected_size=len(PAYLOAD))
    assert destination.read_bytes() == PAYLOAD


def test_expected_metadata_with_legacy_cache_arguments(source, tmp_path, monkeypatch):
    monkeypatch.setattr(common, "get_data_dir", lambda subdir: str(tmp_path / subdir))
    kwargs = dict(filename="legacy.bin", subdir="release-v1",
                  expected_sha256=sha256(PAYLOAD), expected_size=len(PAYLOAD))
    destination = tmp_path / "release-v1" / "legacy.bin"
    assert fetch_file(source.as_uri(), **kwargs) == str(destination)
    assert fetch_file("https://unavailable.invalid/file", **kwargs) == str(destination)
    destination.write_bytes(b"incomplete")
    with pytest.raises(FileValidationError, match="size mismatch"):
        fetch_file("https://unavailable.invalid/file", **kwargs)


@pytest.mark.parametrize("metadata", [
    {"expected_sha256": "bad"}, {"expected_sha256": 123},
    {"expected_size": -1}, {"expected_size": 1.5}, {"expected_size": True},
    {"chunk_size": 0}, {"chunk_size": -1}, {"chunk_size": True},
    {"filename": "conflict"}, {"subdir": "conflict"},
])
def test_invalid_arguments_do_not_create_directories(tmp_path, metadata):
    destination = tmp_path / "absent" / "file"
    with pytest.raises(ValueError):
        fetch_file("https://unavailable.invalid/file", destination=destination, **metadata)
    assert not destination.parent.exists()


def test_empty_file_and_relative_destination(tmp_path, monkeypatch):
    source = tmp_path / "empty"
    source.touch()
    monkeypatch.chdir(tmp_path)
    assert fetch_file(
        source.as_uri(), destination="relative", expected_size=0,
        expected_sha256=sha256(b"")) == "relative"
    assert (tmp_path / "relative").read_bytes() == b""


@pytest.mark.parametrize("kind", ["gzip", "zip", "stored-gzip", "named-gzip", "html"])
def test_integrity_describes_installed_bytes(tmp_path, monkeypatch, kind):
    if kind == "html":
        source = tmp_path / "source.html"
        source.write_text("<table><tr><th>value</th></tr><tr><td>1</td></tr></table>")
        # Avoid requiring an optional HTML parser in the test environment.
        monkeypatch.setattr(download.pd, "read_html", lambda *a, **k: [pd.DataFrame({"value": [1]})])
        destination = tmp_path / "cache" / "file.csv"
        installed = pd.DataFrame({"value": [1]}).to_csv(index=False).encode("utf-8")
    else:
        source = tmp_path / ("source.zip" if kind == "zip" else "source.gz")
        if kind == "zip":
            with zipfile.ZipFile(source, "w") as archive:
                archive.writestr("file", PAYLOAD)
                archive.writestr("larger", b"ignore" * len(PAYLOAD))
        else:
            source.write_bytes(gzip.compress(PAYLOAD))
        destination = tmp_path / "cache" / ("file.gz" if kind in ("stored-gzip", "named-gzip") else "file")
        installed = source.read_bytes() if kind == "stored-gzip" else PAYLOAD
    fetch_file(
        source.as_uri(), destination=destination, decompress=kind == "named-gzip",
        expected_sha256=sha256(installed), expected_size=len(installed))
    assert destination.read_bytes() == installed
    assert list(destination.parent.iterdir()) == [destination]
    if kind != "stored-gzip":
        with pytest.raises(FileValidationError):
            fetch_file(
                source.as_uri(), destination=destination, force=True,
                decompress=kind == "named-gzip", expected_sha256=sha256(source.read_bytes()))
        assert destination.read_bytes() == installed
        assert list(destination.parent.iterdir()) == [destination]


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("failure", ["transfer", "callback", "gzip", "zip", "html", "size", "hash", "publish", "cancel"])
def test_failed_download_preserves_destination_and_cleans_staging(
        source, tmp_path, monkeypatch, existing, failure):
    destination = tmp_path / "cache" / ("file.csv" if failure == "html" else "file")
    destination.parent.mkdir()
    if existing:
        destination.write_bytes(b"previous complete artifact")
    kwargs = {}
    expected_error = Exception
    if failure in ("transfer", "cancel"):
        expected_error = requests.ConnectionError if failure == "transfer" else KeyboardInterrupt

        def interrupted(url, stream, **kwargs):
            stream.write(b"partial transfer")
            raise expected_error("injected failure")

        monkeypatch.setattr(download, "_stream_to_file", interrupted)
    elif failure == "callback":
        def interrupted_callback(*args):
            raise RuntimeError("progress failed")
        kwargs["progress_callback"] = interrupted_callback
    elif failure in ("gzip", "zip"):
        source = tmp_path / ("bad.gz" if failure == "gzip" else "bad.zip")
        if failure == "gzip":
            source.write_bytes(gzip.compress(PAYLOAD)[:-8])
        else:
            source.write_bytes(b"invalid zip")
    elif failure == "html":
        source = tmp_path / "source.html"
        source.write_bytes(b"html fixture")
        monkeypatch.setattr(download.pd, "read_html", lambda *a, **k: [pd.DataFrame()])

        def interrupted_csv(self, path, **kwargs):
            Path(path).write_bytes(b"partial conversion")
            raise RuntimeError("conversion failed")

        monkeypatch.setattr(download.pd.DataFrame, "to_csv", interrupted_csv)
    elif failure == "size":
        kwargs["expected_size"] = len(PAYLOAD) + 1
        expected_error = FileValidationError
    elif failure == "hash":
        kwargs["expected_sha256"] = "0" * 64
        expected_error = FileValidationError
    else:
        def interrupted_publish(src, dst):
            raise PermissionError("publication denied")
        monkeypatch.setattr(download.os, "replace", interrupted_publish)
        expected_error = PermissionError
    with pytest.raises(expected_error):
        fetch_file(source.as_uri(), destination=destination, force=True, **kwargs)
    if existing:
        assert destination.read_bytes() == b"previous complete artifact"
        assert list(destination.parent.iterdir()) == [destination]
    else:
        assert list(destination.parent.iterdir()) == []


@pytest.mark.parametrize("compressed", [False, True])
def test_staging_uses_destination_filesystem(source, tmp_path, monkeypatch, compressed):
    system_temp = tmp_path / "different-filesystem"
    system_temp.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(system_temp))
    if compressed:
        source = tmp_path / "source.gz"
        source.write_bytes(gzip.compress(PAYLOAD))
    destination = tmp_path / "cache" / "file"
    real_temp = download._open_staging_file
    real_replace = os.replace
    staging_paths = []

    def checked_temp(*args, **kwargs):
        assert Path(kwargs["directory"]) == destination.parent
        result = real_temp(*args, **kwargs)
        staging_paths.append(result.name)
        return result

    def checked_replace(src, dst):
        # Model EXDEV deterministically without requiring two mounted devices.
        if Path(src).parent != Path(dst).parent:
            raise OSError(errno.EXDEV, "cross-device rename")
        assert Path(src).read_bytes() == PAYLOAD
        real_replace(src, dst)

    monkeypatch.setattr(download, "_open_staging_file", checked_temp)
    monkeypatch.setattr(download.os, "replace", checked_replace)
    fetch_file(source.as_uri(), destination=destination, expected_sha256=sha256(PAYLOAD))
    assert len(staging_paths) == (2 if compressed else 1)
    assert destination.read_bytes() == PAYLOAD
    assert list(system_temp.iterdir()) == []
    assert list(destination.parent.iterdir()) == [destination]


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("compressed", [False, True])
def test_simultaneous_fetches_publish_only_complete_files(
        source, tmp_path, monkeypatch, existing, compressed):
    if compressed:
        source = tmp_path / "source.gz"
        source.write_bytes(gzip.compress(PAYLOAD))
    destination = tmp_path / "cache" / "file"
    if existing:
        destination.parent.mkdir()
        destination.write_bytes(b"old complete file")
    started = threading.Barrier(3, timeout=10)
    release = threading.Event()
    ready_to_publish = threading.Barrier(2, timeout=10)
    real_stream = download._stream_to_file
    real_replace = os.replace
    staging_paths = []

    def paused_stream(url, stream, **kwargs):
        staging_paths.append(stream.name)
        stream.write(b"partial")
        stream.flush()
        started.wait()
        assert release.wait(timeout=10)
        stream.seek(0)
        stream.truncate()
        return real_stream(url, stream, **kwargs)

    def checked_replace(src, dst):
        assert Path(src).read_bytes() == PAYLOAD
        ready_to_publish.wait()
        real_replace(src, dst)
        assert Path(dst).read_bytes() == PAYLOAD

    monkeypatch.setattr(download, "_stream_to_file", paused_stream)
    monkeypatch.setattr(download.os, "replace", checked_replace)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(
            fetch_file, source.as_uri(), destination=destination, force=existing,
            expected_sha256=sha256(PAYLOAD), expected_size=len(PAYLOAD),
        ) for _ in range(2)]
        try:
            started.wait()
            assert len(set(staging_paths)) == 2
            if existing:
                assert destination.read_bytes() == b"old complete file"
            else:
                assert not destination.exists()
        finally:
            release.set()
        assert [future.result(timeout=10) for future in futures] == [str(destination)] * 2
    assert destination.read_bytes() == PAYLOAD
    assert list(destination.parent.iterdir()) == [destination]


def test_permission_errors_are_not_cache_misses(source, tmp_path, monkeypatch):
    destination = tmp_path / "unreadable"
    destination.write_bytes(PAYLOAD)
    original_open = builtins.open

    def denied_open(file, *args, **kwargs):
        if os.fspath(file) == str(destination):
            raise PermissionError("unreadable installation")
        return original_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", denied_open)
    monkeypatch.setattr(download.os, "makedirs", reject_mutation)
    with pytest.raises(PermissionError, match="unreadable installation"):
        validate_file(destination)
    with pytest.raises(PermissionError, match="unreadable installation"):
        fetch_file(source.as_uri(), destination=destination)


def test_directory_is_not_a_cached_file(tmp_path):
    with pytest.raises(FileValidationError, match="regular file"):
        fetch_file("https://unavailable.invalid/file", destination=tmp_path)


@pytest.mark.parametrize("failed", [False, True])
def test_http_timeout_progress_and_response_cleanup(tmp_path, monkeypatch, failed):
    destination = tmp_path / "cache" / "file"

    class Response:
        headers = {"Content-Length": str(len(PAYLOAD))}
        closed = False

        def close(self):
            self.closed = True

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            assert chunk_size == 8
            yield b""
            yield PAYLOAD[:8]
            if failed:
                raise requests.ConnectionError("interrupted HTTP")
            yield PAYLOAD[8:]

    response = Response()

    def get(url, timeout, stream):
        assert timeout == 3
        assert stream is True
        return response

    monkeypatch.setattr(download.requests, "get", get)
    progress = []
    kwargs = dict(destination=destination, timeout=3, chunk_size=8,
                  progress_callback=lambda done, total: progress.append((done, total)))
    if failed:
        with pytest.raises(requests.ConnectionError):
            fetch_file("https://example.invalid/file", **kwargs)
        assert list(destination.parent.iterdir()) == []
    else:
        fetch_file("https://example.invalid/file", **kwargs)
        assert destination.read_bytes() == PAYLOAD
        assert progress == [(8, len(PAYLOAD)), (len(PAYLOAD), len(PAYLOAD))]
    assert response.closed
