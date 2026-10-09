"""max_bytes: no file a fetch writes ever holds more than the caller allows (#108)."""

import gzip
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import os
from pathlib import Path
import threading
import zipfile

import pytest

from datacache import Cache, FileValidationError, fetch_bytes, fetch_file, install_bundle
from datacache import download
from datacache.download import validate_download_options

BODY = bytes(range(256)) * 256  # 64 KiB that doesn't compress to nothing.


@pytest.fixture(autouse=True)
def local_requests(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setattr(download.time, "sleep", lambda seconds: None)


@pytest.fixture
def serve():
    """serve(body, how) -> (url, request paths); how is how the length is sent."""
    servers = []

    def start(body=BODY, how="length", path="/data"):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                requests.append(self.path)
                self.send_response(200)
                self.send_header("Connection", "close")
                payload = body
                if how == "gzip":
                    payload = gzip.compress(body, mtime=0)
                    self.send_header("Content-Encoding", "gzip")
                if how in ("length", "gzip"):
                    self.send_header("Content-Length", str(len(payload)))
                if how == "overstated":
                    # Chunked transfer overrides any Content-Length (RFC 9112).
                    self.send_header("Content-Length", str(100 * len(payload)))
                if how in ("chunked", "overstated"):
                    self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                if how in ("chunked", "overstated"):
                    for start in range(0, len(payload), 4096):
                        piece = payload[start:start + 4096]
                        self.wfile.write(b"%x\r\n%s\r\n" % (len(piece), piece))
                    self.wfile.write(b"0\r\n\r\n")
                else:
                    self.wfile.write(payload)
                self.wfile.flush()
                self.close_connection = True

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        worker.start()
        servers.append((server, worker))
        return "http://127.0.0.1:%d%s" % (server.server_port, path), requests

    yield start
    for server, worker in servers:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)


def leftovers(directory, keep):
    return sorted(path.name for path in Path(directory).iterdir() if path.name not in keep)


def test_a_content_length_over_the_limit_is_refused_before_reading(serve, tmp_path):
    url, requests = serve(how="length")
    destination = tmp_path / "data"
    written = []
    with pytest.raises(FileValidationError, match="max_bytes=1000"):
        fetch_file(url, destination=destination, raw=True, max_bytes=1000,
                   progress_callback=lambda done, total: written.append(done))
    assert written == []  # Nothing was written.
    assert requests == ["/data"]  # Not retried.
    assert leftovers(tmp_path, keep=()) == []


@pytest.mark.parametrize("how", ["none", "chunked", "gzip"])
def test_bodies_of_unknown_length_never_write_past_the_limit(serve, tmp_path, how):
    # With no Content-Length, or one that counts gzip-encoded bytes, the
    # limit has to be enforced chunk by chunk as the decoded body arrives.
    url, requests = serve(how=how)
    destination = tmp_path / "data"
    destination.write_bytes(b"the previous file")
    written = []
    with pytest.raises(FileValidationError, match="max_bytes=10000"):
        fetch_file(url, destination=destination, raw=True, force=True, max_bytes=10000,
                   chunk_size=1024, progress_callback=lambda done, total: written.append(done))
    assert written and max(written) <= 10000
    assert requests == ["/data"]
    assert destination.read_bytes() == b"the previous file"
    assert leftovers(tmp_path, keep=("data",)) == []


@pytest.mark.parametrize("how", ["length", "none", "chunked", "gzip", "overstated"])
def test_a_body_of_exactly_max_bytes_is_accepted(serve, tmp_path, how):
    url, _ = serve(how=how)
    destination = tmp_path / "data"
    fetch_file(url, destination=destination, raw=True, max_bytes=len(BODY), expected_size=len(BODY))
    assert destination.read_bytes() == BODY


def test_cache_hits_are_unaffected(serve, tmp_path):
    url, requests = serve()
    destination = tmp_path / "data"
    destination.write_bytes(BODY)
    assert fetch_file(url, destination=destination, raw=True, max_bytes=1) == str(destination)
    assert requests == []


def test_decompressed_files_are_limited_too(serve, tmp_path):
    # 64 KiB of zeros compresses to well under the limit, then expands past it.
    zeros = bytes(len(BODY))
    url, _ = serve(gzip.compress(zeros, mtime=0), path="/data.gz")
    destination = tmp_path / "data.txt"
    destination.write_bytes(b"the previous file")
    with pytest.raises(FileValidationError, match="decompressed file has more than max_bytes=10000"):
        fetch_file(url, destination=destination, decompress=True, force=True, max_bytes=10000)
    assert destination.read_bytes() == b"the previous file"
    assert leftovers(tmp_path, keep=("data.txt",)) == []

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zipped:
        zipped.writestr("data.txt", zeros)
    url, _ = serve(archive.getvalue(), path="/data.zip")
    with pytest.raises(FileValidationError, match="decompressed file has more than max_bytes=10000"):
        fetch_file(url, destination=destination, decompress=True, force=True, max_bytes=10000)
    assert destination.read_bytes() == b"the previous file"
    fetch_file(url, destination=destination, decompress=True, force=True, max_bytes=len(zeros))
    assert destination.read_bytes() == zeros


def test_local_files_over_the_limit_are_refused(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(BODY)
    with pytest.raises(FileValidationError, match="max_bytes"):
        fetch_file(source.as_uri(), destination=tmp_path / "copy", raw=True, max_bytes=100)
    assert leftovers(tmp_path, keep=("source",)) == []


@pytest.mark.parametrize("how", ["length", "chunked", "gzip"])
def test_fetch_bytes_keeps_no_more_than_max_bytes(serve, how):
    url, requests = serve(how=how)
    with pytest.raises(ValueError, match="more than max_bytes=100"):
        fetch_bytes(url, max_bytes=100)
    assert requests == ["/data"]
    assert fetch_bytes(url, max_bytes=len(BODY)) == BODY


@pytest.mark.parametrize("options, message", [
    (dict(max_bytes=-1), "non-negative"),
    (dict(max_bytes=True), "non-negative"),
    (dict(max_bytes=1.5), "non-negative"),
    (dict(max_bytes=10, expected_size=11), "larger than max_bytes"),
])
def test_invalid_limits_are_rejected_before_any_request(serve, tmp_path, options, message):
    url, requests = serve()
    with pytest.raises(ValueError, match=message):
        fetch_file(url, destination=tmp_path / "data", raw=True, **options)
    assert requests == []


def test_html_conversion_cant_be_bounded(serve, tmp_path):
    url, requests = serve(path="/table.html")
    with pytest.raises(ValueError, match="HTML-to-CSV"):
        fetch_file(url, destination=tmp_path / "table.csv", max_bytes=10)
    # The converter guards itself too, for direct callers of the old entry point.
    with pytest.raises(ValueError, match="HTML-to-CSV"):
        download._download_and_decompress_if_necessary(str(tmp_path / "table.csv"), url, max_bytes=10)
    assert requests == []


def test_a_zero_limit_needs_allow_empty(serve, tmp_path):
    url, requests = serve(b"", how="length")
    with pytest.raises(ValueError, match="allow_empty"):
        fetch_file(url, destination=tmp_path / "empty", raw=True, max_bytes=0)
    with pytest.raises(ValueError, match="allow_empty"):
        fetch_bytes(url, max_bytes=0)
    with pytest.raises(ValueError, match="empty"):
        validate_download_options({"max_bytes": 0}, "bundle")
    assert requests == []
    fetch_file(url, destination=tmp_path / "empty", raw=True, max_bytes=0, allow_empty=True)
    assert (tmp_path / "empty").read_bytes() == b""
    assert fetch_bytes(url, max_bytes=0, allow_empty=True) == b""


def test_installs_check_known_sizes_before_creating_anything(serve, tmp_path):
    from datacache import install_archive, materialize
    url, requests = serve()
    small = {"url": url, "sha256": "0" * 64, "size": 10}
    large = {"url": url, "sha256": "0" * 64, "size": 10 ** 9}
    options = {"max_bytes": 10000}
    if os.name == "posix":
        with pytest.raises(ValueError, match="b size 1000000000 is larger than max_bytes 10000"):
            install_bundle(tmp_path / "bundle", {"a": small, "b": large}, download_options=options)
        with pytest.raises(ValueError, match="b size 1000000000 is larger than max_bytes 10000"):
            materialize(tmp_path / "derived", {"a": small, "b": large}, transform={"version": "1"},
                        outputs={"out": {}}, builder=lambda inputs, outputs: None, download_options=options)
    with pytest.raises(ValueError, match="part 1 size 1000000000 is larger than max_bytes 10000"):
        install_archive(tmp_path / "archive", [small, large], download_options=options)
    assert requests == []
    assert sorted(path.name for path in tmp_path.iterdir()) == []


def test_cache_fetch_and_install_download_options_take_max_bytes(serve, tmp_path):
    url, _ = serve()
    with pytest.raises(FileValidationError, match="max_bytes=100"):
        Cache(cache_root=tmp_path / "cache").fetch(url, filename="data", raw=True, max_bytes=100)
    assert validate_download_options({"max_bytes": 100}, "bundle") == {"max_bytes": 100}
    with pytest.raises(ValueError, match="max_bytes"):
        validate_download_options({"max_bytes": -1}, "bundle")
    if os.name != "posix":
        return  # Bundle installation is POSIX-only (#89).
    store = tmp_path / "bundle"
    with pytest.raises(FileValidationError, match="max_bytes=100"):
        install_bundle(store, {"data": {"url": url}}, verified=False, download_options={"max_bytes": 100})
    assert not list((store / "bundles").iterdir())


@pytest.mark.skipif(os.name != "posix", reason="resume=True requires POSIX")
def test_resumable_downloads_check_their_size_against_the_limit(serve, tmp_path):
    url, _ = serve()
    with pytest.raises(ValueError, match="larger than max_bytes"):
        fetch_file(url, destination=tmp_path / "data", raw=True, resume=True,
                   expected_size=len(BODY), max_bytes=len(BODY) - 1)
    path = fetch_file(url, destination=tmp_path / "data", raw=True, resume=True,
                      expected_sha256=sha256(BODY).hexdigest(), expected_size=len(BODY), max_bytes=len(BODY))
    assert Path(path).read_bytes() == BODY
