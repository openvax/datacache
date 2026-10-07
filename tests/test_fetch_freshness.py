"""fetch_bytes, and fetch_file's expire_after, stale_if_error and validator."""

from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
import threading

import pytest
import requests

from datacache import Cache, FileValidationError, fetch_bytes, fetch_file
from datacache import download, provenance


@pytest.fixture(autouse=True)
def waits(monkeypatch):
    delays = []
    monkeypatch.setattr(download.time, "sleep", delays.append)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    return delays


@pytest.fixture
def server():
    """A local server answering each GET with the next of its responses (the
    last repeats); a response is a body, or a (status, body) pair."""
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            index = len(self.server.requests)
            self.server.requests.append(self.path)
            responses = self.server.responses
            response = responses[min(index, len(responses) - 1)]
            status, body = response if isinstance(response, tuple) else (200, response)
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.requests = []
    httpd.responses = [b"first\n"]
    httpd.url = "http://127.0.0.1:%d/listing.html" % httpd.server_port
    worker = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    worker.start()
    yield httpd
    httpd.shutdown()
    httpd.server_close()
    worker.join(timeout=5)


def age(path, seconds):
    """Make path look fetched `seconds` ago by its modification time."""
    past = datetime.now().timestamp() - seconds
    os.utime(path, (past, past))


# fetch_bytes


def test_fetch_bytes_returns_the_body_without_writing(server, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert fetch_bytes(server.url, timeout=5) == b"first\n"
    assert list(tmp_path.iterdir()) == []


def test_fetch_bytes_retries_like_downloads(server, waits):
    server.responses = [(503, b"busy"), (429, b"slow down"), b"done"]
    assert fetch_bytes(server.url) == b"done"
    assert len(server.requests) == 3 and len(waits) == 2


def test_fetch_bytes_does_not_retry_permanent_errors(server):
    server.responses = [(404, b"missing")]
    with pytest.raises(requests.HTTPError):
        fetch_bytes(server.url)
    assert len(server.requests) == 1


def test_fetch_bytes_rejects_empty_bodies_unless_allowed(server):
    server.responses = [b""]
    with pytest.raises(ValueError, match="returned no bytes"):
        fetch_bytes(server.url)
    assert len(server.requests) == 3  # Retried as transient first.
    assert fetch_bytes(server.url, allow_empty=True) == b""


def test_fetch_bytes_reads_file_urls(tmp_path):
    source = tmp_path / "local.txt"
    source.write_bytes(b"local\n")
    assert fetch_bytes(source.as_uri()) == b"local\n"


@pytest.mark.parametrize("options", [
    {"max_retries": -1}, {"allow_empty": "yes"}, {"retry_backoff": float("nan")},
])
def test_fetch_bytes_validates_options(server, options):
    with pytest.raises(ValueError):
        fetch_bytes(server.url, **options)
    assert server.requests == []


# expire_after


def test_fresh_copy_is_reused_without_a_request(server, tmp_path):
    path = fetch_file(server.url, destination=tmp_path / "listing.html", raw=True)
    server.responses = [b"second\n"]
    assert fetch_file(server.url, destination=path, raw=True, expire_after=3600) == path
    assert len(server.requests) == 1


@pytest.mark.parametrize("expire_after", [60, timedelta(minutes=1), 0])
def test_expired_copy_is_fetched_again(server, tmp_path, expire_after):
    path = fetch_file(server.url, destination=tmp_path / "listing.html", raw=True)
    age(path, 120)
    server.responses = [b"second\n"]
    fetch_file(server.url, destination=path, raw=True, expire_after=expire_after)
    assert open(path, "rb").read() == b"second\n"


def test_age_comes_from_the_provenance_record(server, tmp_path):
    path = fetch_file(server.url, destination=tmp_path / "listing.html", raw=True,
                      record_provenance=True)
    # The file is new by its modification time, but the record says it was
    # fetched two hours ago.
    record_path = provenance.sidecar_path(path)
    record = json.loads(open(record_path).read())
    record["fetched_at"] = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with open(record_path, "w") as handle:
        json.dump(record, handle)
    server.responses = [b"second\n"]
    fetch_file(server.url, destination=path, raw=True, expire_after=3600)
    assert open(path, "rb").read() == b"second\n"


@pytest.mark.parametrize("expire_after", [-1, True, float("inf"), "60"])
def test_invalid_expiry_is_rejected(server, tmp_path, expire_after):
    with pytest.raises(ValueError, match="expire_after"):
        fetch_file(server.url, destination=tmp_path / "x", expire_after=expire_after)
    assert server.requests == []


# stale_if_error


def test_failed_refresh_returns_the_cached_copy(server, tmp_path, caplog):
    path = fetch_file(server.url, destination=tmp_path / "listing.html", raw=True)
    age(path, 120)
    server.responses = [(503, b"down")]
    with caplog.at_level(logging.WARNING, logger="datacache"):
        assert fetch_file(server.url, destination=path, raw=True, expire_after=60,
                          stale_if_error=True) == path
    assert open(path, "rb").read() == b"first\n"
    assert "using the cached copy" in caplog.text


def test_forced_refresh_can_fall_back_too(server, tmp_path):
    path = fetch_file(server.url, destination=tmp_path / "listing.html", raw=True)
    server.responses = [(500, b"error")]
    assert fetch_file(server.url, destination=path, raw=True, force=True,
                      stale_if_error=True) == path


def test_errors_propagate_without_a_cached_copy_or_the_option(server, tmp_path):
    server.responses = [(503, b"down")]
    with pytest.raises(requests.HTTPError):
        fetch_file(server.url, destination=tmp_path / "missing.html", raw=True,
                   stale_if_error=True)
    server.responses = [b"first\n"]
    path = fetch_file(server.url, destination=tmp_path / "listing.html", raw=True)
    age(path, 120)
    server.responses = [(503, b"down")]
    with pytest.raises(requests.HTTPError):
        fetch_file(server.url, destination=path, raw=True, expire_after=60)
    assert open(path, "rb").read() == b"first\n"


# validator


def has_first(path):
    if b"first" not in open(path, "rb").read():
        raise ValueError("not a listing")


def test_validator_keeps_wrong_content_out_of_the_cache(server, tmp_path):
    server.responses = [b"proxy error page"]
    destination = tmp_path / "listing.html"
    with pytest.raises(FileValidationError, match="failed validation: not a listing") as raised:
        fetch_file(server.url, destination=destination, raw=True, validator=has_first)
    assert isinstance(raised.value.__cause__, ValueError)
    assert not destination.exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == []  # No staging files left.


def test_validator_returning_false_rejects(server, tmp_path):
    with pytest.raises(FileValidationError, match="failed validation"):
        fetch_file(server.url, destination=tmp_path / "x", raw=True,
                   validator=lambda path: False)


def test_rejected_refresh_keeps_and_can_return_the_cached_copy(server, tmp_path):
    path = fetch_file(server.url, destination=tmp_path / "listing.html", raw=True,
                      validator=has_first)
    server.responses = [b"proxy error page"]
    with pytest.raises(FileValidationError):
        fetch_file(server.url, destination=path, raw=True, force=True, validator=has_first)
    assert fetch_file(server.url, destination=path, raw=True, force=True,
                      validator=has_first, stale_if_error=True) == path
    assert open(path, "rb").read() == b"first\n"


def test_validator_checks_cache_hits(server, tmp_path):
    destination = tmp_path / "listing.html"
    destination.write_bytes(b"something else")
    with pytest.raises(FileValidationError, match="cached file failed validation.*force=True"):
        fetch_file(server.url, destination=destination, raw=True, validator=has_first)
    assert server.requests == []


def test_validator_is_not_combined_with_resume(server, tmp_path):
    with pytest.raises(ValueError, match="validator"):
        fetch_file(server.url, destination=tmp_path / "x", raw=True, resume=True,
                   expected_size=6, validator=has_first)


def test_cache_fetch_forwards_the_options(server, tmp_path):
    cache = Cache("freshness", cache_root=tmp_path)
    path = cache.fetch(server.url, filename="listing.html", raw=True, validator=has_first)
    age(path, 120)
    server.responses = [(503, b"down")]
    assert cache.fetch(server.url, filename="listing.html", raw=True, expire_after=60,
                       stale_if_error=True, validator=has_first) == path
    assert len(server.requests) == 4  # One download, then three refresh attempts.
