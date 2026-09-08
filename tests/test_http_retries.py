"""Exercise retries against a local HTTP server, with deterministic fake waits."""

from datetime import datetime, timezone
from email.utils import format_datetime
from fractions import Fraction
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import ssl
import stat
import threading
from time import sleep as real_sleep

import numpy as np
import pytest
import requests
from requests.packages.urllib3 import exceptions as urllib3_errors

from datacache import Cache, FileValidationError, fetch_file
from datacache import download, retries


PAYLOAD = b"complete downloaded data\n"


@pytest.fixture(autouse=True)
def waits(monkeypatch):
    delays = []
    monkeypatch.setattr(download.time, "sleep", delays.append)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    return delays


@pytest.fixture
def http_server():
    servers = []

    def start(*responses):
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                index = len(self.server.requests)
                self.server.requests.append(self.path)
                response = responses[min(index, len(responses) - 1)]
                body = response.get("body", PAYLOAD)
                self.send_response(response.get("status", 200))
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                for name, value in response.get("headers", {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(body[:response.get("cut_after", len(body))])
                self.wfile.flush()
                self.close_connection = True

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.requests = []
        server.url = "http://127.0.0.1:%d/file" % server.server_port
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        worker.start()
        servers.append((server, worker))
        return server

    yield start
    for server, worker in servers:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
        assert not worker.is_alive()


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_transient_http_error_then_success(http_server, tmp_path, waits, status, caplog):
    server = http_server({"status": status}, {})
    destination = tmp_path / "data"
    fetch_file(server.url, destination=destination, timeout=2,
               expected_sha256=hashlib.sha256(PAYLOAD).hexdigest())
    assert destination.read_bytes() == PAYLOAD
    assert len(server.requests) == 2
    assert waits == [1]
    assert "attempt 1/3 failed (HTTP %d)" % status in caplog.text
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("existing", [False, True])
def test_perpetual_504_is_bounded_and_preserves_destination(http_server, tmp_path, waits, existing, caplog):
    server = http_server({"status": 504})
    destination = tmp_path / "data"
    if existing:
        destination.write_bytes(PAYLOAD)
    with pytest.raises(requests.HTTPError) as caught:
        fetch_file(server.url, destination=destination, force=True,
                   expected_sha256=hashlib.sha256(PAYLOAD).hexdigest())
    assert caught.value.response.status_code == 504
    assert caught.value.request.url == server.url
    assert caught.value.response.raw.closed
    assert len(server.requests) == 3
    assert waits == [1, 2]
    assert "failed after 3 attempt(s): HTTP 504" in caplog.text
    assert list(tmp_path.iterdir()) == ([destination] if existing else [])
    if existing:
        assert destination.read_bytes() == PAYLOAD


def test_partial_stream_restarts_in_fresh_private_staging(http_server, tmp_path, monkeypatch, waits):
    server = http_server({"body": b"partial" * 100, "cut_after": 80}, {})
    destination = tmp_path / "data"
    destination.write_bytes(b"previous verified artifact")
    real_open = download._open_staging_file
    real_get = download.requests.get
    paths, responses, progress = [], [], []

    def open_staging(*args, **kwargs):
        assert destination.read_bytes() == b"previous verified artifact"
        assert list(tmp_path.iterdir()) == [destination]
        stream = real_open(*args, **kwargs)
        paths.append(stream.name)
        assert stat.S_IMODE(Path(stream.name).stat().st_mode) == 0o600
        return stream

    def get(*args, **kwargs):
        assert kwargs["timeout"] == 2
        response = real_get(*args, **kwargs)
        responses.append(response)
        return response

    monkeypatch.setattr(download, "_open_staging_file", open_staging)
    monkeypatch.setattr(download.requests, "get", get)
    fetch_file(server.url, destination=destination, force=True, chunk_size=8, timeout=2,
               expected_sha256=hashlib.sha256(PAYLOAD).hexdigest(),
               progress_callback=lambda done, total: progress.append((done, total)))
    assert len(server.requests) == len(set(paths)) == 2
    assert all(response.raw.closed for response in responses)
    assert waits == [1]
    assert progress[0] == (8, 700)
    assert progress[-1] == (len(PAYLOAD), len(PAYLOAD))
    assert (8, len(PAYLOAD)) in progress
    assert destination.read_bytes() == PAYLOAD
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("header,expected", [
    ("4", 4), ("0", 1), ("future-date", 4), ("past-date", 1),
    ("invalid", 1), ("-1", 1), ("1.5", 1), ("NaN", 1),
])
def test_retry_after_seconds_dates_and_invalid_values(http_server, tmp_path, monkeypatch, waits, header, expected):
    now = 1700000000
    monkeypatch.setattr(retries.time, "time", lambda: now)
    if header in ("future-date", "past-date"):
        date = datetime.fromtimestamp(now + (4 if header == "future-date" else -4), tz=timezone.utc)
        header = format_datetime(date, usegmt=True)
    server = http_server({"status": 429, "headers": {"Retry-After": header}}, {})
    fetch_file(server.url, destination=tmp_path / "data")
    assert waits == [expected]
    assert len(server.requests) == 2


@pytest.mark.parametrize("header", ["31", pytest.param("9" * 5000, id="huge-retry-after")])
def test_excessive_retry_after_stops_without_retrying_early(http_server, tmp_path, waits, header, caplog):
    server = http_server({"status": 503, "headers": {"Retry-After": header}}, {})
    with pytest.raises(requests.HTTPError) as caught:
        fetch_file(server.url, destination=tmp_path / "data")
    assert caught.value.response.status_code == 503
    assert len(server.requests) == 1
    assert waits == []
    assert list(tmp_path.iterdir()) == []
    assert "Retry-After exceeds retry_max_delay" in caplog.text


def test_configurable_backoff_is_capped(http_server, tmp_path, waits):
    server = http_server(*([{"status": 504}] * 4), {})
    fetch_file(server.url, destination=tmp_path / "data",
               max_retries=4, retry_backoff=2, retry_max_delay=3)
    assert waits == [2, 3, 3, 3]
    assert len(server.requests) == 5


@pytest.mark.parametrize("number", [Fraction, np.float32], ids=["fraction", "numpy-float32"])
@pytest.mark.parametrize("option", ["retry_backoff", "retry_max_delay"])
def test_real_valued_delays_work_with_actual_sleep(http_server, tmp_path, monkeypatch, number, option):
    server = http_server({"status": 504}, {"status": 504}, {})
    options = dict(retry_backoff=0.001, retry_max_delay=0.002)
    options[option] = number("0.001")
    delays = []

    def sleep(delay):
        # Keep the real sleep's numeric conversion; an append-only fake hid
        # the TypeError for Fraction and numpy.float32. Waits stay under 2ms.
        real_sleep(delay)
        delays.append(delay)

    monkeypatch.setattr(download.time, "sleep", sleep)
    fetch_file(server.url, destination=tmp_path / "data", **options)
    expected = [0.001, 0.002] if option == "retry_backoff" else [0.001, 0.001]
    assert delays == pytest.approx(expected)
    assert len(server.requests) == 3
    assert (tmp_path / "data").read_bytes() == PAYLOAD


@pytest.mark.parametrize("status,max_retries", [(504, 0), (401, 2), (403, 2), (404, 2), (410, 2), (501, 2)])
def test_disabled_retries_and_permanent_http_errors(http_server, tmp_path, waits, status, max_retries):
    server = http_server({"status": status})
    with pytest.raises(requests.HTTPError):
        fetch_file(server.url, destination=tmp_path / "data", max_retries=max_retries)
    assert len(server.requests) == 1
    assert waits == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("error_type", [requests.ConnectTimeout, requests.ReadTimeout, requests.ConnectionError])
def test_transport_failure_then_success(http_server, tmp_path, monkeypatch, waits, error_type):
    server = http_server({})
    original_get = download.requests.get
    attempts = []

    def get(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise error_type("temporary failure")
        return original_get(*args, **kwargs)

    monkeypatch.setattr(download.requests, "get", get)
    fetch_file(server.url, destination=tmp_path / "data")
    assert len(attempts) == 2
    assert waits == [1]


def test_exhaustion_preserves_original_transport_exception(tmp_path, monkeypatch, waits):
    error = requests.ConnectionError("original transport failure")
    attempts = []

    def get(*args, **kwargs):
        attempts.append(1)
        raise error

    monkeypatch.setattr(download.requests, "get", get)
    with pytest.raises(requests.ConnectionError) as caught:
        fetch_file("https://host/file", destination=tmp_path / "data", max_retries=1)
    assert caught.value is error
    assert len(attempts) == 2
    assert waits == [1]


@pytest.mark.parametrize("error_type", [
    requests.exceptions.SSLError, requests.exceptions.InvalidURL,
    requests.exceptions.ContentDecodingError, requests.TooManyRedirects, requests.HTTPError,
])
def test_non_transient_request_failures_are_not_retried(tmp_path, monkeypatch, waits, error_type):
    attempts = []

    def get(*args, **kwargs):
        attempts.append(1)
        raise error_type("permanent failure")

    monkeypatch.setattr(download.requests, "get", get)
    with pytest.raises(error_type):
        fetch_file("https://host/file", destination=tmp_path / "data")
    assert len(attempts) == 1
    assert waits == []


@pytest.mark.parametrize("cause_type", [
    ssl.SSLCertVerificationError, urllib3_errors.SSLError, requests.exceptions.SSLError,
    ConnectionRefusedError,
])
def test_proxy_retry_policy_uses_wrapped_cause(tmp_path, monkeypatch, waits, cause_type):
    cause = cause_type("HTTPS proxy connection failed")
    # The actual Requests / urllib3 layout: args -> reason -> original_error.
    proxy = urllib3_errors.ProxyError("Unable to connect to proxy", cause)
    wrapped = urllib3_errors.MaxRetryError(None, "https://host/file", proxy)
    error = requests.exceptions.ProxyError(wrapped)
    attempts = []
    destination = tmp_path / "data"
    destination.write_bytes(PAYLOAD)

    def get(*args, **kwargs):
        attempts.append(1)
        raise error

    monkeypatch.setattr(download.requests, "get", get)
    with pytest.raises(requests.exceptions.ProxyError) as caught:
        fetch_file("https://host/file", destination=destination, force=True)
    transient = cause_type is ConnectionRefusedError
    assert len(attempts) == (3 if transient else 1)
    assert waits == ([1, 2] if transient else [])
    assert caught.value is error
    assert destination.read_bytes() == PAYLOAD
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("chain", ["cause", "context", "suppressed", "overridden", "cycle"])
def test_tls_cause_inspection_respects_exception_chaining(chain):
    error = requests.ConnectionError("outer transport failure")
    tls = ssl.SSLError("certificate verification failed")
    if chain == "cause":
        error.__cause__ = tls
    elif chain == "cycle":
        wrapped = requests.exceptions.ProxyError(error)
        error.__cause__ = wrapped
    else:
        error.__context__ = tls
        if chain == "suppressed":
            error.__suppress_context__ = True
        elif chain == "overridden":
            error.__cause__ = ConnectionRefusedError("active cause")
    assert retries.is_retryable_http_error(error) == (chain in ("suppressed", "overridden", "cycle"))


@pytest.mark.parametrize("error_type", [requests.ConnectionError, RuntimeError, KeyboardInterrupt])
def test_callback_failures_are_not_transfer_retries(http_server, tmp_path, waits, error_type):
    server = http_server({})
    error = error_type("callback failed")

    def callback(*args):
        raise error

    with pytest.raises(error_type) as caught:
        fetch_file(server.url, destination=tmp_path / "data", progress_callback=callback)
    assert caught.value is error
    assert len(server.requests) == 1
    assert waits == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("failure", ["hash", "gzip", "publish"])
def test_validation_transform_and_publication_are_not_retried(http_server, tmp_path, monkeypatch, waits, failure):
    server = http_server({"body": gzip.compress(PAYLOAD)[:-8] if failure == "gzip" else PAYLOAD})
    destination = tmp_path / "data"
    destination.write_bytes(b"previous verified artifact")
    kwargs = {}
    url = server.url
    if failure == "hash":
        kwargs["expected_sha256"] = "0" * 64
        error_type = FileValidationError
    elif failure == "gzip":
        url += ".gz"
        error_type = EOFError
    else:
        error_type = PermissionError

        def denied(*args, **kwargs):
            raise PermissionError("publication denied")

        monkeypatch.setattr(download, "_publish_staged_file", denied)
    with pytest.raises(error_type):
        fetch_file(url, destination=destination, force=True, **kwargs)
    assert destination.read_bytes() == b"previous verified artifact"
    assert len(server.requests) == 1
    assert waits == []
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("scheme", ["file", "ftp"])
def test_non_http_transfers_are_not_retried(tmp_path, monkeypatch, waits, scheme):
    attempts = []

    def stream(*args, **kwargs):
        attempts.append(1)
        raise requests.ConnectionError("non-HTTP failure")

    monkeypatch.setattr(download, "_stream_to_file", stream)
    with pytest.raises(requests.ConnectionError):
        fetch_file(scheme + "://host/file", destination=tmp_path / "data")
    assert len(attempts) == 1
    assert waits == []


@pytest.mark.parametrize("api", ["fetch", "cache", "private"])
def test_default_retries_reach_public_and_legacy_private_callers(http_server, tmp_path, waits, api):
    payload = gzip.compress(PAYLOAD)
    server = http_server({"status": 504}, {"body": payload})
    url = server.url + ".gz"
    destination = tmp_path / "file.gz"
    if api == "fetch":
        fetch_file(url, destination=destination)
    elif api == "cache":
        Cache(cache_root=tmp_path).fetch(url, filename="file.gz")
    else:
        # Matches pyensembl.DownloadCache's call with no new flags.
        download._download_and_decompress_if_necessary(str(destination), url, timeout=3600)
    assert destination.read_bytes() == payload
    assert waits == [1]
    assert len(server.requests) == 2


@pytest.mark.parametrize("api", ["cache", "private"])
def test_retry_configuration_reaches_wrappers(http_server, tmp_path, waits, api):
    server = http_server({"status": 504})
    options = dict(max_retries=1, retry_backoff=7, retry_max_delay=0)
    with pytest.raises(requests.HTTPError):
        if api == "cache":
            Cache(cache_root=tmp_path).fetch(server.url, filename="file", **options)
        else:
            download._download_and_decompress_if_necessary(tmp_path / "file", server.url, **options)
    assert len(server.requests) == 2
    assert waits == []


def test_interruption_during_backoff_preserves_cache(http_server, tmp_path, monkeypatch):
    server = http_server({"status": 504})
    destination = tmp_path / "file"
    destination.write_bytes(PAYLOAD)

    def interrupt(delay):
        assert list(tmp_path.iterdir()) == [destination]
        raise KeyboardInterrupt()

    monkeypatch.setattr(download.time, "sleep", interrupt)
    with pytest.raises(KeyboardInterrupt):
        fetch_file(server.url, destination=destination, force=True)
    assert len(server.requests) == 1
    assert destination.read_bytes() == PAYLOAD
    assert list(tmp_path.iterdir()) == [destination]


def test_verified_cache_reuse_never_requests_or_waits(tmp_path, monkeypatch):
    destination = tmp_path / "file"
    destination.write_bytes(PAYLOAD)

    def reject(*args, **kwargs):
        pytest.fail("cache reuse must not make a request or sleep")

    monkeypatch.setattr(download.requests, "get", reject)
    monkeypatch.setattr(download.time, "sleep", reject)
    assert fetch_file("https://host/file", destination=destination,
                      expected_sha256=hashlib.sha256(PAYLOAD).hexdigest()) == str(destination)


@pytest.mark.parametrize("options", [
    {"max_retries": -1}, {"max_retries": True}, {"max_retries": 1.5},
    {"retry_backoff": -1}, {"retry_backoff": True}, {"retry_backoff": "1"},
    {"retry_backoff": float("nan")}, {"retry_backoff": 10 ** 1000},
    {"retry_max_delay": -1}, {"retry_max_delay": float("inf")},
])
def test_invalid_retry_options_fail_before_creating_cache(tmp_path, options):
    with pytest.raises(ValueError):
        fetch_file("https://host/file", destination=tmp_path / "missing" / "data", **options)
    assert list(tmp_path.iterdir()) == []
