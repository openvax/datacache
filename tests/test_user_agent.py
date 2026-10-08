"""Every HTTP request names datacache instead of Requests' default User-Agent."""

from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import threading

import pytest

from datacache import fetch_bytes, fetch_file
from datacache.version import __version__

PAYLOAD = b"0123456789abcdef" * 4


@pytest.fixture
def server():
    agents = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            agent = self.headers.get("User-Agent", "")
            agents.append(agent)
            # Like IEDB, refuse any client that announces python-requests.
            status = 403 if "python-requests" in agent else 200
            body = b"" if status == 403 else PAYLOAD
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield "http://127.0.0.1:%d/data" % httpd.server_port, agents
    httpd.shutdown()
    httpd.server_close()
    thread.join()


def plain(url, tmp_path):
    return Path(fetch_file(url, destination=tmp_path / "plain", raw=True,
                           max_retries=0)).read_bytes()


def resumable(url, tmp_path):
    return Path(fetch_file(url, destination=tmp_path / "resumed", raw=True,
                           resume=True, expected_size=len(PAYLOAD),
                           expected_sha256=sha256(PAYLOAD).hexdigest(),
                           max_retries=0)).read_bytes()


def in_memory(url, tmp_path):
    return fetch_bytes(url, max_retries=0)


@pytest.mark.parametrize("fetch", [
    plain,
    pytest.param(resumable, marks=pytest.mark.skipif(
        os.name != "posix", reason="POSIX resume locks")),
    in_memory,
])
def test_requests_name_datacache(server, tmp_path, fetch):
    url, agents = server
    assert fetch(url, tmp_path) == PAYLOAD
    assert len(agents) == 1
    assert agents[0].startswith("datacache/%s " % __version__)
    assert "python-requests" not in agents[0]
