from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import re
import struct
import threading
import warnings
import zipfile
import zlib

import pytest
from urllib3.exceptions import ProtocolError

from datacache import FileValidationError, install_zip_member, list_bundles


PAYLOAD = b"peptide\tintensity\nEXAMPLE\t25\n" * 10000


def archive_bytes(method=zipfile.ZIP_DEFLATED, *, duplicate=False, zip64=False):
    handle = io.BytesIO()
    with zipfile.ZipFile(handle, "w", compression=method) as archive:
        archive.writestr("other", b"x" * 2**20)
        if zip64:
            with archive.open("txt/peptides.txt", "w", force_zip64=True) as member:
                member.write(PAYLOAD)
        else:
            archive.writestr("txt/peptides.txt", PAYLOAD)
        if duplicate:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                archive.writestr("txt/peptides.txt", b"ambiguous")
    return handle.getvalue()


@pytest.fixture
def server():
    running = []

    def start(payload=None, **options):
        state = dict(
            payload=archive_bytes() if payload is None else payload,
            requests=[],
            **options,
        )

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                state["requests"].append(dict(self.headers))
                match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers["Range"])
                first, last = map(int, match.groups())
                data = state["payload"][first : last + 1]
                bad = (
                    state.get("bad")
                    if len(state["requests"]) > state.get("after", 0)
                    else None
                )
                self.send_response(200 if bad == "status" else 206)
                if bad != "validator":
                    if state.get("last_modified"):
                        self.send_header(
                            "Last-Modified", "Thu, 18 Nov 2021 01:40:15 GMT"
                        )
                    else:
                        self.send_header("ETag", '"v2"' if bad == "changed" else '"v1"')
                total = len(state["payload"]) + (1 if bad == "size" else 0)
                self.send_header(
                    "Content-Range",
                    "bytes %d-%d/%d"
                    % (first + (1 if bad == "range" else 0), last, total),
                )
                self.send_header(
                    "Content-Length", str(len(data) + (1 if bad == "length" else 0))
                )
                if bad == "encoding":
                    self.send_header("Content-Encoding", "gzip")
                self.end_headers()
                try:
                    self.wfile.write(data[:-1] if bad == "truncated" else data)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        running.append((httpd, thread))
        return "http://127.0.0.1:%d/archive.zip" % httpd.server_port, state

    yield start
    for httpd, thread in running:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


def install(tmp_path, url, **kwargs):
    return install_zip_member(tmp_path / "store", url, "txt/peptides.txt", **kwargs)


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
@pytest.mark.parametrize("zip64", [False, True])
def test_member_integrity_and_offline_reuse(
    tmp_path, server, method, zip64, monkeypatch
):
    url, state = server(archive_bytes(method, zip64=zip64))
    digest = hashlib.sha256(PAYLOAD).hexdigest()
    paths = install(tmp_path, url, expected_sha256=digest, expected_size=len(PAYLOAD))
    assert Path(paths["payload"]).read_bytes() == PAYLOAD
    receipt = json.loads(Path(paths["receipt.json"]).read_text())
    assert receipt["payload_verified"] and not receipt["compressed_verified"]
    assert receipt["payload"] == dict(sha256=digest, size=len(PAYLOAD))
    assert (
        receipt["compressed"]["sha256"]
        == hashlib.sha256(Path(paths["compressed"]).read_bytes()).hexdigest()
    )
    assert receipt["archive_size_bytes"] == len(state["payload"])
    assert all(item["Accept-Encoding"] == "identity" for item in state["requests"])
    assert all(item["If-Range"] == '"v1"' for item in state["requests"][1:])
    assert len(list_bundles(tmp_path / "store")) == 1
    monkeypatch.setattr(
        "requests.Session.get",
        lambda *args, **kwargs: pytest.fail("cache hit used network"),
    )
    assert (
        install(tmp_path, url, expected_sha256=digest, expected_size=len(PAYLOAD))
        == paths
    )


def test_last_modified_and_metadata_budget(tmp_path, server):
    url, state = server(last_modified=True)
    paths = install(tmp_path, url)
    receipt = json.loads(Path(paths["receipt.json"]).read_text())
    assert receipt["validator"]["header"] == "Last-Modified"
    assert not receipt["payload_verified"]
    assert receipt["metadata_size_bytes"] < 100000


@pytest.mark.parametrize(
    "bad,after",
    [
        ("status", 0),
        ("validator", 0),
        ("changed", 1),
        ("size", 1),
        ("range", 0),
        ("length", 0),
        ("encoding", 0),
        ("truncated", 0),
    ],
)
def test_bad_http_never_publishes(tmp_path, server, bad, after):
    url, state = server(bad=bad, after=after)
    with pytest.raises((ValueError, zipfile.BadZipFile, ProtocolError)):
        install(tmp_path, url)
    assert list_bundles(tmp_path / "store") == []
    assert not list((tmp_path / "store").glob(".staging-*"))


@pytest.mark.parametrize(
    "options",
    [
        dict(max_bytes=5),
        dict(max_compressed_bytes=5),
        dict(max_metadata_bytes=5),
        dict(expected_size=1),
        dict(expected_sha256="0" * 64),
        dict(compressed_sha256="0" * 64),
    ],
)
def test_integrity_and_resource_limits(tmp_path, server, options):
    url, state = server()
    with pytest.raises((ValueError, FileValidationError, zipfile.BadZipFile)):
        install(tmp_path, url, **options)
    assert not list_bundles(tmp_path / "store")


@pytest.mark.parametrize(
    "kind", ["duplicate", "missing", "unsupported", "crc", "encrypted", "local_name"]
)
def test_invalid_members(tmp_path, server, kind):
    data = archive_bytes(
        duplicate=kind == "duplicate",
        method=zipfile.ZIP_BZIP2 if kind == "unsupported" else zipfile.ZIP_STORED,
    )
    if kind in ("crc", "encrypted", "local_name"):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            info = archive.getinfo("txt/peptides.txt")
        data = bytearray(data)
        if kind == "crc":
            data[info.header_offset + 30 + len(info.filename)] ^= 1
        elif kind == "local_name":
            data[info.header_offset + 30] ^= 1
        else:
            offset = data.index(
                b"PK\x01\x02", data.index(b"PK\x01\x02", info.header_offset) + 4
            )
            struct.pack_into("<H", data, offset + 8, 1)
    url, state = server(bytes(data))
    with pytest.raises((ValueError, zipfile.BadZipFile)):
        install_zip_member(
            tmp_path / "store",
            url,
            "absent" if kind == "missing" else "txt/peptides.txt",
        )
    assert not list_bundles(tmp_path / "store")


def test_failure_preserves_previous_bundle_and_refresh_keeps_paths(tmp_path, server):
    url, state = server()
    first = install(tmp_path, url)
    state["bad"] = "status"
    with pytest.raises(ValueError):
        install(tmp_path, url, force=True)
    assert Path(first["payload"]).read_bytes() == PAYLOAD
    assert install(tmp_path, url) == first
    state.pop("bad")
    second = install(tmp_path, url, force=True)
    assert second != first and len(list_bundles(tmp_path / "store")) == 2


@pytest.mark.parametrize("name", ["payload", "compressed", "receipt.json"])
def test_corrupt_cache_requires_explicit_repair(tmp_path, server, name):
    url, state = server()
    paths = install(tmp_path, url)
    Path(paths[name]).write_bytes(b"corrupt")
    count = len(state["requests"])
    with pytest.raises(FileValidationError):
        install(tmp_path, url)
    assert len(state["requests"]) == count
    assert Path(install(tmp_path, url, force=True)["payload"]).read_bytes() == PAYLOAD


def test_concurrent_first_acquisition_publishes_once(tmp_path, server):
    url, _ = server()
    with ThreadPoolExecutor(2) as pool:
        paths = list(pool.map(lambda _: install(tmp_path, url), range(2)))
    assert paths[0] == paths[1]
    assert len(list_bundles(tmp_path / "store")) == 1


@pytest.mark.parametrize("member", ["../escape", "/absolute", "dir/", "a\\b"])
def test_unsafe_names_fail_before_network(tmp_path, member):
    with pytest.raises(ValueError):
        install_zip_member(
            tmp_path / "store", "https://example.org/archive.zip", member
        )


def test_zip64_directory_and_large_expansion(tmp_path, server, monkeypatch):
    monkeypatch.setattr(
        __import__(__name__, fromlist=["PAYLOAD"]), "PAYLOAD", b"A" * (8 * 2**20)
    )
    with monkeypatch.context() as writer:
        writer.setattr(zipfile, "ZIP64_LIMIT", 64)
        data = archive_bytes()
    assert b"PK\x06\x06" in data
    url, _ = server(data)
    paths = install(tmp_path, url)
    assert Path(paths["payload"]).read_bytes() == PAYLOAD


@pytest.mark.parametrize("kind", ["method", "flags", "link"])
def test_header_disagreement_and_nonregular_member(tmp_path, server, kind):
    data = bytearray(archive_bytes())
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        info = archive.getinfo("txt/peptides.txt")
    if kind == "method":
        struct.pack_into("<H", data, info.header_offset + 8, zipfile.ZIP_STORED)
    elif kind == "flags":
        struct.pack_into("<H", data, info.header_offset + 6, 1)
    else:
        offset = data.index(
            b"PK\x01\x02", data.index(b"PK\x01\x02", info.header_offset) + 4
        )
        struct.pack_into("<I", data, offset + 38, 0o120777 << 16)
    url, _ = server(bytes(data))
    with pytest.raises(ValueError):
        install(tmp_path, url)


def test_changed_identity_and_smaller_cache_limit_fail_offline(tmp_path, server):
    url, state = server()
    install(tmp_path, url)
    count = len(state["requests"])
    for options in (
        dict(expected_size=1),
        dict(max_bytes=1),
        dict(max_compressed_bytes=1),
    ):
        with pytest.raises(FileValidationError):
            install(tmp_path, url, **options)
    assert len(state["requests"]) == count


@pytest.mark.parametrize(
    "options",
    [
        dict(max_bytes=0),
        dict(max_compressed_bytes=True),
        dict(max_metadata_bytes=-1),
        dict(force=1),
        dict(expected_size=2, max_bytes=1),
    ],
)
def test_invalid_contract_fails_before_acquisition(tmp_path, options):
    with pytest.raises(ValueError):
        install(tmp_path, "https://example.org/archive.zip", **options)
    assert not (tmp_path / "store").exists()


def test_non_http_source_rejected(tmp_path):
    with pytest.raises(ValueError, match="HTTP"):
        install(tmp_path, "ftp://example.org/archive.zip")


@pytest.mark.parametrize("kind", ["expanded", "truncated", "trailing"])
def test_complete_deflate_stream_is_required(tmp_path, kind):
    from datacache.zip_members import _extract

    compressor = zlib.compressobj(wbits=-15)
    compressed = compressor.compress(PAYLOAD) + compressor.flush()
    if kind == "truncated":
        compressed = compressed[:-1]
    if kind == "trailing":
        compressed += b"x"
    info = zipfile.ZipInfo("table")
    info.compress_type = zipfile.ZIP_DEFLATED
    info.file_size = len(PAYLOAD) - (1 if kind == "expanded" else 0)
    info.compress_size = len(compressed)
    info.CRC = zlib.crc32(PAYLOAD)

    class Source:
        def chunks(self, offset, size):
            yield compressed

    with pytest.raises(
        ValueError,
        match={"expanded": "exceeds", "truncated": "truncated", "trailing": "trailing"}[
            kind
        ],
    ):
        _extract(
            Source(),
            info,
            0,
            {name: tmp_path / name for name in ("payload", "compressed")},
            len(PAYLOAD),
        )


@pytest.mark.parametrize("kind", ["inventory", "receipt"])
def test_inconsistent_manifest_rejected_offline(tmp_path, server, kind):
    url, state = server()
    paths = install(tmp_path, url)
    manifest_path = Path(paths["payload"]).parent / ".datacache-manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if kind == "inventory":
        manifest["files"].pop("compressed")
    else:
        receipt_path = Path(paths["receipt.json"])
        receipt = json.loads(receipt_path.read_text())
        receipt["payload"]["size"] += 1
        payload = json.dumps(receipt).encode()
        receipt_path.write_bytes(payload)
        manifest["files"]["receipt.json"] = dict(
            sha256=hashlib.sha256(payload).hexdigest(), size=len(payload)
        )
    manifest_path.write_text(json.dumps(manifest))
    count = len(state["requests"])
    with pytest.raises(FileValidationError):
        install(tmp_path, url)
    assert len(state["requests"]) == count
