"""Bounded HTTP ZIP member acquisition, stored as an atomic evidence bundle."""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import io
import os
from pathlib import Path
import re
import struct
from urllib.parse import urlsplit
from uuid import uuid4
import zipfile
import zlib

from ._filesystem import read_json, write_json
from .bundle_store import (
    BundleStore,
    CHUNK_SIZE,
    INVALID_STORE_ERRORS,
    MANIFEST,
    check_expected,
    check_tree,
    discard_private_directory,
    hash_file,
    private_directory,
    source_fingerprint,
    validate_file_record,
    validate_relative_name,
)
from .bundles import BundleInspection
from .download import normal_creation_mode
from .integrity import _validate_expectations
from .provenance import redact_url
from .resume import _strong_etag


def _positive(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("%s must be a positive integer" % name)
    return value


class _HTTPFile(io.RawIOBase):
    """Seekable metadata reads; data transfers explicitly stream one range."""

    def __init__(self, url, timeout, max_metadata_bytes):
        import requests

        self.url = url
        self.timeout = timeout
        self.max_metadata_bytes = max_metadata_bytes
        self.metadata_bytes = 0
        self.position = 0
        self.size = None
        self.validator = None
        self.session = requests.Session()
        try:
            self.read(1)
        except BaseException:
            self.close()
            raise
        self.position = 0

    def close(self):
        self.session.close()
        super().close()

    def seekable(self):
        return True

    def readable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        if whence not in (0, 1, 2):
            raise ValueError("invalid seek origin")
        position = offset + (
            self.position if whence == 1 else self.size if whence == 2 else 0
        )
        if position < 0:
            raise ValueError("negative seek")
        self.position = position
        return position

    def chunks(self, start, size):
        if not size:
            return
        end = start + size - 1
        headers = {"Range": "bytes=%d-%d" % (start, end), "Accept-Encoding": "identity"}
        if self.validator:
            headers["If-Range"] = self.validator["value"]
        with self.session.get(
            self.url, headers=headers, stream=True, timeout=self.timeout
        ) as response:
            if response.status_code != 206:
                raise ValueError("ZIP source must honor byte ranges with HTTP 206")
            if (
                response.headers.get("Content-Encoding", "identity").lower()
                != "identity"
            ):
                raise ValueError("encoded range responses are not supported")
            match = re.fullmatch(
                r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", "")
            )
            if not match or tuple(map(int, match.groups()[:2])) != (start, end):
                raise ValueError("incorrect HTTP Content-Range")
            total = int(match.group(3))
            if total <= end or (self.size is not None and self.size != total):
                raise ValueError("archive length changed or range exceeds archive")
            length = response.headers.get("Content-Length")
            if length is not None and length != str(size):
                raise ValueError("incorrect HTTP Content-Length")
            if self.validator is None:
                etag = response.headers.get("ETag")
                modified = response.headers.get("Last-Modified")
                if _strong_etag(etag):
                    validator = {"header": "ETag", "value": etag}
                elif modified:
                    # Last-Modified is a transport consistency check, not a hash.
                    if parsedate_to_datetime(modified).tzinfo is None:
                        raise ValueError("Last-Modified must include a timezone")
                    validator = {"header": "Last-Modified", "value": modified}
                else:
                    raise ValueError(
                        "ZIP source requires a strong ETag or Last-Modified"
                    )
                self.validator = validator
            elif (
                response.headers.get(self.validator["header"])
                != self.validator["value"]
            ):
                raise ValueError("archive validator changed between ranges")
            self.size = total
            remaining = size
            while remaining:
                chunk = response.raw.read(
                    min(CHUNK_SIZE, remaining), decode_content=False
                )
                if not chunk:
                    raise ValueError("truncated HTTP range")
                remaining -= len(chunk)
                yield chunk
            if response.raw.read(1, decode_content=False):
                raise ValueError("HTTP range exceeds requested size")

    def read(self, size=-1):
        if self.size is not None:
            size = (
                max(0, self.size - self.position)
                if size < 0
                else min(size, max(0, self.size - self.position))
            )
        if size < 0 or self.metadata_bytes + size > self.max_metadata_bytes:
            raise ValueError("ZIP metadata exceeds max_metadata_bytes")
        self.metadata_bytes += size
        result = b"".join(self.chunks(self.position, size))
        self.position += len(result)
        return result


def _definition(url, member, expected_sha256, expected_size, compressed_sha256):
    if not isinstance(url, str) or urlsplit(url).scheme not in ("http", "https"):
        raise ValueError("ZIP source must be an HTTP(S) URL")
    validate_relative_name(member)
    _validate_expectations(expected_sha256, expected_size)
    _validate_expectations(compressed_sha256, None)
    return dict(
        url=redact_url(url),
        url_identity=source_fingerprint(url),
        member=member,
        sha256=expected_sha256.lower() if expected_sha256 else None,
        size=expected_size,
        compressed_sha256=compressed_sha256.lower() if compressed_sha256 else None,
    )


def _inspect(destination, definition, max_compressed_bytes, max_bytes):
    store = BundleStore(destination, "zip-member")
    try:
        directory = store.current_bundle()
        if directory is None:
            return BundleInspection(str(destination), "missing")
        manifest = read_json(directory / MANIFEST)
        if manifest.get("format") != 1 or manifest.get("definition") != definition:
            raise ValueError("ZIP member dependency identity changed")
        files = manifest.get("files")
        if not isinstance(files, dict) or set(files) != {
            "payload",
            "compressed",
            "receipt.json",
        }:
            raise ValueError("invalid ZIP member inventory")
        for record in files.values():
            validate_file_record(record)
        if (
            files["payload"]["size"] > max_bytes
            or files["compressed"]["size"] > max_compressed_bytes
        ):
            raise ValueError("cached ZIP member exceeds requested size limits")
        check_expected(directory, files["payload"], definition)
        check_expected(
            directory, files["compressed"], {"sha256": definition["compressed_sha256"]}
        )
        hashed = check_tree(directory, files, [], ignore=(MANIFEST,))
        receipt = read_json(directory / "receipt.json")
        if (
            receipt.get("definition") != definition
            or receipt.get("payload") != files["payload"]
            or receipt.get("compressed") != files["compressed"]
        ):
            raise ValueError("ZIP member acquisition receipt disagrees with inventory")
        return BundleInspection(
            str(destination),
            "available",
            bool(definition["sha256"]),
            str(directory),
            {
                name: value.inspection(
                    verified=bool(definition["sha256"]) and name == "payload"
                )
                for name, value in hashed.items()
            },
        )
    except PermissionError as error:
        return BundleInspection(str(destination), "inaccessible", error=error)
    except INVALID_STORE_ERRORS as error:
        return BundleInspection(str(destination), "invalid", error=error)


def _extract(remote, info, offset, paths, max_bytes):
    """Retain exact compressed bytes and validate the complete deflate stream."""
    decompressor = (
        zlib.decompressobj(-15) if info.compress_type == zipfile.ZIP_DEFLATED else None
    )
    crc, size = 0, 0
    with (
        open(paths["payload"], "xb") as output,
        open(paths["compressed"], "xb") as compressed,
    ):

        def write(chunk):
            nonlocal crc, size
            size += len(chunk)
            if size > min(max_bytes, info.file_size):
                raise ValueError(
                    "expanded ZIP member exceeds declared size or max_bytes"
                )
            crc = zlib.crc32(chunk, crc)
            output.write(chunk)

        for chunk in remote.chunks(offset, info.compress_size):
            compressed.write(chunk)
            if decompressor is None:
                write(chunk)
            else:
                while chunk:
                    write(decompressor.decompress(chunk, CHUNK_SIZE))
                    chunk = decompressor.unconsumed_tail
                    if decompressor.unused_data:
                        raise ValueError("trailing bytes after compressed ZIP member")
        if decompressor is not None and not decompressor.eof:
            raise ValueError("truncated deflate stream")
        if size != info.file_size or crc != info.CRC:
            raise ValueError("ZIP member size or CRC mismatch")


def install_zip_member(
    destination,
    url,
    member,
    *,
    expected_sha256=None,
    expected_size=None,
    compressed_sha256=None,
    max_compressed_bytes=512 * 2**20,
    max_bytes=4 * 2**30,
    max_metadata_bytes=8 * 2**20,
    timeout=60,
    force=False,
):
    """Fetch one named HTTP ZIP member into a shared, atomic bundle.

    Return paths named payload, compressed and receipt.json. The receipt records
    the archive URL/validator/size, member metadata, CRC and both SHA-256 hashes.
    Expected hashes are optional during discovery; observed hashes are not
    independent verification. Pin expected_sha256 (and preferably compressed_sha256)
    for subsequent reproduction. Last-Modified is weaker than a strong ETag.

    Only stored and deflated, unencrypted regular members are supported. Require
    exact byte ranges; never fall back to a whole-archive transfer. Metadata and
    compressed/expanded payloads have separate finite limits. Failed attempts
    preserve previous generations; retries restart the selected member. Cache
    hits verify all retained bytes offline. Corruption or changed identity requires
    explicit force=True. No source is refreshed automatically.
    """
    definition = _definition(
        url, member, expected_sha256, expected_size, compressed_sha256
    )
    for name, value in [
        ("max_compressed_bytes", max_compressed_bytes),
        ("max_bytes", max_bytes),
        ("max_metadata_bytes", max_metadata_bytes),
    ]:
        _positive(value, name)
    if not isinstance(force, bool):
        raise ValueError("force must be a boolean")
    if expected_size is not None and expected_size > max_bytes:
        raise ValueError("expected_size exceeds max_bytes")
    destination = Path(destination).absolute()
    store = BundleStore(destination, "zip-member")

    def reuse():
        inspection = _inspect(destination, definition, max_compressed_bytes, max_bytes)
        if store.can_reuse(inspection, force=force):
            return {name: entry.path for name, entry in inspection.files.items()}
        return None

    existing = reuse()
    if existing:
        return existing
    destination.parent.mkdir(parents=True, exist_ok=True)
    with store.lock():
        store.create()
        existing = reuse()
        if existing:
            return existing
        working = private_directory(destination / (".staging-" + uuid4().hex))
        staged = working / "files"
        staged.mkdir()
        paths = {
            name: staged / name for name in ("payload", "compressed", "receipt.json")
        }
        try:
            with (
                _HTTPFile(url, timeout, max_metadata_bytes) as remote,
                zipfile.ZipFile(remote) as archive,
            ):
                matches = [
                    info for info in archive.infolist() if info.filename == member
                ]
                if len(matches) != 1:
                    raise ValueError(
                        "ZIP must contain exactly one member named %r" % member
                    )
                info = matches[0]
                if (
                    info.is_dir()
                    or info.flag_bits & 1
                    or info.compress_type
                    not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
                ):
                    raise ValueError(
                        "ZIP member must be unencrypted, stored or deflated"
                    )
                mode = (info.external_attr >> 16) & 0o170000
                if mode not in (0, 0o100000):
                    raise ValueError("ZIP member must be a regular file")
                if (
                    info.compress_size > max_compressed_bytes
                    or info.file_size > max_bytes
                ):
                    raise ValueError(
                        "ZIP member exceeds compressed or expanded size limits"
                    )
                check_expected(
                    member, {"size": info.file_size, "sha256": None}, definition
                )
                remote.seek(info.header_offset)
                header = remote.read(30)
                if len(header) != 30 or header[:4] != b"PK\x03\x04":
                    raise ValueError("invalid ZIP local header")
                flags, method = struct.unpack_from("<HH", header, 6)
                if flags != info.flag_bits or method != info.compress_type:
                    raise ValueError("ZIP local and central header disagree")
                # The standard parser checks local/central header agreement,
                # overlapping members, unsupported flags and ZIP64 metadata.
                with archive.open(info):
                    offset = remote.tell()
                _extract(remote, info, offset, paths, max_bytes)
                payload = hash_file(paths["payload"], definition).record
                compressed = hash_file(
                    paths["compressed"], {"sha256": definition["compressed_sha256"]}
                ).record
                receipt = dict(
                    format=1,
                    definition=definition,
                    fetched_at=datetime.now(timezone.utc).isoformat(),
                    archive_size_bytes=remote.size,
                    validator=remote.validator,
                    metadata_size_bytes=remote.metadata_bytes,
                    data_offset_bytes=offset,
                    compression_method=info.compress_type,
                    crc32="%08x" % info.CRC,
                    payload=payload,
                    compressed=compressed,
                    payload_verified=expected_sha256 is not None,
                    compressed_verified=compressed_sha256 is not None,
                )
                write_json(paths["receipt.json"], receipt)
            files = dict(payload=payload, compressed=compressed)
            files["receipt.json"] = hash_file(paths["receipt.json"]).record
            mode = normal_creation_mode(staged)
            for path in paths.values():
                os.chmod(path, mode)
            write_json(
                staged / MANIFEST,
                dict(format=1, definition=definition, files=files),
                mode=mode,
            )
            check_tree(staged, files, [], ignore=(MANIFEST,), hash_contents=False)
            bundle = store.publish(staged)
            return {name: str(bundle / name) for name in paths}
        finally:
            discard_private_directory(working)
