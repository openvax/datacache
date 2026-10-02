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

"""Where a downloaded file came from, recorded beside it for offline inspection.

fetch_file(..., record_provenance=True) writes a small hidden record,
".<name>.datacache.json", beside the file it publishes. Any later replacement
of the file removes the record first, so a record never describes bytes it was
not written for. Inspection only reads records: a missing, unreadable, or
malformed record means no recorded provenance.
"""

from datetime import datetime, timezone
import errno
import json
import os
import re
import stat
from urllib.parse import urlsplit, urlunsplit

SUFFIX = ".datacache.json"
FORMAT = 1
# A record is a few hundred bytes; anything much larger is not one of ours.
_MAX_RECORD_BYTES = 64 * 1024
# Never block on a FIFO or follow a link planted at a record's name.
_OPEN_FLAGS = (os.O_RDONLY | getattr(os, "O_BINARY", 0) |
               getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0))


def sidecar_path(path):
    """Path of the provenance record kept for the file at path (str or bytes)."""
    directory, name = os.path.split(os.fspath(path))
    if isinstance(name, bytes):
        return os.path.join(directory, b"." + name + os.fsencode(SUFFIX))
    return os.path.join(directory, "." + name + SUFFIX)


def redact_url(url):
    """Drop the user name, password, query string, and fragment.

    Those are where URLs usually carry secrets, such as signed-URL signatures.
    The path is kept as is, so a secret embedded in the path, as in some share
    links, would be recorded.
    """
    parts = urlsplit(url)
    host = parts.hostname or ""
    if ":" in host:
        host = "[%s]" % host  # IPv6 literal
    if parts.port is not None:
        host = "%s:%d" % (host, parts.port)
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def describe(download_url, info, sha256=None, *, transport=None):
    """Provenance, as JSON text, for the bytes that stat info describes.

    info must come from the exact bytes being published (the staged file), not
    from whatever is at the destination afterwards, which a concurrent writer
    may have replaced. sha256 is recorded only when those bytes were verified
    against a trusted expectation: a digest of what a server sent proves nothing.
    transport optionally records the validator accepted by resumable transport;
    it is evidence of representation consistency, not a trusted content hash.
    """
    record = {
        "format": FORMAT,
        "url": redact_url(download_url),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "size": info.st_size,
        "mtime_ns": info.st_mtime_ns,
        "sha256": sha256.lower() if sha256 else None,
    }
    if transport is not None:
        record['transport'] = transport
    return json.dumps(record, sort_keys=True)


def remove(path):
    """Remove the record kept for path, if there is one.

    A record name longer than the filesystem allows cannot exist, so it counts
    as absent. Other errors propagate.
    """
    try:
        os.remove(sidecar_path(path))
    except FileNotFoundError:
        pass
    except OSError as error:
        if error.errno != errno.ENAMETOOLONG:
            raise


def _valid(record):
    def integer(value):
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0
    return (isinstance(record, dict) and record.get("format") == FORMAT and
            isinstance(record.get("url"), str) and isinstance(record.get("fetched_at"), str) and
            integer(record.get("size")) and integer(record.get("mtime_ns")) and
            (record.get("sha256") is None or (isinstance(record.get("sha256"), str) and
                                              re.fullmatch(r"[0-9a-f]{64}", record["sha256"]))))


def read(path, info):
    """The record for path if it still describes the file with stat info.

    Returns None when there is no usable record, or when the file's size or
    modification time changed since it was recorded. Never writes and never
    blocks; any problem with the record means no recorded provenance.
    """
    try:
        descriptor = os.open(sidecar_path(path), _OPEN_FLAGS)
    except OSError:
        return None
    try:
        # Check the opened object itself, so it cannot be swapped after a check.
        record_info = os.fstat(descriptor)
        if not stat.S_ISREG(record_info.st_mode) or record_info.st_size > _MAX_RECORD_BYTES:
            return None
        chunks, remaining = [], _MAX_RECORD_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        record = json.loads(b"".join(chunks))
    except (OSError, ValueError, RecursionError):
        return None
    finally:
        os.close(descriptor)
    if not _valid(record) or record["size"] != info.st_size or record["mtime_ns"] != info.st_mtime_ns:
        return None
    return record
