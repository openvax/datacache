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

After fetch_file publishes a download it writes a small hidden sidecar,
".<name>.datacache.json", in the same directory. Inspection only reads it: a
missing, unreadable, or malformed sidecar simply means no recorded provenance.
"""

from datetime import datetime, timezone
import json
import os
import re
import stat
from urllib.parse import urlsplit, urlunsplit

SUFFIX = ".datacache.json"
FORMAT = 1
# A record is a few hundred bytes; anything much larger is not one of ours.
_MAX_RECORD_BYTES = 64 * 1024


def sidecar_path(path):
    """Path of the provenance record kept for the file at path."""
    directory, name = os.path.split(os.fspath(path))
    return os.path.join(directory, "." + name + SUFFIX)


def redact_url(url):
    """Drop credentials, query text, and fragments, which may carry secrets."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    if ":" in host:
        host = "[%s]" % host  # IPv6 literal
    if parts.port is not None:
        host = "%s:%d" % (host, parts.port)
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def describe(path, download_url, sha256=None):
    """Provenance of a just-published file as JSON text.

    sha256 is recorded only when the installed bytes were verified against a
    trusted expectation; a digest of whatever a server sent proves nothing.
    """
    info = os.stat(path)
    return json.dumps({
        "format": FORMAT,
        "url": redact_url(download_url),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "size": info.st_size,
        "mtime_ns": info.st_mtime_ns,
        "sha256": sha256.lower() if sha256 else None,
    }, sort_keys=True)


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
    modification time changed since it was recorded, so provenance is only
    ever reported for the bytes that were actually fetched. Never writes.
    """
    record_path = sidecar_path(path)
    try:
        record_info = os.stat(record_path)
        # Never open a FIFO or device that happens to have the record's name.
        if not stat.S_ISREG(record_info.st_mode) or record_info.st_size > _MAX_RECORD_BYTES:
            return None
        with open(record_path, encoding="utf-8") as source:
            record = json.load(source)
    except (OSError, ValueError):
        return None
    if not _valid(record) or record["size"] != info.st_size or record["mtime_ns"] != info.st_mtime_ns:
        return None
    return record
