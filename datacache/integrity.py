# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import os
import re
import stat

from .progress import Progress, should_show_progress


class FileValidationError(ValueError):
    """A local file does not match its supplied integrity expectations."""

    def __init__(self, path, reason):
        self.path = os.fspath(path)
        self.reason = reason
        super().__init__("%s: %s" % (self.path, reason))


def _validate_expectations(expected_sha256, expected_size):
    if expected_sha256 is not None and (
            not isinstance(expected_sha256, str) or
            re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256) is None):
        raise ValueError("expected_sha256 must be a 64-character hexadecimal digest")
    if expected_size is not None and (
            isinstance(expected_size, bool) or
            not isinstance(expected_size, int) or expected_size < 0):
        raise ValueError("expected_size must be a non-negative integer")


def validate_file(path, expected_sha256=None, expected_size=None, *, show_progress=False):
    """Check a readable regular file without writes or network access.

    Expectations describe the bytes at ``path`` (after any decompression).
    Return the path as a string on success. Missing files raise
    ``FileNotFoundError``; permission errors propagate; non-regular files and
    size/hash mismatches raise ``FileValidationError``. Without expectations,
    this only checks that the file is readable and regular, not its integrity.
    show_progress=True displays optional tqdm progress during SHA-256 hashing.
    """
    return _validate_file(path, expected_sha256, expected_size, show_progress=show_progress)[0]


def sha256_stream(
        handle,
        *,
        total=None,
        show_progress=False,
        label="Verifying",
        chunk_size=2 ** 20,
        writer=None,
        extra_digest=None,
        progress_callback=None):
    """Hash ``handle`` to EOF with SHA-256 and return ``(hex_digest, bytes_read)``.

    Uses ``hashlib.file_digest`` when no progress reporting, output mirroring,
    or secondary digest is requested.
    """
    if not (should_show_progress(show_progress) or writer is not None or extra_digest is not None or progress_callback is not None):
        start = handle.tell()
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
        return digest, handle.tell() - start
    digest = hashlib.sha256()
    size = 0
    with Progress(show_progress, label, total) as progress:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            if writer is not None:
                writer.write(chunk)
            if extra_digest is not None:
                extra_digest.update(chunk)
            digest.update(chunk)
            size += len(chunk)
            progress(size, total)
            if progress_callback is not None:
                progress_callback(size, total)
    return digest.hexdigest(), size


def _validate_file(path, expected_sha256=None, expected_size=None, *, show_progress=False):
    """validate_file, also returning the stat of the bytes that were checked.

    The stat comes from the open file, so callers reporting size or modification
    time describe exactly the bytes validated, even if the path is replaced.
    """
    _validate_expectations(expected_sha256, expected_size)
    path = os.fspath(path)
    if not stat.S_ISREG(os.stat(path).st_mode):
        raise FileValidationError(path, "expected a regular file")
    with open(path, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise FileValidationError(path, "expected a regular file")
        if expected_size is not None and info.st_size != expected_size:
            raise FileValidationError(
                path, "size mismatch: expected %d bytes, found %d" % (
                    expected_size, info.st_size))
        if expected_sha256 is not None:
            actual_sha256, _ = sha256_stream(source, total=info.st_size, show_progress=show_progress)
            if actual_sha256 != expected_sha256.lower():
                raise FileValidationError(
                    path, "SHA-256 mismatch: expected %s, found %s" % (
                        expected_sha256.lower(), actual_sha256))
    return path, info
