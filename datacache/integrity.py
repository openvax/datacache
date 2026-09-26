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

from .progress import Progress


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
            digest = hashlib.sha256()
            with Progress(show_progress, "Verifying", info.st_size) as progress:
                completed = 0
                for chunk in iter(lambda: source.read(2 ** 20), b""):
                    digest.update(chunk)
                    completed += len(chunk)
                    progress(completed, info.st_size)
            actual_sha256 = digest.hexdigest()
            if actual_sha256 != expected_sha256.lower():
                raise FileValidationError(
                    path, "SHA-256 mismatch: expected %s, found %s" % (
                        expected_sha256.lower(), actual_sha256))
    return path
