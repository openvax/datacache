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

"""
Tests for streaming downloads and the progress_callback hook
(regression tests for https://github.com/openvax/datacache/issues/49).

These use local file:// URLs so they don't depend on the network.
"""

import os
from tempfile import NamedTemporaryFile, mkdtemp

from datacache.download import (
    _stream_to_file,
    _download_to_temp_file,
    _download_and_decompress_if_necessary,
)


def _make_temp_file(contents=b""):
    with NamedTemporaryFile(delete=False) as f:
        f.write(contents)
        return f.name


def _file_url(path):
    return "file://" + path


def test_stream_to_file_writes_all_bytes_and_reports_progress():
    contents = b"abc" * 5000  # 15000 bytes
    src_path = _make_temp_file(contents)
    dst_path = _make_temp_file()
    progress = []
    try:
        with open(dst_path, "wb") as dst:
            total = _stream_to_file(
                _file_url(src_path),
                dst,
                chunk_size=4096,
                progress_callback=lambda done, total: progress.append((done, total)))
        assert total == len(contents)
        with open(dst_path, "rb") as f:
            assert f.read() == contents
        # callback fired and the final report equals the full size
        assert progress, "progress_callback was never called"
        assert progress[-1][0] == len(contents)
        # byte counts are monotonically non-decreasing
        counts = [done for (done, _total) in progress]
        assert counts == sorted(counts)
        # more than one chunk was written for a 15kB file at 4kB chunks
        assert len(progress) > 1
    finally:
        os.remove(src_path)
        os.remove(dst_path)


def test_download_to_temp_file_streams_local_file():
    contents = b"hello streaming world"
    src_path = _make_temp_file(contents)
    tmp_path = None
    try:
        tmp_path = _download_to_temp_file(_file_url(src_path))
        with open(tmp_path, "rb") as f:
            assert f.read() == contents
    finally:
        os.remove(src_path)
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


def test_download_and_decompress_threads_progress_callback():
    contents = b"x" * 1234
    src_path = _make_temp_file(contents)
    out_dir = mkdtemp()
    out_path = os.path.join(out_dir, "out.bin")
    progress = []
    try:
        _download_and_decompress_if_necessary(
            full_path=out_path,
            download_url=_file_url(src_path),
            chunk_size=100,
            progress_callback=lambda done, _total: progress.append(done))
        with open(out_path, "rb") as f:
            assert f.read() == contents
        assert progress and progress[-1] == len(contents)
    finally:
        os.remove(src_path)
        if os.path.exists(out_path):
            os.remove(out_path)
        os.rmdir(out_dir)
