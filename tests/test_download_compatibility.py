"""Offline compatibility checks for URL formats and publication permissions."""

import gzip
import hashlib
import io
import os
from pathlib import Path
import stat
from uuid import UUID
import zipfile

import pandas as pd
import pytest

from datacache import common, download, fetch_file


CONTENTS = b"complete contents\n"


@pytest.fixture(autouse=True)
def offline_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "get_data_dir", lambda subdir=None: str(tmp_path / "cache"))

    def reject_network(*args, **kwargs):
        pytest.fail("unexpected network access")

    monkeypatch.setattr(download.requests, "get", reject_network)
    monkeypatch.setattr(download.urllib.request, "urlopen", reject_network)


def serve(monkeypatch, contents):
    def stream(url, output, **kwargs):
        output.write(contents)
        return len(contents)

    monkeypatch.setattr(download, "_stream_to_file", stream)


def archive_bytes(extension):
    if extension.lower() == "gz":
        return gzip.compress(CONTENTS)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("data", CONTENTS)
        archive.writestr("other", b"second member")
    return output.getvalue()


@pytest.mark.parametrize("extension", ["gz", "zip"])
@pytest.mark.parametrize("decoration,legacy_suffix", [
    ("?token=abc", "_token_abc"),
    ("#download", "#download"),
    ("?token=abc#download", "_token_abc#download"),
])
def test_default_archive_retention_and_cache_key(monkeypatch, extension, decoration, legacy_suffix):
    url = "https://host/data." + extension + decoration
    payload = archive_bytes(extension)
    serve(monkeypatch, payload)
    kwargs = dict(expected_sha256=hashlib.sha256(payload).hexdigest())
    path = Path(fetch_file(url, **kwargs))
    # Preserve the key used by master, as well as the bytes stored under it.
    assert path.name == (hashlib.md5(url.encode()).hexdigest() +
                         ".https___host_data." + extension + legacy_suffix)
    assert path.read_bytes() == payload
    assert fetch_file(url, **kwargs) == str(path)
    assert fetch_file(url, force=True, **kwargs) == str(path)
    if extension == "zip":
        with zipfile.ZipFile(path) as archive:
            assert archive.namelist() == ["data", "other"]
    else:
        with gzip.open(path, "rb") as archive:
            assert archive.read() == CONTENTS


@pytest.mark.parametrize("extension", ["gz", "zip", "GZ", "ZIP"])
@pytest.mark.parametrize("decoration", ["", "?token=abc", "#download", "?next=file.zip#download"])
def test_inferred_archive_and_decompressed_paths_are_distinct(monkeypatch, extension, decoration):
    url = "https://host/data." + extension + decoration
    payload = archive_bytes(extension)
    serve(monkeypatch, payload)
    archive_path = Path(fetch_file(url))
    installed_path = Path(fetch_file(
        url, decompress=True, expected_sha256=hashlib.sha256(CONTENTS).hexdigest()))
    assert archive_path != installed_path
    assert archive_path.read_bytes() == payload
    assert installed_path.read_bytes() == CONTENTS
    assert not installed_path.name.lower().endswith((".gz", ".zip"))
    assert fetch_file(url) == str(archive_path)
    assert fetch_file(url, decompress=True) == str(installed_path)


@pytest.mark.parametrize("explicit", ["filename", "destination"])
@pytest.mark.parametrize("extension", ["gz", "zip"])
@pytest.mark.parametrize("keep_suffix,decompress", [(True, False), (False, False), (True, True)])
def test_explicit_archive_output_with_query_string(
        tmp_path, monkeypatch, explicit, extension, keep_suffix, decompress):
    payload = archive_bytes(extension)
    serve(monkeypatch, payload)
    filename = "data." + extension if keep_suffix else "data"
    kwargs = {explicit: tmp_path / filename if explicit == "destination" else filename}
    expected = payload if keep_suffix and not decompress else CONTENTS
    path = fetch_file(
        "https://host/data." + extension + "?token=abc", decompress=decompress,
        expected_sha256=hashlib.sha256(expected).hexdigest(), **kwargs)
    assert Path(path).read_bytes() == expected


@pytest.mark.parametrize("source_name", ["notgz", "notzip", "nothtml", "data.txt?format=.gz"])
def test_transform_detection_uses_actual_extensions(tmp_path, monkeypatch, source_name):
    serve(monkeypatch, CONTENTS)
    destination = tmp_path / "data.csv"
    fetch_file("https://host/" + source_name, destination=destination)
    assert destination.read_bytes() == CONTENTS


@pytest.fixture
def output_source(monkeypatch):
    def prepare(kind):
        if kind == "html":
            frame = pd.DataFrame({"value": [1]})
            monkeypatch.setattr(download.pd, "read_html", lambda *a, **kw: [frame])
            serve(monkeypatch, b"<table>fixture</table>")
            return "https://host/table.html", frame.to_csv(index=False).encode()
        serve(monkeypatch, CONTENTS if kind == "raw" else archive_bytes(kind))
        return "https://host/data." + kind, CONTENTS
    return prepare


@pytest.mark.skipif(os.name != "posix", reason="POSIX file-mode semantics")
@pytest.mark.parametrize("kind", ["raw", "gz", "zip", "html"])
@pytest.mark.parametrize("mode", [0o640, 0o664, 0o440])
def test_refresh_preserves_existing_permissions(tmp_path, output_source, kind, mode):
    url, expected = output_source(kind)
    destination = tmp_path / "data.csv"
    destination.write_bytes(b"old contents")
    destination.chmod(mode)
    fetch_file(url, destination=destination, force=True)
    assert destination.read_bytes() == expected
    assert stat.S_IMODE(destination.stat().st_mode) == mode


@pytest.mark.skipif(os.name != "posix", reason="POSIX umask semantics")
@pytest.mark.parametrize("creation_mask", [0o022, 0o002, 0o077])
def test_new_csv_uses_normal_creation_permissions(tmp_path, monkeypatch, output_source, creation_mask):
    url, expected = output_source("html")
    destination = tmp_path / "data.csv"
    previous_mask = os.umask(creation_mask)
    try:
        # Implementation must not read/reset the process-wide umask, even briefly.
        with monkeypatch.context() as guard:
            def reject_umask(*args):
                pytest.fail("download changed the process umask")
            guard.setattr(download.os, "umask", reject_umask)
            fetch_file(url, destination=destination)
    finally:
        os.umask(previous_mask)
    assert destination.read_bytes() == expected
    assert stat.S_IMODE(destination.stat().st_mode) == 0o666 & ~creation_mask


@pytest.mark.parametrize("kind", ["raw", "gz", "zip", "html"])
@pytest.mark.parametrize("failure", ["chmod", "replace"])
def test_permission_or_publication_failure_preserves_file_and_mode(
        tmp_path, monkeypatch, output_source, kind, failure):
    url, expected = output_source(kind)
    destination = tmp_path / "data.csv"
    destination.write_bytes(b"old complete file")
    destination.chmod(0o640)
    original_mode = stat.S_IMODE(destination.stat().st_mode)

    def fail(path, *args):
        assert Path(path).read_bytes() == expected
        if failure == "replace":
            assert stat.S_IMODE(Path(path).stat().st_mode) == original_mode
        raise PermissionError("injected " + failure + " failure")

    monkeypatch.setattr(download.os, failure, fail)
    with pytest.raises(PermissionError, match="injected"):
        fetch_file(url, destination=destination, force=True)
    assert destination.read_bytes() == b"old complete file"
    assert stat.S_IMODE(destination.stat().st_mode) == original_mode
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.skipif(os.name != "posix", reason="POSIX file-mode semantics")
@pytest.mark.parametrize("kind", ["raw", "gz", "zip"])
def test_new_binary_files_remain_private(tmp_path, output_source, kind):
    url, expected = output_source(kind)
    destination = tmp_path / "data"
    fetch_file(url, destination=destination)
    assert destination.read_bytes() == expected
    assert stat.S_IMODE(destination.stat().st_mode) & 0o077 == 0


@pytest.mark.parametrize("exhausted", [False, True])
def test_staging_collisions_do_not_overwrite_unrelated_files(tmp_path, monkeypatch, exhausted):
    first, second = UUID(int=1), UUID(int=2)
    collision = tmp_path / (".datacache-download-" + first.hex + ".tmp")
    collision.write_bytes(b"unrelated file")
    candidates = iter([first, second])
    monkeypatch.setattr(download, "uuid4", lambda: first if exhausted else next(candidates))
    serve(monkeypatch, CONTENTS)
    destination = tmp_path / "data"
    if exhausted:
        with pytest.raises(FileExistsError, match="unique staging file"):
            fetch_file("https://host/data", destination=destination)
        assert list(tmp_path.iterdir()) == [collision]
    else:
        fetch_file("https://host/data", destination=destination)
        assert destination.read_bytes() == CONTENTS
        assert set(tmp_path.iterdir()) == {collision, destination}
    assert collision.read_bytes() == b"unrelated file"
