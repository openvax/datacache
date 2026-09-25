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


@pytest.mark.parametrize("decoration", ["?filename=table.csv", "#table.csv", "?token=abc#table.csv"])
@pytest.mark.parametrize("decompress", [False, True])
def test_inferred_html_is_not_converted_from_query_text(monkeypatch, decoration, decompress):
    url = "https://host/table.html" + decoration
    html = b"<html><table><tr><td>private data</td></tr></table></html>"
    serve(monkeypatch, html)

    def reject_parser(*args, **kwargs):
        pytest.fail("inferred HTML must not require an HTML parser")

    monkeypatch.setattr(download.pd, "read_html", reject_parser)
    kwargs = dict(decompress=decompress, expected_sha256=hashlib.sha256(html).hexdigest())
    path = Path(fetch_file(url, **kwargs))
    assert path.name == common.build_local_filename(url, decompress=decompress)
    assert path.read_bytes() == html
    assert fetch_file(url, **kwargs) == str(path)
    assert fetch_file(url, force=True, **kwargs) == str(path)
    assert path.read_bytes() == html


@pytest.mark.parametrize("output_argument", ["filename", "destination"])
def test_explicit_csv_conversion_ignores_query_text(tmp_path, monkeypatch, output_argument):
    html = b"<table><tr><td>fixture</td></tr></table>"
    frame = pd.DataFrame({"value": [1]})
    expected = frame.to_csv(index=False).encode()
    serve(monkeypatch, html)
    monkeypatch.setattr(download.pd, "read_html", lambda *a, **kw: [frame])
    output = tmp_path / "table.csv" if output_argument == "destination" else "table.csv"
    path = fetch_file(
        "https://host/table.html?filename=source.html#raw", **{output_argument: output},
        expected_sha256=hashlib.sha256(expected).hexdigest())
    assert Path(path).read_bytes() == expected


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
@pytest.mark.parametrize("mode", [0o600, 0o640, 0o664, 0o440])
def test_refresh_preserves_existing_permissions(tmp_path, output_source, kind, mode):
    url, expected = output_source(kind)
    destination = tmp_path / "data.csv"
    destination.write_bytes(b"old contents")
    destination.chmod(mode)
    fetch_file(url, destination=destination, force=True)
    assert destination.read_bytes() == expected
    assert stat.S_IMODE(destination.stat().st_mode) == mode


@pytest.mark.skipif(os.name != "posix", reason="POSIX umask semantics")
@pytest.mark.parametrize("creation_mask", [0o022, 0o002, 0o027, 0o077])
@pytest.mark.parametrize("kind", ["raw", "gz", "zip", "html"])
@pytest.mark.parametrize("entry_point", ["public", "private"])
def test_new_files_use_normal_creation_permissions(
        tmp_path, monkeypatch, output_source, creation_mask, kind, entry_point):
    url, expected = output_source(kind)
    destination = tmp_path / "data.csv"
    previous_mask = os.umask(creation_mask)
    try:
        # Implementation must not read/reset the process-wide umask, even briefly.
        with monkeypatch.context() as guard:
            def reject_umask(*args):
                pytest.fail("download changed the process umask")
            guard.setattr(download.os, "umask", reject_umask)
            if entry_point == "public":
                fetch_file(url, destination=destination)
            else:
                # pyensembl downloads GTF and FASTA files through this helper.
                download._download_and_decompress_if_necessary(str(destination), url)
    finally:
        os.umask(previous_mask)
    assert destination.read_bytes() == expected
    assert stat.S_IMODE(destination.stat().st_mode) == 0o666 & ~creation_mask
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.skipif(os.name != "posix", reason="POSIX file-mode semantics")
@pytest.mark.parametrize("existing_mode", [None, 0o600, 0o640, 0o644])
@pytest.mark.parametrize("failure", [None, "conversion", "validation"])
def test_csv_contents_remain_private_until_validated(
        tmp_path, monkeypatch, output_source, existing_mode, failure):
    url, expected = output_source("html")
    destination = tmp_path / "private.csv"
    if existing_mode is not None:
        destination.write_bytes(b"old private data")
        destination.chmod(existing_mode)
    original_csv = pd.DataFrame.to_csv
    original_validate = download.validate_file
    original_replace = os.replace
    observations = []

    def check_private(path):
        assert stat.S_IMODE(Path(path).stat().st_mode) & 0o077 == 0
        for sibling in tmp_path.glob(".datacache-*"):
            assert stat.S_IMODE(sibling.stat().st_mode) & 0o077 == 0

    def checked_csv(frame, path, **kwargs):
        check_private(path)
        Path(path).write_bytes(b"partial private data")
        check_private(path)
        if failure == "conversion":
            raise RuntimeError("injected conversion failure")
        result = original_csv(frame, path, **kwargs)
        check_private(path)
        observations.append("converted")
        return result

    def checked_validate(path, *args, **kwargs):
        check_private(path)
        assert Path(path).read_bytes() == expected
        if failure == "validation":
            raise RuntimeError("injected validation failure")
        result = original_validate(path, *args, **kwargs)
        observations.append("validated")
        return result

    def checked_replace(source, target):
        assert observations == ["converted", "validated"]
        assert Path(source).read_bytes() == expected
        final_mode = 0o644 if existing_mode is None else existing_mode
        assert stat.S_IMODE(Path(source).stat().st_mode) == final_mode
        return original_replace(source, target)

    monkeypatch.setattr(pd.DataFrame, "to_csv", checked_csv)
    monkeypatch.setattr(download, "validate_file", checked_validate)
    monkeypatch.setattr(download.os, "replace", checked_replace)
    previous_mask = os.umask(0o022)
    try:
        if failure is None:
            fetch_file(url, destination=destination, force=True)
            assert destination.read_bytes() == expected
        else:
            with pytest.raises(RuntimeError, match="injected"):
                fetch_file(url, destination=destination, force=True)
            if existing_mode is not None:
                assert destination.read_bytes() == b"old private data"
                assert stat.S_IMODE(destination.stat().st_mode) == existing_mode
            else:
                assert not destination.exists()
    finally:
        os.umask(previous_mask)
    assert not list(tmp_path.glob(".datacache-*"))


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("kind", ["raw", "gz", "zip", "html"])
def test_creation_mode_probe_never_contains_data(tmp_path, monkeypatch, output_source, failed, kind):
    url, expected = output_source(kind)
    destination = tmp_path / "data.csv"
    original_open = download._open_staging_file
    original_fstat = os.fstat
    readers = []
    probe_fd = None

    def observed_open(*args, **kwargs):
        nonlocal probe_fd
        result = original_open(*args, **kwargs)
        if kwargs.get("prefix") == ".datacache-mode-":
            probe_fd = result.fileno()
            # Model a reader that opened the publicly readable empty file and
            # retains its descriptor even after it has been unlinked.
            readers.append(open(result.name, "rb"))
            assert readers[-1].read() == b""
        return result

    def checked_fstat(fd):
        if failed and fd == probe_fd:
            raise OSError("injected mode-probe failure")
        return original_fstat(fd)

    monkeypatch.setattr(download, "_open_staging_file", observed_open)
    monkeypatch.setattr(download.os, "fstat", checked_fstat)
    try:
        if failed:
            with pytest.raises(OSError, match="mode-probe failure"):
                fetch_file(url, destination=destination)
            assert not destination.exists()
        else:
            fetch_file(url, destination=destination)
            assert destination.read_bytes() == expected
        assert len(readers) == 1
        assert readers[0].read() == b""
        assert not list(tmp_path.glob(".datacache-*"))
    finally:
        for reader in readers:
            reader.close()


@pytest.mark.parametrize("kind", ["raw", "gz", "zip", "html"])
@pytest.mark.parametrize("failure", ["chmod", "replace"])
@pytest.mark.parametrize("existing", [False, True])
def test_permission_or_publication_failure_preserves_file_and_mode(
        tmp_path, monkeypatch, output_source, kind, failure, existing):
    url, expected = output_source(kind)
    destination = tmp_path / "data.csv"
    if existing:
        destination.write_bytes(b"old complete file")
        destination.chmod(0o640)
        original_mode = stat.S_IMODE(destination.stat().st_mode)

    def fail(path, *args):
        assert Path(path).read_bytes() == expected
        if failure == "replace" and existing:
            assert stat.S_IMODE(Path(path).stat().st_mode) == original_mode
        raise PermissionError("injected " + failure + " failure")

    monkeypatch.setattr(download.os, failure, fail)
    with pytest.raises(PermissionError, match="injected"):
        fetch_file(url, destination=destination, force=True)
    if existing:
        assert destination.read_bytes() == b"old complete file"
        assert stat.S_IMODE(destination.stat().st_mode) == original_mode
        assert list(tmp_path.iterdir()) == [destination]
    else:
        assert list(tmp_path.iterdir()) == []


@pytest.mark.skipif(os.name != "posix", reason="POSIX file-mode semantics")
@pytest.mark.parametrize("kind", ["raw", "gz", "zip"])
@pytest.mark.parametrize("failed", [False, True])
def test_binary_staging_remains_private_until_validated(tmp_path, monkeypatch, output_source, kind, failed):
    url, expected = output_source(kind)
    destination = tmp_path / "data"
    original_stream = download._stream_to_file
    original_copy = download.copyfileobj
    original_validate = download.validate_file
    original_replace = os.replace
    observations = []

    def check_private():
        assert not destination.exists()
        for sibling in tmp_path.glob(".datacache-*"):
            assert stat.S_IMODE(sibling.stat().st_mode) & 0o077 == 0

    def checked_stream(*args, **kwargs):
        check_private()
        result = original_stream(*args, **kwargs)
        check_private()
        observations.append("downloaded")
        return result

    def checked_copy(*args, **kwargs):
        check_private()
        result = original_copy(*args, **kwargs)
        check_private()
        observations.append("decompressed")
        return result

    def checked_validate(path, *args, **kwargs):
        check_private()
        assert Path(path).read_bytes() == expected
        result = original_validate(path, *args, **kwargs)
        observations.append("validated")
        return result

    def checked_replace(source, target):
        assert observations[-1] == "validated"
        assert Path(source).read_bytes() == expected
        assert stat.S_IMODE(Path(source).stat().st_mode) == 0o644
        return original_replace(source, target)

    monkeypatch.setattr(download, "_stream_to_file", checked_stream)
    monkeypatch.setattr(download, "copyfileobj", checked_copy)
    monkeypatch.setattr(download, "validate_file", checked_validate)
    monkeypatch.setattr(download.os, "replace", checked_replace)
    previous_mask = os.umask(0o022)
    try:
        kwargs = dict(destination=destination, force=True,
                      expected_sha256="0" * 64 if failed else hashlib.sha256(expected).hexdigest())
        if failed:
            with pytest.raises(download.FileValidationError, match="SHA-256 mismatch"):
                fetch_file(url, **kwargs)
            assert not destination.exists()
        else:
            fetch_file(url, **kwargs)
            assert destination.read_bytes() == expected
    finally:
        os.umask(previous_mask)
    assert observations == (["downloaded"] + ([] if kind == "raw" else ["decompressed"]) +
                            ([] if failed else ["validated"]))
    assert not list(tmp_path.glob(".datacache-*"))


@pytest.mark.parametrize("exhausted", [False, True])
def test_staging_collisions_do_not_overwrite_unrelated_files(tmp_path, monkeypatch, exhausted):
    first, second = UUID(int=1), UUID(int=2)
    collision = tmp_path / (".datacache-download-" + first.hex + ".tmp")
    collision.write_bytes(b"unrelated file")
    candidates = iter([first, second, UUID(int=3)])
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
