"""Offline fixtures matching private/public call patterns in downstream packages."""

import gzip
import io
import os
import stat
import zipfile

import pytest

from datacache import Cache, build_local_filename, download


DATA = b">reference\nACGT\n"


def archive_bytes(suffix):
    if suffix == "gz":
        return gzip.compress(DATA)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("reference.fa", DATA)
    return output.getvalue()


@pytest.mark.parametrize("suffix", ["gz", "zip"])
@pytest.mark.parametrize("decoration", ["", "?token=abc", "#download", "?token=abc#download"])
@pytest.mark.parametrize("decompress_on_download", [False, True])
def test_pyensembl_private_download_contract(tmp_path, monkeypatch, suffix, decoration, decompress_on_download):
    url = "https://host/reference.fa." + suffix + decoration
    # pyensembl passes the remote basename explicitly and strips a trailing
    # compression suffix itself, before calling the helper without flags.
    filename = build_local_filename(url, filename=url.rsplit("/", 1)[-1])
    if decompress_on_download and filename.endswith("." + suffix):
        filename = filename[:-(len(suffix) + 1)]
    destination = tmp_path / filename
    payload = archive_bytes(suffix)

    def stream(url, output, **kwargs):
        output.write(payload)

    monkeypatch.setattr(download, "_stream_to_file", stream)
    expected = DATA if decompress_on_download and not decoration else payload
    for previous in (None, b"previous artifact"):
        if previous is not None:
            destination.write_bytes(previous)
        download._download_and_decompress_if_necessary(str(destination), url, timeout=3600)
        assert destination.read_bytes() == expected
        assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("url", ["https://host/table.html?name=table.csv", "https://host/table.htm#table.csv"])
def test_private_helper_preserves_legacy_html_with_query_or_fragment(tmp_path, monkeypatch, url):
    payload = b"<table>original HTML</table>"
    destination = tmp_path / "table.csv"
    monkeypatch.setattr(download, "_stream_to_file", lambda url, output, **kw: output.write(payload))

    def reject_parser(*args, **kwargs):
        pytest.fail("legacy query/fragment URL must retain HTML")

    monkeypatch.setattr(download.pd, "read_html", reject_parser)
    download._download_and_decompress_if_necessary(str(destination), url)
    assert destination.read_bytes() == payload


@pytest.mark.parametrize("decompress", [False, True])
def test_private_helper_explicit_decompression_overrides_legacy_inference(tmp_path, monkeypatch, decompress):
    payload = gzip.compress(DATA)
    destination = tmp_path / "reference.gz_token_abc"
    monkeypatch.setattr(download, "_stream_to_file", lambda url, output, **kw: output.write(payload))
    download._download_and_decompress_if_necessary(
        destination, "https://host/reference.gz?token=abc", decompress=decompress)
    assert destination.read_bytes() == (DATA if decompress else payload)


@pytest.mark.parametrize("failed", [False, True])
def test_decompress_stream_compatibility_entry_point(tmp_path, failed):
    destination = tmp_path / "reference"
    destination.write_bytes(b"previous artifact")
    source = gzip.GzipFile(fileobj=io.BytesIO(gzip.compress(DATA)[:-8] if failed else gzip.compress(DATA)))
    with source:
        if failed:
            with pytest.raises(EOFError):
                download._decompress_to_file(source, destination)
            assert destination.read_bytes() == b"previous artifact"
        else:
            download._decompress_to_file(source, destination)
            assert destination.read_bytes() == DATA
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.skipif(os.name != "posix", reason="POSIX umask semantics")
@pytest.mark.parametrize("creation_mask", [0o022, 0o002, 0o077])
@pytest.mark.parametrize("existing_mode", [None, 0o600, 0o640])
def test_decompress_stream_publication_permissions(tmp_path, creation_mask, existing_mode):
    destination = tmp_path / "reference.fa"
    if existing_mode is not None:
        destination.write_bytes(b"previous artifact")
        destination.chmod(existing_mode)
    previous_mask = os.umask(creation_mask)
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(gzip.compress(DATA))) as source:
            download._decompress_to_file(source, destination)
    finally:
        os.umask(previous_mask)
    expected_mode = 0o666 & ~creation_mask if existing_mode is None else existing_mode
    assert stat.S_IMODE(destination.stat().st_mode) == expected_mode
    assert destination.read_bytes() == DATA
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("suffix", ["gz", "zip"])
@pytest.mark.parametrize("decompress", [False, True])
def test_pepdata_download_endpoint_contract(tmp_path, monkeypatch, suffix, decompress):
    # IEDB's real URLs name the archive in a query, while the path ends in .php.
    url = "https://www.iedb.org/downloader.php?file_name=doc/records." + suffix
    archive = archive_bytes(suffix)
    calls = []

    def stream(url, output, **kwargs):
        calls.append(url)
        output.write(archive)

    monkeypatch.setattr(download, "_stream_to_file", stream)
    cache = Cache("pepdata", cache_root=tmp_path)
    for force in (False, False, True):
        path = cache.fetch(url, filename="records.csv", decompress=decompress, force=force)
        with open(path, "rb") as source:
            assert source.read() == DATA
    assert len(calls) == 2


@pytest.mark.parametrize("suffix", ["gz", "zip"])
@pytest.mark.parametrize("template", ["?file=records.{}", "?file=records.{}#download", "?file=records%2E{}"])
def test_endpoint_default_retention_and_explicit_decompression(tmp_path, monkeypatch, suffix, template):
    url = "https://host/download" + template.format(suffix)
    archive = archive_bytes(suffix)
    monkeypatch.setattr(download, "_stream_to_file", lambda url, output, **kw: output.write(archive))
    retained = download.fetch_file(url, cache_root=tmp_path)
    expanded = download.fetch_file(url, decompress=True, cache_root=tmp_path)
    assert retained != expanded
    with open(retained, "rb") as source:
        assert source.read() == archive
    with open(expanded, "rb") as source:
        assert source.read() == DATA
