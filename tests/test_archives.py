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

"""Transactional whole-archive installation for directory-tree consumers."""

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import gzip
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import threading

import pytest

from datacache import (
    FileValidationError, VersionedArchiveRegistry, inspect_archive,
    install_archive,
)
from datacache import archives, bundle_store, download


def make_tar(path, members, *, mode="w:bz2"):
    with tarfile.open(path, mode) as archive:
        for name, value in members:
            if isinstance(value, tarfile.TarInfo):
                archive.addfile(value)
                continue
            info = tarfile.TarInfo(name)
            if value is None:
                info.type = tarfile.DIRTYPE
                archive.addfile(info)
            else:
                info.size = len(value)
                archive.addfile(info, io.BytesIO(value))
    return path


@pytest.fixture
def archive_file(tmp_path):
    path = make_tar(tmp_path / "models.tar.bz2", [
        ("models", None),
        ("models/a/model.json", b'{"model": "a"}\n'),
        ("models/b/model.json", b'{"model": "b"}\n'),
        ("README.txt", b"released weights\n"),
    ])
    data = path.read_bytes()
    return path, data, sha256(data).hexdigest()


def test_install_inspect_and_read_only_reuse(tmp_path, archive_file, monkeypatch):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    receipt = "url\nhttps://example.test/models.tar.bz2\n"
    sources = [{"url": "https://example.test/models.tar.bz2", "path": source}]

    bundle = install_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt})

    assert (bundle / "models/a/model.json").read_text() == '{"model": "a"}\n'
    assert (bundle / "DOWNLOAD_INFO.csv").read_text() == receipt
    inspected = inspect_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt})
    assert inspected.status == "available" and inspected.verified
    assert Path(inspected.bundle) == bundle
    assert inspected.source_urls == ("https://example.test/models.tar.bz2",)
    assert inspected.archive_size == len(data)
    assert inspected.recorded_sha256 == digest
    assert inspected.fetched_at.endswith("+00:00")
    assert inspect_archive(destination).status == "available"
    assert not inspect_archive(destination).verified
    # Files are verified only by hashes the caller supplied, never the manifest's own.
    assert all(item.verified for item in inspected.files.values())
    assert not any(item.verified for item in inspect_archive(destination).files.values())
    fast = inspect_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt}, verify_files=False)
    assert fast.status == "available" and not fast.verified and Path(fast.bundle) == bundle
    # Every file is listed with its size, but none is hashed or marked verified.
    assert set(fast.files) == set(inspected.files)
    assert all(not item.verified and item.size is not None for item in fast.files.values())

    hash_file = bundle_store.hash_file

    def size_only(*args, **kwargs):
        assert kwargs.get("hash_contents") is False, "fast inspection hashed the tree"
        return hash_file(*args, **kwargs)

    monkeypatch.setattr(bundle_store, "hash_file", size_only)
    assert inspect_archive(
        destination, sources, extra_files={"DOWNLOAD_INFO.csv": receipt},
        verify_files=False).status == "available"
    monkeypatch.setattr(bundle_store, "hash_file", hash_file)
    (bundle / "README.txt").unlink()
    assert inspect_archive(destination, verify_files=False).status == "invalid"
    (bundle / "README.txt").write_bytes(b"released weights\n")

    refreshed = install_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt}, force=True)
    assert refreshed != bundle
    assert (bundle / "models/a/model.json").read_text() == '{"model": "a"}\n'

    def forbidden(*args, **kwargs):
        raise AssertionError("a valid cache hit attempted a write, lock, or transfer")

    monkeypatch.setattr(bundle_store.BundleStore, "lock", forbidden)
    monkeypatch.setattr(download, "fetch_file", forbidden)
    monkeypatch.setattr(archives, "write_json", forbidden)
    monkeypatch.setattr(bundle_store, "write_json", forbidden)
    assert install_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt}) == refreshed


def test_line_endings_and_control_bytes_are_installed_unchanged(tmp_path):
    # Windows translates line endings, and stops reading at 0x1A, in files
    # that aren't opened as binary.
    contents = b"a,b\r\n1,2\r\n\x1a after the control byte\n"
    source = make_tar(tmp_path / "table.tar.bz2", [("table.csv", contents)])
    data = source.read_bytes()
    destination = tmp_path / "download"
    bundle = install_archive(
        destination, source, expected_sha256=sha256(data).hexdigest(), expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": "url\r\nhttps://example.test/table.tar.bz2\r\n"})
    assert (bundle / "table.csv").read_bytes() == contents
    manifest = json.loads((bundle / archives.MANIFEST).read_text())
    assert manifest["files"]["table.csv"] == {"sha256": sha256(contents).hexdigest(), "size": len(contents)}
    assert inspect_archive(destination).status == "available"


def test_install_hashes_the_extracted_tree_once(tmp_path, archive_file, monkeypatch):
    source, data, digest = archive_file
    destination = tmp_path / "download"

    hash_file = bundle_store.hash_file

    def size_only(*args, **kwargs):
        # The tree's own check; the manifest's hashing uses archives.hash_file.
        assert kwargs.get("hash_contents") is False, "hashed the extracted tree a second time"
        return hash_file(*args, **kwargs)

    monkeypatch.setattr(bundle_store, "hash_file", size_only)
    install_archive(destination, source, expected_sha256=digest, expected_size=len(data))
    monkeypatch.undo()
    assert inspect_archive(destination, source, expected_sha256=digest, expected_size=len(data)).verified


def test_standard_dot_prefixed_tar_tree_installs_and_inspects(tmp_path):
    source = make_tar(tmp_path / "standard.tar.bz2", [
        (".", None), ("././", None), ("./models/", None),
        ("./models/model.json", b"model"), ("././README.txt", b"readme"),
    ])
    data = source.read_bytes()
    destination = tmp_path / "download"
    bundle = install_archive(
        destination, source, expected_sha256=sha256(data).hexdigest(),
        expected_size=len(data))

    assert (bundle / "models/model.json").read_bytes() == b"model"
    inspected = inspect_archive(destination)
    assert inspected.status == "available"
    assert set(inspected.files) == {"models/model.json", "README.txt"}


@pytest.mark.parametrize("members", [
    [("./file", b"one"), ("file", b"two")],
    [("./Models/file", b"one"), ("models/other", b"two")],
    [("./../escape", b"unsafe")],
    [("./.datacache-archive-manifest.json", b"forged")],
])
def test_dot_prefix_normalization_preserves_path_safety(tmp_path, members):
    source = make_tar(tmp_path / "unsafe.tar.bz2", members)
    destination = tmp_path / "download"
    with pytest.raises(FileValidationError):
        install_archive(destination, source, verified=False)
    assert inspect_archive(destination).status == "missing"
    assert not (tmp_path / "escape").exists()


def test_large_tree_manifest_installs_and_is_reused_offline(tmp_path):
    source = make_tar(tmp_path / "large.tar.bz2", [
        ("file-%05d" % index, b"") for index in range(12000)
    ])
    data = source.read_bytes()
    options = {"expected_sha256": sha256(data).hexdigest(), "expected_size": len(data)}
    destination = tmp_path / "download"
    bundle = install_archive(destination, source, **options)
    assert (bundle / archives.MANIFEST).stat().st_size > 1024 * 1024
    inspected = inspect_archive(destination, source, **options)
    assert inspected.status == "available" and inspected.verified
    assert len(inspected.files) == 12000

    source.unlink()
    assert install_archive(destination, source, **options) == bundle
    assert inspect_archive(destination, source, verify_files=False, **options).status == "available"


@pytest.mark.skipif(os.name != "posix", reason="POSIX FIFO sources")
def test_fifo_source_fails_promptly_and_releases_writer_lock(tmp_path, archive_file):
    fifo = tmp_path / "source.fifo"
    os.mkfifo(fifo)
    destination = tmp_path / "download"
    # A subprocess timeout makes a blocking open a bounded test failure.
    script = """
import sys
from pathlib import Path
from datacache import FileValidationError, install_archive
try:
    install_archive(Path(sys.argv[1]), Path(sys.argv[2]), verified=False)
except FileValidationError:
    pass
else:
    raise AssertionError("FIFO archive source was accepted")
"""
    subprocess.run(
        [sys.executable, "-c", script, str(destination), str(fifo)],
        timeout=5, check=True, capture_output=True, text=True)
    assert inspect_archive(destination).status == "missing"
    source, data, digest = archive_file
    assert install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data)).is_dir()


@pytest.mark.parametrize("new_extras", [
    {}, {"DOWNLOAD_INFO.csv": "url\noriginal\n"},
    {"DOWNLOAD_INFO.csv": "url\nchanged\n", "INFO.txt": "old"},
    {"NEW_INFO.txt": "new"},
])
def test_changed_or_removed_consumer_metadata_requires_refresh(
        tmp_path, archive_file, new_extras):
    source, data, digest = archive_file
    options = {"expected_sha256": digest, "expected_size": len(data)}
    destination = tmp_path / "download"
    old_extras = {"DOWNLOAD_INFO.csv": "url\noriginal\n", "INFO.txt": "old"}
    old = install_archive(destination, source, extra_files=old_extras, **options)
    manifest = json.loads((old / archives.MANIFEST).read_text())
    assert set(manifest["extra_files"]) == set(old_extras)
    for verify_files in (True, False):
        assert inspect_archive(
            destination, source, extra_files=new_extras,
            verify_files=verify_files, **options).status == "invalid"
    with pytest.raises(FileValidationError, match="force=True"):
        install_archive(destination, source, extra_files=new_extras, **options)

    refreshed = install_archive(
        destination, source, extra_files=new_extras, force=True, **options)
    assert refreshed != old
    assert inspect_archive(destination, source, extra_files=new_extras, **options).verified
    assert {name for name in old_extras if (refreshed / name).exists()} == set(old_extras) & set(new_extras)
    assert all((old / name).read_text() == value for name, value in old_extras.items())


def test_ordered_split_archive_and_logical_source_urls(tmp_path, archive_file):
    _, data, _ = archive_file
    cuts = (len(data) // 3, 2 * len(data) // 3)
    pieces = (data[:cuts[0]], data[cuts[0]:cuts[1]], data[cuts[1]:])
    sources, urls = [], []
    for index, piece in enumerate(pieces):
        path = tmp_path / ("models.part.%d" % index)
        path.write_bytes(piece)
        url = "https://example.test/models.tar.bz2.part.%d?token=secret" % index
        urls.append(url)
        sources.append({"url": url, "path": path})
    csv_receipt = "url\n" + "".join(url.split("?", 1)[0] + "\n" for url in urls)
    destination = tmp_path / "split"

    bundle = install_archive(
        destination, sources, verified=False,
        extra_files={"DOWNLOAD_INFO.csv": csv_receipt})

    assert (bundle / "models/b/model.json").is_file()
    assert (bundle / "DOWNLOAD_INFO.csv").read_text() == csv_receipt
    state = inspect_archive(destination, sources, extra_files={"DOWNLOAD_INFO.csv": csv_receipt})
    assert state.status == "available" and not state.verified
    manifest = json.loads((bundle / archives.MANIFEST).read_text())
    assert [item["url"] for item in manifest["sources"]] == [url.split("?", 1)[0] for url in urls]
    assert [item["fingerprint"] for item in manifest["sources"]] != [
        sha256(url.split("?", 1)[0].encode()).hexdigest() for url in urls]

    assert inspect_archive(
        destination, list(reversed(sources)), extra_files={"DOWNLOAD_INFO.csv": csv_receipt}).status == "invalid"
    with pytest.raises(FileValidationError, match="force=True"):
        install_archive(
            destination, list(reversed(sources)), verified=False,
            extra_files={"DOWNLOAD_INFO.csv": csv_receipt})


def test_trusted_part_hashes_allow_mirror_reuse(tmp_path, archive_file):
    _, data, _ = archive_file
    middle = len(data) // 2
    pieces = [data[:middle], data[middle:]]
    sources = []
    for index, piece in enumerate(pieces):
        path = tmp_path / ("part-%d" % index)
        path.write_bytes(piece)
        sources.append({
            "url": "https://one.example/part-%d" % index,
            "path": path,
            "sha256": sha256(piece).hexdigest(),
            "size": len(piece),
        })
    destination = tmp_path / "download"
    bundle = install_archive(destination, sources)
    mirrored = [dict(source, url=source["url"].replace("one", "two"))
                for source in sources]

    state = inspect_archive(destination, mirrored)
    assert state.status == "available" and state.verified
    assert install_archive(destination, mirrored) == bundle


def test_url_parts_use_raw_fetch_and_forward_download_options(tmp_path, archive_file, monkeypatch):
    source, data, digest = archive_file
    calls = []
    original = download.fetch_file

    def recording(url, **options):
        calls.append((url, dict(options)))
        return original(url, **options)

    monkeypatch.setattr(download, "fetch_file", recording)
    bundle = install_archive(
        tmp_path / "download", source.as_uri(),
        expected_sha256=digest, expected_size=len(data),
        download_options={"timeout": 12, "max_retries": 4})

    assert bundle.is_dir()
    assert len(calls) == 1
    assert calls[0][1]["raw"] is True
    assert calls[0][1]["timeout"] == 12
    assert calls[0][1]["max_retries"] == 4
    assert calls[0][1]["expected_sha256"] == digest


@pytest.mark.parametrize("bad_member", [
    tarfile.TarInfo("../escape"),
    tarfile.TarInfo("/absolute"),
    tarfile.TarInfo("models/link"),
    tarfile.TarInfo("models/fifo"),
])
def test_unsafe_members_are_rejected_without_publication(tmp_path, bad_member):
    if bad_member.name.endswith("link"):
        bad_member.type = tarfile.SYMTYPE
        bad_member.linkname = "../outside"
    elif bad_member.name.endswith("fifo"):
        bad_member.type = tarfile.FIFOTYPE
    path = make_tar(tmp_path / "bad.tar.bz2", [(bad_member.name, bad_member)])
    outside = tmp_path / "escape"
    destination = tmp_path / "download"

    with pytest.raises(FileValidationError):
        install_archive(destination, path, verified=False)

    assert not outside.exists()
    assert inspect_archive(destination).status == "missing"
    assert not list((destination / "bundles").iterdir())


@pytest.mark.parametrize("members", [
    [("A.txt", b"a"), ("a.txt", b"b")],
    [("models/file", b"a"), ("models/file", b"b")],
    [("models/file", b"a"), ("models/file/child", b"b")],
    [(".datacache-archive-manifest.json", b"forged")],
])
def test_colliding_and_reserved_member_names_are_rejected(tmp_path, members):
    path = make_tar(tmp_path / "bad.tar.bz2", members)
    with pytest.raises(FileValidationError):
        install_archive(tmp_path / "download", path, verified=False)


def test_extra_file_collision_is_not_published(tmp_path, archive_file):
    source, _, _ = archive_file
    destination = tmp_path / "download"
    with pytest.raises(FileValidationError, match="collides"):
        install_archive(
            destination, source, verified=False,
            extra_files={"README.txt": "consumer receipt"})
    assert inspect_archive(destination).status == "missing"


def test_extra_file_casefolded_parent_collision_is_rejected(tmp_path):
    archive = make_tar(tmp_path / "archive.tar.bz2", [("Models/model.json", b"model")])
    with pytest.raises(FileValidationError, match="collides"):
        install_archive(
            tmp_path / "download", archive, verified=False,
            extra_files={"models/DOWNLOAD_INFO.csv": "url\n"})


def test_failed_refresh_preserves_old_bundle(tmp_path, archive_file, monkeypatch):
    old_source, old_data, old_digest = archive_file
    destination = tmp_path / "download"
    old = install_archive(
        destination, old_source, expected_sha256=old_digest,
        expected_size=len(old_data))
    old_bundle = inspect_archive(destination).bundle
    replacement = make_tar(tmp_path / "replacement.tar.bz2", [("new.txt", b"new")])
    replacement_data = replacement.read_bytes()

    def fail(*args, **kwargs):
        raise OSError("injected extraction failure")

    monkeypatch.setattr(archives, "_extract_tar", fail)
    with pytest.raises(OSError, match="injected"):
        install_archive(
            destination, replacement, expected_sha256=sha256(replacement_data).hexdigest(),
            expected_size=len(replacement_data), force=True)

    assert inspect_archive(destination).bundle == old_bundle
    assert inspect_archive(
        destination, old_source, expected_sha256=old_digest,
        expected_size=len(old_data)).status == "available"
    assert (old / "README.txt").is_file()


def test_interrupted_first_extraction_is_hidden_and_retryable(
        tmp_path, archive_file, monkeypatch):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    original = archives._extract_tar

    def interrupt(archive_path, tree, *args, **kwargs):
        (tree / "partial.txt").write_text("not complete")
        raise OSError("interrupted extraction")

    monkeypatch.setattr(archives, "_extract_tar", interrupt)
    with pytest.raises(OSError, match="interrupted extraction"):
        install_archive(
            destination, source, expected_sha256=digest, expected_size=len(data))
    assert inspect_archive(destination).status == "missing"
    assert not list((destination / "bundles").iterdir())

    monkeypatch.setattr(archives, "_extract_tar", original)
    bundle = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    assert (bundle / "models/a/model.json").is_file()


def test_tree_is_installed_once_renamed(tmp_path, archive_file, monkeypatch):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    replace = os.replace

    def interrupt(source_path, target):
        replace(source_path, target)
        if Path(target).parent == destination / "bundles":
            raise KeyboardInterrupt("right after the bundle rename")

    monkeypatch.setattr(os, "replace", interrupt)
    with pytest.raises(KeyboardInterrupt):
        install_archive(
            destination, source, expected_sha256=digest, expected_size=len(data))
    monkeypatch.setattr(os, "replace", replace)
    source.unlink()

    def forbidden(*args, **kwargs):
        raise AssertionError("an installed archive was acquired again")

    monkeypatch.setattr(archives, "_assemble_archive", forbidden)
    bundle = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    assert bundle.is_dir()
    assert inspect_archive(
        destination, source, expected_sha256=digest,
        expected_size=len(data)).verified


def test_failed_manifest_write_leaves_no_apparently_installed_tree(
        tmp_path, archive_file, monkeypatch):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    original = archives.write_json

    def fail(path, value, **kwargs):
        if Path(path).name == archives.MANIFEST:
            raise OSError("receipt publication failed")
        return original(path, value, **kwargs)

    monkeypatch.setattr(archives, "write_json", fail)
    with pytest.raises(OSError, match="receipt publication"):
        install_archive(
            destination, source, expected_sha256=digest, expected_size=len(data))
    assert inspect_archive(destination).status == "missing"
    assert not list((destination / "bundles").iterdir())

    monkeypatch.setattr(archives, "write_json", original)
    bundle = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    assert bundle.is_dir()


def test_unreadable_generated_manifest_is_rejected_before_the_bundle_rename(
        tmp_path, archive_file, monkeypatch):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    original = archives.write_json

    def corrupt(path, value, **kwargs):
        original(path, value, **kwargs)
        if Path(path).name == archives.MANIFEST:
            Path(path).write_text("invalid JSON")

    monkeypatch.setattr(archives, "write_json", corrupt)
    with pytest.raises(ValueError):
        install_archive(destination, source, expected_sha256=digest, expected_size=len(data))
    assert inspect_archive(destination).status == "missing"
    assert not list((destination / "bundles").iterdir())


def test_populated_directories_are_never_claimed_even_with_force(tmp_path, archive_file):
    source, data, digest = archive_file
    destination = tmp_path / "legacy"
    destination.mkdir()
    (destination / "legacy.txt").write_text("keep me")
    for force in (False, True):
        with pytest.raises(FileValidationError, match="never taken over"):
            install_archive(
                destination, source, expected_sha256=digest,
                expected_size=len(data), force=force)
    assert [path.name for path in destination.iterdir()] == ["legacy.txt"]


def test_an_empty_directory_becomes_the_store(tmp_path, archive_file):
    source, data, digest = archive_file
    destination = tmp_path / "precreated"
    destination.mkdir()
    assert inspect_archive(destination).status == "missing"
    bundle = install_archive(destination, source, expected_sha256=digest, expected_size=len(data))
    assert (bundle / "README.txt").is_file()


def test_tree_changes_and_extra_files_are_detected(tmp_path, archive_file):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    bundle = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    (bundle / "README.txt").write_text("changed")
    assert inspect_archive(destination).status == "invalid"
    (bundle / "unexpected.txt").write_text("extra")
    assert inspect_archive(destination).status == "invalid"
    with pytest.raises(FileValidationError, match="force=True"):
        install_archive(
            destination, source, expected_sha256=digest, expected_size=len(data))
    repaired = install_archive(
        destination, source, expected_sha256=digest,
        expected_size=len(data), force=True)
    assert repaired != bundle
    assert inspect_archive(destination).verified is False
    assert inspect_archive(
        destination, source, expected_sha256=digest,
        expected_size=len(data)).verified


def test_links_added_after_installation_are_invalid_and_never_followed(
        tmp_path, archive_file):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    bundle = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    outside = tmp_path / "outside.txt"
    outside.write_text("outside")
    target = bundle / "README.txt"
    target.unlink()
    try:
        target.symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are unavailable")
    assert inspect_archive(destination).status == "invalid"
    assert outside.read_text() == "outside"


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission modes")
@pytest.mark.parametrize("creation_mask", [0o022, 0o002, 0o077])
def test_published_tree_uses_normal_creation_permissions(
        tmp_path, archive_file, creation_mask):
    source, data, digest = archive_file
    previous = os.umask(creation_mask)
    try:
        destination = tmp_path / "download"
        bundle = install_archive(
            destination, source, expected_sha256=digest, expected_size=len(data))
    finally:
        os.umask(previous)
    assert destination.stat().st_mode & 0o777 == 0o777 & ~creation_mask
    assert bundle.stat().st_mode & 0o777 == 0o777 & ~creation_mask
    assert (bundle / "README.txt").stat().st_mode & 0o777 == 0o666 & ~creation_mask
    assert (bundle / archives.MANIFEST).stat().st_mode & 0o777 == 0o666 & ~creation_mask
    assert (destination / bundle_store.MARKER).stat().st_mode & 0o777 == 0o666 & ~creation_mask


def test_concurrent_installers_converge_on_one_bundle(tmp_path, archive_file):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    barrier = threading.Barrier(4)

    def install(_):
        barrier.wait(timeout=5)
        return install_archive(
            destination, source, expected_sha256=digest, expected_size=len(data))

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(install, range(4)))

    assert len(set(results)) == 1
    assert len(list((destination / "bundles").iterdir())) == 1

    with ThreadPoolExecutor(max_workers=2) as pool:
        refreshed = list(pool.map(
            lambda _: install_archive(
                destination, source, expected_sha256=digest,
                expected_size=len(data), force=True),
            range(2)))
    assert len(set(refreshed)) == 2
    assert all(path.is_dir() for path in results + refreshed)
    assert inspect_archive(
        destination, source, expected_sha256=digest,
        expected_size=len(data)).verified


def test_limits_and_verification_requirements_fail_before_publication(tmp_path, archive_file):
    source, data, digest = archive_file
    with pytest.raises(ValueError, match="verified archive"):
        install_archive(tmp_path / "unverified", source)
    assert not (tmp_path / "unverified").exists()
    with pytest.raises(FileValidationError, match="members"):
        install_archive(
            tmp_path / "members", source, expected_sha256=digest,
            expected_size=len(data), max_members=1)
    with pytest.raises(FileValidationError, match="expands"):
        install_archive(
            tmp_path / "size", source, expected_sha256=digest,
            expected_size=len(data), max_extracted_size=1)


def test_member_limit_stops_before_parsing_later_headers(tmp_path):
    # Parsing the deliberately invalid third header must never be necessary.
    source = tmp_path / "many.tar"
    source.write_bytes(
        tarfile.TarInfo("first").tobuf() + tarfile.TarInfo("second").tobuf() +
        b"invalid header".ljust(512, b"\0"))
    with pytest.raises(FileValidationError, match="more than 1 members"):
        install_archive(tmp_path / "download", source, verified=False, max_members=1)


def test_expansion_limit_stops_before_traversing_compressed_member_data(tmp_path):
    member = tarfile.TarInfo("huge")
    member.size = 2 ** 30
    # The advertised payload is absent: scanning to the next header would fail.
    source = tmp_path / "huge.tar.gz"
    source.write_bytes(gzip.compress(member.tobuf()))
    with pytest.raises(FileValidationError, match="expands to more than 1024 bytes"):
        install_archive(
            tmp_path / "download", source, verified=False, max_extracted_size=1024)


def test_resumable_split_downloads_require_part_integrity(tmp_path, archive_file):
    source, data, digest = archive_file
    half = len(data) // 2
    first, second = tmp_path / "one", tmp_path / "two"
    first.write_bytes(data[:half])
    second.write_bytes(data[half:])
    with pytest.raises(ValueError, match="resumable archive parts"):
        install_archive(
            tmp_path / "download", [first.as_uri(), second.as_uri()],
            expected_sha256=digest, expected_size=len(data),
            download_options={"resume": True})


def test_versioned_archive_registry_supports_consumer_layout_and_status(
        tmp_path, archive_file):
    source, data, digest = archive_file
    url = "https://example.test/models.tar.bz2"
    receipt = "url\n%s\n" % url
    paths = []

    def consumer_path(name, version):
        paths.append((name, version))
        return tmp_path / "releases" / version / name

    registry = VersionedArchiveRegistry({
        "models": {
            "default_version": "2.3.0",
            "description": "Presentation models",
            "versions": {
                "2.3.0": {
                    "sources": url,
                    "expected_sha256": digest,
                    "expected_size": len(data),
                    "extra_files": {"DOWNLOAD_INFO.csv": receipt},
                },
                "2.2.0": {
                    "sources": "https://example.test/older.tar.bz2",
                    "expected_sha256": digest,
                    "expected_size": len(data),
                },
            },
        },
    }, store_path=consumer_path)

    assert registry.resolve_version("models") == "2.3.0"
    assert registry.store_path("models") == tmp_path / "releases/2.3.0/models"
    assert not (tmp_path / "releases").exists()
    rows = registry.status()
    assert [(row["version"], row["status"]) for row in rows] == [
        ("2.3.0", "missing"), ("2.2.0", "missing")]
    assert rows[0]["default"] and rows[0]["sources"] == [url]
    assert rows[0]["downloaded_sources"] == []

    bundle = registry.download("models", source_paths=source)
    assert (bundle / "models/a/model.json").is_file()
    assert registry.is_cached("models")
    assert registry.local_path("models") == bundle
    assert registry.inspect("models").verified
    assert not registry.is_cached("models", "2.2.0")
    installed = registry.status("models")[0]
    assert installed["bundle"] == str(bundle)
    assert installed["archive_size"] == len(data)
    assert installed["downloaded_sources"] == [url]
    assert ("models", "2.3.0") in paths

    source.unlink()
    assert registry.local_path("models") == bundle
    with pytest.raises(FileNotFoundError):
        registry.local_path("models", "2.2.0")
    with pytest.raises(ValueError, match="unknown archive"):
        registry.resolve_version("unknown")


def test_versioned_archive_registry_default_layout_and_unverified_history(
        tmp_path, archive_file):
    source, _, _ = archive_file
    registry = VersionedArchiveRegistry({
        "data": {
            "default_version": "historical",
            "versions": {"historical": source},
        },
    }, cache_root=tmp_path / "cache", verified=False)

    assert registry.store_path("data") == tmp_path / "cache/data/historical"
    bundle = registry.ensure("data")
    assert bundle.is_dir()
    assert registry.inspect("data").status == "available"
    assert not registry.inspect("data").verified
