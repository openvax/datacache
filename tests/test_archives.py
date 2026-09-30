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
import io
import json
import os
from pathlib import Path
import tarfile
import threading

import pytest

from datacache import (
    FileValidationError, VersionedArchiveRegistry, inspect_archive,
    install_archive,
)
from datacache import archives, download


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

    generation = install_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt})

    assert (generation / "models/a/model.json").read_text() == '{"model": "a"}\n'
    assert (generation / "DOWNLOAD_INFO.csv").read_text() == receipt
    inspected = inspect_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt})
    assert inspected.status == "available" and inspected.verified
    assert Path(inspected.generation) == generation
    assert inspected.source_urls == ("https://example.test/models.tar.bz2",)
    assert inspected.archive_size == len(data)
    assert inspected.recorded_sha256 == digest
    assert inspected.fetched_at.endswith("+00:00")
    assert inspect_archive(destination).status == "available"
    assert not inspect_archive(destination).verified
    fast = inspect_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt}, verify_files=False)
    assert fast.status == "available" and not fast.verified
    assert fast.files == {} and Path(fast.generation) == generation

    walk_tree = archives._walk_tree
    monkeypatch.setattr(
        archives, "_walk_tree",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("fast inspection hashed the tree")))
    assert inspect_archive(destination, sources, verify_files=False).status == "available"
    monkeypatch.setattr(archives, "_walk_tree", walk_tree)

    refreshed = install_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt}, force=True)
    assert refreshed != generation
    assert (generation / "models/a/model.json").read_text() == '{"model": "a"}\n'

    def forbidden(*args, **kwargs):
        raise AssertionError("a valid cache hit attempted a write, lock, or transfer")

    monkeypatch.setattr(archives, "FileLock", forbidden)
    monkeypatch.setattr(download, "fetch_file", forbidden)
    monkeypatch.setattr(archives, "write_json", forbidden)
    assert install_archive(
        destination, sources, expected_sha256=digest, expected_size=len(data),
        extra_files={"DOWNLOAD_INFO.csv": receipt}) == refreshed


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

    generation = install_archive(
        destination, sources, verified=False,
        extra_files={"DOWNLOAD_INFO.csv": csv_receipt})

    assert (generation / "models/b/model.json").is_file()
    assert (generation / "DOWNLOAD_INFO.csv").read_text() == csv_receipt
    state = inspect_archive(destination, sources, extra_files={"DOWNLOAD_INFO.csv": csv_receipt})
    assert state.status == "available" and not state.verified
    manifest = json.loads((generation / archives.MANIFEST).read_text())
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
    generation = install_archive(destination, sources)
    mirrored = [dict(source, url=source["url"].replace("one", "two"))
                for source in sources]

    state = inspect_archive(destination, mirrored)
    assert state.status == "available" and state.verified
    assert install_archive(destination, mirrored) == generation


def test_url_parts_use_raw_fetch_and_forward_download_options(tmp_path, archive_file, monkeypatch):
    source, data, digest = archive_file
    calls = []
    original = download.fetch_file

    def recording(url, **options):
        calls.append((url, dict(options)))
        return original(url, **options)

    monkeypatch.setattr(download, "fetch_file", recording)
    generation = install_archive(
        tmp_path / "download", source.as_uri(),
        expected_sha256=digest, expected_size=len(data),
        download_options={"timeout": 12, "max_retries": 4})

    assert generation.is_dir()
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
    assert not list((destination / "generations").iterdir())


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


def test_failed_refresh_preserves_old_generation(tmp_path, archive_file, monkeypatch):
    old_source, old_data, old_digest = archive_file
    destination = tmp_path / "download"
    old = install_archive(
        destination, old_source, expected_sha256=old_digest,
        expected_size=len(old_data))
    pointer = (destination / archives.CURRENT).read_bytes()
    replacement = make_tar(tmp_path / "replacement.tar.bz2", [("new.txt", b"new")])
    replacement_data = replacement.read_bytes()

    def fail(*args, **kwargs):
        raise OSError("injected extraction failure")

    monkeypatch.setattr(archives, "_extract_tar", fail)
    with pytest.raises(OSError, match="injected"):
        install_archive(
            destination, replacement, expected_sha256=sha256(replacement_data).hexdigest(),
            expected_size=len(replacement_data), force=True)

    assert (destination / archives.CURRENT).read_bytes() == pointer
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
    assert not list((destination / "generations").iterdir())

    monkeypatch.setattr(archives, "_extract_tar", original)
    generation = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    assert (generation / "models/a/model.json").is_file()


def test_interrupted_pointer_publication_recovers_without_source(tmp_path, archive_file, monkeypatch):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    original = archives.write_json

    def interrupt(path, value, **kwargs):
        if Path(path).name == archives.CURRENT:
            raise KeyboardInterrupt("after generation rename")
        return original(path, value, **kwargs)

    monkeypatch.setattr(archives, "write_json", interrupt)
    with pytest.raises(KeyboardInterrupt):
        install_archive(
            destination, source, expected_sha256=digest, expected_size=len(data))
    assert inspect_archive(
        destination, source, expected_sha256=digest,
        expected_size=len(data)).status == "recovery-required"

    monkeypatch.setattr(archives, "write_json", original)
    source.unlink()

    def forbidden(*args, **kwargs):
        raise AssertionError("recovery attempted to reacquire the archive")

    monkeypatch.setattr(archives, "_assemble_archive", forbidden)
    generation = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    assert generation.is_dir()
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
    assert not list((destination / "generations").iterdir())

    monkeypatch.setattr(archives, "write_json", original)
    generation = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    assert generation.is_dir()


def test_foreign_directories_are_never_claimed_even_with_force(tmp_path, archive_file):
    source, data, digest = archive_file
    for name, contents in (("empty", None), ("legacy", "keep me")):
        destination = tmp_path / name
        destination.mkdir()
        if contents:
            (destination / "legacy.txt").write_text(contents)
        with pytest.raises((FileNotFoundError, FileValidationError)):
            install_archive(
                destination, source, expected_sha256=digest,
                expected_size=len(data), force=True)
        assert not contents or (destination / "legacy.txt").read_text() == contents


def test_tree_changes_and_extra_files_are_detected(tmp_path, archive_file):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    generation = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    (generation / "README.txt").write_text("changed")
    assert inspect_archive(destination).status == "invalid"
    (generation / "unexpected.txt").write_text("extra")
    assert inspect_archive(destination).status == "invalid"
    with pytest.raises(FileValidationError, match="force=True"):
        install_archive(
            destination, source, expected_sha256=digest, expected_size=len(data))
    repaired = install_archive(
        destination, source, expected_sha256=digest,
        expected_size=len(data), force=True)
    assert repaired != generation
    assert inspect_archive(destination).verified is False
    assert inspect_archive(
        destination, source, expected_sha256=digest,
        expected_size=len(data)).verified


def test_links_added_after_installation_are_invalid_and_never_followed(
        tmp_path, archive_file):
    source, data, digest = archive_file
    destination = tmp_path / "download"
    generation = install_archive(
        destination, source, expected_sha256=digest, expected_size=len(data))
    outside = tmp_path / "outside.txt"
    outside.write_text("outside")
    target = generation / "README.txt"
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
        generation = install_archive(
            destination, source, expected_sha256=digest, expected_size=len(data))
    finally:
        os.umask(previous)
    assert destination.stat().st_mode & 0o777 == 0o777 & ~creation_mask
    assert generation.stat().st_mode & 0o777 == 0o777 & ~creation_mask
    assert (generation / "README.txt").stat().st_mode & 0o777 == 0o666 & ~creation_mask
    assert (generation / archives.MANIFEST).stat().st_mode & 0o777 == 0o666 & ~creation_mask
    assert (destination / archives.CURRENT).stat().st_mode & 0o777 == 0o666 & ~creation_mask


def test_concurrent_installers_converge_on_one_generation(tmp_path, archive_file):
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
    assert len(list((destination / "generations").iterdir())) == 1

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

    generation = registry.download("models", source_paths=source)
    assert (generation / "models/a/model.json").is_file()
    assert registry.is_cached("models")
    assert registry.local_path("models") == generation
    assert registry.inspect("models").verified
    assert not registry.is_cached("models", "2.2.0")
    installed = registry.status("models")[0]
    assert installed["generation"] == str(generation)
    assert installed["archive_size"] == len(data)
    assert installed["downloaded_sources"] == [url]
    assert ("models", "2.3.0") in paths

    source.unlink()
    assert registry.local_path("models") == generation
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
    generation = registry.ensure("data")
    assert generation.is_dir()
    assert registry.inspect("data").status == "available"
    assert not registry.inspect("data").verified
