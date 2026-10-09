"""Listing and pruning bundles (#91), and local or predownloaded bundle files (#94)."""

import gzip
from hashlib import sha256
import io
from pathlib import Path
import tarfile

import pytest

from datacache import (
    FileValidationError, VersionedArchiveRegistry, VersionedDatasetRegistry, inspect_bundle, install_bundle, list_bundles, materialize, prune_bundles,
)
from datacache import download

REMOTE = "https://data.example.invalid/release-1/genes.gtf"


def describe(data):
    return {"sha256": sha256(data).hexdigest(), "size": len(data)}


@pytest.fixture
def local_file(tmp_path):
    path = tmp_path / "genes.gtf"
    path.write_bytes(b"gene\n")
    return path


@pytest.fixture
def never_downloads(monkeypatch):
    """Fail if anything but a local file:// source is fetched."""
    fetch_file = download.fetch_file

    def local_only(url, *args, **kwargs):
        assert url.startswith("file:"), "downloaded %s" % url
        return fetch_file(url, *args, **kwargs)

    monkeypatch.setattr(download, "fetch_file", local_only)


def test_bundles_are_listed_oldest_first_and_pruned_to_the_newest(tmp_path, local_file):
    store = tmp_path / "store"
    assets = {"genes.gtf": dict(describe(b"gene\n"), path=local_file)}
    assert list_bundles(store) == [] and prune_bundles(store) == []
    first = Path(install_bundle(store, assets)["genes.gtf"]).parent
    second = Path(install_bundle(store, assets, force=True)["genes.gtf"]).parent
    third = Path(install_bundle(store, assets, force=True)["genes.gtf"]).parent
    assert list_bundles(store) == [first, second, third]
    assert Path(inspect_bundle(store).bundle) == third

    assert prune_bundles(store, keep=2) == [first]
    assert prune_bundles(store) == [second]
    assert prune_bundles(store) == []
    assert list_bundles(store) == [third] and inspect_bundle(store, assets).verified
    for keep in (0, -1, True, 1.5):
        with pytest.raises(ValueError, match="current bundle is always kept"):
            prune_bundles(store, keep=keep)


def test_every_kind_of_store_can_be_pruned(tmp_path, local_file):
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as tar:
        info = tarfile.TarInfo("README")
        info.size = 5
        tar.addfile(info, io.BytesIO(b"hello"))
    tar_path = tmp_path / "data.tar"
    tar_path.write_bytes(archive.getvalue())
    registry = VersionedArchiveRegistry(
        {"data": {"default_version": "v1", "versions": {"v1": {"path": tar_path, **describe(tar_path.read_bytes())}}}},
        cache_root=tmp_path / "archives")
    old = registry.download("data")
    new = registry.download("data", force=True)
    assert registry.prune("data") == [old] and list_bundles(registry.store_path("data")) == [new]

    options = dict(sources={"in": {"path": local_file}}, transform={"version": "1"}, outputs={"out": {}},
                   builder=lambda inputs, outputs: Path(outputs["out"]).write_bytes(b"x"),
                   download_options={"show_progress": False})
    store = tmp_path / "derived"
    old = Path(materialize(store, **options)["out"]).parent
    new = Path(materialize(store, force=True, **options)["out"]).parent
    assert prune_bundles(store) == [old] and list_bundles(store) == [new]

    datasets = VersionedDatasetRegistry(
        {"genes": {"default_version": "v1", "versions": {"v1": {"genes.gtf": dict(describe(b"gene\n"), path=local_file)}}}},
        cache_root=tmp_path / "datasets")
    old = datasets.ensure("genes").parent
    datasets.download("genes", force=True)
    assert datasets.prune("genes") == [old]


def test_only_datacache_stores_are_pruned(tmp_path):
    foreign = tmp_path / "foreign"
    (foreign / "bundles" / "2026-01-01T00-00-00Z").mkdir(parents=True)
    (foreign / "keep.txt").write_text("keep")
    with pytest.raises(FileValidationError, match="not a datacache store"):
        prune_bundles(foreign)
    assert (foreign / "bundles" / "2026-01-01T00-00-00Z").is_dir()


def test_predownloaded_files_keep_the_declared_source(tmp_path, local_file, never_downloads):
    assets = {"genes.gtf": dict(describe(b"gene\n"), url=REMOTE)}
    store = tmp_path / "store"
    paths = install_bundle(store, assets, source_paths={"genes.gtf": local_file})
    assert Path(paths["genes.gtf"]).read_bytes() == b"gene\n"
    # Recorded as if downloaded, so later installs reuse it without the file.
    local_file.unlink()
    assert install_bundle(store, assets) == paths
    assert inspect_bundle(store, assets).verified


def test_predownloaded_files_without_trusted_hashes(tmp_path, local_file, never_downloads):
    registry = VersionedDatasetRegistry(
        {"genes": {"default_version": "v1", "versions": {"v1": {"genes.gtf": {"url": REMOTE}}}}},
        cache_root=tmp_path / "cache", verified=False)
    path = registry.ensure("genes", source_paths={"genes.gtf": local_file})
    assert path.read_bytes() == b"gene\n"
    # The declared URL's identity was recorded, so this is a cache hit.
    assert registry.download("genes")["genes.gtf"] == str(path)


def test_predownloaded_archives_are_decompressed(tmp_path, never_downloads):
    compressed = tmp_path / "genes.gtf.gz"
    compressed.write_bytes(gzip.compress(b"gene\n", mtime=0))
    assets = {"genes.gtf": dict(describe(b"gene\n"), url=REMOTE + ".gz", decompress=True)}
    paths = install_bundle(tmp_path / "store", assets, source_paths={"genes.gtf": compressed})
    assert Path(paths["genes.gtf"]).read_bytes() == b"gene\n"


@pytest.mark.parametrize("source_paths, message", [
    ({"other.gtf": "x"}, "unknown assets"),
    ({"genes.gtf": 7}, "must be a path"),
    ([], "must map asset names"),
])
def test_invalid_source_paths_are_rejected_before_anything_is_created(tmp_path, source_paths, message):
    assets = {"genes.gtf": dict(describe(b"gene\n"), url=REMOTE)}
    with pytest.raises(ValueError, match=message):
        install_bundle(tmp_path / "store", assets, source_paths=source_paths)
    assets["genes.gtf"]["decompress"] = True
    with pytest.raises(ValueError, match=r"\.gz or \.zip"):
        install_bundle(tmp_path / "store", assets, source_paths={"genes.gtf": tmp_path / "genes.gtf"})
    with pytest.raises(ValueError, match="exactly one of url or path"):
        install_bundle(tmp_path / "store", {"genes.gtf": {"url": REMOTE, "path": "x"}}, verified=False)
    assert not (tmp_path / "store").exists()


def test_stores_are_recognized_only_by_a_known_marker(tmp_path):
    from datacache.bundle_store import MARKER
    (tmp_path / "empty").mkdir()
    assert list_bundles(tmp_path / "empty") == [] and prune_bundles(tmp_path / "empty") == []
    (tmp_path / "odd").mkdir()
    (tmp_path / "odd" / MARKER).write_text('{"format": 2, "kind": "something-else"}')
    with pytest.raises(FileValidationError, match="not a datacache store"):
        list_bundles(tmp_path / "odd")


def test_users_are_told_apart_without_numeric_ids(monkeypatch):
    import getpass
    import os
    from datacache.bundle_store import user_key
    monkeypatch.delattr(os, "getuid", raising=False)  # As on Windows.
    assert user_key() == getpass.getuser()


def test_local_files_need_no_resume_support(tmp_path, local_file, never_downloads):
    assets = {"genes.gtf": dict(describe(b"gene\n"), url=REMOTE)}
    paths = install_bundle(tmp_path / "store", assets, download_options={"resume": True},
                           source_paths={"genes.gtf": local_file})
    assert Path(paths["genes.gtf"]).read_bytes() == b"gene\n"
