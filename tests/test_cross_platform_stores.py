"""Bundle and materialization installs that also run on Windows CI (#89)."""

from hashlib import sha256
from pathlib import Path

from datacache import (
    VersionedDatasetRegistry, inspect_bundle, inspect_materialization, install_bundle, materialize,
)


def asset(path, data):
    path.write_bytes(data)
    return {"url": path.as_uri(), "sha256": sha256(data).hexdigest(), "size": len(data)}


def test_bundles_install_reuse_and_refresh(tmp_path):
    upstream = tmp_path / "upstream"
    upstream.mkdir()
    assets = {"genes.gtf": asset(upstream / "genes.gtf", b"gene\r\n"),
              "dna/chr1.fa": asset(upstream / "chr1.fa", b">chr1\nACGT\n")}
    store = tmp_path / "store"
    paths = install_bundle(store, assets)
    assert Path(paths["genes.gtf"]).read_bytes() == b"gene\r\n"
    assert inspect_bundle(store, assets).verified
    assert install_bundle(store, assets) == paths
    refreshed = install_bundle(store, assets, force=True)
    assert refreshed != paths and all(Path(path).is_file() for path in paths.values())

    registry = VersionedDatasetRegistry(
        {"reference": {"default_version": "v1", "versions": {"v1": assets}}}, cache_root=tmp_path / "cache")
    bundle = registry.ensure("reference")
    assert registry.local_path("reference", asset="genes.gtf") == bundle / "genes.gtf"
    assert registry.is_cached("reference")


def test_materializations_build_and_reuse(tmp_path):
    source = tmp_path / "input.txt"
    source.write_bytes(b"acgt")
    built = []

    def build(inputs, outputs):
        built.append(True)
        Path(outputs["upper.txt"]).write_bytes(Path(inputs["input.txt"]).read_bytes().upper())

    options = dict(sources={"input.txt": {"path": source}}, transform={"version": "upper-1"},
                   outputs={"upper.txt": {}}, builder=build, download_options={"show_progress": False})
    store = tmp_path / "derived"
    paths = materialize(store, **options)
    assert Path(paths["upper.txt"]).read_bytes() == b"ACGT"
    assert materialize(store, **options) == paths and built == [True]
    assert inspect_materialization(store).status == "available"
    assert materialize(store, force=True, **options) != paths
