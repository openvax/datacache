"""Fixed-path single-file registry for applications with established caches.

Unlike generation bundles, this preserves legacy files and a root manifest.
The caller owns trusted dataset definitions; writers serialize per root. Use bundles for transactional multi-file data.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from filelock import FileLock

from ._filesystem import write_json
from .download import fetch_file


class VersionedFileRegistry:
    """Download + cache for versioned, version-pinned external datasets.

    Parameters
    ----------
    datasets
        Mapping of ``name -> spec`` where each spec has::

            {
                "filename": "local_name.tsv",      # name on disk (post-decompress)
                "urls": {"v23": "https://...zip", "latest": "https://..."},
                "default_version": "v23",          # used when caller passes version=None
                "description": "...",              # optional, for status()
            }

    cache_dir
        Zero-arg callable returning the cache root :class:`~pathlib.Path`
        (created on demand by the caller). The on-disk layout is
        ``<cache>/<name>/<version>/<filename>`` plus a ``<cache>/manifest.json``
        provenance file.
    error_cls
        Exception type raised for unknown datasets/versions and download
        failures. Defaults to :class:`RuntimeError`; consumers may pass
        their own subclass to preserve their public error type.
    """

    def __init__(self, datasets, *, cache_dir, error_cls=RuntimeError):
        self._datasets = datasets
        self._cache_dir = cache_dir
        self._error_cls = error_cls

    # -- dataset / version resolution --

    def _dataset(self, name: str) -> dict:
        try:
            return self._datasets[name]
        except KeyError:
            known = ", ".join(sorted(self._datasets))
            raise self._error_cls(f"unknown dataset {name!r}; known: {known}") from None

    def resolve_version(self, name: str, version: str | None = None) -> str:
        """Return the concrete version for *name*, applying its default."""
        spec = self._dataset(name)
        if version is None:
            version = spec["default_version"]
        if version not in spec["urls"]:
            avail = ", ".join(sorted(spec["urls"]))
            raise self._error_cls(f"{name!r} has no version {version!r}; available: {avail}")
        return version

    # -- cache paths / manifest --

    def _manifest_path(self) -> Path:
        return Path(self._cache_dir()) / "manifest.json"

    @staticmethod
    def _read_manifest_at(path: Path) -> dict:
        try:
            manifest = json.loads(path.read_text())
            return manifest if isinstance(manifest, dict) else {}
        except (json.JSONDecodeError, OSError):
            return {}

    def local_path(self, name: str, version: str | None = None) -> Path:
        """Expected cache path for *name*/*version* (may not exist yet)."""
        version = self.resolve_version(name, version)
        spec = self._dataset(name)
        return Path(self._cache_dir()) / name / version / spec["filename"]

    def is_cached(self, name: str, version: str | None = None) -> bool:
        return self.local_path(name, version).exists()

    # -- fetch --

    def download(
        self, name: str, version: str | None = None, *, force: bool = False, **download_options
    ) -> Path:
        """Fetch a fixed path, serializing writers and forwarding fetch_file options.

        Ordinary cache hits neither hash nor write. Explicit integrity options
        validate cache hits. New bytes and the root receipt publish separately.
        """
        version = self.resolve_version(name, version)
        spec = self._dataset(name)
        root = Path(self._cache_dir())
        dest = root / name / version / spec["filename"]
        url = spec["urls"][version]

        def reuse():
            if force or not dest.exists():
                return False
            if any(download_options.get(key) is not None
                   for key in ('expected_sha256', 'expected_size')):
                acquire()
            return True

        def acquire():
            try:
                fetch_file(url, destination=dest, force=force, **download_options)
            except Exception as error:
                raise self._error_cls(f"failed to download {name} ({url}): {error}") from error

        if reuse():
            return dest
        root.mkdir(parents=True, exist_ok=True)
        with FileLock(str(root / '.datacache-file-registry.lock')):
            if reuse():
                return dest
            acquire()
            digest = hashlib.sha256()
            with dest.open("rb") as handle:
                for chunk in iter(lambda: handle.read(2 ** 20), b""):
                    digest.update(chunk)
            manifest_path = root / 'manifest.json'
            manifest = self._read_manifest_at(manifest_path)
            manifest[name] = {
                "version": version, "url": url, "path": str(dest),
                "bytes": dest.stat().st_size, "sha256": digest.hexdigest(),
                "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            write_json(manifest_path, manifest)
        return dest

    def ensure(self, name: str, version: str | None = None, **download_options) -> Path:
        """Return a local path to *name*/*version*, downloading if absent."""
        return self.download(name, version, **download_options)

    def status(self) -> list[dict]:
        """Return one status row per dataset (for a ``... list`` CLI command)."""
        root = Path(self._cache_dir())
        manifest = self._read_manifest_at(root / "manifest.json")
        rows = []
        for name, spec in sorted(self._datasets.items()):
            default_v = spec["default_version"]
            path = root / name / default_v / spec["filename"]
            record = manifest.get(name, {})
            rows.append(
                {
                    "name": name,
                    "description": spec.get("description", ""),
                    "default_version": default_v,
                    "available_versions": sorted(spec["urls"]),
                    "cached": path.exists(),
                    "cached_version": record.get("version") if record else None,
                    "bytes": record.get("bytes") if path.exists() else None,
                    "downloaded_at": record.get("downloaded_at") if path.exists() else None,
                    "path": str(path),
                }
            )
        return rows
