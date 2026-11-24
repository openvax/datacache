"""Tests for shared cache resolution, global offline mode, progress policy, and CLI."""

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

import datacache
from datacache import (
    NO_PROGRESS_ENV_KEYS,
    OFFLINE_ENV_KEYS,
    SHARED_CACHE_ENV_KEY,
    Cache,
    CacheLocation,
    OfflineError,
    fetch_bytes,
    fetch_file,
    format_bytes,
    install_bundle,
    install_zip_member,
    is_offline,
    offline_source,
    require_online,
    resolve_cache_dir,
    should_show_progress,
)
from datacache import cli, download


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in (
        SHARED_CACHE_ENV_KEY,
        *OFFLINE_ENV_KEYS,
        *NO_PROGRESS_ENV_KEYS,
        "PYENSEMBL_CACHE_DIR",
        "ORFDATA_CACHE",
        "WEIRDO_DATA_DIR",
        "NO_COLOR",
    ):
        monkeypatch.delenv(key, raising=False)


class TestResolveCacheDir:
    def test_precedence_override_env_shared_platform_legacy_default(self, tmp_path, monkeypatch):
        platform_root = tmp_path / "platform"
        monkeypatch.setattr(datacache.common, "_platform_cache_dir", lambda name: str(platform_root / name))

        legacy = tmp_path / "legacy_pkg"
        override = tmp_path / "override_pkg"
        env_dir = tmp_path / "env_pkg"
        shared_dir = tmp_path / "shared_root"

        # 6. Default when nothing exists (creates nothing on disk)
        loc = resolve_cache_dir("demo", "DEMO_DIR", legacy=(legacy,))
        assert loc == CacheLocation(str(platform_root / "openvax" / "demo"), "default", "default")
        assert not Path(loc.path).exists()

        # 5. Legacy wins when populated and default platform dir is empty
        legacy.mkdir()
        (legacy / "data.bin").write_bytes(b"old")
        loc = resolve_cache_dir("demo", "DEMO_DIR", legacy=(legacy,))
        assert loc == CacheLocation(str(legacy), "legacy", "legacy cache")

        # 4. Populated default platform dir wins over populated legacy
        default_dir = platform_root / "openvax" / "demo"
        default_dir.mkdir(parents=True)
        (default_dir / "new.bin").write_bytes(b"new")
        loc = resolve_cache_dir("demo", "DEMO_DIR", legacy=(legacy,))
        assert loc == CacheLocation(str(default_dir), "platform", "shared platform cache")

        # 3. OPENVAX_DATA_CACHE wins over platform & legacy
        monkeypatch.setenv(SHARED_CACHE_ENV_KEY, str(shared_dir))
        loc = resolve_cache_dir("demo", "DEMO_DIR", legacy=(legacy,))
        assert loc == CacheLocation(str(shared_dir / "demo"), "shared_env", SHARED_CACHE_ENV_KEY)

        # 2. Package-specific env var wins over OPENVAX_DATA_CACHE
        monkeypatch.setenv("DEMO_DIR", str(env_dir))
        loc = resolve_cache_dir("demo", "DEMO_DIR", legacy=(legacy,))
        assert loc == CacheLocation(str(env_dir), "env", "DEMO_DIR")

        # With append_package_to_env=True (pyensembl compatibility)
        loc = resolve_cache_dir("demo", "DEMO_DIR", append_package_to_env=True)
        assert loc == CacheLocation(str(env_dir / "demo"), "env", "DEMO_DIR")

        # 1. Explicit override wins over everything
        loc = resolve_cache_dir("demo", "DEMO_DIR", override=override, legacy=(legacy,))
        assert loc == CacheLocation(str(override), "override", "explicit override")

    def test_shared_subdir_none_uses_package_platform_dir(self, tmp_path, monkeypatch):
        platform_root = tmp_path / "platform"
        monkeypatch.setattr(datacache.common, "_platform_cache_dir", lambda name: str(platform_root / name))
        loc = resolve_cache_dir("standalone", shared_subdir=None)
        assert loc == CacheLocation(str(platform_root / "standalone"), "default", "default")

    def test_validation_errors(self):
        with pytest.raises(ValueError):
            resolve_cache_dir("")
        with pytest.raises(ValueError):
            resolve_cache_dir("demo", legacy=("relative/path",))
        with pytest.raises(ValueError):
            resolve_cache_dir("demo", append_package_to_env="yes")


class TestFormatBytes:
    def test_units(self):
        assert format_bytes(0) == "0 B"
        assert format_bytes(512) == "512 B"
        assert format_bytes(1024) == "1.0 KiB"
        assert format_bytes(1536) == "1.5 KiB"
        assert format_bytes(5 * 1024 ** 2) == "5.0 MiB"
        assert format_bytes(2 * 1024 ** 3) == "2.0 GiB"

    def test_invalid(self):
        with pytest.raises(ValueError):
            format_bytes(-1)
        with pytest.raises(ValueError):
            format_bytes(True)


class TestOfflineMode:
    def test_env_precedence_and_validation(self, monkeypatch):
        assert not is_offline()
        assert offline_source() is None

        monkeypatch.setenv("OPENVAX_OFFLINE", "1")
        assert is_offline()
        assert offline_source() == "OPENVAX_OFFLINE=1"

        # DATACACHE_OFFLINE=0 overrides OPENVAX_OFFLINE=1
        monkeypatch.setenv("DATACACHE_OFFLINE", "0")
        assert not is_offline()

        # Explicit kwarg overrides env
        assert is_offline(offline=True)
        monkeypatch.setenv("DATACACHE_OFFLINE", "true")
        assert is_offline()
        assert not is_offline(offline=False)

        monkeypatch.setenv("DATACACHE_OFFLINE", "maybe")
        with pytest.raises(ValueError, match="DATACACHE_OFFLINE"):
            is_offline()

    def test_require_online_allows_file_urls_and_blocks_remote(self, monkeypatch):
        monkeypatch.setenv("OPENVAX_OFFLINE", "1")
        require_online("file:///tmp/local.txt")
        with pytest.raises(OfflineError, match="OPENVAX_OFFLINE=1"):
            require_online("https://example.org/data.gz")
        with pytest.raises(OfflineError):
            require_online("ftp://ftp.ensembl.org/pub/file.gz")

    def test_fetch_file_blocks_miss_without_mkdir_or_retries(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OPENVAX_OFFLINE", "1")
        calls = []
        monkeypatch.setattr(
            download.requests, "get",
            lambda *a, **kw: calls.append((a, kw)),
        )
        dest = tmp_path / "nested" / "file.bin"
        with pytest.raises(OfflineError):
            fetch_file("https://example.org/file.bin", destination=dest, retry_backoff=0)
        assert not calls
        assert not dest.parent.exists()

    def test_fetch_file_cache_hit_and_stale_fallback_work_offline(self, tmp_path, monkeypatch):
        dest = tmp_path / "cached.txt"
        dest.write_bytes(b"hello world")
        monkeypatch.setenv("DATACACHE_OFFLINE", "1")

        # Direct cache hit succeeds offline
        assert fetch_file("https://example.org/cached.txt", destination=dest) == str(dest)

        # Expired cache with return_stale_on_error=True falls back to cached copy offline
        assert fetch_file(
            "https://example.org/cached.txt",
            destination=dest,
            expire_after=0,
            return_stale_on_error=True,
        ) == str(dest)

        # Without return_stale_on_error, expired cache raises OfflineError
        with pytest.raises(OfflineError):
            fetch_file("https://example.org/cached.txt", destination=dest, expire_after=0)

    def test_fetch_file_local_file_url_allowed_offline(self, tmp_path, monkeypatch):
        src = tmp_path / "src.txt"
        src.write_bytes(b"local bytes")
        dest = tmp_path / "dest.txt"
        monkeypatch.setenv("OPENVAX_OFFLINE", "1")
        out = fetch_file(src.as_uri(), destination=dest)
        assert Path(out).read_bytes() == b"local bytes"

    def test_fetch_bytes_and_cache_and_zip_member_offline(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OPENVAX_OFFLINE", "1")
        with pytest.raises(OfflineError):
            fetch_bytes("https://example.org/meta.json")
        with pytest.raises(OfflineError):
            Cache(cache_root=tmp_path / "c").fetch("https://example.org/a.bin")
        with pytest.raises(OfflineError):
            install_zip_member(tmp_path / "zipstore", "https://example.org/a.zip", "member.txt")
        assert not (tmp_path / "zipstore").exists()

    def test_fetch_file_offline_kwarg_overrides_env(self, tmp_path):
        with pytest.raises(OfflineError):
            fetch_file("https://example.org/x", destination=tmp_path / "x", offline=True)
        with pytest.raises(ValueError, match="offline"):
            fetch_file("https://example.org/x", destination=tmp_path / "x", offline="yes")


class TestProgressPolicy:
    def test_env_disables_progress(self, monkeypatch):
        assert should_show_progress(True) is True
        monkeypatch.setenv("OPENVAX_NO_PROGRESS", "1")
        assert should_show_progress(True) is False
        assert should_show_progress("auto") is False
        monkeypatch.setenv("DATACACHE_NO_PROGRESS", "0")
        assert should_show_progress(True) is True

    def test_auto_respects_stream_tty(self):
        class FakeStream(io.StringIO):
            def __init__(self, tty):
                super().__init__()
                self._tty = tty

            def isatty(self):
                return self._tty

        assert should_show_progress("auto", stream=FakeStream(True)) is True
        assert should_show_progress("auto", stream=FakeStream(False)) is False

    def test_fetch_file_accepts_show_progress_auto(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_bytes(b"abc")
        dest = tmp_path / "dst.txt"
        assert fetch_file(src.as_uri(), destination=dest, show_progress="auto") == str(dest)


class TestCLI:
    def test_path_and_dirs_readonly_and_json(self, tmp_path, monkeypatch, capsys):
        shared = tmp_path / "openvax"
        monkeypatch.setenv(SHARED_CACHE_ENV_KEY, str(shared))
        monkeypatch.setenv("OPENVAX_OFFLINE", "1")

        # datacache path
        cli.run(["path"])
        assert capsys.readouterr().out.strip() == str(shared)

        cli.run(["path", "pyensembl"])
        assert capsys.readouterr().out.strip() == str(shared / "pyensembl")

        # datacache dirs --all --json creates nothing
        cli.run(["dirs", "--all", "--json"])
        payload = json.loads(capsys.readouterr().out)
        assert payload["shared_root"] == str(shared)
        assert payload["offline"] is True
        assert payload["offline_source"] == "OPENVAX_OFFLINE=1"
        assert not shared.exists()

        # Populate one package cache and check human table output
        pkg_dir = shared / "orfdata"
        pkg_dir.mkdir(parents=True)
        (pkg_dir / "payload.bin").write_bytes(b"x" * 2048)

        cli.run(["dirs"])
        out = capsys.readouterr().out
        assert "orfdata" in out
        assert "2.0 KiB" in out
        assert "Offline mode:" in out

    def test_scan_and_prune_dry_run_then_apply(self, tmp_path, capsys):
        root = tmp_path / "cache"
        store_dir = root / "pkg" / "dataset_store"

        # Install two generations so one stale generation exists.
        src1 = tmp_path / "v1.txt"
        b1 = b"generation-1" * 100
        src1.write_bytes(b1)
        h1 = hashlib.sha256(b1).hexdigest()
        install_bundle(store_dir, {"data.txt": {"url": src1.as_uri(), "sha256": h1, "size": len(b1)}})

        src2 = tmp_path / "v2.txt"
        b2 = b"generation-2" * 120
        src2.write_bytes(b2)
        h2 = hashlib.sha256(b2).hexdigest()
        install_bundle(
            store_dir,
            {"data.txt": {"url": src2.as_uri(), "sha256": h2, "size": len(b2)}},
            force=True,
        )

        # Scan finds 2 bundles, 1 stale
        cli.run(["scan", str(root), "--json"])
        scan_data = json.loads(capsys.readouterr().out)
        assert len(scan_data["stores"]) == 1
        assert scan_data["stores"][0]["bundles"] == 2
        assert scan_data["stores"][0]["stale_bundles"] == 1
        assert scan_data["reclaimable_bytes"] > 0

        # Prune dry-run (default) removes nothing
        cli.run(["prune", str(root), "--json"])
        dry = json.loads(capsys.readouterr().out)
        assert dry["dry_run"] is True
        assert dry["removed_count"] == 1
        cli.run(["scan", str(root), "--json"])
        assert json.loads(capsys.readouterr().out)["stores"][0]["stale_bundles"] == 1

        # Prune --yes removes the stale generation and keeps current
        cli.run(["prune", str(root), "--yes", "--json"])
        applied = json.loads(capsys.readouterr().out)
        assert applied["dry_run"] is False
        assert applied["removed_count"] == 1

        cli.run(["scan", str(root), "--json"])
        after = json.loads(capsys.readouterr().out)
        assert after["stores"][0]["bundles"] == 1
        assert after["stores"][0]["stale_bundles"] == 0

    def test_dirs_detects_mhcflurry_v4_legacy_and_oncoref_cache_dir(self, tmp_path, monkeypatch, capsys):
        import platformdirs
        import datacache.common as dc_common

        shared_platform = tmp_path / "platform-openvax"
        legacy_mhcflurry_v4 = tmp_path / "data" / "mhcflurry" / "4"
        legacy_mhcflurry_v4.mkdir(parents=True)
        (legacy_mhcflurry_v4 / "weights.bin").write_bytes(b"123456")

        oncoref_override = tmp_path / "custom-oncoref"
        oncoref_override.mkdir()
        (oncoref_override / "bundle.dat").write_bytes(b"abc")

        monkeypatch.delenv("OPENVAX_DATA_CACHE", raising=False)
        monkeypatch.delenv("MHCFLURRY_DATA_DIR", raising=False)
        monkeypatch.delenv("MHCFLURRY_DOWNLOADS_DIR", raising=False)
        monkeypatch.setenv("ONCOREF_CACHE_DIR", str(oncoref_override))
        monkeypatch.setattr(
            dc_common,
            "_platform_cache_dir",
            lambda app: str(shared_platform if app == "openvax" else tmp_path / "cache" / app),
        )
        monkeypatch.setattr(
            platformdirs,
            "user_data_dir",
            lambda app, version=None, **kw: str(
                tmp_path / "data" / app / version if version else tmp_path / "data" / app
            ),
        )

        cli.run(["dirs", "--all", "--json"])
        payload = json.loads(capsys.readouterr().out)
        by_pkg = {row["package"]: row for row in payload["caches"]}

        assert by_pkg["mhcflurry"]["path"] == str(legacy_mhcflurry_v4)
        assert by_pkg["mhcflurry"]["origin"] == "legacy"
        assert by_pkg["mhcflurry"]["size_bytes"] == 6

        assert by_pkg["oncoref"]["path"] == str(oncoref_override)
        assert by_pkg["oncoref"]["origin"] == "env"
        assert by_pkg["oncoref"]["detail"] == "ONCOREF_CACHE_DIR"
        assert by_pkg["oncoref"]["size_bytes"] == 3


def test_import_stays_lazy():
    code = (
        "import sys, datacache; "
        "heavy = [m for m in ('requests', 'tqdm', 'pandas', 'datacache.cli') if m in sys.modules]; "
        "assert not heavy, heavy"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
