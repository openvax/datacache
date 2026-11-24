# PYTHON_ARGCOMPLETE_OK
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

"""Friendly cache inspection and maintenance CLI for datacache / OpenVax caches.

Keep module-level imports stdlib-only so ``datacache --help`` and tab completion
stay instantaneous.
"""

import argparse
import json
import os
from pathlib import Path
import sys

from .version import __version__


# Known OpenVax/PIRL packages and their historical environment variables / legacy
# cache locations, used by ``datacache dirs`` to give users one coherent overview.
_KNOWN_PACKAGES = (
    ("pyensembl", ("PYENSEMBL_CACHE_DIR",), True, ("platform:pyensembl",)),
    ("orfdata", ("ORFDATA_CACHE",), False, ()),
    ("osteosarc", ("OSTEOSARC_CACHE",), False, ()),
    ("hitlist", ("HITLIST_DATA_DIR",), False, ("platform:hitlist", "~/.hitlist")),
    ("oncoref", (
        "ONCOREF_CACHE_DIR",
        "CANCERDATA_DATA_DIR",
        "CANCERDATA_BUNDLED_DATA",
        "ONCOREF_ASSET_CACHE",
        "PIRLYGENES_BUNDLED_DATA",
    ), False, ("platform:oncoref", "~/.cache/oncoref", "~/.cache/pirlygenes")),
    ("pirlygenes", ("PIRLYGENES_CACHE",), False, ("~/.cache/pirlygenes",)),
    ("vaxrank", ("VAXRANK_REF_PEPTIDES_DIR",), False, ("platform:vaxrank",)),
    ("weirdo", ("WEIRDO_DATA_DIR",), False, ("~/.weirdo", "platform:weirdo")),
    ("mhcflurry", ("MHCFLURRY_DATA_DIR", "MHCFLURRY_DOWNLOADS_DIR"), False,
     ("data:mhcflurry:4", "data:mhcflurry")),
    ("mhcseqs", ("MHCSEQS_DATA",), False, ("~/.cache/mhcseqs",)),
    ("pepdata", (), False, ("platform:pepdata",)),
)


def _use_color(stream=None):
    target = sys.stdout if stream is None else stream
    if os.environ.get("NO_COLOR"):
        return False
    return getattr(target, "isatty", lambda: False)()


def _display_path(path):
    home = os.path.expanduser("~")
    text = os.fspath(path)
    if text == home or text.startswith(home + os.sep):
        return "~" + text[len(home):]
    return text


def _format_table(headers, rows, *, use_color=None):
    if use_color is None:
        use_color = _use_color()
    str_rows = [[str(cell) for cell in row] for row in rows]
    widths = [max(len(h), *(len(r[i]) for r in str_rows)) if str_rows else len(h)
              for i, h in enumerate(headers)]
    bold, reset = ("\x1b[1m", "\x1b[0m") if use_color else ("", "")

    def fmt(cells):
        return "  ".join(cell.ljust(w) for cell, w in zip(cells, widths)).rstrip()

    lines = [bold + fmt(headers) + reset]
    lines.extend(fmt(row) for row in str_rows)
    return "\n".join(lines)


def _dir_stats(path):
    """Return (exists, total_bytes, file_count) without following symlinks or mutating disk."""
    root = Path(path)
    try:
        if not root.exists():
            return False, 0, 0
    except OSError:
        return False, 0, 0
    total_bytes = 0
    file_count = 0
    for dirpath, _dirnames, filenames in os.walk(root, followlinks=False):
        for name in filenames:
            try:
                st = os.lstat(os.path.join(dirpath, name))
            except OSError:
                continue
            total_bytes += st.st_size
            file_count += 1
    return True, total_bytes, file_count


def _expand_legacy_specs(specs):
    from .common import platform_cache_dir

    paths = []
    for spec in specs:
        if spec.startswith("platform:"):
            paths.append(platform_cache_dir(spec.split(":", 1)[1]))
        elif spec.startswith("data:"):
            import platformdirs
            parts = spec.split(":", 2)
            if len(parts) == 3 and parts[2]:
                paths.append(platformdirs.user_data_dir(parts[1], version=parts[2]))
            else:
                paths.append(platformdirs.user_data_dir(parts[1]))
        else:
            paths.append(os.path.expanduser(spec))
    return tuple(paths)


def _shared_root(override=None):
    from .common import SHARED_CACHE_ENV_KEY, get_cache_root
    return get_cache_root("openvax", SHARED_CACHE_ENV_KEY, override=override)


def collect_cache_dirs(*, root_override=None, include_missing=False):
    """Collect resolved cache directories across known OpenVax/PIRL packages (read-only)."""
    from .common import SHARED_CACHE_ENV_KEY, resolve_cache_dir

    rows = []
    seen_paths = set()
    shared = _shared_root(override=root_override)

    for package, envkeys, append_pkg, legacy_specs in _KNOWN_PACKAGES:
        legacy = _expand_legacy_specs(legacy_specs)
        loc = resolve_cache_dir(
            package,
            *envkeys,
            shared_envkey=SHARED_CACHE_ENV_KEY,
            override=os.path.join(root_override, package) if root_override else None,
            legacy=legacy,
            append_package_to_env=append_pkg,
        )
        exists, size_bytes, file_count = _dir_stats(loc.path)
        if not exists and not include_missing:
            continue
        seen_paths.add(os.path.realpath(loc.path))
        rows.append(dict(
            package=package,
            path=loc.path,
            origin=loc.origin,
            detail=loc.detail,
            exists=exists,
            size_bytes=size_bytes,
            file_count=file_count,
        ))

    # Also surface any extra subdirectories sitting inside <shared_root> (e.g. objects/).
    if os.path.isdir(shared):
        try:
            entries = sorted(os.scandir(shared), key=lambda e: e.name)
        except OSError:
            entries = []
        for entry in entries:
            if entry.name.startswith(".") or not entry.is_dir(follow_symlinks=False):
                continue
            real = os.path.realpath(entry.path)
            if real in seen_paths:
                continue
            exists, size_bytes, file_count = _dir_stats(entry.path)
            if not exists and not include_missing:
                continue
            seen_paths.add(real)
            rows.append(dict(
                package=entry.name,
                path=entry.path,
                origin="shared_root",
                detail="inside shared cache",
                exists=exists,
                size_bytes=size_bytes,
                file_count=file_count,
            ))

    return rows


def find_bundle_stores(root):
    """Discover all datacache BundleStore directories under ``root`` (read-only)."""
    from .bundle_store import MARKER

    root_path = Path(root)
    if not root_path.is_dir():
        return []
    stores = []
    for dirpath, dirnames, filenames in os.walk(root_path, followlinks=False):
        if MARKER in filenames:
            stores.append(dirpath)
            dirnames[:] = []  # Don't descend into a store's bundles/
            continue
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
    return sorted(stores)


def scan_stores(root):
    """Inspect all datacache BundleStores under ``root`` without modifying disk."""
    from .bundle_store import MARKER, list_bundles
    from ._filesystem import read_json

    records = []
    for store_dir in find_bundle_stores(root):
        try:
            meta = read_json(Path(store_dir) / MARKER)
            kind = meta.get("kind", "unknown") if isinstance(meta, dict) else "unknown"
        except Exception:
            kind = "invalid"
        try:
            bundles = list_bundles(store_dir)
        except Exception as error:
            records.append(dict(
                store=store_dir,
                kind=kind,
                status="error",
                error=str(error),
                bundles=0,
                stale_bundles=0,
                total_bytes=0,
                reclaimable_bytes=0,
            ))
            continue
        total_bytes = 0
        reclaimable_bytes = 0
        stale_count = 0
        # list_bundles returns paths oldest-first; the final entry is current.
        last_index = len(bundles) - 1
        for index, bundle_path in enumerate(bundles):
            _, b_bytes, _ = _dir_stats(bundle_path)
            total_bytes += b_bytes
            if index < last_index:
                stale_count += 1
                reclaimable_bytes += b_bytes
        records.append(dict(
            store=store_dir,
            kind=kind,
            status="ok",
            error=None,
            bundles=len(bundles),
            stale_bundles=stale_count,
            total_bytes=total_bytes,
            reclaimable_bytes=reclaimable_bytes,
        ))
    return records


def prune_root(root, *, keep=1, apply=False):
    """Prune (or dry-run) stale bundle generations across all stores under ``root``."""
    from .bundle_store import list_bundles, prune_bundles

    if isinstance(keep, bool) or not isinstance(keep, int) or keep < 1:
        raise ValueError("keep must be a positive integer; the current bundle is always kept")
    results = []
    for store_dir in find_bundle_stores(root):
        if apply:
            candidates = list_bundles(store_dir)[:-keep]
            reclaimed = sum(_dir_stats(path)[1] for path in candidates)
            removed = [str(p) for p in prune_bundles(store_dir, keep=keep)]
        else:
            candidates = list_bundles(store_dir)[:-keep]
            reclaimed = sum(_dir_stats(path)[1] for path in candidates)
            removed = [str(p) for p in candidates]
        results.append(dict(
            store=store_dir,
            removed=removed,
            removed_count=len(removed),
            reclaimable_bytes=reclaimed,
            applied=bool(apply),
        ))
    return results


def build_parser():
    parser = argparse.ArgumentParser(
        prog="datacache",
        description=(
            "Inspect and maintain local OpenVax / datacache data caches.\n\n"
            "Environment variables:\n"
            "  OPENVAX_DATA_CACHE     Shared cache root for all OpenVax packages\n"
            "  OPENVAX_OFFLINE=1      Forbid all remote downloads (alias: DATACACHE_OFFLINE=1)\n"
            "  OPENVAX_NO_PROGRESS=1  Disable download progress bars (alias: DATACACHE_NO_PROGRESS=1)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version="datacache %s" % __version__)
    subparsers = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    p_dirs = subparsers.add_parser(
        "dirs",
        help="Show where OpenVax/datacache caches live, why each resolved there, and disk usage",
    )
    p_dirs.add_argument("--root", default=None, help="Inspect an explicit shared cache root")
    p_dirs.add_argument("--all", action="store_true", help="Include packages with no local cache yet")
    p_dirs.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    p_path = subparsers.add_parser(
        "path",
        help="Print the resolved cache directory for a package (or the shared root)",
    )
    p_path.add_argument("package", nargs="?", default=None, help="Package name (e.g. pyensembl, orfdata)")
    p_path.add_argument("--root", default=None, help="Override shared cache root")

    p_scan = subparsers.add_parser(
        "scan",
        help="Scan a cache root for datacache bundle stores and reclaimable old generations",
    )
    p_scan.add_argument("root", nargs="?", default=None, help="Cache directory to scan (default: shared OpenVax root)")
    p_scan.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    p_prune = subparsers.add_parser(
        "prune",
        help="Remove stale bundle generations from datacache stores (dry-run by default)",
    )
    p_prune.add_argument("root", nargs="?", default=None, help="Cache directory to prune (default: shared OpenVax root)")
    p_prune.add_argument("--keep", type=int, default=1, help="Generations to keep per store (default: 1)")
    p_prune.add_argument("--yes", action="store_true", help="Actually delete stale generations (default is dry-run)")
    p_prune.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    return parser


def run(argv=None):
    parser = build_parser()
    try:
        import argcomplete
        argcomplete.autocomplete(parser)
    except ImportError:
        pass
    args = parser.parse_args(argv)

    from .common import format_bytes, resolve_cache_dir
    from .offline import offline_source

    if args.command == "path":
        if args.package is None:
            print(_shared_root(override=args.root))
            return
        known = {pkg: (envs, app, leg) for pkg, envs, app, leg in _KNOWN_PACKAGES}
        if args.package in known:
            envs, app, leg = known[args.package]
            loc = resolve_cache_dir(
                args.package, *envs,
                override=os.path.join(args.root, args.package) if args.root else None,
                legacy=_expand_legacy_specs(leg),
                append_package_to_env=app,
            )
        else:
            loc = resolve_cache_dir(
                args.package,
                override=os.path.join(args.root, args.package) if args.root else None,
            )
        print(loc.path)
        return

    if args.command == "dirs":
        shared = _shared_root(override=args.root)
        rows = collect_cache_dirs(root_override=args.root, include_missing=args.all)
        offline = offline_source()
        if args.json:
            print(json.dumps(dict(
                shared_root=shared,
                offline=offline is not None,
                offline_source=offline,
                total_bytes=sum(r["size_bytes"] for r in rows),
                caches=rows,
            ), indent=2))
            return
        print("Shared cache root: %s" % _display_path(shared))
        if offline:
            print("Offline mode:      enabled (%s)" % offline)
        if not rows:
            print("No local caches found. Pass --all to see default locations.")
            return
        print()
        table = _format_table(
            ("PACKAGE", "SIZE", "FILES", "ORIGIN", "PATH"),
            [
                (
                    r["package"],
                    format_bytes(r["size_bytes"]) if r["exists"] else "-",
                    str(r["file_count"]) if r["exists"] else "-",
                    r["detail"],
                    _display_path(r["path"]),
                )
                for r in rows
            ],
        )
        print(table)
        total = sum(r["size_bytes"] for r in rows)
        print("\nTotal: %s across %d cache(s)." % (format_bytes(total), len(rows)))
        return

    if args.command == "scan":
        target = os.path.expanduser(args.root) if args.root else _shared_root()
        records = scan_stores(target)
        if args.json:
            print(json.dumps(dict(
                root=target,
                stores=records,
                total_bytes=sum(r["total_bytes"] for r in records),
                reclaimable_bytes=sum(r["reclaimable_bytes"] for r in records),
            ), indent=2))
            return
        if not records:
            print("No datacache bundle stores found under %s" % _display_path(target))
            return
        print(_format_table(
            ("KIND", "BUNDLES", "STALE", "SIZE", "RECLAIMABLE", "STORE"),
            [
                (
                    r["kind"],
                    r["bundles"],
                    r["stale_bundles"],
                    format_bytes(r["total_bytes"]),
                    format_bytes(r["reclaimable_bytes"]),
                    _display_path(r["store"]),
                )
                for r in records
            ],
        ))
        total = sum(r["total_bytes"] for r in records)
        reclaim = sum(r["reclaimable_bytes"] for r in records)
        print("\nTotal: %s (%s reclaimable across %d store(s))." % (
            format_bytes(total), format_bytes(reclaim), len(records)))
        if reclaim > 0:
            print("Run 'datacache prune --yes' to remove stale generations.")
        return

    if args.command == "prune":
        target = os.path.expanduser(args.root) if args.root else _shared_root()
        results = prune_root(target, keep=args.keep, apply=args.yes)
        total_removed = sum(r["removed_count"] for r in results)
        if args.json:
            print(json.dumps(dict(
                root=target,
                dry_run=not args.yes,
                removed_count=total_removed,
                stores=results,
            ), indent=2))
            return
        if total_removed == 0:
            print("Nothing to prune under %s." % _display_path(target))
            return
        verb = "Removed" if args.yes else "Would remove"
        for r in results:
            if r["removed_count"]:
                extra = (
                    " (%s)" % format_bytes(r["reclaimable_bytes"])
                    if r["reclaimable_bytes"] is not None else ""
                )
                print("%s %d stale generation(s)%s from %s" % (
                    verb, r["removed_count"], extra, _display_path(r["store"])))
        if not args.yes:
            print("\nDry run only. Pass --yes to delete these stale generations.")
        return
