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

"""Safe, transactional installation of complete directory trees from tar archives."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import shutil
import stat
import tarfile
import tempfile
from uuid import uuid4

from filelock import FileLock

from ._filesystem import open_regular, path_present, read_json, write_json
from .bundles import _relative_name
from .inspection import FileInspection
from .integrity import FileValidationError, _validate_expectations
from .provenance import redact_url


STORE = ".datacache-archive-store.json"
MANIFEST = ".datacache-archive-manifest.json"
CURRENT = "current.json"
FORMAT = 1
CHUNK_SIZE = 2 ** 20


@dataclass(frozen=True)
class ArchiveInspection:
    """Read-only status for one installed archive-tree generation.

    ``verified`` means caller-supplied trusted archive or part hashes identified
    the installed bytes. Receipt-only inspection still hashes every installed
    file, but an observed receipt is not an independent source of trust.
    """

    path: str
    status: str
    verified: bool = False
    generation: object = None
    files: dict = field(default_factory=dict)
    error: object = None
    source_urls: tuple = ()
    fetched_at: object = None
    archive_size: object = None
    recorded_sha256: object = None


def _fingerprint(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _local_identity(path):
    return Path(path).absolute().as_uri()


def _normalize_sources(sources):
    """Normalize one source or an explicitly ordered sequence of archive parts."""
    if isinstance(sources, (str, os.PathLike, dict)):
        sources = [sources]
    elif not isinstance(sources, (list, tuple)):
        raise ValueError("sources must be a URL, path, mapping, or ordered sequence")
    if not sources:
        raise ValueError("sources must contain at least one archive source")

    result = []
    for source in sources:
        if isinstance(source, os.PathLike):
            source = {"path": source}
        elif isinstance(source, str):
            source = {"url": source}
        elif isinstance(source, dict):
            source = dict(source)
        else:
            raise ValueError("each archive source must be a URL, path, or mapping")
        unknown = set(source) - {"url", "path", "sha256", "size"}
        if unknown:
            raise ValueError("unknown archive source options: %s" % sorted(unknown))
        url, path = source.get("url"), source.get("path")
        if url is not None and (not isinstance(url, str) or not url):
            raise ValueError("archive source url must be a nonempty string")
        if path is not None:
            try:
                path = Path(path)
            except TypeError as error:
                raise ValueError("archive source path must be path-like") from error
        if url is None and path is None:
            raise ValueError("each archive source requires url or path")
        identity = url if url is not None else _local_identity(path)
        digest, size = source.get("sha256"), source.get("size")
        _validate_expectations(digest, size)
        result.append({
            "url": url,
            "path": path,
            "identity": identity,
            "fingerprint": _fingerprint(identity),
            "sha256": digest.lower() if digest else None,
            "size": size,
        })
    return tuple(result)


def _normalize_extra_files(extra_files):
    if extra_files is None:
        return {}
    if not isinstance(extra_files, dict):
        raise ValueError("extra_files must be a mapping of relative paths to text or bytes")
    result = {}
    nodes = {}
    for name, value in extra_files.items():
        _relative_name(name)
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif not isinstance(value, bytes):
            raise ValueError("extra file contents must be text or bytes")
        parts = name.split("/")
        for index in range(1, len(parts)):
            parent = "/".join(parts[:index])
            key = parent.casefold()
            previous = nodes.get(key)
            if previous is None:
                nodes[key] = (parent, "directory")
            elif previous != (parent, "directory"):
                raise ValueError("extra file paths collide as files and directories or ignoring case")
        key = name.casefold()
        if key in nodes:
            raise ValueError("extra file paths collide as files and directories or ignoring case")
        nodes[key] = (name, "file")
        result[name] = value
    return result


def _definition(sources, expected_sha256, expected_size, extra_files, require_verified=False):
    normalized = _normalize_sources(sources)
    _validate_expectations(expected_sha256, expected_size)
    expected_sha256 = expected_sha256.lower() if expected_sha256 else None
    extras = _normalize_extra_files(extra_files)
    trusted = bool(expected_sha256 and expected_size is not None) or all(
        source["sha256"] and source["size"] is not None for source in normalized)
    if require_verified and not trusted:
        raise ValueError(
            "verified archive installs require expected_sha256 and expected_size "
            "for the assembled archive, or sha256 and size for every part")
    return {
        "sources": normalized,
        "expected_sha256": expected_sha256,
        "expected_size": expected_size,
        "extra_files": extras,
        "trusted": trusted,
    }


def _directory(path):
    if not stat.S_ISDIR(Path(path).lstat().st_mode):
        raise FileValidationError(path, "expected a directory, not a link")


def _file_mode(path):
    from .download import _normal_creation_mode
    return _normal_creation_mode(path)


def _store(path):
    _directory(path)
    if read_json(path / STORE) != {"format": FORMAT, "kind": "archive-tree"}:
        raise FileValidationError(path, "unrecognized archive store")
    _directory(path / "generations")


def _generation(store, generation):
    if (not isinstance(generation, str) or len(generation) != 32 or
            any(char not in "0123456789abcdef" for char in generation)):
        raise FileValidationError(store, "invalid generation pointer")
    result = store / "generations" / generation
    _directory(result)
    return result


def _manifest(receipt):
    if (not isinstance(receipt, dict) or receipt.get("format") != FORMAT or
            receipt.get("kind") != "archive-tree"):
        raise ValueError("unrecognized archive manifest")
    archive = receipt.get("archive")
    sources = receipt.get("sources")
    files = receipt.get("files")
    directories = receipt.get("directories")
    fetched_at = receipt.get("fetched_at")
    if (not isinstance(archive, dict) or not isinstance(sources, list) or
            not sources or not isinstance(files, dict) or not files or
            not isinstance(directories, list) or not isinstance(fetched_at, str)):
        raise ValueError("invalid archive manifest")
    _validate_expectations(archive.get("sha256"), archive.get("size"))
    if archive.get("sha256") is None or archive.get("size") is None:
        raise ValueError("archive manifest does not identify assembled bytes")
    normalized_sources = []
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("invalid archive source receipt")
        if set(source) != {"url", "fingerprint", "sha256", "size"}:
            raise ValueError("invalid archive source receipt")
        if (not isinstance(source["url"], str) or
                not isinstance(source["fingerprint"], str) or
                len(source["fingerprint"]) != 64 or
                any(char not in "0123456789abcdef" for char in source["fingerprint"])):
            raise ValueError("invalid archive source receipt")
        _validate_expectations(source["sha256"], source["size"])
        if source["sha256"] is None or source["size"] is None:
            raise ValueError("archive source receipt does not identify observed bytes")
        normalized_sources.append(source)
    normalized_files = {}
    for name, spec in files.items():
        _relative_name(name)
        if not isinstance(spec, dict) or set(spec) != {"sha256", "size"}:
            raise ValueError("invalid extracted file receipt")
        _validate_expectations(spec["sha256"], spec["size"])
        if spec["sha256"] is None or spec["size"] is None:
            raise ValueError("extracted file receipt does not identify observed bytes")
        normalized_files[name] = spec
    normalized_directories = []
    folded = set()
    for name in directories:
        _relative_name(name)
        key = name.casefold()
        if key in folded:
            raise ValueError("duplicate directory in archive manifest")
        folded.add(key)
        normalized_directories.append(name)
    file_keys = {name.casefold() for name in normalized_files}
    if len(file_keys) != len(normalized_files) or file_keys & folded:
        raise ValueError("colliding paths in archive manifest")
    return archive, normalized_sources, normalized_files, normalized_directories, fetched_at


def _hash_handle(handle):
    digest, size = hashlib.sha256(), 0
    for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _open_local_source(path):
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise FileValidationError(path, "expected a regular archive part")
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


def _walk_tree(root, receipt=None):
    """Return observed file metadata, directories, and FileInspection values."""
    observed, directories, inspections = {}, [], {}
    stack = [(Path(root), "")]
    while stack:
        directory, prefix = stack.pop()
        with os.scandir(directory) as scanned:
            entries = sorted(
                scanned, key=lambda entry: entry.name.casefold(), reverse=True)
        for entry in entries:
            relative = entry.name if not prefix else prefix + "/" + entry.name
            if not prefix and relative == MANIFEST:
                continue
            _relative_name(relative)
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                directories.append(relative)
                stack.append((Path(entry.path), relative))
                continue
            if not stat.S_ISREG(info.st_mode):
                raise FileValidationError(entry.path, "archive trees may contain only regular files and directories")
            descriptor = open_regular(entry.path)
            with os.fdopen(descriptor, "rb") as handle:
                current = os.fstat(handle.fileno())
                digest, size = _hash_handle(handle)
            observed[relative] = {"sha256": digest, "size": size}
            expected = receipt.get(relative) if receipt is not None else None
            if expected is not None and expected != observed[relative]:
                raise FileValidationError(entry.path, "installed file disagrees with archive manifest")
            inspections[relative] = FileInspection(
                entry.path, "available", verified=expected is not None,
                size=current.st_size, mtime=current.st_mtime)
    directories.sort()
    if receipt is not None and set(observed) != set(receipt):
        missing = sorted(set(receipt) - set(observed))
        extra = sorted(set(observed) - set(receipt))
        raise FileValidationError(root, "archive tree file set differs from manifest; missing=%r extra=%r" % (
            missing, extra))
    return observed, directories, inspections


def _matches_definition(receipt_archive, receipt_sources, receipt_files, definition):
    expected_sha256 = definition["expected_sha256"]
    expected_size = definition["expected_size"]
    if expected_sha256 is not None and receipt_archive["sha256"] != expected_sha256:
        raise FileValidationError("archive", "assembled archive hash disagrees with requested source")
    if expected_size is not None and receipt_archive["size"] != expected_size:
        raise FileValidationError("archive", "assembled archive size disagrees with requested source")
    if len(receipt_sources) != len(definition["sources"]):
        raise FileValidationError("archive", "archive part count disagrees with requested source")
    for recorded, expected in zip(receipt_sources, definition["sources"]):
        if expected["sha256"] is not None and recorded["sha256"] != expected["sha256"]:
            raise FileValidationError("archive", "archive part hash disagrees with requested source")
        if expected["size"] is not None and recorded["size"] != expected["size"]:
            raise FileValidationError("archive", "archive part size disagrees with requested source")
    if not definition["trusted"]:
        fingerprints = [source["fingerprint"] for source in receipt_sources]
        expected = [source["fingerprint"] for source in definition["sources"]]
        if fingerprints != expected:
            raise FileValidationError("archive", "ordered archive source identities disagree")
    for name, value in definition["extra_files"].items():
        expected = {"sha256": hashlib.sha256(value).hexdigest(), "size": len(value)}
        if receipt_files.get(name) != expected:
            raise FileValidationError(name, "extra file disagrees with requested content")


def _inspect_generation(store, generation, definition, verify_files=True):
    directory = _generation(store, generation)
    receipt = read_json(directory / MANIFEST)
    archive, sources, files, directories, fetched_at = _manifest(receipt)
    if definition is not None:
        _matches_definition(archive, sources, files, definition)
    metadata = {
        "source_urls": tuple(source["url"] for source in sources),
        "fetched_at": fetched_at,
        "archive_size": archive["size"],
        "recorded_sha256": archive["sha256"],
    }
    if not verify_files:
        return ArchiveInspection(
            str(store), "available", False, str(directory), **metadata)
    _, observed_directories, inspections = _walk_tree(directory, files)
    if observed_directories != sorted(directories):
        raise FileValidationError(directory, "archive tree directories disagree with manifest")
    return ArchiveInspection(
        str(store), "available", bool(definition and definition["trusted"]),
        str(directory), inspections, **metadata)


def inspect_archive(
        destination, sources=None, *, expected_sha256=None, expected_size=None,
        extra_files=None, verify_files=True):
    """Inspect one installed archive tree without writes, locks, or network.

    Omit ``sources`` for receipt-only consistency checking. Supplying sources
    checks the requested ordered archive identity or trusted content hashes.
    ``verify_files=False`` validates publication and source metadata without
    hashing the extracted tree; its result is never marked verified.
    """
    if not isinstance(verify_files, bool):
        raise ValueError("verify_files must be a boolean")
    definition = None
    if sources is not None:
        definition = _definition(
            sources, expected_sha256, expected_size, extra_files,
            require_verified=False)
    elif any(value is not None for value in (expected_sha256, expected_size, extra_files)):
        raise ValueError("sources are required with archive expectations or extra_files")
    path = Path(destination)
    try:
        if not path_present(path):
            return ArchiveInspection(str(path), "missing")
        _store(path)
        try:
            pointer = read_json(path / CURRENT)
        except FileNotFoundError:
            if any((path / "generations").iterdir()):
                return ArchiveInspection(str(path), "recovery-required")
            return ArchiveInspection(str(path), "missing")
        return _inspect_generation(
            path, pointer["generation"], definition, verify_files=verify_files)
    except PermissionError as error:
        return ArchiveInspection(str(path), "inaccessible", error=error)
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as error:
        return ArchiveInspection(str(path), "invalid", error=error)


def _initialize(path):
    if path_present(path):
        _store(path)  # Never claim a legacy, empty, or otherwise foreign path.
        return
    staging = Path(tempfile.mkdtemp(prefix=".datacache-archive-store-", dir=path.parent))
    try:
        write_json(
            staging / STORE, {"format": FORMAT, "kind": "archive-tree"},
            mode=_file_mode(staging))
        (staging / "generations").mkdir()
        os.chmod(staging, stat.S_IMODE((staging / "generations").stat().st_mode))
        os.replace(staging, path)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _recover(path, definition):
    for entry in sorted((path / "generations").iterdir(), reverse=True):
        try:
            candidate = _inspect_generation(path, entry.name, definition)
        except (OSError, ValueError, KeyError, TypeError, RecursionError):
            continue
        write_json(path / CURRENT, {"generation": entry.name}, mode=_file_mode(path))
        return candidate
    return None


def _archive_members(
        archive, max_members=None, max_extracted_size=None, extra_names=()):
    members = archive.getmembers()
    if not members:
        raise FileValidationError(archive.name, "archive is empty")
    if max_members is not None and len(members) > max_members:
        raise FileValidationError(archive.name, "archive has more than %d members" % max_members)
    nodes = {}
    planned = []
    total = 0
    regular_files = 0
    for member in members:
        if member.isdir():
            name, kind = member.name.rstrip("/"), "directory"
        elif member.isfile():
            name, kind = member.name, "file"
            if member.size < 0:
                raise FileValidationError(archive.name, "archive member has negative size")
            total += member.size
            regular_files += 1
        else:
            raise FileValidationError(
                archive.name, "archive contains a link or special member: %s" % member.name)
        try:
            _relative_name(name)
        except ValueError as error:
            raise FileValidationError(
                archive.name, "unsafe archive member path: %s" % member.name) from error
        parts = name.split("/")
        for index in range(1, len(parts)):
            parent = "/".join(parts[:index])
            key = parent.casefold()
            previous = nodes.get(key)
            if previous is None:
                nodes[key] = (parent, "directory", False)
            elif previous[0] != parent or previous[1] != "directory":
                raise FileValidationError(archive.name, "archive member paths collide: %s" % name)
        key = name.casefold()
        previous = nodes.get(key)
        if kind == "directory" and previous is not None and previous[1] == "directory" and not previous[2]:
            if previous[0] != name:
                raise FileValidationError(archive.name, "archive member paths collide: %s" % name)
            nodes[key] = (name, kind, True)
        elif previous is not None:
            raise FileValidationError(archive.name, "archive member paths collide: %s" % name)
        else:
            nodes[key] = (name, kind, True)
        planned.append((member, name, kind))
    for name in extra_names:
        parts = name.split("/")
        for index in range(1, len(parts)):
            parent = "/".join(parts[:index])
            key = parent.casefold()
            previous = nodes.get(key)
            if previous is None:
                nodes[key] = (parent, "directory", False)
            elif previous[0] != parent or previous[1] != "directory":
                raise FileValidationError(
                    archive.name, "extra file path collides with archive content: %s" % name)
        if name.casefold() in nodes:
            raise FileValidationError(
                archive.name, "extra file path collides with archive content: %s" % name)
        nodes[name.casefold()] = (name, "file", True)
    if not regular_files:
        raise FileValidationError(archive.name, "archive contains no regular files")
    if max_extracted_size is not None and total > max_extracted_size:
        raise FileValidationError(
            archive.name, "archive expands to more than %d bytes" % max_extracted_size)
    return planned


def _extract_tar(
        archive_path, destination, max_members=None, max_extracted_size=None,
        extra_names=()):
    try:
        with tarfile.open(archive_path, "r:*") as archive:
            planned = _archive_members(
                archive, max_members, max_extracted_size, extra_names)
            directories = sorted(
                {name for _, name, kind in planned if kind == "directory"} |
                {"/".join(name.split("/")[:index])
                 for _, name, _ in planned for index in range(1, len(name.split("/")))},
                key=lambda name: (name.count("/"), name.casefold()))
            for name in directories:
                (destination / Path(*name.split("/"))).mkdir(exist_ok=True)
            for member, name, kind in planned:
                if kind == "directory":
                    continue
                target = destination / Path(*name.split("/"))
                source = archive.extractfile(member)
                if source is None:
                    raise FileValidationError(archive_path, "could not read archive member %s" % name)
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
                flags |= getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(target, flags, 0o666)
                count = 0
                try:
                    output = os.fdopen(descriptor, "wb")
                except BaseException:
                    os.close(descriptor)
                    source.close()
                    raise
                with source, output:
                    for chunk in iter(lambda: source.read(CHUNK_SIZE), b""):
                        output.write(chunk)
                        count += len(chunk)
                if count != member.size:
                    raise FileValidationError(
                        archive_path, "archive member %s is truncated" % name)
    except FileValidationError:
        raise
    except (tarfile.TarError, EOFError) as error:
        raise FileValidationError(archive_path, "invalid tar archive: %s" % error) from error


def _validate_limit(name, value):
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
        raise ValueError("%s must be a non-negative integer" % name)


def _working_key(definition):
    serializable = {
        "sources": [{key: source[key] for key in ("identity", "sha256", "size")}
                    for source in definition["sources"]],
        "expected_sha256": definition["expected_sha256"],
        "expected_size": definition["expected_size"],
        "extra_files": {name: hashlib.sha256(value).hexdigest()
                        for name, value in definition["extra_files"].items()},
    }
    import json
    return hashlib.sha256(json.dumps(serializable, sort_keys=True).encode("utf-8")).hexdigest()


def _part_expectations(definition, index):
    source = definition["sources"][index]
    digest, size = source["sha256"], source["size"]
    if len(definition["sources"]) == 1:
        digest = digest or definition["expected_sha256"]
        size = size if size is not None else definition["expected_size"]
    return digest, size


def _assemble_archive(working, definition, options):
    from .download import fetch_file

    archive_path = working / "archive.tar"
    aggregate = hashlib.sha256()
    aggregate_size = 0
    recorded = []
    keep_parts = options.get("resume", False)
    with archive_path.open("wb") as assembled:
        for index, source in enumerate(definition["sources"]):
            expected_sha256, expected_size = _part_expectations(definition, index)
            temporary = None
            if source["path"] is not None:
                handle = _open_local_source(source["path"])
            else:
                temporary = working / ("part-%06d" % index)
                fetch_file(
                    source["url"], destination=temporary, raw=True,
                    expected_sha256=expected_sha256, expected_size=expected_size,
                    **options)
                handle = _open_local_source(temporary)
            digest, size = hashlib.sha256(), 0
            with handle:
                for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
                    assembled.write(chunk)
                    aggregate.update(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                    aggregate_size += len(chunk)
            actual = digest.hexdigest()
            if expected_size is not None and size != expected_size:
                raise FileValidationError(source["path"] or source["url"], "archive part size mismatch")
            if expected_sha256 is not None and actual != expected_sha256:
                raise FileValidationError(source["path"] or source["url"], "archive part SHA-256 mismatch")
            recorded.append({
                "url": redact_url(source["identity"]),
                "fingerprint": source["fingerprint"],
                "sha256": actual,
                "size": size,
            })
            if temporary is not None and not keep_parts:
                temporary.unlink()
    actual_sha256 = aggregate.hexdigest()
    if (definition["expected_size"] is not None and
            aggregate_size != definition["expected_size"]):
        raise FileValidationError(archive_path, "assembled archive size mismatch")
    if (definition["expected_sha256"] is not None and
            actual_sha256 != definition["expected_sha256"]):
        raise FileValidationError(archive_path, "assembled archive SHA-256 mismatch")
    return archive_path, {"sha256": actual_sha256, "size": aggregate_size}, recorded


def install_archive(
        destination, sources, *, expected_sha256=None, expected_size=None,
        extra_files=None, force=False, verified=True, download_options=None,
        max_members=None, max_extracted_size=None):
    """Safely install a complete tar archive tree as one immutable generation.

    ``sources`` is one URL/path or an ordered sequence. Source mappings accept
    ``url``, ``path``, ``sha256``, and ``size``; supplying both URL and path uses
    local bytes while retaining the URL as source identity. Split parts are
    concatenated byte-for-byte in the supplied order. ``extra_files`` are
    written after extraction and before the tree receipt, which lets consumers
    publish their own source record in the same transaction.
    """
    if not isinstance(force, bool) or not isinstance(verified, bool):
        raise ValueError("force and verified must be booleans")
    _validate_limit("max_members", max_members)
    _validate_limit("max_extracted_size", max_extracted_size)
    definition = _definition(
        sources, expected_sha256, expected_size, extra_files,
        require_verified=verified)
    options = dict(download_options or {})
    allowed = {"timeout", "chunk_size", "progress_callback", "show_progress",
               "max_retries", "retry_backoff", "retry_max_delay", "resume"}
    if set(options) - allowed:
        raise ValueError("unsupported archive download options: %s" % sorted(set(options) - allowed))
    if options.get("resume"):
        for index, source in enumerate(definition["sources"]):
            digest, size = _part_expectations(definition, index)
            if source["path"] is None and (digest is None or size is None):
                raise ValueError("resumable archive parts require sha256 and size")

    path = Path(destination)
    inspection = inspect_archive(
        path, sources, expected_sha256=expected_sha256,
        expected_size=expected_size, extra_files=extra_files)
    if not force and inspection.status == "available":
        return Path(inspection.generation)
    if inspection.status == "inaccessible":
        raise inspection.error
    if not force and inspection.status == "invalid":
        raise FileValidationError(path, "invalid archive installation; use force=True to explicitly repair") from inspection.error

    path.parent.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(os.fsencode(path.name.casefold())).hexdigest()[:32]
    with FileLock(str(path.parent / (".datacache-archive-lock-" + key))):
        _initialize(path)
        inspection = inspect_archive(
            path, sources, expected_sha256=expected_sha256,
            expected_size=expected_size, extra_files=extra_files)
        if not force and inspection.status == "available":
            return Path(inspection.generation)
        if inspection.status == "invalid" and not force:
            raise FileValidationError(path, "invalid archive installation; use force=True to explicitly repair") from inspection.error
        if inspection.status in ("missing", "recovery-required", "invalid"):
            recovered = _recover(path, definition)
            if recovered is not None:
                return Path(recovered.generation)

        resumable = options.get("resume", False)
        working = path / (".staging-" + (_working_key(definition) if resumable else uuid4().hex))
        working.mkdir(mode=0o700, exist_ok=resumable)
        if os.name == "posix":
            info = working.lstat()
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or
                    stat.S_IMODE(info.st_mode) & 0o077):
                raise FileValidationError(working, "archive staging must be a private directory")
        tree = working / "tree"
        if tree.exists():
            shutil.rmtree(tree)
        tree.mkdir()
        published = False
        try:
            archive_path, archive_record, source_records = _assemble_archive(
                working, definition, options)
            _extract_tar(
                archive_path, tree, max_members, max_extracted_size,
                definition["extra_files"])
            for name, value in definition["extra_files"].items():
                target = tree / Path(*name.split("/"))
                target.parent.mkdir(parents=True, exist_ok=True)
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
                flags |= getattr(os, "O_NOFOLLOW", 0)
                try:
                    descriptor = os.open(target, flags, 0o666)
                except FileExistsError as error:
                    raise FileValidationError(target, "extra file collides with archive content") from error
                with os.fdopen(descriptor, "wb") as output:
                    output.write(value)
            files, directories, _ = _walk_tree(tree)
            receipt = {
                "format": FORMAT,
                "kind": "archive-tree",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "archive": archive_record,
                "sources": source_records,
                "files": files,
                "directories": directories,
            }
            write_json(tree / MANIFEST, receipt, mode=_file_mode(tree))
            generation = uuid4().hex
            final = path / "generations" / generation
            os.replace(tree, final)
            published = True
            candidate = _inspect_generation(path, generation, definition)
            write_json(path / CURRENT, {"generation": generation}, mode=_file_mode(path))
            return Path(candidate.generation)
        finally:
            if published or not resumable:
                shutil.rmtree(working)


class VersionedArchiveRegistry:
    """Versioned catalogue of complete archive-tree downloads.

    Each item has ``default_version`` and a ``versions`` mapping. A version is
    one source, an ordered source sequence, or a mapping containing ``sources``
    plus archive expectations, consumer ``extra_files``, and extraction limits.
    ``cache_root`` uses ``<root>/<name>/<version>`` stores; ``store_path`` can
    instead preserve a consumer's established ``(name, version)`` layout.
    Construction validates definitions without touching the filesystem.
    """

    def __init__(
            self, archives, *, cache_root=None, store_path=None, verified=True):
        if (cache_root is None) == (store_path is None):
            raise ValueError("provide exactly one of cache_root or store_path")
        if store_path is not None and not callable(store_path):
            raise ValueError("store_path must be callable")
        if not isinstance(verified, bool):
            raise ValueError("verified must be a boolean")
        if not isinstance(archives, dict) or not archives:
            raise ValueError("archives must be a nonempty mapping")
        self.verified = verified
        self._path = (store_path if store_path is not None else
                      lambda name, version: Path(cache_root) / name / version)
        self._archives = {}
        version_options = {
            "sources", "expected_sha256", "expected_size", "extra_files",
            "max_members", "max_extracted_size",
        }
        for name, archive in archives.items():
            from .bundles import _component
            _component(name)
            if not isinstance(archive, dict):
                raise ValueError("archive definitions must be mappings")
            unknown = set(archive) - {"default_version", "versions", "description"}
            if unknown:
                raise ValueError("unknown archive definition options: %s" % sorted(unknown))
            versions = archive.get("versions")
            if not isinstance(versions, dict) or not versions:
                raise ValueError("each archive requires a nonempty versions mapping")
            normalized_versions = {}
            for version, value in versions.items():
                _component(version)
                if isinstance(value, dict) and "sources" in value:
                    options = dict(value)
                    unknown = set(options) - version_options
                    if unknown:
                        raise ValueError("unknown archive version options: %s" % sorted(unknown))
                else:
                    options = {"sources": value}
                _validate_limit("max_members", options.get("max_members"))
                _validate_limit("max_extracted_size", options.get("max_extracted_size"))
                definition = _definition(
                    options["sources"], options.get("expected_sha256"),
                    options.get("expected_size"), options.get("extra_files"),
                    require_verified=verified)
                normalized_versions[version] = {
                    "sources": tuple({key: source[key] for key in
                                      ("url", "path", "sha256", "size")
                                      if source[key] is not None}
                                     for source in definition["sources"]),
                    "expected_sha256": definition["expected_sha256"],
                    "expected_size": definition["expected_size"],
                    "extra_files": definition["extra_files"],
                    "max_members": options.get("max_members"),
                    "max_extracted_size": options.get("max_extracted_size"),
                }
            default = archive.get("default_version")
            if default not in normalized_versions:
                raise ValueError("default_version must name a concrete archive version")
            self._archives[name] = {
                "default_version": default,
                "versions": normalized_versions,
                "description": archive.get("description", ""),
            }

    def resolve_version(self, name, version=None):
        if name not in self._archives:
            raise ValueError(
                "unknown archive %r; known: %s" % (
                    name, ", ".join(sorted(self._archives))))
        definition = self._archives[name]
        version = definition["default_version"] if version is None else version
        if version not in definition["versions"]:
            raise ValueError("unknown version %r for %s" % (version, name))
        return version

    def store_path(self, name, version=None):
        """Return the managed store path without creating or inspecting it."""
        version = self.resolve_version(name, version)
        return Path(self._path(name, version))

    def _version(self, name, version):
        version = self.resolve_version(name, version)
        return version, self._archives[name]["versions"][version]

    @staticmethod
    def _local_sources(sources, source_paths):
        if source_paths is None:
            return sources
        if isinstance(source_paths, (str, os.PathLike)):
            source_paths = [source_paths]
        elif not isinstance(source_paths, (list, tuple)):
            raise ValueError("source_paths must be a path or ordered sequence")
        if len(source_paths) != len(sources):
            raise ValueError("source_paths must have one path for every archive part")
        result = []
        for source, path in zip(sources, source_paths):
            item = dict(source)
            if "url" not in item:
                item["url"] = _local_identity(item["path"])
            item["path"] = Path(path)
            result.append(item)
        return tuple(result)

    def inspect(self, name, version=None, *, verify_files=True):
        """Inspect one version offline; no destination or lock is created."""
        version, definition = self._version(name, version)
        return inspect_archive(
            self.store_path(name, version), definition["sources"],
            expected_sha256=definition["expected_sha256"],
            expected_size=definition["expected_size"],
            extra_files=definition["extra_files"], verify_files=verify_files)

    def download(
            self, name, version=None, *, force=False, source_paths=None,
            **download_options):
        """Install/reuse one version and return its immutable extracted root."""
        version, definition = self._version(name, version)
        sources = self._local_sources(definition["sources"], source_paths)
        return install_archive(
            self.store_path(name, version), sources,
            expected_sha256=definition["expected_sha256"],
            expected_size=definition["expected_size"],
            extra_files=definition["extra_files"], force=force,
            verified=self.verified, download_options=download_options,
            max_members=definition["max_members"],
            max_extracted_size=definition["max_extracted_size"])

    def local_path(self, name, version=None, *, verify_files=False):
        """Return an installed generation without downloading or repairing."""
        inspected = self.inspect(name, version, verify_files=verify_files)
        if inspected.status == "missing":
            raise FileNotFoundError(inspected.path)
        if inspected.status == "inaccessible":
            raise inspected.error
        if inspected.status != "available":
            raise FileValidationError(
                inspected.path, "archive installation is %s" % inspected.status)
        return Path(inspected.generation)

    def ensure(self, name, version=None, **download_options):
        """Install if needed, then return the extracted generation path."""
        return self.download(name, version, **download_options)

    def is_cached(self, name, version=None, *, verify_files=False):
        """Whether one version has an available published generation."""
        return self.inspect(
            name, version, verify_files=verify_files).status == "available"

    def status(self, name=None):
        """Return one offline status row per concrete archive version."""
        if name is not None:
            self.resolve_version(name)
            names = [name]
        else:
            names = sorted(self._archives)
        rows = []
        for archive_name in names:
            archive = self._archives[archive_name]
            for version, definition in archive["versions"].items():
                inspection = self.inspect(
                    archive_name, version, verify_files=False)
                identities = [source.get("url") or _local_identity(source["path"])
                              for source in definition["sources"]]
                rows.append({
                    "name": archive_name,
                    "version": version,
                    "default": version == archive["default_version"],
                    "description": archive["description"],
                    "sources": [redact_url(identity) for identity in identities],
                    "downloaded_sources": list(inspection.source_urls),
                    "path": str(self.store_path(archive_name, version)),
                    "generation": inspection.generation,
                    "status": inspection.status,
                    "cached": inspection.status == "available",
                    "fetched_at": inspection.fetched_at,
                    "archive_size": inspection.archive_size,
                    "recorded_sha256": inspection.recorded_sha256,
                    "inspection": inspection,
                })
        return rows
