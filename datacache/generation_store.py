"""Store directories that publish complete, immutable generations.

Bundles, archive trees and materialized artifacts keep their installed copies
in the same layout::

    <store>/
        <marker>                      claims the directory, e.g. {"format": 1}
        current.json                  {"generation": "<32 hex digits>"}
        generations/<32 hex digits>/  one complete copy and its receipt

A new copy is built in a private working directory, moved into
``generations/`` with one rename, and published by atomically rewriting
``current.json``. Readers see the old copy or the new one, never a mixture,
and published generations are never modified or removed. If ``current.json``
is lost, the newest generation that still checks out can be published again
without downloading anything.

`GenerationStore` implements that layout once; a `StoreKind` holds what
differs between bundles, archives and materializations. The functions after
it list and hash the files of a generation and validate the relative names
stored in one.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from typing import NamedTuple, Optional
from uuid import uuid4

from ._filesystem import open_regular, path_present, read_json, write_json
from .download import normal_creation_mode
from .inspection import FileInspection
from .integrity import FileValidationError, _validate_expectations
from .progress import Progress
from .provenance import sidecar_path

CURRENT = 'current.json'
CHUNK_SIZE = 2 ** 20

# Exceptions meaning a store or generation is unusable as found: corrupt,
# incomplete, foreign or unreadable. PermissionError is one of them, so callers
# that report it separately must catch it first.
INVALID_STORE_ERRORS = (OSError, ValueError, KeyError, TypeError, RecursionError)


@dataclass(frozen=True)
class StoreKind:
    """What one kind of store records, and whether it adopts empty directories.

    name is used in messages, such as 'bundle'. marker is the file at the
    store root that claims the directory, and marker_contents the exact JSON
    it holds. receipt is the file inside each generation that records its
    contents, and created_at the receipt field holding the UTC time the
    generation was made. adopts_empty_directory says whether an existing empty
    directory may become a store; archives refuse, so that an empty legacy
    directory an application already uses is never claimed.
    """
    name: str
    marker: str
    marker_contents: dict
    receipt: str
    created_at: str
    adopts_empty_directory: bool = True


class StoreState(NamedTuple):
    """What a store publishes now, read without locks or writes.

    status is 'published', 'missing' (nothing installed yet) or
    'recovery-required' (generations exist but current.json was lost).
    generation is the published generation's directory, otherwise None.
    """
    status: str
    generation: Optional[Path]


class GenerationStore:
    """One store directory: its marker, current.json and its generations.

    Reading methods never write or lock. The writing methods create,
    add_generation, publish and recover expect the caller to hold the store's
    installation lock.
    """

    def __init__(self, path, kind):
        self.path = Path(path)
        self.kind = kind
        self.generations = self.path / 'generations'
        self.pointer = self.path / CURRENT

    def __repr__(self):
        return 'GenerationStore(%r, %s)' % (str(self.path), self.kind.name)

    def check(self):
        """Raise unless path is a directory claimed by this kind's marker."""
        require_directory(self.path)
        if read_json(self.path / self.kind.marker) != self.kind.marker_contents:
            raise FileValidationError(self.path, 'unrecognized %s store' % self.kind.name)
        require_directory(self.generations)

    def read_state(self):
        """Find the published generation without writing, locking or repairing.

        Raises one of INVALID_STORE_ERRORS when the path exists but is not a
        usable store of this kind, or names a generation that is not there.
        """
        if not path_present(self.path):
            return StoreState('missing', None)
        try:
            self.check()
        except FileNotFoundError:
            if not self.kind.adopts_empty_directory:
                raise
            if not any(self.path.iterdir()):
                # Precreated for us; installing will turn it into the store.
                return StoreState('missing', None)
            # Another installer may have created the store since the check.
            self.check()
        try:
            pointer = read_json(self.pointer)
        except FileNotFoundError:
            lost = any(self.generations.iterdir())
            return StoreState('recovery-required' if lost else 'missing', None)
        return StoreState('published', self.generation_directory(pointer['generation']))

    def generation_directory(self, name):
        """The directory of the generation called name, after checking both.

        A generation name is 32 lowercase hexadecimal digits, so a damaged
        current.json can never point outside generations/.
        """
        if not isinstance(name, str) or re.fullmatch('[0-9a-f]{32}', name) is None:
            raise FileValidationError(self.path, 'invalid generation pointer')
        directory = self.generations / name
        require_directory(directory)
        return directory

    def generation_names_newest_first(self):
        """Names of every generation, newest first.

        Age is the creation time a generation's receipt records, or its
        directory's modification time when the receipt cannot be read. The
        names are random, so their own order says nothing about age.
        """
        def created(entry):
            try:
                recorded = datetime.fromisoformat(
                    read_json(entry / self.kind.receipt, limit=None)[self.kind.created_at])
                if recorded.tzinfo is None:
                    recorded = recorded.replace(tzinfo=timezone.utc)
                return recorded.timestamp()
            except (OSError, ValueError, KeyError, TypeError, RecursionError, OverflowError):
                try:
                    return entry.lstat().st_mtime
                except OSError:
                    return float('-inf')
        entries = sorted(self.generations.iterdir(), key=lambda entry: (created(entry), entry.name), reverse=True)
        return [entry.name for entry in entries]

    def create(self):
        """Create the store if it does not exist, never taking over a directory.

        An existing store of this kind is left as it is. An existing empty
        directory becomes the store, keeping its permissions, when this kind
        adopts empty directories. Anything else raises FileValidationError.
        The marker and generations/ appear together with one rename, so
        concurrent readers see no store or a complete one.
        """
        existing_mode = None
        if path_present(self.path):
            require_directory(self.path)
            if any(self.path.iterdir()) or not self.kind.adopts_empty_directory:
                self.refuse_foreign_directory()
                self.check()
                return
            existing_mode = stat.S_IMODE(self.path.lstat().st_mode)
        staging = Path(tempfile.mkdtemp(prefix='.datacache-store-', dir=self.path.parent))
        try:
            write_json(staging / self.kind.marker, self.kind.marker_contents,
                       mode=normal_creation_mode(self.path.parent))
            (staging / 'generations').mkdir()
            os.chmod(staging, existing_mode if existing_mode is not None else
                     stat.S_IMODE((staging / 'generations').stat().st_mode))
            os.replace(staging, self.path)
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    def refuse_foreign_directory(self):
        """Raise if path is a directory this store may never take over.

        That is an existing directory without this kind's marker, unless it is
        empty and this kind adopts empty directories. force=True never
        changes this, so callers check it before suggesting force.
        """
        if (path_present(self.path) and stat.S_ISDIR(self.path.lstat().st_mode)
                and not path_present(self.path / self.kind.marker)
                and (not self.kind.adopts_empty_directory or any(self.path.iterdir()))):
            raise FileValidationError(
                self.path, 'not a datacache %s store; an existing directory is never taken over' % self.kind.name)

    def add_generation(self, staged):
        """Move a complete, checked directory into generations/ by one rename.

        Returns the new generation's directory. Readers do not see it until
        publish() points current.json at it.
        """
        directory = self.generations / uuid4().hex
        os.replace(staged, directory)
        return directory

    def publish(self, generation):
        """Atomically point current.json at one of this store's generations."""
        if Path(generation).parent != self.generations:
            raise ValueError('%s is not a generation of %s' % (generation, self.path))
        write_json(self.pointer, {'generation': Path(generation).name}, mode=normal_creation_mode(self.path))

    def recover(self, inspect_generation):
        """Publish the newest generation that still checks out.

        For use when current.json is lost (or, with force=True, invalid).
        inspect_generation(directory) fully checks one generation and returns
        its inspection, raising one of INVALID_STORE_ERRORS if it is unusable.
        Returns the inspection of the generation now published, or None when
        no generation checks out.
        """
        for name in self.generation_names_newest_first():
            try:
                directory = self.generation_directory(name)
                inspection = inspect_generation(directory)
            except INVALID_STORE_ERRORS:
                continue
            self.publish(directory)
            return inspection
        return None


@dataclass(frozen=True)
class ObservedFile:
    """What was read from one regular file.

    sha256 is None when only metadata was read. info is the os.stat_result of
    the open file that was read, so it describes exactly those bytes.
    """
    path: str
    sha256: Optional[str]
    info: os.stat_result

    @property
    def size(self):
        return self.info.st_size

    @property
    def record(self):
        """The {'sha256', 'size'} entry a receipt stores for these bytes."""
        return {'sha256': self.sha256, 'size': self.size}

    def inspection(self, *, verified):
        """This file as an available FileInspection."""
        return FileInspection(self.path, 'available', verified=verified,
                              size=self.info.st_size, mtime=self.info.st_mtime)


def observe_file(path, expected=None, *, read_contents=True, show_progress=False):
    """Hash one regular file, optionally checking it against expected values.

    The file is opened without following links; a link, a special file or a
    file with several hard links raises FileValidationError, as does a file
    that changes while it is read. read_contents=False reads only metadata,
    so sha256 is None and only the size is checked. expected is a mapping
    with sha256 and size; None values are not checked. See check_expected.
    """
    with os.fdopen(open_regular(path), 'rb') as handle:
        info = os.fstat(handle.fileno())
        sha256 = None
        if read_contents:
            digest, size = hashlib.sha256(), 0
            with Progress(show_progress, 'Verifying', info.st_size) as progress:
                for chunk in iter(lambda: handle.read(CHUNK_SIZE), b''):
                    digest.update(chunk)
                    size += len(chunk)
                    progress(size, info.st_size)
            if size != info.st_size or os.fstat(handle.fileno()).st_mtime_ns != info.st_mtime_ns:
                raise FileValidationError(path, 'file changed while it was being read')
            sha256 = digest.hexdigest()
    observed = ObservedFile(os.fspath(path), sha256, info)
    if expected is not None:
        check_expected(path, observed.record, expected)
    return observed


def check_expected(path, record, expected):
    """Raise FileValidationError if record's bytes disagree with expected.

    Both are mappings with sha256 and size. A None value on either side is
    not compared, and extra keys in expected are ignored.
    """
    if expected.get('size') is not None and record['size'] != expected['size']:
        raise FileValidationError(path, 'size is %d bytes, expected %d' % (record['size'], expected['size']))
    if (expected.get('sha256') is not None and record['sha256'] is not None
            and record['sha256'] != expected['sha256']):
        raise FileValidationError(path, 'SHA-256 disagrees with the expected digest')


def validate_file_record(record):
    """Return record if it is a receipt entry for observed bytes, else raise.

    A file record is exactly {'sha256': <64 hex digits>, 'size': <bytes>};
    neither value may be missing, because a receipt records what was seen.
    """
    if not isinstance(record, dict) or set(record) != {'sha256', 'size'}:
        raise ValueError('invalid file record: %r' % (record,))
    _validate_expectations(record['sha256'], record['size'])
    if record['sha256'] is None or record['size'] is None:
        raise ValueError('a file record needs both sha256 and size')
    return record


@dataclass(frozen=True)
class TreeListing:
    """Every file and directory below a root, by relative POSIX name.

    files maps each regular file's name to its path; directories lists every
    subdirectory, parents included. Both are sorted by name.
    """
    files: dict
    directories: list


def list_tree(root, *, ignore=()):
    """List a directory tree without following links or reading any file.

    Names in ignore are skipped at the root, such as a generation's own
    receipt. Raises FileValidationError for a link or special file, and
    ValueError for a name that validate_relative_name rejects.
    """
    files, directories, pending = {}, [], [(Path(root), '')]
    while pending:
        directory, prefix = pending.pop()
        require_directory(directory)
        with os.scandir(directory) as entries:
            entries = list(entries)
        for entry in entries:
            name = prefix + entry.name
            if name in ignore:
                continue
            validate_relative_name(name)
            mode = entry.stat(follow_symlinks=False).st_mode
            if stat.S_ISDIR(mode):
                directories.append(name)
                pending.append((Path(entry.path), name + '/'))
            elif stat.S_ISREG(mode):
                files[name] = Path(entry.path)
            else:
                raise FileValidationError(entry.path, 'expected only regular files and directories, not a link or special file')
    return TreeListing(dict(sorted(files.items())), sorted(directories))


def check_tree(root, files, directories, *, ignore=(), read_contents=True, show_progress=False):
    """Check that root holds exactly the recorded files and directories.

    files maps relative names to records with sha256 and size (None values
    are not checked); directories names every directory the tree should hold.
    The whole tree is listed before any file is read, so an unexpected file,
    directory, link or special file is rejected without hashing anything.
    Then every file is observed (see observe_file). Returns a dict of names
    to ObservedFile.
    """
    listing = list_tree(root, ignore=ignore)
    for what, recorded, found in (('files', set(files), set(listing.files)),
                                  ('directories', set(directories), set(listing.directories))):
        if recorded != found:
            raise FileValidationError(root, '%s differ from the record: missing %s, unexpected %s' % (
                what, sorted(recorded - found), sorted(found - recorded)))
    return {name: observe_file(path, files[name], read_contents=read_contents, show_progress=show_progress)
            for name, path in listing.files.items()}


def parent_directories(names):
    """Every directory that relative names imply: 'a/b/c' implies 'a' and 'a/b'."""
    return {'/'.join(parts[:index]) for parts in (name.split('/') for name in names)
            for index in range(1, len(parts))}


def validate_relative_name(name):
    """Return name if it is a safe relative POSIX path inside a store.

    Rejects empty names and components, backslashes, '.' and '..', names
    Windows cannot store (reserved device names, control and punctuation
    characters, trailing dots or spaces) and DataCache's own '.datacache-*'
    metadata names. Raises ValueError otherwise.
    """
    if not isinstance(name, str) or not name or '\\' in name:
        raise ValueError('names must be nonempty relative POSIX paths')
    for part in name.split('/'):
        if (part in ('', '.', '..') or part.endswith((' ', '.')) or
                re.search(r'[\x00-\x1f<>:"|?*]', part) or
                part.lower().startswith('.datacache-') or
                re.fullmatch(r'(?i:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?', part)):
            raise ValueError('unsafe relative path: %r' % name)
    return name


def validate_path_component(name):
    """Return name if it is a safe relative name without '/', such as a version."""
    validate_relative_name(name)
    if '/' in name:
        raise ValueError('names and versions must be single path components')
    return name


def validate_distinct_paths(names):
    """Raise ValueError if names can't all exist together in one directory tree.

    That is when two differ only by letter case, which case-insensitive
    filesystems store as one file, or one is a directory of another.
    """
    folded = {name.casefold() for name in names}
    if len(folded) != len(names) or folded & {name.casefold() for name in parent_directories(names)}:
        raise ValueError('paths collide as files/directories or ignoring case')


def validate_no_sidecar_collisions(names):
    """Raise ValueError if a name is at another name's provenance record path.

    fetch_file(..., record_provenance=True) writes a hidden record beside each
    download, so those paths must stay free.
    """
    occupied = {name.casefold() for name in names} | {name.casefold() for name in parent_directories(names)}
    if {Path(sidecar_path(name)).as_posix().casefold() for name in names} & occupied:
        raise ValueError('paths collide with automatic provenance sidecars')


def local_file_identity(path):
    """The file:// URL identifying a local source, such as file:///data/a.fa.

    Relative paths are made absolute first, so the identity doesn't depend on
    the working directory of whichever process records it.
    """
    return Path(path).absolute().as_uri()


def require_directory(path):
    """Raise FileValidationError unless path is a directory, not a link to one."""
    if not stat.S_ISDIR(Path(path).lstat().st_mode):
        raise FileValidationError(path, 'expected a directory, not a link')
