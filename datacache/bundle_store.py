"""Bundle stores: directories that keep complete sets of files.

install_bundle, install_archive and materialize each keep one store directory
per dataset version, for example <root>/ensembl/110::

    <root>/ensembl/110/
        .datacache-store.json            {"format": 2, "kind": "bundle"}
        bundles/
            2026-09-30T14-11-05Z/        an older bundle
            2026-10-08T17-02-42Z/        the current bundle: always the newest
                genes.gtf
                .datacache-manifest.json what the bundle contains

A bundle is a complete set of files that belong together. An install builds
the new bundle in a hidden staging directory, checks it, then renames it into
bundles/. A rename happens completely or not at all, so everything in
bundles/ is complete, and readers see the old bundle or the new one, never a
mix. Old bundles are kept because a running program may still be reading
them; nothing deletes them automatically.

The functions after BundleStore check the files inside a bundle: their
names, their hashes, and that a bundle holds exactly the files it should.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import logging
import os
from pathlib import Path
import re
import shutil
import stat
from typing import Optional
import unicodedata

from filelock import FileLock

from ._filesystem import file_lock, open_regular, path_present, read_json, write_json
from .download import normal_creation_mode
from .inspection import FileInspection
from .integrity import FileValidationError, _validate_expectations
from .progress import Progress
from .provenance import sidecar_path

FORMAT = 2
MARKER = '.datacache-store.json'
MANIFEST = '.datacache-manifest.json'
CHUNK_SIZE = 2 ** 20
# UTC install time. Windows forbids ':', and this sorts oldest to newest.
BUNDLE_NAME_FORMAT = '%Y-%m-%dT%H-%M-%SZ'

# Exceptions meaning a store or bundle is unusable as found: corrupt,
# incomplete, foreign or unreadable. PermissionError is one of them, so callers
# that report it separately must catch it first.
INVALID_STORE_ERRORS = (OSError, ValueError, KeyError, TypeError, RecursionError)

logger = logging.getLogger(__name__)


class BundleStore:
    """The store directory for one dataset version, and the bundles in it.

    kind is 'bundle', 'archive' or 'materialization', recorded in the marker
    so that no kind of install takes over another's store. Reading never
    writes or locks. Installs hold lock() while they call create and publish.
    """

    def __init__(self, path, kind):
        self.path = Path(path)
        self.kind = kind
        self.bundles = self.path / 'bundles'
        # Beside the store, so it exists before the store does. Named for the
        # store's own name, case-folded and Unicode-normalized, so spellings a
        # filesystem treats as one directory share a lock.
        absolute = Path(os.path.abspath(self.path))
        name = unicodedata.normalize('NFC', absolute.name.casefold())
        key = hashlib.sha256(os.fsencode(name)).hexdigest()[:32]
        self.lock_path = absolute.parent / ('.datacache-lock-' + key)

    def __repr__(self):
        return 'BundleStore(%r, %r)' % (str(self.path), self.kind)

    def lock(self):
        """The lock installs into this store take turns on."""
        if os.name == 'posix':
            return file_lock(self.lock_path)
        return FileLock(str(self.lock_path))

    def can_reuse(self, inspection, *, force):
        """Whether an install can return the bundle it just inspected.

        Raises when the install can't go ahead either: the store is
        unreadable, or its current bundle is invalid and force=True wasn't
        given. A directory this store may never take over says so, rather
        than suggesting force=True.
        """
        if inspection.status == 'inaccessible':
            raise inspection.error
        if inspection.status == 'invalid' and not force:
            self.refuse_takeover()
            raise FileValidationError(
                self.path, "the current bundle is damaged or doesn't match this request; "
                "use force=True to install a new one") from inspection.error
        return not force and inspection.status == 'available'

    def check(self):
        """Raise unless path holds this kind's store marker."""
        require_directory(self.path)
        if read_json(self.path / MARKER) != {'format': FORMAT, 'kind': self.kind}:
            raise FileValidationError(self.path, 'not a datacache %s store' % self.kind)

    def bundle_names(self):
        """Names of every bundle in the store, oldest first. Other entries are ignored."""
        return sorted(entry.name for entry in self.bundles.iterdir() if is_bundle_name(entry.name))

    def current_bundle(self):
        """The newest bundle's directory, or None when nothing is installed.

        Raises one of INVALID_STORE_ERRORS when path exists but isn't a store
        of this kind. An empty directory counts as nothing installed: it's
        where the first install will create the store.
        """
        if not path_present(self.path):
            return None
        try:
            self.check()
        except FileNotFoundError:
            if not holds_files(self.path):
                return None  # Empty: the first install will create the store here.
            self.check()  # Another installer may have placed the marker since.
        if not path_present(self.bundles):
            return None  # create() places the marker first, then bundles/.
        require_directory(self.bundles)
        names = self.bundle_names()
        if not names:
            return None
        bundle = self.bundles / names[-1]
        require_directory(bundle)
        return bundle

    def create(self):
        """Make path a store of this kind, never taking over a directory.

        A missing or empty directory becomes the store in place, so it keeps
        its owner, group and permissions: first the marker, then bundles/.
        Readers see nothing installed until a bundle is published. An
        existing store of this kind gets back anything that is missing.
        Anything else raises FileValidationError (see refuse_takeover).
        """
        self.path.mkdir(exist_ok=True)
        self.refuse_takeover()
        if not path_present(self.path / MARKER):
            write_json(self.path / MARKER, {'format': FORMAT, 'kind': self.kind},
                       mode=normal_creation_mode(self.path))
        self.check()
        self.bundles.mkdir(exist_ok=True)
        require_directory(self.bundles)

    def refuse_takeover(self):
        """Raise if path can never become, or be repaired as, this kind's store.

        That is anything but a real directory, a directory holding files
        without this kind's marker, or a store whose bundles/ isn't a real
        directory. force=True never changes this, so installs check it before
        suggesting force.
        """
        if not path_present(self.path):
            return
        if not stat.S_ISDIR(self.path.lstat().st_mode):
            raise FileValidationError(self.path, 'not a directory, and never taken over')
        if not holds_files(self.path):
            return
        try:
            marker = read_json(self.path / MARKER)
        except (FileNotFoundError, ValueError, RecursionError):
            marker = None
        if marker != {'format': FORMAT, 'kind': self.kind}:
            raise FileValidationError(
                self.path, 'not a datacache %s store; a directory with files in it is never taken over' % self.kind)
        if path_present(self.bundles) and not stat.S_ISDIR(self.bundles.lstat().st_mode):
            raise FileValidationError(self.bundles, 'not a directory, and never taken over')

    def publish(self, staged):
        """Rename a complete, checked directory into bundles/ as the newest.

        Returns the new bundle's directory. It is named for the current UTC
        time, or one second after the newest bundle if the clock is behind,
        so the newest name is always the current bundle.
        """
        name = datetime.now(timezone.utc).strftime(BUNDLE_NAME_FORMAT)
        names = self.bundle_names()
        if names and name <= names[-1]:
            newest = datetime.strptime(names[-1], BUNDLE_NAME_FORMAT)
            try:
                name = (newest + timedelta(seconds=1)).strftime(BUNDLE_NAME_FORMAT)
            except OverflowError:
                raise FileValidationError(
                    self.bundles / names[-1], 'this bundle is dated so late that no newer name exists') from None
        bundle = self.bundles / name
        os.replace(staged, bundle)
        return bundle


def holds_files(directory):
    """Whether directory holds anything besides DataCache's temporary files.

    Writing a file such as the store marker briefly leaves a temporary
    .datacache-json-* or .datacache-mode-* file beside it, and an interrupted
    write can leave one behind. Neither makes a directory someone else's.
    """
    return any(not entry.name.startswith(('.datacache-json-', '.datacache-mode-'))
               for entry in Path(directory).iterdir())


def user_key():
    """Identifies the current user in the names of private working directories.

    The numeric user id on POSIX, the login name elsewhere, so two users of
    a shared store never use each other's unfinished files.
    """
    if hasattr(os, 'getuid'):
        return str(os.getuid())
    import getpass
    return getpass.getuser()


def private_directory(path):
    """Create path if needed as a directory only its owner can use, and check it.

    Installs keep unfinished files here until they publish them. Windows has
    no such modes, so there it only checks that path is a directory.
    """
    path = Path(path)
    if not path_present(path):
        path.mkdir(mode=0o700)
    require_directory(path)
    if os.name == 'posix':
        info = path.lstat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise FileValidationError(path, 'working directory must be private and owner-owned')
    return path


def discard_private_directory(path):
    """Remove a private working directory, logging rather than raising on failure.

    Cleanup never hides an install's real outcome: its error, or the bundle
    it published.
    """
    try:
        if path_present(path):
            private_directory(path)  # Only ever remove a directory of our own.
            shutil.rmtree(path)
    except (OSError, ValueError) as error:
        logger.warning('Could not remove the working directory %s: %s', path, error)


def is_bundle_name(name):
    """Whether name is a bundle's UTC install time, exactly as publish writes it."""
    try:
        return datetime.strptime(name, BUNDLE_NAME_FORMAT).strftime(BUNDLE_NAME_FORMAT) == name
    except ValueError:
        return False


def source_fingerprint(source):
    """SHA-256 identifying a source URL or path without storing its text."""
    return hashlib.sha256(source.encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class HashedFile:
    """What was read from one regular file.

    sha256 is None when the contents weren't hashed. info is the
    os.stat_result of the open file that was read, so it describes exactly
    those bytes.
    """
    path: str
    sha256: Optional[str]
    info: os.stat_result

    @property
    def size(self):
        return self.info.st_size

    @property
    def record(self):
        """The {'sha256', 'size'} entry a manifest stores for these bytes."""
        return {'sha256': self.sha256, 'size': self.size}

    def inspection(self, *, verified):
        """This file as an available FileInspection."""
        return FileInspection(self.path, 'available', verified=verified,
                              size=self.info.st_size, mtime=self.info.st_mtime)


def hash_file(path, expected=None, *, hash_contents=True, show_progress=False):
    """Hash one regular file, optionally checking it against expected values.

    The file is opened without following links; a link, a special file or a
    file with several hard links raises FileValidationError, as does a file
    that changes while it is read. hash_contents=False skips reading the
    bytes: sha256 is None and only the size is checked. expected is a mapping
    with sha256 and size; None values aren't checked.
    """
    # O_BINARY: Windows would otherwise translate line endings as it reads.
    with os.fdopen(open_regular(path, os.O_RDONLY | getattr(os, 'O_BINARY', 0)), 'rb') as handle:
        info = os.fstat(handle.fileno())
        sha256 = None
        if hash_contents:
            digest, size = hashlib.sha256(), 0
            with Progress(show_progress, 'Verifying', info.st_size) as progress:
                for chunk in iter(lambda: handle.read(CHUNK_SIZE), b''):
                    digest.update(chunk)
                    size += len(chunk)
                    progress(size, info.st_size)
            if size != info.st_size or os.fstat(handle.fileno()).st_mtime_ns != info.st_mtime_ns:
                raise FileValidationError(path, 'file changed while it was being read')
            sha256 = digest.hexdigest()
    hashed = HashedFile(os.fspath(path), sha256, info)
    if expected is not None:
        check_expected(path, hashed.record, expected)
    return hashed


def check_expected(path, record, expected):
    """Raise FileValidationError if record's bytes disagree with expected.

    Both are mappings with sha256 and size. A None value on either side
    isn't compared, and other keys in expected are ignored.
    """
    if expected.get('size') is not None and record['size'] != expected['size']:
        raise FileValidationError(path, 'size is %d bytes, expected %d' % (record['size'], expected['size']))
    if (expected.get('sha256') is not None and record['sha256'] is not None
            and record['sha256'] != expected['sha256']):
        raise FileValidationError(path, 'SHA-256 disagrees with the expected digest')


def validate_file_record(record):
    """Return record if it is a manifest entry for one file, else raise.

    A file record is exactly {'sha256': <64 hex digits>, 'size': <bytes>};
    neither may be missing, because a manifest records what was seen.
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

    Names in ignore are skipped at the root, such as a bundle's manifest.
    Raises FileValidationError for a link or special file, and ValueError
    for a name that validate_relative_name rejects.
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


def check_tree(root, files, directories, *, ignore=(), hash_contents=True, show_progress=False):
    """Check that root holds exactly the recorded files and directories.

    files maps relative names to records with sha256 and size (None values
    aren't checked); directories names every directory the tree should
    hold. The whole tree is listed before any file is read, so an unexpected
    file, directory, link or special file is rejected without hashing
    anything. Then every file is hashed (see hash_file). Returns a dict of
    names to HashedFile.
    """
    listing = list_tree(root, ignore=ignore)
    for what, recorded, found in (('files', set(files), set(listing.files)),
                                  ('directories', set(directories), set(listing.directories))):
        if recorded != found:
            raise FileValidationError(root, '%s differ from the record: missing %s, unexpected %s' % (
                what, sorted(recorded - found), sorted(found - recorded)))
    return {name: hash_file(path, files[name], hash_contents=hash_contents, show_progress=show_progress)
            for name, path in listing.files.items()}


def parent_directories(names):
    """Every directory that relative names imply: 'a/b/c' implies 'a' and 'a/b'."""
    return {'/'.join(parts[:index]) for parts in (name.split('/') for name in names)
            for index in range(1, len(parts))}


def validate_relative_name(name):
    """Return name if it is a safe relative POSIX path inside a bundle.

    Rejects empty names and components, backslashes, '.' and '..', names
    Windows can't store (reserved device names, control and punctuation
    characters, trailing dots or spaces) and DataCache's own '.datacache-*'
    names. Raises ValueError otherwise.
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

    That is when two paths, or any of their parent directories, differ only
    by letter case, which case-insensitive filesystems store as one; or when
    one name is a directory of another.
    """
    seen = {}  # Case-folded path: (path as written, whether it is a file).
    for name in names:
        parts = name.split('/')
        for depth in range(1, len(parts) + 1):
            path, is_file = '/'.join(parts[:depth]), depth == len(parts)
            key = path.casefold()
            # Only the same directory, spelled the same way, may appear twice.
            if key in seen and (is_file or seen[key] != (path, False)):
                raise ValueError('paths collide as files/directories or ignoring case')
            seen[key] = (path, is_file)


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
