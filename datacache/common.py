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

from dataclasses import dataclass
import hashlib
import os
from os import makedirs, environ
from os.path import join, exists, split, splitext
import re
from urllib.parse import parse_qsl, urlsplit

from shutil import rmtree
import platformdirs
from platformdirs.macos import MacOS
from platformdirs.unix import Unix

from .version import __version__

COMPRESSION_SUFFIXES = (".gz", ".zip")

# Sent on every HTTP(S) request. Some data hosts (IEDB among them) reject any
# User-Agent containing Requests' default "python-requests" token, so this
# names datacache alone rather than extending that default.
USER_AGENT = "datacache/%s (+https://github.com/openvax/datacache)" % __version__

# Longest file name ext4 accepts, in UTF-8 bytes. APFS and NTFS instead limit
# characters or UTF-16 units, which a UTF-8 byte count never undercounts.
MAX_NAME_BYTES = 255

# Shared root environment variable honored by all OpenVax/PIRL packages when
# a package-specific override is not set.
SHARED_CACHE_ENV_KEY = "OPENVAX_DATA_CACHE"
"""Default shared cache-root environment variable honored by ``resolve_cache_dir`` and the CLI."""


@dataclass(frozen=True)
class CacheLocation:
    """Resolved cache directory along with why that location was chosen.

    Attributes
    ----------
    path : str
        Expanded path to the package's cache directory (not created on disk).
    origin : str
        Machine-readable provenance: one of ``"override"``, ``"env"``,
        ``"shared_env"``, ``"platform"``, ``"legacy"``, or ``"default"``.
    detail : str
        Human-readable label suitable for CLI tables (for example the winning
        environment variable name or ``"legacy cache"``).
    """

    path: str
    origin: str
    detail: str


def format_bytes(num_bytes):
    """Format a non-negative byte count as a concise binary-prefixed string.

    Uses ``B``, ``KiB``, ``MiB``, ``GiB``, and ``TiB`` units.

    Parameters
    ----------
    num_bytes : int or float
        Non-negative byte count (booleans are rejected).

    Returns
    -------
    str
        Formatted human-readable byte string such as ``"1.5 KiB"``.

    Raises
    ------
    ValueError
        If ``num_bytes`` is negative, boolean, or not numeric.
    """
    if isinstance(num_bytes, bool) or not isinstance(num_bytes, (int, float)) or num_bytes < 0:
        raise ValueError("num_bytes must be a non-negative number")
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            if unit == "B":
                return "%d B" % int(round(value))
            return "%.1f %s" % (value, unit)
        value /= 1024.0
    return "%.1f TiB" % value


def parse_content_length(header_value):
    """Parse an HTTP ``Content-Length`` header value into a non-negative integer.

    Parameters
    ----------
    header_value : str, int, or None
        Raw ``Content-Length`` header value.

    Returns
    -------
    int or None
        Non-negative integer byte count, or ``None`` if absent, negative, or
        not a valid integer.
    """
    if header_value is None or isinstance(header_value, bool):
        return None
    try:
        length = int(header_value)
    except (TypeError, ValueError):
        return None
    return length if length >= 0 else None


def source_suffix(download_url):
    """Infer a supported lowercase file suffix from a download URL.

    Prefers a supported path suffix (``.gz``, ``.zip``, ``.html``, ``.htm``),
    then a filename suffix in the final query parameter value. This supports
    IEDB/pepdata download endpoints without interpreting bare format hints such
    as ``?format=.gz`` or fragments as a file format.

    Parameters
    ----------
    download_url : str
        Source URL to inspect.

    Returns
    -------
    str
        Lowercase suffix including the leading dot, or ``""`` if none applies.
    """
    supported = COMPRESSION_SUFFIXES + (".html", ".htm")
    parsed = urlsplit(download_url)
    path_suffix = splitext(parsed.path)[1].lower()
    if path_suffix in supported:
        return path_suffix
    query = parse_qsl(parsed.query, keep_blank_values=True)
    query_suffix = splitext(query[-1][1])[1].lower() if query else ""
    return query_suffix if query_suffix in supported else path_suffix


def ensure_dir(path):
    """Create ``path`` and its parents unless something already exists there.

    An existing path is left alone; this helper does not verify that an
    existing path is a directory. A directory another process creates at the
    same moment is accepted.

    Parameters
    ----------
    path : str or os.PathLike
        Directory path to create if absent.

    Raises
    ------
    FileExistsError
        If ``path`` is a broken symlink or a file is created there concurrently.
    OSError
        For other directory creation failures such as ``PermissionError``.
    """
    if not exists(path):
        makedirs(path, exist_ok=True)


def get_data_dir(subdir=None, envkey=None):
    """Return the platform cache directory for an application, without creating it.

    ``subdir`` is an application name (default ``"datacache"``), not a directory
    nested under datacache's own cache:

    - Linux / Unix: ``$XDG_CACHE_HOME/<subdir>``, or ``~/.cache/<subdir>`` when
      ``XDG_CACHE_HOME`` is unset, empty, or relative.
    - macOS: ``~/Library/Caches/<subdir>``, even when ``XDG_CACHE_HOME`` is set.
    - Windows: ``%LOCALAPPDATA%\\<subdir>\\<subdir>\\Cache`` (per ``platformdirs``).

    If ``envkey`` names a nonempty environment variable, that variable's value
    is used as the root instead, with ``subdir`` appended when supplied.
    ``envkey`` is opt-in; there is no implicit ``DATACACHE_*`` override for all
    APIs. To use a variable's value as the root itself without appending
    ``subdir``, see ``get_cache_root``.

    Parameters
    ----------
    subdir : str or None, optional
        Application name; defaults to ``"datacache"`` when omitted or empty.
    envkey : str or None, optional
        Optional environment variable name overriding the platform cache root.

    Returns
    -------
    str
        Selected directory path (not created on disk).

    Raises
    ------
    RuntimeError
        If ``platformdirs`` cannot determine the platform cache directory (for
        example when no home directory is available).
    TypeError
        If an argument has an unsupported type.
    """
    if envkey and environ.get(envkey):
        envdir = environ[envkey]
        if subdir:
            return join(envdir, subdir)
        else:
            return envdir
    return platform_cache_dir(subdir if subdir else "datacache")


def platform_cache_dir(name):
    """Return ``name``'s directory in the platform's user cache.

    Follows ``platformdirs.user_cache_dir(name)`` with two exceptions that keep
    the location stable across ``platformdirs`` versions:

    - macOS always uses ``~/Library/Caches/<name>``, matching ``appdirs``
      before DataCache 1.25.0 even when ``XDG_CACHE_HOME`` is set.
    - On Linux and other Unix systems, ``XDG_CACHE_HOME`` is stripped of
      surrounding whitespace and falls back to ``~/.cache`` if not absolute.

    Parameters
    ----------
    name : str
        Application or cache namespace name.

    Returns
    -------
    str
        Platform user cache directory path for ``name``.
    """
    if issubclass(platformdirs.PlatformDirs, MacOS):
        return join(os.path.expanduser("~/Library/Caches"), name)
    if issubclass(platformdirs.PlatformDirs, Unix):
        xdg = environ.get("XDG_CACHE_HOME", "").strip()
        return join(xdg if os.path.isabs(xdg) else os.path.expanduser("~/.cache"), name)
    path = platformdirs.user_cache_dir(name)
    if not os.path.isabs(path):
        return join(os.path.expanduser("~/.cache"), name)
    return path


def get_cache_root(name, *envkeys, override=None, legacy=()):
    """Return where ``name``'s cached data lives on this machine, without creating it.

    The first of these that applies is returned, with ``~`` expanded:

    1. ``override``, when not ``None``: an explicit choice, such as a
       command-line flag;
    2. the first environment variable in ``envkeys`` set to a non-blank value;
    3. the platform cache directory for ``name`` (from ``get_data_dir(name)``),
       when it already holds data;
    4. the first path in ``legacy`` that already holds data, so a cache made
       before a move keeps working;
    5. the platform cache directory for ``name``.

    A directory holds data when it contains a file anywhere inside other than
    incidental OS or DataCache files (``.DS_Store``, ``Thumbs.db``,
    ``desktop.ini``, ``.datacache-*``); empty folders do not count and
    unreadable entries are skipped. Unlike ``get_data_dir(subdir, envkey)``,
    nothing is appended to an environment value, so packages reading the same
    variable agree on one location. List a package-specific variable before a
    shared one so it can override it. The legacy check reads the disk, so
    resolve the root once and pass it on as ``cache_root``.

    Parameters
    ----------
    name : str
        Non-empty application or package name.
    *envkeys : str
        Non-empty environment variable names checked in priority order.
    override : str or os.PathLike or None, optional
        Explicit cache root taking precedence over environment and defaults.
    legacy : str or os.PathLike or iterable of (str or os.PathLike), optional
        Absolute legacy cache paths checked if the platform cache is empty.

    Returns
    -------
    str
        Resolved directory path (a relative override or env value stays
        relative to the working directory).

    Raises
    ------
    ValueError
        If ``name`` or any environment variable name is empty, ``override`` is
        empty or non-text, or any ``legacy`` path is empty, non-text, or
        relative.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string")
    if not all(isinstance(envkey, str) and envkey for envkey in envkeys):
        raise ValueError("environment variable names must be non-empty strings")
    if legacy is None:
        legacy = ()
    elif isinstance(legacy, (str, os.PathLike)):
        legacy = (legacy,)
    legacy = [_path_text(path, "legacy") for path in legacy]
    if any(not os.path.isabs(path) for path in legacy):
        raise ValueError("legacy locations must be absolute paths")
    if override is not None:
        return _path_text(override, "override")
    for envkey in envkeys:
        value = environ.get(envkey, "").strip()
        if value:
            return os.path.expanduser(value)
    platform = platform_cache_dir(name)
    if legacy and not _holds_data(platform):
        for path in legacy:
            if _holds_data(path):
                return path
    return platform


def resolve_cache_dir(
        name,
        *envkeys,
        override=None,
        legacy=(),
        shared_envkey=SHARED_CACHE_ENV_KEY,
        shared_subdir="openvax",
        append_package_to_env=False):
    """Resolve a package's cache directory and report why it was chosen.

    Creates nothing on disk. Precedence (with ``~`` expanded):

    1. ``override``, when not ``None`` (for example a CLI ``--cache-root`` flag).
    2. The first non-blank environment variable in ``envkeys``. When
       ``append_package_to_env=True`` (legacy ``get_data_dir`` semantics,
       e.g. ``PYENSEMBL_CACHE_DIR``), ``name`` is joined under the value.
    3. ``shared_envkey`` (default ``OPENVAX_DATA_CACHE``), when set and
       non-blank: ``<shared_env>/<name>``. Pass ``shared_envkey=None`` to
       disable shared-root fallback.
    4. The default platform directory (``<platform_cache_dir(shared_subdir)>/<name>``
       when ``shared_subdir`` is set, otherwise ``platform_cache_dir(name)``),
       when it already holds data.
    5. The first populated absolute path in ``legacy``, so existing caches stay
       put without forced re-downloads.
    6. The default platform directory.

    Parameters
    ----------
    name : str
        Non-empty package name.
    *envkeys : str
        Package-specific environment variable names checked in priority order.
    override : str or os.PathLike or None, optional
        Explicit cache root overriding environment variables and defaults.
    legacy : str or os.PathLike or iterable of (str or os.PathLike), optional
        Absolute legacy cache paths checked if the default platform directory
        does not yet hold data.
    shared_envkey : str or None, optional
        Shared root environment variable name. Default is
        ``SHARED_CACHE_ENV_KEY`` (``"OPENVAX_DATA_CACHE"``). Pass ``None`` to
        disable shared-root fallback.
    shared_subdir : str or None, optional
        Shared platform cache namespace (default ``"openvax"``). Pass ``None``
        to use ``platform_cache_dir(name)`` directly.
    append_package_to_env : bool, optional
        If ``True``, join ``name`` under the winning ``envkeys`` directory.
        Default is ``False``.

    Returns
    -------
    CacheLocation
        Frozen dataclass with ``path``, ``origin``, and ``detail``.

    Raises
    ------
    ValueError
        If any argument is invalid or any ``legacy`` path is relative.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string")
    if not all(isinstance(envkey, str) and envkey for envkey in envkeys):
        raise ValueError("environment variable names must be non-empty strings")
    if shared_envkey is not None and (not isinstance(shared_envkey, str) or not shared_envkey):
        raise ValueError("shared_envkey must be a non-empty string or None")
    if shared_subdir is not None and (not isinstance(shared_subdir, str) or not shared_subdir):
        raise ValueError("shared_subdir must be a non-empty string or None")
    if not isinstance(append_package_to_env, bool):
        raise ValueError("append_package_to_env must be a boolean")
    if legacy is None:
        legacy = ()
    elif isinstance(legacy, (str, os.PathLike)):
        legacy = (legacy,)
    legacy = [_path_text(path, "legacy") for path in legacy]
    if any(not os.path.isabs(path) for path in legacy):
        raise ValueError("legacy locations must be absolute paths")

    if override is not None:
        return CacheLocation(_path_text(override, "override"), "override", "explicit override")

    for envkey in envkeys:
        value = environ.get(envkey, "").strip()
        if value:
            expanded = os.path.expanduser(value)
            path = join(expanded, name) if append_package_to_env else expanded
            return CacheLocation(path, "env", envkey)

    if shared_envkey:
        shared_val = environ.get(shared_envkey, "").strip()
        if shared_val:
            return CacheLocation(join(os.path.expanduser(shared_val), name), "shared_env", shared_envkey)

    default_path = (
        join(platform_cache_dir(shared_subdir), name)
        if shared_subdir else platform_cache_dir(name)
    )
    if _holds_data(default_path):
        detail = "shared platform cache" if shared_subdir else "platform cache"
        return CacheLocation(default_path, "platform", detail)

    for path in legacy:
        if _holds_data(path):
            return CacheLocation(path, "legacy", "legacy cache")

    return CacheLocation(default_path, "default", "default")


def _path_text(value, what):
    """value as a stripped, ~-expanded text path, or ValueError naming what."""
    if not isinstance(value, (str, os.PathLike)) or not isinstance(os.fspath(value), str):
        raise ValueError("%s must be a text path, not %r" % (what, value))
    text = os.fspath(value).strip()
    if not text:
        raise ValueError("%s must be a non-empty path" % what)
    return os.path.expanduser(text)


# Files operating systems and DataCache leave in a folder on their own.
_INCIDENTAL_FILES = (".DS_Store", "Thumbs.db", "desktop.ini")


def _holds_data(directory):
    """Whether directory has a file anywhere inside, besides incidental ones."""
    pending = [directory]
    while pending:
        try:
            with os.scandir(pending.pop()) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(entry.path)
                        elif (entry.is_file() and entry.name not in _INCIDENTAL_FILES
                              and not entry.name.startswith(".datacache-")):
                            return True
                    except OSError:
                        continue  # An unreadable entry says nothing either way.
        except OSError:
            continue
    return False


def resolve_path(filename, subdir=None, *, cache_root=None):
    """Resolve a cache path without filesystem access or directory creation.

    cache_root is the directory containing cached files, overriding the platform
    location selected by subdir. Relative roots remain relative to the cwd.
    """
    data_dir = get_data_dir(subdir) if cache_root is None else os.fspath(cache_root)
    return join(data_dir, os.fspath(filename))


def build_path(filename, subdir=None, *, cache_root=None):
    """Resolve a writable cache path and create its parent directories if needed.

    Does not create or validate the file itself. Use ``expected_path`` or
    ``Cache.local_path(download=False)`` for lookup without filesystem writes.

    Parameters
    ----------
    filename : str or os.PathLike
        Relative cache filename under the selected root.
    subdir : str or None, optional
        Application name passed to ``get_data_dir`` when ``cache_root`` is
        omitted.
    cache_root : str or os.PathLike or None, optional
        Explicit directory containing cached files, overriding ``subdir``.

    Returns
    -------
    str
        Resolved path string.

    Raises
    ------
    ValueError
        If the path arguments are invalid.
    OSError
        If creating parent directories fails.
    """
    full_path = resolve_path(filename, subdir, cache_root=cache_root)
    os.makedirs(os.path.dirname(full_path) or ".", exist_ok=True)
    return full_path


def clear_cache(subdir=None):
    """Recursively delete an application's entire platform cache directory.

    Deletes the entire directory selected by ``get_data_dir(subdir)``,
    including the root itself. ``subdir=None`` selects the default
    ``"datacache"`` cache. Deletion is not transactional and may partially
    complete on error. To empty an explicit ``cache_root`` while preserving the
    root directory, use ``Cache.delete_all``.

    Parameters
    ----------
    subdir : str or None, optional
        Application name selecting the platform cache directory to remove.

    Raises
    ------
    FileNotFoundError
        If the cache directory does not exist.
    OSError
        For permission failures, non-directory roots, or a root symlink
        (rejected by ``shutil.rmtree``).
    """
    data_dir = get_data_dir(subdir)
    rmtree(data_dir)

def name_digest(text):
    """MD5 hex digest used in cache names: a key, not a security measure."""
    return hashlib.md5(text.encode("utf-8", "surrogatepass"), usedforsecurity=False).hexdigest()


def name_length(name):
    """Measure a file name in UTF-8 bytes, so a name within limits fits anywhere."""
    return len(name.encode("utf-8", "surrogatepass"))


def _fit_name(filename):
    """Shorten a cache name the filesystem could not hold, keeping its end.

    normalize_filename shortens by characters, so a non-ASCII name can still
    exceed ext4's 255 bytes and could never be created on Linux. Keep a digest
    of such a name plus as much of its end, and so its extension, as fits.
    Every name within the limit is unchanged, so existing cache keys still work.
    """
    if name_length(filename) <= MAX_NAME_BYTES:
        return filename
    digest = name_digest(filename)
    budget = MAX_NAME_BYTES - len(digest)
    start, used = len(filename), 0
    while start > 0 and used + name_length(filename[start - 1]) <= budget:
        start -= 1
        used += name_length(filename[start])
    return digest + filename[start:]


def normalize_filename(filename):
    """Make filename safe to use as one cache file name.

    The characters / \\ ; : ? and = become underscores. A result longer than
    150 characters is replaced by an MD5 digest of it followed by its last 140
    characters. build_local_filename applies this and then also limits the
    name's UTF-8 byte length, so use that for real cache keys.

    Returns the normalized name; does not touch the filesystem.
    """
    # if the url pointed to a directory then just replace all the special chars
    filename = re.sub(r"/|\\|;|:|\?|=", "_", filename)

    if len(filename) > 150:
        prefix = name_digest(filename)
        filename = prefix + filename[-140:]

    return filename

def build_local_filename(download_url=None, filename=None, decompress=False):
    """Choose a cache key filename from a URL or an explicit filename.

    At least one of ``download_url`` or ``filename`` must be nonempty. An
    explicit ``filename`` takes precedence; otherwise the name combines an MD5
    digest of the full URL (for naming, not integrity checking) with the URL
    text. Special characters ``/``, ``\\``, ``;``, ``:``, ``?``, and ``=`` are
    replaced with underscores, long names are shortened with a digest, and any
    name exceeding 255 UTF-8 bytes keeps a digest plus as much of its end
    (including the extension) as fits. This is a cache key, not an exact path.

    With ``decompress=True``, a trailing ``.gz`` or ``.zip`` suffix is removed;
    an inferred archive endpoint without a removable suffix gets
    ``.decompressed`` instead so archive and decompressed keys never collide.
    No filesystem or network access occurs.

    Parameters
    ----------
    download_url : str or None, optional
        Source URL used when ``filename`` is omitted or to detect archive
        endpoints when decompressing.
    filename : str or None, optional
        Explicit cache key name. Takes precedence over ``download_url``.
    decompress : bool, optional
        If ``True``, select the decompressed cache key. Default is ``False``.

    Returns
    -------
    str
        Normalized cache filename (not a directory path).

    Raises
    ------
    ValueError
        If neither ``download_url`` nor ``filename`` is supplied.
    """
    if not (download_url or filename):
        raise ValueError("Either filename or URL must be specified")

    inferred = not filename
    # if no filename provided, use the original filename on the server
    if not filename:
        digest = name_digest(download_url)
        filename_url = download_url
        if decompress:
            parsed = urlsplit(download_url)
            if splitext(parsed.path)[1].lower() in COMPRESSION_SUFFIXES:
                # Keep the full URL in the digest, but let the compression
                # suffix be stripped from the display name. Default archive
                # keys remain unchanged, including their query/fragment text.
                filename_url = download_url.split("?", 1)[0].split("#", 1)[0]
        parts = split(filename_url)
        filename = digest + "." + "_".join(parts)

    filename = normalize_filename(filename)

    if decompress:
        (base, ext) = splitext(filename)
        if ext.lower() in COMPRESSION_SUFFIXES:
            filename = base
        elif inferred and source_suffix(download_url) in COMPRESSION_SUFFIXES:
            # Endpoint filenames may be encoded or followed by a fragment.
            # Keep raw and decompressed contents under different cache keys.
            filename += ".decompressed"

    return _fit_name(filename)
