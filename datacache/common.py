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

import hashlib
import os
from os import makedirs, environ
from os.path import join, exists, split, splitext
import re
from urllib.parse import parse_qsl, urlsplit

from shutil import rmtree
import appdirs

from .version import __version__

COMPRESSION_SUFFIXES = (".gz", ".zip")

# Sent on every HTTP(S) request. Some data hosts (IEDB among them) reject any
# User-Agent containing Requests' default "python-requests" token, so this
# names datacache alone rather than extending that default.
USER_AGENT = "datacache/%s (+https://github.com/openvax/datacache)" % __version__

# Longest file name ext4 accepts, in UTF-8 bytes. APFS and NTFS instead limit
# characters or UTF-16 units, which a UTF-8 byte count never undercounts.
MAX_NAME_BYTES = 255


def _source_suffix(download_url):
    """Prefer a supported path suffix, then a filename in the final query value.

    This supports IEDB/pepdata download endpoints without interpreting bare
    format hints such as ?format=.gz or fragments as a file format.
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
    """Create path and its parents unless something already exists there.

    An existing path is left alone, even if it is not a directory. A directory
    another process creates at the same moment is fine; a broken symlink raises
    FileExistsError, since no directory can be created in its place.
    """
    if not exists(path):
        makedirs(path, exist_ok=True)

def get_data_dir(subdir=None, envkey=None):
    """Return the platform cache directory for an application, without creating it.

    subdir is the application name, "datacache" when omitted or empty. If the
    environment variable named by envkey is set and nonempty, its value is the
    root instead, with subdir appended when supplied. To use a variable's value
    as the root itself, see get_cache_root.
    """
    if envkey and environ.get(envkey):
        envdir = environ[envkey]
        if subdir:
            return join(envdir, subdir)
        else:
            return envdir
    return appdirs.user_cache_dir(subdir if subdir else "datacache")


def get_cache_root(name, *envkeys, override=None, legacy=()):
    """Return where name's cached data lives on this machine.

    The first of these that applies, with ~ expanded:

    1. override, when not None: an explicit choice, such as a command-line flag;
    2. the first environment variable in envkeys set to a non-blank value;
    3. the platform's cache directory for name, when it already holds data;
    4. the first path in legacy that already holds data, so a cache made
       before a move keeps working;
    5. the platform's cache directory for name.

    A directory holds data when it contains a file anywhere inside, other than
    files operating systems or DataCache leave on their own (.DS_Store,
    Thumbs.db, desktop.ini, .datacache-*). Empty folders don't count, and
    unreadable entries are skipped. legacy paths must be absolute; a relative
    one would depend on the working directory. Unlike get_data_dir(subdir,
    envkey), nothing is appended to an environment value, so every package
    reading the same variable agrees on one location. Nothing is created.
    The legacy check reads the disk, so resolve the root once and pass it on.
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
    platform = appdirs.user_cache_dir(name)
    if legacy and not _holds_data(platform):
        for path in legacy:
            if _holds_data(path):
                return path
    return platform


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
    """Resolve a writable cache path, creating its parent when necessary."""
    full_path = resolve_path(filename, subdir, cache_root=cache_root)
    os.makedirs(os.path.dirname(full_path) or ".", exist_ok=True)
    return full_path

def clear_cache(subdir=None):
    """Recursively delete an application's entire platform cache directory.

    The directory itself is removed too; subdir=None selects the default
    "datacache" cache. Use Cache.delete_all to empty an explicit cache_root
    while keeping it. Raises FileNotFoundError if the directory is absent.
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
    """
    Remove special characters and shorten if name is too long
    """
    # if the url pointed to a directory then just replace all the special chars
    filename = re.sub(r"/|\\|;|:|\?|=", "_", filename)

    if len(filename) > 150:
        prefix = name_digest(filename)
        filename = prefix + filename[-140:]

    return filename

def build_local_filename(download_url=None, filename=None, decompress=False):
    """
    Determine which local filename to use based on the file's source URL,
    an optional desired filename, and whether a compression suffix needs
    to be removed
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
        elif inferred and _source_suffix(download_url) in COMPRESSION_SUFFIXES:
            # Endpoint filenames may be encoded or followed by a fragment.
            # Keep raw and decompressed contents under different cache keys.
            filename += ".decompressed"

    return _fit_name(filename)
