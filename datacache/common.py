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

COMPRESSION_SUFFIXES = (".gz", ".zip")


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
    root instead, with subdir appended when supplied.
    """
    if envkey and environ.get(envkey):
        envdir = environ[envkey]
        if subdir:
            return join(envdir, subdir)
        else:
            return envdir
    return appdirs.user_cache_dir(subdir if subdir else "datacache")

def resolve_path(filename, subdir=None, *, cache_root=None):
    """Resolve a cache path without filesystem access or directory creation.

    cache_root is the directory containing cached files, overriding the appdirs
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

    return filename
