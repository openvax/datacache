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
from os import makedirs, environ
from os.path import join, exists, split, splitext
import re
from urllib.parse import urlsplit

from shutil import rmtree
import appdirs

COMPRESSION_SUFFIXES = (".gz", ".zip")

def ensure_dir(path):
    if not exists(path):
        makedirs(path)

def get_data_dir(subdir=None, envkey=None):
    if envkey and environ.get(envkey):
        envdir = environ[envkey]
        if subdir:
            return join(envdir, subdir)
        else:
            return envdir
    return appdirs.user_cache_dir(subdir if subdir else "datacache")

def build_path(filename, subdir=None):
    data_dir = get_data_dir(subdir)
    ensure_dir(data_dir)
    return join(data_dir, filename)

def clear_cache(subdir=None):
    data_dir = get_data_dir(subdir)
    rmtree(data_dir)

def normalize_filename(filename):
    """
    Remove special characters and shorten if name is too long
    """
    # if the url pointed to a directory then just replace all the special chars
    filename = re.sub(r"/|\\|;|:|\?|=", "_", filename)

    if len(filename) > 150:
        prefix = hashlib.md5(filename.encode('utf-8')).hexdigest()
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

    # if no filename provided, use the original filename on the server
    if not filename:
        digest = hashlib.md5(download_url.encode('utf-8')).hexdigest()
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

    return filename
