# Copyright (c) 2015. Mount Sinai School of Medicine
#
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

import gzip
import logging
from os import remove
from os.path import exists, splitext
from shutil import move
from tempfile import NamedTemporaryFile
import zipfile


import requests
import pandas as pd

from .common import build_path, build_local_filename

try:
    import urllib.request
    import urllib.error
    import urllib.parse

    # Python 3
    def urllib_response(url):
        req = urllib.request.Request(url)
        return urllib.request.urlopen(req)
except ImportError:
    # Python 2
    import urllib2

    def urllib_response(url):
        req = urllib2.Request(url)
        return urllib2.urlopen(req)

def _download(filename, full_path, download_url):
    """
    Downloads remote file at `download_url` to local file at `full_path`
    """
    print("Downloading %s to %s" % (download_url, full_path))

    base_name, ext = splitext(filename)
    if download_url.startswith("http"):
        response = requests.get(download_url)
        response.raise_for_status()
        data = response.content
    else:
        response = urllib_response(download_url)
        data = response.read()
    tmp_file = NamedTemporaryFile(
        suffix='.' + ext,
        prefix=base_name,
        delete=False)
    tmp_file.write(data)
    tmp_path = tmp_file.name
    tmp_file.close()

    if download_url.endswith("zip") and not filename.endswith("zip"):
        logging.info("Decompressing zip into %s...", filename)
        with zipfile.ZipFile(tmp_path) as z:
            names = z.namelist()
            assert len(names) > 0, "Empty zip archive"
            if filename in names:
                chosen_filename = filename
            else:
                # If zip archive contains multiple files, choose the biggest.
                biggest_size = 0
                chosen_filename = names[0]
                for info in z.infolist():
                    if info.file_size > biggest_size:
                        chosen_filename = info.filename
                        biggest_size = info.file_size
            extract_path = z.extract(chosen_filename)
        move(extract_path, full_path)
        remove(tmp_path)
    elif download_url.endswith("gz") and not filename.endswith("gz"):
        logging.info("Decompressing gzip into %s...", filename)
        with gzip.GzipFile(tmp_path) as src:
            contents = src.read()
        remove(tmp_path)
        with open(full_path, 'wb') as dst:
            dst.write(contents)
    elif download_url.endswith(("html", "htm")):
        logging.info("Extracting HTML table into CSV %s...", filename)
        df = pd.read_html(tmp_path, header=0, infer_types=False)[0]
        df.to_csv(full_path, sep=',', index=False, encoding='utf-8')
    else:
        move(tmp_path, full_path)


def file_exists(download_url,
                filename=None,
                decompress=False,
                subdir=None):
    """
    Return True if a local file corresponding to these arguments
    exists.
    """
    filename = build_local_filename(download_url, filename, decompress)
    full_path = build_path(filename, subdir)
    return exists(full_path)


def fetch_file(
        download_url,
        filename=None,
        decompress=False,
        subdir=None,
        force=False):
    """
    Download a remote file and store it locally in a cache directory. Don't
    download it again if it's already present (unless `force` is True.)

    Parameters
    ----------
    download_url : str
        Remote URL of file to download.

    filename : str, optional
        Local filename, used as cache key. If omitted, then determine the local
        filename from the URL.

    decompress : bool, optional
        By default any file whose remote extension is one of (".zip", ".gzip")
        and whose local filename lacks this suffix is decompressed. If a local
        filename wasn't provided but you still want to decompress the stored
        data then set this option to True.

    subdir : str, optional
        Group downloads in a single subdirectory.

    force : bool, optional
        By default, a remote file is not downloaded if it's already present.
        However, with this argument set to True, it will be overwritten.

    Returns the full path of the local file.
    """
    filename = build_local_filename(download_url, filename, decompress)
    full_path = build_path(filename, subdir)
    if not exists(full_path) or force:
        logging.info("Fetching %s from URL %s", filename, download_url)
        _download(filename, full_path, download_url)
    else:
        logging.info("Cached file %s from URL %s", filename, download_url)
    return full_path


def fetch_and_transform(
        transformed_filename,
        transformer,
        loader,
        source_filename,
        source_url,
        subdir=None):
    """
    Fetch a remote file from `source_url`, save it locally as `source_filename` and then use
    the `loader` and `transformer` function arguments to turn this saved data into an in-memory
    object.
    """
    transformed_path = build_path(transformed_filename, subdir)
    if not exists(transformed_path):
        source_path = fetch_file(source_url, source_filename, subdir)
        logging.info("Generating data file %s from %s", transformed_path, source_path)
        result = transformer(source_path, transformed_path)
    else:
        logging.info("Cached data file: %s", transformed_path)
        result = loader(transformed_path)
    assert exists(transformed_path)
    return result


def fetch_csv_dataframe(
        download_url,
        filename=None,
        subdir=None,
        **pandas_kwargs):
    """
    Download a remote file from `download_url` and save it locally as `filename`.
    Load that local file as a CSV into Pandas using extra keyword arguments such as sep='\t'.
    """
    path = fetch_file(
        download_url=download_url,
        filename=filename,
        decompress=True,
        subdir=subdir)
    return pd.read_csv(path, **pandas_kwargs)
