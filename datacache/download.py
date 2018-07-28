# Copyright (c) 2015-2018. Mount Sinai School of Medicine
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

from __future__ import print_function, division, absolute_import

import gzip
import logging
import os
import subprocess
from shutil import move
from tempfile import NamedTemporaryFile
import zipfile


import requests
import pandas as pd
from six.moves import urllib

from .common import build_path, build_local_filename

logger = logging.getLogger(__name__)


def _download(download_url, timeout=None):
    if download_url.startswith("http"):
        response = requests.get(download_url, timeout=timeout)
        response.raise_for_status()
        return response.content
    else:
        req = urllib.request.Request(download_url)
        response = urllib.request.urlopen(req, data=None, timeout=timeout)
        return response.read()


def _download_to_temp_file(
        download_url,
        timeout=None,
        base_name="download",
        ext="tmp",
        use_wget_if_available=False):

    if not download_url:
        raise ValueError("URL not provided")

    with NamedTemporaryFile(
            suffix='.' + ext,
            prefix=base_name,
            delete=False) as tmp:
        tmp_path = tmp.name

    def download_using_python():
        with open(tmp_path, mode="w+b") as tmp_file:
            tmp_file.write(
                _download(download_url, timeout=timeout))

    if not use_wget_if_available:
        download_using_python()
    else:
        try:
            # first try using wget to download since this works on Travis
            # even when FTP otherwise fails
            wget_command_list = [
                "wget",
                download_url,
                "-O", tmp_path,
                "--no-verbose",
            ]
            if download_url.startswith("ftp"):
                wget_command_list.extend(["--passive-ftp"])
            if timeout:
                wget_command_list.extend(["-T", "%s" % timeout])
            logger.info("Running: %s" % (" ".join(wget_command_list)))
            subprocess.call(wget_command_list)
        except OSError as e:
            if e.errno == os.errno.ENOENT:
                # wget not found
                download_using_python()
            else:
                raise
    return tmp_path


def _download_and_decompress_if_necessary(
        full_path,
        download_url,
        timeout=None,
        use_wget_if_available=False):
    """
    Downloads remote file at `download_url` to local file at `full_path`
    """
    logger.info("Downloading %s to %s", download_url, full_path)
    filename = os.path.split(full_path)[1]
    base_name, ext = os.path.splitext(filename)
    tmp_path = _download_to_temp_file(
        download_url=download_url,
        timeout=timeout,
        base_name=base_name,
        ext=ext,
        use_wget_if_available=use_wget_if_available)

    if download_url.endswith("zip") and not filename.endswith("zip"):
        logger.info("Decompressing zip into %s...", filename)
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
        os.remove(tmp_path)
    elif download_url.endswith("gz") and not filename.endswith("gz"):
        logger.info("Decompressing gzip into %s...", filename)
        with gzip.GzipFile(tmp_path) as src:
            contents = src.read()
        os.remove(tmp_path)
        with open(full_path, 'wb') as dst:
            dst.write(contents)
    elif download_url.endswith(("html", "htm")) and full_path.endswith(".csv"):
        logger.info("Extracting HTML table into CSV %s...", filename)
        df = pd.read_html(tmp_path, header=0)[0]
        df.to_csv(full_path, sep=',', index=False, encoding='utf-8')
    else:
        move(tmp_path, full_path)


def file_exists(
        download_url,
        filename=None,
        decompress=False,
        subdir=None):
    """
    Return True if a local file corresponding to these arguments
    exists.
    """
    filename = build_local_filename(download_url, filename, decompress)
    full_path = build_path(filename, subdir)
    return os.path.exists(full_path)


def fetch_file(
        download_url,
        filename=None,
        decompress=False,
        subdir=None,
        force=False,
        timeout=None,
        use_wget_if_available=False):
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

    timeout : float, optional
        Timeout for download in seconds, default is None which uses
        global timeout.

    use_wget_if_available: bool, optional
        If the `wget` command is available, use that for download instead
        of Python libraries (default True)

    Returns the full path of the local file.
    """
    filename = build_local_filename(download_url, filename, decompress)
    full_path = build_path(filename, subdir)
    if not os.path.exists(full_path) or force:
        logger.info("Fetching %s from URL %s", filename, download_url)
        _download_and_decompress_if_necessary(
            full_path=full_path,
            download_url=download_url,
            timeout=timeout,
            use_wget_if_available=use_wget_if_available)
    else:
        logger.info("Cached file %s from URL %s", filename, download_url)
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
    if not os.path.exists(transformed_path):
        source_path = fetch_file(source_url, source_filename, subdir)
        logger.info("Generating data file %s from %s", transformed_path, source_path)
        result = transformer(source_path, transformed_path)
    else:
        logger.info("Cached data file: %s", transformed_path)
        result = loader(transformed_path)
    assert os.path.exists(transformed_path)
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
