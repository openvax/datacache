import gzip

from os.path import (exists, isdir, splitext)
from shutil import move, rmtree, copyfileobj
from tempfile import NamedTemporaryFile
import zipfile
import logging
try:
    from urllib2 import urlretrieve, urlopen
except ImportError:
    from urllib import urlretrieve, urlopen

import pandas as pd
from progressbar import ProgressBar
from Bio import SeqIO

from common import build_path, db_table_exists

def _download(filename, full_path, download_url):
    """
    Downloads remote file at `download_url` to local file at `full_path`
    """
    logging.info("Downloading %s",  download_url)

    base_name, ext = splitext(filename)
    tmp_file = NamedTemporaryFile(
        suffix='.' + ext,
        prefix = base_name,
        delete = False)
    tmp_path = tmp_file.name
    in_stream = urlopen(download_url)
    copyfileobj(in_stream, tmp_file)
    in_stream.close()
    tmp_file.close()

    if download_url.endswith("zip") and not filename.endswith("zip"):
        logging.info("Decompressing zip into %s...", filename)
        with zipfile.ZipFile(tmp_path) as z:
            extract_path = z.extract(filename)
        move(extract_path, full_path)
        remove(tmp_path)
    elif download_url.endswith("gz") and not filename.endswith("gz"):
        logging.info("Decompressing gzip into %s...", filename)
        with gzip.GzipFile(tmp_path) as src:
            contents = src.read()
        remove(tmp_path)
        with open(full_path, 'w') as dst:
            dst.write(contents)
    elif download_url.endswith(("html", "htm")):
        logging.info("Extracting HTML table into CSV %s...", filename)
        df = pd.read_html(tmp_path, header=0, infer_types=False)[0]
        df.to_csv(full_path, sep=',', index=False, encoding='utf-8')
    else:
        move(tmp_path, full_path)


def fetch_file(filename, download_url, subdir = None):
    """
    Download a remote file from `download_url` and store it locally as `filename`. 
    Returns the full path of the local file.
    """
    logging.info("Fetching %s", filename)
    full_path = build_path(filename, subdir)
    if not exists(full_path):
        _download(filename, full_path, download_url)
    return full_path

def fetch_and_transform(
        transformed_filename,
        transformer,
        loader,
        source_filename,
        source_url,
        subdir = None):
    """
    Fetch a remote file from `source_url`, save it locally as `source_filename` and then use 
    the `loader` and `transformer` function arguments to turn this saved data into an in-memory
    object. 
    """
    transformed_path = build_path(transformed_filename, subdir)
    if not exists(transformed_path):
        source_path = fetch_file(source_filename, source_url, subdir)
        result = transformer(source_path, transformed_path)
    else:
        result = loader(transformed_path)
    assert exists(transformed_path)
    return result

def fetch_csv_dataframe(filename, download_url, subdir = None, **pandas_kwargs):
    """
    Download a remote file from `download_url` and save it locally as `filename`. 
    Load that local file as a CSV into Pandas using extra keyword arguments such as sep='\t'.
    """
    path = fetch_file(filename, download_url, subdir)
    return pandas.read_csv(path, **pandas_kwargs)

def fetch_fasta_dict(filename, download_url, subdir = None):
    """
    Download a remote FASTA file from `download_url` and save it locally as `filename`. 
    Load the file using BioPython and return an index mapping from entry keys to their sequence.
    """
    fasta_path = fetch_file(filename, download_url, subdir)
    return SeqIO.index(fasta_path, 'fasta')


