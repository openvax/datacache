
from os import makedirs, remove, environ
from os.path import (join, exists)
from shutil import move, rmtree, copyfileobj
import logging

import appdirs 

def ensure_dir(path):
    if not exists(path):
        makedirs(path)

def get_data_dir(subdir = None, envkey =  None):
    if envkey and envkey in environ:
        return environ[envkey]
    if subdir is None:
        subdir = "datacache"
    return appdirs.user_cache_dir(subdir)

def build_path(filename, subdir = None):
    data_dir = get_data_dir(subdir)
    ensure_dir(data_dir)
    return join(data_dir, filename)

def clear_cache(subdir = None):
    data_dir = get_data_dir(subdir)
    rmtree(data_dir)
