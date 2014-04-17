# Copyright (c) 2014. Mount Sinai School of Medicine
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
