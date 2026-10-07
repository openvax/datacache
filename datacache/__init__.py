# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .download import (
    expected_path, file_exists, fetch_bytes, fetch_file, fetch_and_transform, fetch_csv_dataframe,
)
from .integrity import FileValidationError, validate_file
from .inspection import CacheInspection, FileInspection, inspect_file, inspect_files
from .permissions import make_file_readable
from .database_helpers import (
    db_from_dataframe,
    db_from_dataframes,
    db_from_dataframes_with_absolute_path,
    fetch_csv_db,
    connect_if_correct_version
)
from .common import (
    ensure_dir,
    get_data_dir,
    get_cache_root,
    build_path,
    resolve_path,
    clear_cache,
    build_local_filename
)
from .cache import Cache
from .resume import discard_partial
from .bundles import BundleInspection, VersionedDatasetRegistry, inspect_bundle, install_bundle
from .archives import (
    ArchiveInspection, VersionedArchiveRegistry, inspect_archive, install_archive)
from .file_registry import VersionedFileRegistry
from .version import __version__

__all__ = [
    '__version__',
    'fetch_file',
    'fetch_bytes',
    'discard_partial',
    'BundleInspection',
    'ArchiveInspection',
    'VersionedArchiveRegistry',
    'VersionedDatasetRegistry',
    'VersionedFileRegistry',
    'inspect_bundle',
    'install_bundle',
    'inspect_archive',
    'install_archive',
    'expected_path',
    'file_exists',
    'resolve_path',
    'FileInspection',
    'CacheInspection',
    'inspect_file',
    'inspect_files',
    'FileValidationError',
    'validate_file',
    'make_file_readable',
    'fetch_and_transform',
    'fetch_csv_dataframe',
    'db_from_dataframe',
    'db_from_dataframes',
    'db_from_dataframes_with_absolute_path',
    'fetch_csv_db',
    'connect_if_correct_version',
    'ensure_dir',
    'get_data_dir',
    'get_cache_root',
    'build_path',
    'clear_cache',
    'build_local_filename',
    'Cache',
]
