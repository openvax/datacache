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

"""Download, verify and cache files and datasets.

Public names are imported from their submodules on first access (PEP 562), so
``import datacache`` loads no HTTP, SQLite or file-locking code until a name
that needs it is used. ``from datacache import fetch_file``,
``datacache.fetch_file`` and ``datacache.download`` work as before.
"""

import importlib
from typing import TYPE_CHECKING

from .version import __version__

if TYPE_CHECKING:
    from .archives import ArchiveInspection, VersionedArchiveRegistry, inspect_archive, install_archive
    from .bundle_store import list_bundles, prune_bundles
    from .bundles import BundleInspection, VersionedDatasetRegistry, inspect_bundle, install_bundle
    from .cache import Cache
    from .common import (
        build_local_filename,
        build_path,
        clear_cache,
        ensure_dir,
        get_cache_root,
        get_data_dir,
        resolve_path,
    )
    from .database_helpers import (
        connect_if_correct_version,
        db_from_dataframe,
        db_from_dataframes,
        db_from_dataframes_with_absolute_path,
        fetch_csv_db,
        recover_cached_database,
    )
    from .download import (
        expected_path,
        fetch_and_transform,
        fetch_bytes,
        fetch_csv_dataframe,
        fetch_file,
        file_exists,
    )
    from .file_registry import VersionedFileRegistry
    from .inspection import CacheInspection, FileInspection, inspect_file, inspect_files
    from .integrity import FileValidationError, validate_file
    from .materialization import MaterializationInspection, inspect_materialization, materialize
    from .permissions import make_file_readable
    from .resume import CannotResume, discard_partial
    from .sqlite_recovery import restore_database
    from .zip_members import install_zip_member

# The submodule that defines each public name. ``__getattr__`` imports it the
# first time the name is used.
EXPORT_MODULES = {
    'ArchiveInspection': 'archives',
    'VersionedArchiveRegistry': 'archives',
    'inspect_archive': 'archives',
    'install_archive': 'archives',
    'list_bundles': 'bundle_store',
    'prune_bundles': 'bundle_store',
    'BundleInspection': 'bundles',
    'VersionedDatasetRegistry': 'bundles',
    'inspect_bundle': 'bundles',
    'install_bundle': 'bundles',
    'Cache': 'cache',
    'build_local_filename': 'common',
    'build_path': 'common',
    'clear_cache': 'common',
    'ensure_dir': 'common',
    'get_cache_root': 'common',
    'get_data_dir': 'common',
    'resolve_path': 'common',
    'connect_if_correct_version': 'database_helpers',
    'db_from_dataframe': 'database_helpers',
    'db_from_dataframes': 'database_helpers',
    'db_from_dataframes_with_absolute_path': 'database_helpers',
    'fetch_csv_db': 'database_helpers',
    'recover_cached_database': 'database_helpers',
    'expected_path': 'download',
    'fetch_and_transform': 'download',
    'fetch_bytes': 'download',
    'fetch_csv_dataframe': 'download',
    'fetch_file': 'download',
    'file_exists': 'download',
    'VersionedFileRegistry': 'file_registry',
    'CacheInspection': 'inspection',
    'FileInspection': 'inspection',
    'inspect_file': 'inspection',
    'inspect_files': 'inspection',
    'FileValidationError': 'integrity',
    'validate_file': 'integrity',
    'MaterializationInspection': 'materialization',
    'inspect_materialization': 'materialization',
    'materialize': 'materialization',
    'make_file_readable': 'permissions',
    'CannotResume': 'resume',
    'discard_partial': 'resume',
    'restore_database': 'sqlite_recovery',
    'install_zip_member': 'zip_members',
}

__all__ = [
    'install_zip_member',
    '__version__',
    'fetch_file',
    'fetch_bytes',
    'discard_partial',
    'CannotResume',
    'BundleInspection',
    'MaterializationInspection',
    'inspect_materialization',
    'materialize',
    'ArchiveInspection',
    'VersionedArchiveRegistry',
    'VersionedDatasetRegistry',
    'VersionedFileRegistry',
    'inspect_bundle',
    'install_bundle',
    'list_bundles',
    'prune_bundles',
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
    'recover_cached_database',
    'restore_database',
    'ensure_dir',
    'get_data_dir',
    'get_cache_root',
    'build_path',
    'clear_cache',
    'build_local_filename',
    'Cache',
]


def __getattr__(name):
    """Import a public name or a submodule the first time it is used (PEP 562).

    Python calls this only for names not yet in the package namespace. The
    result is stored there, so later lookups do not come back here and
    ``datacache.fetch_file`` keeps naming the object it named on first use.

    Parameters
    ----------
    name : str
        A name from ``__all__``, or the name of a public submodule such as
        ``"download"`` or ``"bundle_store"``.

    Returns
    -------
    object
        The public object, or the submodule ``datacache.<name>``.

    Raises
    ------
    AttributeError
        If ``name`` is neither a public name nor a public submodule.
    """
    if name in EXPORT_MODULES:
        module = importlib.import_module("." + EXPORT_MODULES[name], __name__)
        value = getattr(module, name)
    elif name.startswith("_"):
        raise AttributeError("module %r has no attribute %r" % (__name__, name))
    else:
        # Before 1.30, ``import datacache`` imported every submodule, so code
        # could use e.g. ``datacache.download`` without importing it.
        try:
            value = importlib.import_module("." + name, __name__)
        except ModuleNotFoundError as error:
            if error.name != "%s.%s" % (__name__, name):
                raise
            raise AttributeError(
                "module %r has no attribute %r" % (__name__, name)) from None
    globals()[name] = value
    return value


def __dir__():
    """List the package namespace, including public names not imported yet.

    Returns
    -------
    list of str
        Sorted names, as ``dir(datacache)`` and interactive completion show them.
    """
    return sorted(set(globals()) | set(__all__))
