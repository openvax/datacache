"""Keep ``import datacache`` cheap enough for command-line tools."""

import json
import subprocess
import sys

import pytest

from datacache import download

# Modules that ``import datacache`` loaded before 1.30 although finding a cache
# directory needs none of them: urllib.request brings in http.client, email
# and ssl; sqlite3 and filelock serve databases and bundle locks.
NETWORK_AND_STORAGE_MODULES = {"urllib.request", "http.client", "ssl", "email.parser", "sqlite3", "filelock"}


def modules_imported_by(code):
    """Run ``code`` in a fresh interpreter and list the modules it imported.

    Modules loaded before ``code`` runs, such as those that coverage
    measurement of subprocesses loads at start-up, are not counted.

    Parameters
    ----------
    code : str
        Python source to execute with ``python -c``.

    Returns
    -------
    list of str
        Sorted names of the modules added to ``sys.modules`` while ``code`` ran.
    """
    script = ("import json, sys\nbefore = set(sys.modules)\n%s\n"
              "print(json.dumps(sorted(set(sys.modules) - before)))" % code)
    output = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True).stdout
    return json.loads(output.splitlines()[-1])


def datacache_modules(modules):
    """Select ``datacache`` and its submodules from a list of module names.

    Parameters
    ----------
    modules : iterable of str
        Fully qualified module names.

    Returns
    -------
    list of str
        The names that are ``datacache`` or start with ``datacache.``, in order.
    """
    return [name for name in modules if name == "datacache" or name.startswith("datacache.")]


def test_importing_datacache_loads_no_heavy_dependencies():
    # Command-line tools resolve cache paths on every run; pandas alone costs
    # hundreds of milliseconds. Import in a fresh interpreter to measure it.
    imported = modules_imported_by("import datacache")
    assert set(imported) & {"pandas", "numpy", "requests", "urllib3", "tqdm"} == set()


def test_importing_datacache_loads_only_the_version_module():
    assert datacache_modules(modules_imported_by("import datacache")) == ["datacache", "datacache.version"]


def test_get_cache_root_loads_no_network_or_storage_modules():
    # The acceptance check from openvax/datacache#142: pyensembl's --release
    # tab completion only needs the cache root.
    imported = modules_imported_by('import datacache; datacache.get_cache_root("pyensembl")')
    assert set(imported) & NETWORK_AND_STORAGE_MODULES == set()
    assert datacache_modules(imported) == ["datacache", "datacache.common", "datacache.version"]


@pytest.mark.parametrize("code", [
    "import datacache.common",
    "from datacache import get_data_dir, build_local_filename",
    "from datacache import FileValidationError, inspect_file",
    "from datacache import fetch_file",
    "import datacache.download",
    "import datacache, tempfile; datacache.Cache('example', cache_root=tempfile.mkdtemp())",
    "from datacache import VersionedDatasetRegistry, VersionedFileRegistry, materialize",
])
def test_names_that_do_not_open_databases_load_no_network_or_storage_modules(code):
    # File and FTP downloads import urllib.request when they run; HTTP(S) goes
    # through requests, and only non-POSIX bundle locks use filelock.
    assert set(modules_imported_by(code)) & NETWORK_AND_STORAGE_MODULES == set()


def test_database_names_import_their_submodule_on_first_use():
    # Positive control: the checks above see modules that the code imports.
    imported = modules_imported_by("from datacache import connect_if_correct_version")
    assert "datacache.database_helpers" in imported


def test_download_module_still_exposes_pandas_and_requests():
    import pandas
    import requests
    assert download.pd is pandas
    assert download.requests is requests
    with pytest.raises(AttributeError):
        download.not_an_attribute
