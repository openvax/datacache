"""Keep ``import datacache`` cheap enough for command-line tools."""

import subprocess
import sys

import pytest

from datacache import download


def test_importing_datacache_loads_no_heavy_dependencies():
    # Command-line tools resolve cache paths on every run; pandas alone costs
    # hundreds of milliseconds. Import in a fresh interpreter to measure it.
    loaded = subprocess.run(
        [sys.executable, "-c",
         "import sys, datacache; "
         "print(sorted(m for m in ('pandas', 'numpy', 'requests', 'urllib3', 'tqdm') if m in sys.modules))"],
        capture_output=True, text=True, check=True).stdout.strip()
    assert loaded == "[]"


def test_download_module_still_exposes_pandas_and_requests():
    import pandas
    import requests
    assert download.pd is pandas
    assert download.requests is requests
    with pytest.raises(AttributeError):
        download.not_an_attribute
