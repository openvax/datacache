# DataCache

[![Tests](https://github.com/openvax/datacache/actions/workflows/tests.yml/badge.svg)](https://github.com/openvax/datacache/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/datacache.svg)](https://pypi.org/project/datacache/)

Download, verify, transform, and cache datasets for Python applications,
including OpenVax libraries such as pyensembl. DataCache provides streaming
downloads, gzip/ZIP decompression, transactional tar-tree installation,
reusable local paths, offline inspection, and SQLite caches built from pandas
DataFrames.

## Install

Python 3.9 or newer is required.

```sh
python -m pip install datacache
# Optional HTML-table conversion:
python -m pip install "datacache[html]"
```

tqdm is installed by default; progress displays are opt-in with
`show_progress=True`. Normal use does not import tqdm or configure your
application's logging. The old `datacache[progress]` install syntax remains
supported as a compatibility alias.

The existing pandas dependency range is unchanged; upgrading DataCache does
not introduce a pandas 1.5 requirement. CI covers pandas 1.4.4, 1.5.3, and
current releases on supported Python versions.

## Quickstart

Download a file once, then reuse its local path on later calls:

```python
from datacache import Cache

cache = Cache("my-project")
url = "https://raw.githubusercontent.com/openvax/datacache/master/LICENSE"
path = cache.fetch(url, filename="LICENSE", timeout=30)
print(path)

# Reuses the cached file without a network request, even in a later process.
assert cache.fetch(url, filename="LICENSE") == path
```

Replace the URL and filename with your dataset. Existing files are reused until
you explicitly refresh them with `force=True`; DataCache does not check whether
the remote file has changed. Add `show_progress=True` to display a download bar;
no extra installation is needed.


<a id="find-inspect-or-clear-your-cache"></a>
<a id="offline-example-with-integrity-checking"></a>
<a id="versioned-datasets-and-large-downloads"></a>
<a id="choose-the-right-api"></a>
<a id="guarantees-and-limits"></a>

## Guides

- [Working offline example, verification and interface choices](docs/index.md)
- [Downloads, inspection, integrity, retries and resumption](docs/downloads.md)
- [Tables, SQLite and transformations](docs/data.md)
- [Shared caches](docs/shared-caches.md)
- [Progress and logging](docs/progress.md)
- [Pinned bundles](docs/bundles.md) and [archive trees](docs/archives.md)
- [Interface selection and guarantees](docs/reference/choosing.md)
- [Complete API reference](docs/api.md)

## Development

```sh
python -m pip install -e ".[test]"
./lint-and-test.sh
python -m examples.basic_usage
```

Tests use local files, mocked responses, and local HTTP servers. They do not
depend on external dataset servers. See CI for the Python, dependency, and
operating-system combinations exercised.

Build the site with `python -m pip install -r requirements-docs.txt` and `./docs.sh`.
