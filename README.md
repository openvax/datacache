# DataCache

[![Tests](https://github.com/openvax/datacache/actions/workflows/tests.yml/badge.svg)](https://github.com/openvax/datacache/actions/workflows/tests.yml)
[![Coverage](https://coveralls.io/repos/github/openvax/datacache/badge.svg?branch=master)](https://coveralls.io/github/openvax/datacache?branch=master)
[![PyPI](https://img.shields.io/pypi/v/datacache.svg)](https://pypi.org/project/datacache/)

Download a dataset once, verify it, and reuse the local file on later runs.
DataCache handles streaming downloads, gzip/ZIP decompression, tar-tree
installation, offline inspection and SQLite caches built from pandas DataFrames.
It is the download layer of the [OpenVax](https://github.com/openvax) tools:
[PyEnsembl](https://github.com/openvax/pyensembl),
[pepdata](https://github.com/openvax/pepdata),
[orfdata](https://github.com/openvax/orfdata) and
[Vaxrank](https://github.com/openvax/vaxrank) use it, and any Python
application can.

## Install

Python 3.11 or newer is required; CI tests Python 3.11–3.14.

```sh
python -m pip install datacache
# Optional HTML-table conversion:
python -m pip install "datacache[html]"
```

## Quick start

<a id="quickstart"></a>Download a file once, then reuse its local path on later calls:

```python
from datacache import Cache

cache = Cache("my-project")
url = "https://raw.githubusercontent.com/openvax/datacache/master/LICENSE"
path = cache.fetch(url, filename="LICENSE", timeout=30)
print(open(path).readline().strip())

# Reuses the cached file without a network request, even in a later process.
print(cache.fetch(url, filename="LICENSE") == path)
```

```text
Apache License
True
```

`path` is under the platform cache directory, for example
`~/Library/Caches/my-project/LICENSE` on macOS or `~/.cache/my-project/LICENSE`
on Linux. Replace the URL and filename with your dataset. Existing files are
reused until you refresh them with `force=True`, or periodically with
`expire_after`; DataCache doesn't ask the server whether a file has changed.
Add `show_progress=True` to display a download bar; no extra installation is
needed.

## Documentation

<a id="guides"></a><a id="offline-example-with-integrity-checking"></a>The [documentation](https://openvax.github.io/datacache/)
starts with an offline example that verifies a file's checksum, then covers one
task per page. In reading order:

1. <a id="find-inspect-or-clear-your-cache"></a>[Download and inspect files](https://openvax.github.io/datacache/downloads/):
   exact destinations, integrity checks, freshness, retries, resumable
   transfers and offline inspection.
2. [Read tables and build SQLite caches](https://openvax.github.io/datacache/data/)
   from CSV files and pandas DataFrames.
3. [Share a cache](https://openvax.github.io/datacache/shared-caches/) between
   users and packages.
4. <a id="versioned-datasets-and-large-downloads"></a>[Install pinned bundles](https://openvax.github.io/datacache/bundles/)
   of files that belong together, or
   [archive trees](https://openvax.github.io/datacache/archives/).
5. [Build derived files](https://openvax.github.io/datacache/materialization/)
   from downloads and publish them together.
6. [Integrate a consuming library](https://openvax.github.io/datacache/integration/).

<a id="choose-the-right-api"></a><a id="guarantees-and-limits"></a>Also see
[choose an interface](https://openvax.github.io/datacache/reference/choosing/),
the [API reference](https://openvax.github.io/datacache/api/) and the
[release notes](https://github.com/openvax/datacache/blob/master/CHANGELOG.md).

## Dependencies and progress display

tqdm is installed by default; existing download APIs enable displays with
`show_progress=True`, while `materialize` enables them by default. Quiet use
does not import tqdm or configure your application's logging. The old
`datacache[progress]` install syntax remains supported as a compatibility alias.

The existing pandas dependency range is unchanged; upgrading DataCache does
not introduce a pandas 1.5 requirement. CI covers pandas 1.4.4, 1.5.3, and
current releases on supported Python versions.

## Development

```sh
python -m pip install -e ".[test]"
./lint.sh   # ruff
./test.sh   # pytest with coverage
python -m examples.basic_usage
```

Tests use local files, mocked responses, and local HTTP servers. They do not
depend on external dataset servers. See CI for the Python, dependency, and
operating-system combinations exercised.

Build the site with `python -m pip install -r requirements-docs.txt` and
`./docs.sh`. See the
[release procedure](https://github.com/openvax/datacache/blob/master/RELEASING.md).

## License

[Apache License 2.0](https://github.com/openvax/datacache/blob/master/LICENSE)
