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

tqdm is installed by default; existing download APIs enable displays with
`show_progress=True`, while `materialize` enables them by default. Quiet use
does not import tqdm or configure your
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
you refresh them with `force=True`, or periodically with `expire_after`;
DataCache doesn't ask the server whether a file has changed. Add
`show_progress=True` to display a download bar; no extra installation is needed.

## Guides

Sections that earlier versions of this README contained now live in these
guides; their old links land on the guide that replaced them.

- <a id="offline-example-with-integrity-checking"></a>[Working offline example, verification and interface choices](https://github.com/openvax/datacache/blob/master/docs/index.md)
- <a id="find-inspect-or-clear-your-cache"></a>[Downloads, inspection, integrity, freshness, retries and resumption](https://github.com/openvax/datacache/blob/master/docs/downloads.md)
- [Tables, SQLite and transformations](https://github.com/openvax/datacache/blob/master/docs/data.md)
- [Shared caches](https://github.com/openvax/datacache/blob/master/docs/shared-caches.md)
- [Progress and logging](https://github.com/openvax/datacache/blob/master/docs/progress.md)
- <a id="versioned-datasets-and-large-downloads"></a>[Pinned bundles](https://github.com/openvax/datacache/blob/master/docs/bundles.md), [archive trees](https://github.com/openvax/datacache/blob/master/docs/archives.md) and the [legacy file registry](https://github.com/openvax/datacache/blob/master/docs/file_registry.md)
- <a id="choose-the-right-api"></a><a id="guarantees-and-limits"></a>[Interface selection and guarantees](https://github.com/openvax/datacache/blob/master/docs/reference/choosing.md)
- [Derived-artifact materialization](https://github.com/openvax/datacache/blob/master/docs/materialization.md): caller-owned
  builders, atomic output generations, dependency receipts and resumable inputs.
- [Complete API reference](https://github.com/openvax/datacache/blob/master/docs/api.md)
- [Downstream integration](https://github.com/openvax/datacache/blob/master/docs/integration.md): contracts for consuming libraries.
- [Release notes](https://github.com/openvax/datacache/blob/master/CHANGELOG.md) and [release procedure](https://github.com/openvax/datacache/blob/master/RELEASING.md).

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
