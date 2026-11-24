# API examples

These self-contained examples exercise the public API in [the API reference](../api.md)
in one Python session. All downloads use local files in a temporary directory, so
no network server or real user cache is touched. Run the setup block first, then
the remaining blocks in order, and finish with the cleanup block.

## Setup

```python
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import io
import os
import tarfile

import pandas as pd
import datacache as dc

temporary = TemporaryDirectory()
root = Path(temporary.name)
cache_root = root / "cache"
source = root / "source.csv"
contents = b"id,value\n1,10\n2,20\n"
source.write_bytes(contents)
url = source.as_uri()
sha256 = hashlib.sha256(contents).hexdigest()
frame = pd.DataFrame({"id": [1, 2], "value": [10, 20]})

archive_source = root / "models.tar.bz2"
with tarfile.open(archive_source, "w:bz2") as archive:
    member_contents = b'{"model": "example"}\n'
    member = tarfile.TarInfo("models/model.json")
    member.size = len(member_contents)
    archive.addfile(member, io.BytesIO(member_contents))
archive_contents = archive_source.read_bytes()
archive_sha256 = hashlib.sha256(archive_contents).hexdigest()
print("setup ready:", len(contents), "bytes")
```

```text
setup ready: 19 bytes
```

## Downloading and transforming

```python
path = dc.fetch_file(
    url, filename="records.csv", cache_root=cache_root,
    expected_sha256=sha256, expected_size=len(contents), timeout=30,
)
assert dc.fetch_file(url, filename="records.csv", cache_root=cache_root) == path


def has_header(candidate):
    if not Path(candidate).read_bytes().startswith(b"id,value"):
        raise ValueError("not the records table")


# Refresh copies older than a day, keep the cached copy if the source is
# unreachable, and never cache content without the expected header.
fresh = dc.fetch_file(
    url, filename="records.csv", cache_root=cache_root, expire_after=86400,
    return_stale_on_error=True, validator=has_header,
)
assert fresh == path
assert dc.fetch_bytes(url, timeout=30) == contents


def double_values(source_path, output_path):
    result = pd.read_csv(source_path)
    result["value"] *= 2
    result.to_csv(output_path, index=False)
    return result


doubled = dc.fetch_and_transform(
    "doubled.csv", double_values, pd.read_csv, "records.csv", url,
    cache_root=cache_root,
)
print(doubled["value"].tolist())
```

```text
[20, 40]
```

## Paths, offline policy, and progress policy

```python
assert dc.expected_path(filename="records.csv", cache_root=cache_root) == path
assert dc.build_local_filename(filename="records.csv.gz", decompress=True) == "records.csv"

selected_root = dc.get_data_dir("my-project", envkey="MY_PROJECT_CACHE_ROOT")
assert isinstance(selected_root, str) and len(selected_root) > 0

os.environ["EXAMPLE_SHARED"] = str(root / "shared")
shared_root = dc.get_cache_root("example", "EXAMPLE_OWN", "EXAMPLE_SHARED")
assert shared_root == str(root / "shared")
del os.environ["EXAMPLE_SHARED"]

loc = dc.resolve_cache_dir("example", override=root / "resolved")
assert isinstance(loc, dc.CacheLocation)
assert loc.origin == "override"

writable_path = dc.build_path("nested/output.txt", cache_root=cache_root)
assert Path(writable_path).parent.is_dir()

dc.ensure_dir(root / "extra" / "nested")
assert (root / "extra" / "nested").is_dir()

assert dc.is_offline(offline=True) is True
assert dc.is_offline(offline=False) is False
dc.require_online(url, offline=True)  # file:// URLs stay allowed offline.
assert dc.should_show_progress(False) is False

# dc.clear_cache("my-project")  # Delete an application's platform cache and root.
print(dc.format_bytes(1536), loc.origin)
```

```text
1.5 KiB override
```

## Integrity and inspection

```python
assert dc.validate_file(path, expected_sha256=sha256, expected_size=len(contents)) == path

inspection = dc.inspect_file(path, expected_sha256=sha256)
assert isinstance(inspection, dc.FileInspection)
assert inspection.status == "available" and inspection.verified
assert dc.inspect_file(root / "absent.csv").status == "missing"
assert inspection.size == len(contents)

manual_result = dc.FileInspection(path=path, status="available")
assert not manual_result.verified

inventory = dc.inspect_files(cache_root, {
    "records.csv": {"expected_sha256": sha256, "expected_size": len(contents)},
})
assert isinstance(inventory, dc.CacheInspection)
assert inventory.status == "available" and inventory.verified
assert inventory.files["records.csv"].path == path

manual_inventory = dc.CacheInspection(
    path=str(cache_root), status="available", files={"records.csv": manual_result},
)
assert not manual_inventory.verified

try:
    dc.validate_file(path, expected_size=0)
except dc.FileValidationError as error:
    assert error.path == path
    print("mismatch:", "size mismatch" in error.reason)
```

```text
mismatch: True
```

## SQLite caches

```python
with closing(dc.db_from_dataframes_with_absolute_path(
    root / "direct.db", {"records": frame},
    table_names_to_primary_keys={"records": "id"},
)) as connection:
    count = connection.execute("SELECT COUNT(*) FROM records").fetchone()[0]

connection = dc.connect_if_correct_version(root / "direct.db", 1, read_only=True)
assert connection is not None
with closing(connection):
    ro_count = connection.execute("SELECT COUNT(*) FROM records").fetchone()[0]
assert dc.connect_if_correct_version(root / "absent.db", 1) is None
print("rows:", count, ro_count)
```

```text
rows: 2 2
```

## `Cache` object

```python
cache = dc.Cache("reference-example", cache_root=root / "object-cache")
assert cache.cache_directory_path == str(root / "object-cache")

cached_path = cache.fetch(url, filename="records.csv", expected_sha256=sha256)
assert Path(cached_path).read_bytes() == contents
assert cache.local_filename(filename="records.csv.gz", decompress=True) == "records.csv"
assert cache.local_path(filename="records.csv") == cached_path
assert cache.exists(filename="records.csv")
assert cache.inspect(filename="records.csv", expected_sha256=sha256).verified

if hasattr(os, "fchmod") and hasattr(os, "O_NOFOLLOW"):
    cache.make_readable(filename="records.csv", group=True)

cache.delete_url(url)
assert not cache.exists(filename="records.csv")

cache.delete_all()  # Safe here: object-cache is inside our temporary directory.
assert Path(cache.cache_directory_path).is_dir()
assert not list(Path(cache.cache_directory_path).iterdir())
print("cache emptied:", True)
```

```text
cache emptied: True
```

## Archive trees and package version

```python
archive_store = root / "archive-store"
archive_bundle = dc.install_archive(
    archive_store,
    archive_source,
    expected_sha256=archive_sha256,
    expected_size=len(archive_contents),
    extra_files={"DOWNLOAD_INFO.csv": "url\n" + archive_source.as_uri() + "\n"},
)
assert (archive_bundle / "models/model.json").is_file()
assert isinstance(dc.__version__, str) and len(dc.__version__) > 0
print("archive member:", (archive_bundle / "models/model.json").read_text().strip())
```

```text
archive member: {"model": "example"}
```

## Cleanup

After finishing the examples, remove their temporary files:

```python
temporary.cleanup()
print("cleaned up")
```

```text
cleaned up
```
