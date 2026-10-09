# Shared caches

See the [API reference](api.md) for signatures, defaults, return values, and errors.

## Choosing where data lives

Most packages let users move their data with an option or an environment
variable, keep using a location from an older release, and otherwise use the
platform's cache directory. `get_cache_root` does all of that in one call:

```python
from datacache import fetch_file, get_cache_root

root = get_cache_root(
    "hitlist",                 # Platform cache directory name, the last resort.
    "HITLIST_DATA_DIR",        # Environment variables, checked in order.
    override=data_dir_option,  # An explicit choice wins, when not None.
    legacy=["~/.hitlist"],     # Older locations, used only if they hold data.
)
path = fetch_file(url, cache_root=root)
```

The first of these that applies is the root: the override, then the first
environment variable that is set, then the platform cache directory if it
already holds data, then the first legacy location holding data, and finally
the platform cache directory. A location holds data when it contains a file
anywhere inside other than the `.DS_Store`-style files operating systems leave
behind, so empty leftover folders never pin a user to an old location, and a
user who has already downloaded into the platform directory stays there.
Legacy paths must be absolute. The legacy check reads the disk, so resolve the
root once, for example at startup, and pass it along.
Environment values are used exactly as given, never with a package name added,
so several packages reading one variable agree on one directory. Nothing is
created until something is downloaded. Pass the root to `fetch_file`, `Cache`
or a registry as `cache_root`; `fetch_file`'s `subdir` alone reads no
environment variables.

## Sharing one root

Select a common root explicitly:

```python
from datacache import Cache

cache = Cache("references", cache_root="/srv/references/v1")
status = cache.inspect(filename="annotations.gtf")
print(status.status)
if status.error is not None:
    print(status.error)
```

Inspection never downloads, creates directories, or repairs files. The root is
the actual directory containing files, not a parent to which `subdir` is added.
For pyensembl, configure its own `PYENSEMBL_CACHE_DIR`; that environment setting
is interpreted by pyensembl, not globally by DataCache.

## New downloads

Files remain private while downloading and validating. New final files use
normal creation permissions measured in the destination directory, without
temporarily changing the process-wide umask:

| Umask | Typical new file mode | Access |
| --- | --- | --- |
| `022` | `0644` | Owner writes; others read |
| `002` | `0664` | Owner/group write; others read |
| `007` | `0660` | Owner/group read and write |
| `077` | `0600` | Owner only |

These modes assume no default ACL modifies creation. Choose the umask in the
shell or application launcher, before starting worker threads. Shared group
directories can use setgid so new files inherit the directory's group; have
the cache administrator configure the intended group and directory access.

Replacing a downloaded file preserves its existing read/write/execute bits.
It creates a new inode, so existing ownership, ACLs, and extended attributes
are not copied. Existing SQLite databases rebuild transactionally in place
and retain their inode and permissions.

## Files already downloaded as `0600`

The fix for [#68](https://github.com/openvax/datacache/issues/68) affects new
files. Reusing or force-refreshing an existing file does not broaden its
permissions. Its owner or an administrator must explicitly grant access to
the files intended for sharing. For example, to grant the owning group read
access to two selected files:

```python
from datacache import Cache, make_file_readable

make_file_readable("/srv/references/v1/annotations.gtf")  # Add group read.
cache = Cache(cache_root="/srv/references/v1")
cache.make_readable(filename="transcripts.fa")
# Add others=True only if everyone should be able to read this file.
```

These POSIX helpers add only the requested read bits, preserve contents and
the inode, and reject symlinks, directories, and other non-regular files.
They do not recurse, change ownership, grant write access, or run implicitly
during normal cache use. Calling them again is harmless. Permission errors
propagate if the caller cannot open or chmod the file.

The equivalent shell command is:

```sh
chmod g+r /srv/references/v1/annotations.gtf /srv/references/v1/transcripts.fa
```

Group membership and directory traversal permissions must also permit access.
Use `ls -ld` on the cache and parent directories, and `ls -l` on the affected
files. A readable file inside an inaccessible parent is still inaccessible.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| `inaccessible` / `PermissionError` | File mode, owning group, ACLs, and traversal permissions on every parent |
| File is present but `corrupt` | Expected hash/size, file type, and whether a previous tool wrote incomplete data |
| Cache root is `available` but not `verified` | Every required file needs a trusted SHA-256 expectation to mark the inventory verified |
| Read-only cache can be read but cannot refresh | Refresh requires write access to the destination directory; database rebuilds also need SQLite journal access |
| Old private files stay private after refresh | Existing file modes are preserved; adjust the selected files explicitly |
| Previously generated SQLite numbers are wrong | Rebuild from source with a new version or `overwrite=True` |

An intact downloaded file can be reused from a read-only installation.
For SQLite, use `connect_if_correct_version(path, version, read_only=True)`.
Keep published releases in distinct versioned directories when readers must
continue using a stable release while another one is being prepared.
