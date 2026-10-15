# Fetch an individual ZIP member

Use `install_zip_member` when a large remote ZIP contains a small subset of files
you need. It requests only ZIP metadata and the named member's compressed bytes.
The server must support exact byte ranges and supply a strong ETag or
Last-Modified. A server that ignores ranges is rejected before its body is read.

```python
import json
from pathlib import Path
from datacache import install_zip_member

paths = install_zip_member(
    "/shared/research/search-2026/peptides",
    "https://example.org/results.zip",
    "txt/peptides.txt",
    max_compressed_bytes=512 * 2**20,
    max_bytes=4 * 2**30,
)
receipt = json.loads(Path(paths["receipt.json"]).read_text())
print(receipt["payload"], receipt["compressed"])
```

Replace the example URL and member with your source. The result contains:

| Key | Content |
| --- | --- |
| `payload` | Complete expanded member bytes |
| `compressed` | Exact compressed bytes, without ZIP headers |
| `receipt.json` | Source/member identity, HTTP validator, archive size, offset, compression method, CRC32, both SHA-256 hashes and sizes |

The first acquisition records observed hashes. Reproduce it in a new destination
with `expected_sha256`, `expected_size` and `compressed_sha256` from an independently
trusted receipt. Those expectations describe the expanded member and exact
compressed stream, respectively. CRC32 detects accidental corruption; it is not
a cryptographic provenance guarantee. Last-Modified can miss changes within its
timestamp resolution, so pin content hashes when reproducibility matters.

Only stored or deflated regular, unencrypted members are supported. ZIP64 is
supported; member names must be safe relative POSIX paths and unique. Metadata
reads default to an 8 MiB total limit. Compressed input and expanded output have
separate limits, checked against the directory and again while streaming. Memory
usage during extraction is bounded; both byte representations remain on disk, so
allow space for their sum plus any retained old generations.

Every acquisition publishes all three files together. Interrupted or rejected
attempts leave the previous generation usable. Another call with identical
arguments verifies retained bytes and returns offline without checking upstream
freshness. Changed expectations or damaged files require explicit `force=True`.
A failed attempt restarts the selected member on retry; it never downloads other
members or falls back to downloading the complete ZIP. Use `list_bundles` to
inspect generations and explicit `prune_bundles` when old paths are no longer used.
