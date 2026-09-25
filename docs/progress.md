# Progress and logging

Install the optional dependency with `python -m pip install "datacache[progress]"`.
Set `show_progress=True` on `fetch_file`, `Cache.fetch`, or the CSV and database
helpers. DataCache uses [tqdm.auto](https://tqdm.github.io/docs/shortcuts/), which
selects a terminal or notebook display. Terminal bars use stderr; stdout remains
available for application output. Displays are quiet by default and close on
success, exceptions, and handled cancellation.

Downloads display bytes and a total when the server provides a usable size.
Compressed HTTP content encoding can make the wire size differ from the
decoded data; in that case the total is unknown. gzip decompression also uses
an unknown total rather than guessing from its compressed size. ZIP entries
use their declared uncompressed size. SHA-256 verification displays bytes read;
database insertion displays rows completed in batches of up to 1,000.

Each retry starts a fresh download bar. A finished transfer is followed by any
decompression and hash-verification work before publication. HTML parsing and
arbitrary user transformations do not expose incremental progress. Ordinary
cache reuse shows no bar; an explicit `validate_file(..., show_progress=True)`
can show a checksum pass over an already cached file.

## Application callbacks

The existing `progress_callback(completed, total)` API remains supported and
can be used with or without tqdm. `completed` is cumulative for the current
download attempt; `total` may be `None`. Retries restart from zero, so counts
may decrease. Callbacks cover transfer bytes only, not transformation or
database work. Empty downloads do not invoke the callback.

```python
from datacache import fetch_file

def report(completed, total):
    # Replace with your UI's progress update.
    print(f"Downloaded {completed} bytes; expected total: {total}")

path = fetch_file(
    "https://example.org/data.tsv",
    destination="references/data.tsv",
    progress_callback=report,
    chunk_size=1024 * 1024,
)
```

Callback exceptions abort the operation, clean up partial downloads, and are
not retried. Keep callbacks inexpensive. Treat `fetch_file` returning
successfully as the publication signal, rather than relying on the byte count.

## CSV options

CSV helpers separate pandas options from download options. Parser options such
as `sep`, `dtype`, and `usecols` stay as keyword arguments. Put `timeout`,
`cache_root`, `expected_sha256`, `expected_size`, retry settings, and callbacks
inside `download_options`:

```python
from datacache import fetch_csv_dataframe

frame = fetch_csv_dataframe(
    "https://example.org/records.tsv.gz",
    filename="records.tsv",
    sep="\t",
    dtype={"sample_id": "string"},
    show_progress=True,
    download_options={"cache_root": "references", "timeout": 30, "max_retries": 2},
)
```

`fetch_csv_db` accepts the same options, and `show_progress=True` also covers
database insertion. If `download_options` contains `show_progress`, it overrides
the outer setting for the download only. Keep filename and subdirectory
selection in the helper's own arguments; CSV helpers decompress archives before
parsing. The options dictionary is copied and never mutated.

## Logging

DataCache uses Python logging without adding handlers or changing global
configuration. Applications can enable informational messages:

```python
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("datacache").setLevel(logging.INFO)
```

Retry warnings include attempt counts, failure category, and delay. HTTP
errors retain their original Requests exception and response. See the
[retry reference](downloads.md#transient-http-failures) for precise behavior.
