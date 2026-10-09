# Derived-artifact materialization

Use `materialize` when an application must download raw inputs, build one or
more derived files, validate them, and publish them together. For example,
PyEnsembl can keep its FASTA parsing and biological validation in a builder
while DataCache owns reference-DNA download, retry, receipts and publication.
Existing `fetch_and_transform` keeps its compatible single-file behavior.

```python
from datacache import materialize, inspect_materialization

sources = {
    "dna.fa.gz": {
        "url": compressed_dna_url,
        "sha256": compressed_dna_sha256,
        "size": compressed_dna_size,
    },
}
outputs = {"dna.fa": {}, "index.json": {}}
transform = {"version": "dna-parser-1", "options": {"assembly": "GRCh38"}}

def build(source_paths, output_paths):
    # These are private staged paths, not the caller's source or public output.
    # Stream gzip into output_paths["dna.fa"], validate sequence/contig contents,
    # and create output_paths["index.json"]. Scientific policy stays here.
    parse_and_validate_dna(source_paths["dna.fa.gz"], output_paths)

paths = materialize(
    artifact_store, sources, transform=transform, outputs=outputs, builder=build,
    download_options={"resume": True, "timeout": 60},
)
dna_path = paths["dna.fa"]
index_path = paths["index.json"]
state = inspect_materialization(
    artifact_store, sources, transform=transform, outputs=outputs,
)
```

The [offline reference-DNA example](https://github.com/openvax/datacache/blob/master/examples/reference_dna_materialization.py)
is runnable with `python -m examples.reference_dna_materialization` and uses
tiny gzip data without any biological-library dependency.

## Definitions and builder contract

Choose an exact managed store path, independently of the Python package
version. Sources are a nonempty mapping from relative names to either `url` or
`path`, plus optional `sha256` and `size`. Local paths and `file://` URLs are
copied into private working storage; their original bytes and permissions are
never modified, and caller-owned inputs are never deleted. Links, hard-linked
files, FIFOs and other nonregular local inputs are rejected without blocking.

Source expectations describe the **raw input bytes**, including compression.
Outputs are a nonempty mapping from relative names to optional `sha256` and
`size`, describing **builder-produced bytes**. Empty `{}` expectations are
supported; observation and a local consistency receipt do not authenticate
these bytes. An expected size of zero supports explicitly empty inputs/outputs.
Names cannot escape the store, collide by case or as files/directories, or
occupy reserved metadata names. Sources also cannot collide with their
automatic provenance-sidecar paths.

`transform` contains a nonempty opaque `version` string and optional JSON
`options`. Bump this identity when parsing, validation, index format or other
scientific decisions change. DataCache does not infer code changes or decide
biological compatibility. Do not put secrets in options: they are recorded.

`builder(source_paths, output_paths)` receives name-to-string mappings. Output
paths are initially absent, with required parent directories already present.
The builder must create exactly those outputs, close all handles, and leave
inputs unchanged before returning. Only regular, single-link files and the
declared output parent directories are accepted. Return values are ignored;
`materialize` returns name-to-absolute-path mappings in the newly published bundle.
The callback is trusted application code, not a sandbox. Builder progress,
streamed decompression and biological validation are caller-owned.

## Identity, receipts and trust

Each bundle's manifest records the complete source/transform/output dependency
definition and observed sizes/SHA-256 hashes. Source receipts also record
redacted origins, acquisition time, acquisition-time trusted-hash verification,
and validated transport metadata when available. Resumable HTTP acquisitions
record the accepted strong ETag or Last-Modified validator. These validators
establish transport consistency, not scientific correctness or authenticity.
Ordinary transfers without validated transport metadata do not invent it.

Full source references are represented by SHA-256 identity fingerprints.
Display origins omit URL credentials, query text and fragments, but retain the
path; avoid secret-bearing paths. Changing the full URL/path (including signed
query text), integrity expectations, transform identity or output inventory
makes the current bundle `invalid` for that request. Refresh is explicit:
`materialize(..., force=True)` builds a replacement. This API does not claim
different mirror URLs are the same dependency; caller-declared source identity
policy is separate. No remote `latest` lookup occurs.

`inspect_materialization` reports `available`, `missing`, `invalid` or
`inaccessible`, with the current bundle's path, output inspections,
source receipts, transform identity and an error cause. Supply all three of
`sources`, `transform`, and `outputs` to check the requested identity, or omit
all three for receipt-only consistency checks. Inspection never accesses the
original inputs, performs network requests, writes, locks or repairs.

Full inspection and cache-hit checks hash outputs by default. `verified=True`
means all outputs matched caller-supplied trusted hashes **now**; source receipt
verification is explicitly historical. Without trusted output hashes, even
fully consistent results have `verified=False`. `verify_files=False` checks
ownership, dependency identity, exact inventory, types, readability and sizes,
but cannot detect same-size corruption and always reports unverified outputs.
This fast flag applies only to reuse. New outputs are always hashed before
they are published.

## Interruption, publication and retention

Writers serialize per artifact; unrelated artifacts have independent locks.
Completed inputs persist in owner-only source-keyed directories even without
HTTP resume. `download_options={"resume": True}` also preserves partial raw
HTTP(S) downloads, using existing hash/size or size/strong-ETag resume rules.
Remote resumable inputs require `size`; local inputs are simply copied.
The supported transport options are timeout, chunk size, progress callback,
progress display, retry count/backoff/delay, resume and `max_bytes`, which
limits each remote download. Download, copy and hash
progress is enabled by default; set `show_progress=False` in download options
for noninteractive use. Valid cache hits remain quiet.

Download, gzip, validation and builder failures leave the current bundle
untouched. Failed builder outputs are discarded; completed
inputs survive for retry without another download. A killed builder can leave
private partial outputs, which the next transaction with the same definition
discards. Changing only the transform can reuse retained matching inputs.
`force=True` always rebuilds outputs, and it doesn't hash the outputs it
replaces.
It reuses remote inputs that still match, copies a local source again while
the original exists (so an in-place correction is picked up; once the original
is gone, a retained copy still serves), and repairs corrupted private inputs or
input receipts. It never adopts unsafe paths or foreign directories.

Materialization stores use the same layout as
[bundles](bundles.md#how-bundles-are-stored). All declared outputs and
`.datacache-manifest.json` are staged and checked as one tree, which is then
renamed into `bundles/` under its UTC install time; the newest bundle is the
current one. Readers see the complete old or complete new tree, and returned
paths keep working across later refreshes. Do not add indexes or edit files in
a published bundle; declare them as outputs or place independently mutable
files outside the store. A failure to remove the private staging directory is
logged; it never replaces a builder's error or fails a published build.

After a successful publication, owned inputs for that source definition are
removed by default. `retain_sources=True` keeps them privately for future
builds. Cache hits never perform cleanup: a crash between publication and
cleanup may leave inputs for an explicit later refresh. Inputs for other source
definitions/users and old bundles are never removed implicitly. Abandoned input/staging variants require deliberate operator
cleanup; general inventory/pruning is separate work. Retention is installation
policy, not output identity, so toggling it on a cache hit does not mutate disk.

The store marker, `.datacache-store.json`, records that this is a
materialization store, so DataCache never adopts a populated legacy directory or
another kind of store, even with force.
An empty precreated directory becomes the store in place, keeping its owner,
group and permissions. As with
current resumable bundles, installation requires a POSIX local filesystem with
`flock` and atomic sibling renames. Read-only existing hits require no lock.
This is atomic visibility, not a guarantee against filesystem/power-loss faults.

## Disk use

Let `C` be total completed raw inputs, `D` the new bundle of outputs, and `O`
all old bundles. During a build, storage is approximately
`O + C + D`, plus scratch files created by the builder. Publication
is a rename and does not duplicate `D`. After successful default cleanup,
retained storage is `O + D`; opt-in source retention uses `O + C + D`.

The existing resumable transport copies a completed private partial into file
publication staging before removing the partial. That phase can need an extra
compressed-input-sized copy: peak is approximately
`O + max(C + largest_input, C + D + builder_scratch)`, excluding any previously
abandoned input/staging variants. For one 1 GB compressed / 3 GB decompressed
human-DNA input, a first build needs roughly 4 GB plus builder scratch, then
retains roughly 3 GB (or 4 GB with source retention). A refresh retaining an old
3 GB output needs roughly 7 GB peak and retains 6 GB afterward. Caller-owned
local originals occupy additional storage outside this managed store.
