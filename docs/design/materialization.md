# Derived-artifact transaction (#90)

Add `materialize` and `inspect_materialization`, leaving `fetch_and_transform`
unchanged. A caller chooses the exact store, raw source definitions, a JSON
transform identity (`version` plus opaque `options`), required output inventory,
and `builder(source_paths, output_paths)`. Biological parsing stays in the
builder. DataCache validates regular files, hashes bytes in bounded memory,
records dependency receipts and publishes the outputs together as one bundle.

Use existing filesystem, transport, resume, progress and bundle-store
primitives. The store marker records that a store holds materializations: neither
bundles nor arbitrary legacy directories can be overwritten even with force.
Source and transform identity mismatches raise until explicit `force=True`.
Input expectations describe raw (including compressed) bytes; output
expectations describe builder-produced bytes. Observed digests are not trust.

Writers serialize per artifact. Inputs live in owner-only, source-keyed working
directories, survive download/build/publication interruptions, and can be
reused after a transform-only change. Builders receive only private staged
paths and must close all handles, leave inputs unchanged, and create exactly
the declared regular outputs. Outputs and the receipt publish together through
one rename into the store's `bundles/` directory; the newest bundle is the
current one. Failed refreshes never modify the current or older bundles.

Default successful publication removes only the transaction's private owned
inputs; retention is opt-in. Cache hits stay read-only and never clean up.
Local inputs are copied and never modified/deleted. Old bundles are retained. Installation initially has the same POSIX-local-filesystem constraint
as resumable bundle installation; cross-platform bundle support is separate.

Test tiny gzip and two-output builds, actual HTTP range resumption, killed
builders, completed-input retry without network, identity mismatch, source and
output corruption, unsafe paths/files, failure at both publication boundaries,
read-only offline reuse, per-artifact serialization and unrelated writer
independence. Document disk use including transport's temporary resume copy.
