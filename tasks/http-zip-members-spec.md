# Bounded, verified HTTP ZIP member acquisition

## Problem and public contract

Hitlist's PXD013455 source is a 33 GiB ZIP containing a roughly 400 MiB
compressed peptide table. Whole-archive downloads prevent practical shared
curation. Add a public datacache API to acquire one explicitly named ZIP member
with bounded HTTP byte ranges, without downloading or extracting sibling members.
Cache member bytes and a legible acquisition receipt together as a transactional
bundle. Keep URL/member identity, validators, CRC, compressed and expanded sizes
and SHA256 digests; distinguish a caller-pinned digest from an observed checksum.

## Design

Use a seekable HTTP reader with a fixed response budget and Python's standard
ZIP parser (including ZIP64). Require identity encoding, exact 206 Content-Range,
stable total length and a strong ETag or Last-Modified validator. Bind every
subsequent range to that validator; reject servers that ignore ranges, truncation,
changed validators, and oversized reads. Limit metadata reads and total wire bytes.
Stream the selected compressed member to a retained datacache input, then extract
with bounded output and verify CRC and caller-supplied hashes before publication.
Reject encryption, unsupported methods, directories, ambiguous duplicate member
names, mismatched headers and unsafe member names. Never use archive paths as
filesystem paths. Support stored/deflate members initially.

Use existing materialization/bundle primitives for private staging, locking,
atomic publication, offline reuse and corruption detection. Preserve the previous
generation on failures. A reusable range-file acquisition primitive may be exposed
if it avoids a second caching system; keep the first API narrow and explicit.

## Verification and release

Local HTTP fixtures cover ZIP64, stored/deflate, bad ranges, changed objects,
CRC/hash/size mismatch, metadata/download/output bounds, duplicate/encrypted
members, interrupted builds, offline cache hits and tampered cached receipts.
Run repository lint and full tests, bump the version, PR, review, merge and deploy.
Probe the real PRIDE archive metadata and verify the small parameters member
before acquiring the large peptide member. Link Hitlist #681 and its consumer PR.
