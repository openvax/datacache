# Caller-declared source identity (#92)

Status: implemented for bundle assets and materialization sources. The public
contract is in [bundles](../../docs/bundles.md#reuse-across-urls-with-a-declared-identity)
and [materialization](../../docs/materialization.md#identity-receipts-and-trust).

`source_identity` is optional bounded JSON supplied by the caller. It identifies
an exact upstream file revision independently of its URL, without asserting
trusted integrity. Object key order does not matter; value types and list order
do. The implementation rejects non-JSON values, non-finite numbers, oversized
or excessively nested declarations, and duplicate receipt keys. It detaches
declarations from mutable caller objects before acquisition.

An explicitly requested identity must be present and equal in the receipt.
This remains required with trusted hashes, resolving the earlier design's open
question conservatively: a trusted digest does not override an explicit
caller assertion or silently migrate an older receipt. Bundle decompression
settings and materialization transform/output settings must also match.
Callers that do not opt in retain their existing URL/hash rules.

Actual acquisition URL fingerprints and redacted origins remain in receipts;
reuse never rewrites them to the current mirror. Materialization source records
must agree with their dependency definitions. The JSON declaration itself is
stored verbatim, so callers must not include credentials in it.

Source identity does not change download/resume validation or trusted-hash
reporting. Bundle expectations still describe installed bytes; materialization
source expectations describe raw bytes and output expectations describe built
bytes. Checksums or sizes inside an opaque identity are never interpreted as
integrity expectations. Full inspection continues to detect same-size payload
corruption. Private inputs and partial transfers retain their existing full
source-definition keys; only completed publications gain cross-URL reuse.

PyEnsembl remains responsible for deriving its complete biological identity and
choosing its meaningful storage layout. This feature supplies no catalogue
lookups, shared-object discovery, external references or cross-store pruning.
