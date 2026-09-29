# Fixed-path registry adoption (#83)

See [fixed_path_registry.md](fixed_path_registry.md) for the specification and checklist.

# Datacache #80 and #81: resumable and raw downloads

## Specification

Release 1.14.0 adds two compatible capabilities to `fetch_file` and `Cache.fetch`.

1. `raw=True` disables archive decompression and HTML-to-CSV conversion regardless
   of the explicit output name. It is a keyword-only boolean, incompatible with
   `decompress=True`. Existing calls retain suffix-based inference. Integrity
   expectations describe the unchanged downloaded payload; HTTP transfer decoding
   retains its existing semantics. Reuse, staging, atomic publication, progress,
   permissions, and optional provenance use the existing paths. Raw mode composes
   with resume and arbitrary exact destination names.
2. `resume=True` requires `expected_size`, with `expected_sha256` optional only
   when each accepted response supplies a syntactically valid strong ETag.
   Persist that validator, send it as `If-Range`, and append only an exact 206
   range with the same validator. A 200 response replaces the private partial;
   mismatched ranges/validators and 416 retain the existing bounded restart
   behavior. Weak, absent, or malformed validators cannot authorize size-only
   resume. Existing SHA-256-pinned behavior remains supported, including servers
   without ETags. Reject encoded responses and validate the final byte count.
3. Persistent size-only partials require a valid strong ETag in their metadata.
   A complete-size partial cannot take the hash-verified publication shortcut:
   restart it so the new request validates its server representation. Existing
   installed cache hits still perform only the caller's requested local checks;
   size-only validation does not imply cryptographic integrity or freshness.
4. Document the new APIs, examples for Ensembl and raw archive adapters, and
   the limitations of size-only validation. Do not expand this release into
   downstream pyensembl/Hitlist migrations or new checksum formats.

## Plan

- [x] Inspect source, issues, repository state, tests, and HTTP validator rules.
- [x] Create an isolated feature checkout and check in the implementation plan.
- [x] Reproduce both missing capabilities with focused regression tests.
- [x] Add raw mode and pass it through Cache.fetch, preserving legacy behavior.
- [x] Extend resumable transfer validation and persistent state for strong ETags.
- [x] Cover interruption/retry, changed and missing validators, bad ranges,
  complete partials, raw gzip/ZIP/HTML outputs, cache reuse, provenance, and
  atomic preservation of existing destinations.
- [x] Update documentation and changelog; bump to 1.14.0.
- [x] Run ./lint.sh and ./test.sh; review behavior and compatibility.
- [ ] Open PR linking #80 and #81; merge after CI passes.
- [ ] Deploy through ./deploy.sh from clean master; verify PyPI artifacts and
  installed-package behavior.
- [ ] Review remaining dependency/urgency groups and report next work.

## Review

The initial focused suite passes all 95 raw/resume tests using real local HTTP
servers. Both 1.13.0 limitations were reproduced: raw is not an accepted keyword,
and resume validation requires a SHA-256 even before making a request. The new
paths preserve existing destinations and record no verified digest for size-only
downloads. The first full run passed 749 tests and flagged two documentation
consistency checks: the API reference version and Cache.fetch signature. Both
references have been updated. Final `./lint.sh` and `./test.sh` pass: 751 tests,
95% coverage on Python 3.9.22. The 95 focused raw/resume tests also passed on
Python 3.12.6, and both offline public examples passed. The diff preserves
existing transformation defaults, hash-pinned servers without validators,
private staging, and atomic publication. API docs describe cache reuse and the
checksum limitations of size-only transfers.
Release verification will be recorded in the PR after merge so the
deployed source remains a clean master checkout.
