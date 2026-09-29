# #83: fixed-path single-file registry

Add VersionedFileRegistry as the shared implementation of hitlist's established
single-file registry. Keep VersionedDatasetRegistry's generation storage
unchanged. The separate type makes the weaker single-file publication contract
explicit rather than making bundles silently use a second layout.

Contracts: accept the filename/urls/default_version/description mapping and a
dynamic cache_dir callable; local_path returns the old fixed Path even before
installation and creates nothing; download/ensure return one Path; cache reuse
does not fetch, hash or rewrite legacy manifests. resolve_version errors and
transfer failures use an optional caller error_cls and preserve causes. status
retains hitlist's keys and root manifest semantics. Legacy manifests contain
the most recently downloaded version per dataset, independently of the pinned
default whose cache presence status reports.

Delegate acquisition and transformation to fetch_file with forwarded keyword
options. Human cache/download messages stay downstream. Hash new installed
files in bounded chunks for the compatibility receipt, then publish JSON with
the existing atomic helper. A failed transfer leaves old files/receipts intact.
Do not create generations, symlinks or migration copies. File and root receipt
remain separate publications, not a multi-file transaction.

Review found and reproduced an existing lost-update race: two concurrent
registry downloads install two files but retain one manifest entry. Re-plan
before adoption: use the cross-platform filelock package to serialize downloads
and receipt updates per root, checking cache presence again inside the lock.
Ordinary read-only reuse must never create/acquire that lock. Test independent
writers and verify this prevents the reproduced race. Resolve the root once per
operation so a dynamic callable cannot split one publication across roots.

- [x] Inspect caller contracts; file compatibility gap and write specification.
- [x] Add the shared registry and public export; document behavior and examples.
- [x] Cover legacy read-only reuse, absent path resolution, all public return
  shapes/errors, refresh failure, root changes, transforms and manifest writes.
- [ ] Bump 1.15.0, run lint.sh and test.sh, review diff and current-head CI.
- [ ] Merge, run deploy.sh from clean master, verify PyPI wheel and sdist.
- [ ] Adopt the released registry in hitlist's compatibility wrapper.

Review: lint and all 764 tests pass with 95% coverage. The independent-process
regression fails without the writer lock (one receipt for two files), and passes
with it. Acquisition and receipt publication failures preserve their documented
legacy behavior; normal cache hits remain offline and create no lock files.
Minimum filelock compatibility and current-head CI remain release checks.
