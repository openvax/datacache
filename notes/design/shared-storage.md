# Shared storage referenced by several consumers (#95)

Status: exploratory; implementation is gated. Assessed 2026-10-19.

Keep reference ownership and cleanup in consumers for now. PyEnsembl has a
working object/reference lifecycle. The other examined consumers establish a
need for shared bytes, but do not yet demonstrate a second adoption of the same
reference-aware cleanup protocol. This investigation adds no storage API,
reference format, migration, or deletion behavior. Keep #95 open until the
adoption gate below is satisfied.

## Evidence from existing consumers

These are source observations at the linked revisions, not claims about every
possible future use of those projects.

| Consumer and examined code | Existing behavior | Evidence for a common reference/pruning contract |
| --- | --- | --- |
| [PyEnsembl shared DNA](https://github.com/openvax/pyensembl/blob/bedb380/pyensembl/genome_fasta_cache.py), with the [adoption and validation contract in PR #401](https://github.com/openvax/pyensembl/pull/401) | Canonical objects use caller-defined biological identity and semantic placement. Per-release references protect objects from explicit orphan pruning. Writers lock one object; reference updates coordinate briefly. Registered reads take no locks and write nothing. | Confirmed first consumer. `_register`, `_reference_manifests`, `_owned_objects`, and `prune_genome_fastas` implement the lifecycle that might eventually be shared. Biological identity and path construction must remain application-owned. |
| [Osteosarc cache](https://github.com/iskandr/osteosarc/blob/ded74a7eaac74f1cd9d617c2c2bf26a0c5644afc/osteosarc/cache.py) | Stores shared bytes under `objects/sha256`, with immutable snapshot receipts and package-owned URL records. `_store` publishes objects; `path` checks recorded content. `prune_own` ages private digest/derived records, rather than implementing a shared-object liveness inventory. Refresh preserves old snapshot objects. | Candidate consumer, not an adoption proof. Receipts describe what was acquired, but this module does not supply the attach/detach and full-inventory orphan pruning protocol that PyEnsembl could share with it. A snapshot-retention policy would need to be stated first. |
| [Vaxrank test-data acquisition](https://github.com/openvax/vaxrank/blob/0c3bb1dce1aa7c8a9c235e23e12764b047d52b8a/vaxrank/download_test_data.py) | The custom-manifest path uses digest-named objects through DataCache, verifies hashes and sizes, and can export independent copies into an atomically published output directory. The default bundled-data path is separate. | Adjacent reuse, not a second reference-aware consumer. Exported copies do not depend on retaining the source cache object. The examined acquisition module has no shared-object attach/detach or orphan-pruning interface. |

The [integration guide](../../docs/integration.md#shared-openvax-cache)
describes a shared cache layout; a common directory name alone does not define
ownership, durable liveness or deletion rights. A receipt that records a
historical download must not automatically become either a permanent reference
or permission to remove the corresponding bytes.

The concrete deletion-code comparison is therefore incomplete: PyEnsembl has
reference inventory and orphan pruning code to replace; the two other examined
modules do not have matching lifecycle code to remove. Common download, hash
checking and publication are already smaller DataCache responsibilities and
are insufficient justification for a new reference framework.

## Adoption gate

Before implementation, record all of the following in #95 or a linked design PR:

1. A second named consumer with a real operation that shares one object across
   independently removed releases, snapshots or datasets. State what deletion
   means for that application and why retaining all objects is insufficient.
2. A small adoption patch or precise before/after code map for both consumers.
   Name the duplicated lifecycle code that disappears and the domain policy
   that remains. A hypothetical second caller or a shared filesystem layout is
   not enough.
3. Agreement on object identity, placement, ownership, durable reference
   inventory, immutable generation lifetime, locking and failure behavior.
   Include read-only users and callers that cannot register references.
4. Acceptance tests for the races and failures below, with explicit migration
   and rollback behavior. Demonstrate the proposed common contract without
   fetching multi-GB domain datasets.

If these conditions are not met, the acceptable outcome remains consumer-owned
reference policy. The [source identity work](source-identity.md) can support
cross-URL reuse independently; it does not satisfy this adoption gate.

## Smallest possible common contract

This is a candidate for evaluation, not an approved public API.

- **Identity and placement:** callers choose the full object identity and store
  path. A caller-declared upstream identity and a trusted SHA-256 retain
  different trust meanings. PyEnsembl keeps species, provider, versioned
  assembly, coverage, masking and encoding in its semantic layout. Short path
  keys must be checked against full identities to reject collisions.
- **Immutable publication:** reuse existing download/materialization and receipt
  validation. A reference names an immutable object/generation, not a mutable
  "current" pointer. Publishing a new generation cannot invalidate references
  or paths to an older one. Objects and references carry explicit ownership;
  arbitrary pre-existing files are never adopted implicitly.
- **Durable references:** the consumer supplies its reference identity and
  lifetime. Attach is explicit and idempotent; detach removes only that
  consumer's reference. An authoritative, enumerable inventory must cover every
  participating consumer. Registration is durable before the object's writer
  lock is released. Publication followed by a separate, unlocked registration
  call leaves a deletion race and is not sufficient.
- **Explicit cleanup:** dry-run and deletion validate the same complete
  reference inventory before deleting anything. Unreadable, malformed,
  conflicting or unknown-version metadata blocks the entire deletion pass.
  Busy objects are skipped. Missing references are not inferred from failed
  reads. No traversal of unsafe symlinks or deletion of foreign ownership is
  allowed; the configured root's handling must be stated separately.
- **Read-only reuse:** ordinary inspection and reads need no lock files,
  registration writes or metadata repairs. Durable references protect declared
  lifetimes, not arbitrary open file handles. An unregistered reader is not
  automatically detectable and has no extra retention guarantee.
- **Permissions:** define which users may attach, detach and prune in each
  namespace. A pruning process that cannot enumerate the full authoritative
  inventory must stop. Sharing read access does not grant permission to delete
  other consumers' references or data.

A few internal operations that combine publication, validated reference updates
and lock ownership may be enough. Evaluate them before designing a new public
object-store class. Existing `materialize()` returns after releasing its lock;
wrapping it with a later reference write would not meet the registration
invariant. Any integration must also avoid accidentally reacquiring the same
non-reentrant lock.

## Locking and generation lifetime to prove

A candidate local-filesystem protocol follows PyEnsembl's existing pattern:

1. Installers hold the object's writer lock while downloading/building. Other
   objects remain independent. Before releasing it, acquire a short-lived
   inventory lock, durably attach the reference, then release the inventory
   lock and finally the object lock.
2. Reference attach/detach coordinates through the inventory lock; attach also
   protects the target object against concurrent deletion. Detach performs no
   implicit payload deletion.
3. Prune holds the inventory lock while validating all references and deciding
   liveness. It only **tries** object locks without waiting and skips busy
   objects. It must never wait on an object lock while holding the inventory
   lock: an installer may already hold that object lock and be waiting to
   register its reference.
4. A crash after publication but before registration leaves an orphan, not a
   registered object with missing payload. A crash after durable registration
   leaves a protected object. Recovery must validate ownership and receipts
   before considering any orphan for deletion.

The full inventory scan and deletion must stay consistent with concurrent
attach/detach. No global lock spans downloads or builds. The adopting consumers
must demonstrate acceptable metadata scan cost and specify filesystem
requirements; this is not distributed coordination.

References must pin the generations they need. Deleting an entire object is
allowed only when none of its generations has a live reference. Pruning older
generations is a separate decision: existing `prune_bundles()` has no external
reference inventory and must not be used unchanged on referenced generations.
That interaction needs an explicit protected-generation contract before
adoption, rather than silently interpreting references as pointing at the
newest generation.

## Required acceptance scenarios

| Scenario | Required result |
| --- | --- |
| Two consumers reference the same object; one detaches | The remaining consumer can still reuse the original immutable generation offline. |
| Installer pauses between publication and registration while prune runs | Prune skips the locked object; a successful attach cannot point at deleted bytes. |
| Attach races with prune of an unreferenced object | The operations serialize; attach either protects a valid object or reports that installation is needed. |
| A long build on object A overlaps install or reads of B | B proceeds; prune skips A without blocking unrelated work. |
| Inventory contains an unreadable, malformed, conflicting or unsupported record | No candidate payload is deleted, including candidates inspected before the bad record. |
| Dry-run sees live, orphaned, busy and foreign objects | It reports eligible candidates without mutation and applies the same validation and skip rules as deletion. |
| A short key collides, a parent is symlinked, or a foreign directory occupies the path | Reject reuse/adoption/deletion without traversing or changing the foreign target. |
| Caller can read objects but cannot write or enumerate all references | Ordinary registered reuse remains read-only; pruning refuses an incomplete inventory. |
| Object refresh publishes a new generation while old references remain | Both generations stay usable; neither object cleanup nor generation cleanup removes a referenced one. |
| Process crashes before/after durable registration or during detach | Recovery preserves every surviving durable reference and does not guess that an unreadable reference is absent. |
| Identity equality is asserted without a trusted hash | Reuse retains unverified status; full consistency inspection still detects same-size corruption. |

## Migration and scope

No automatic move, rekeying or deletion of existing PyEnsembl data. An eventual
opt-in migration must validate old descriptors, preserve semantic paths and
full identity, establish new durable references before retiring old ones, and
leave rollback possible. If layouts cannot coexist safely, keep old stores
consumer-owned and start the new protocol only for explicitly selected stores.
Read-only reuse of old installations must continue without migration writes.

Catalogue retrieval, biological identity resolution, distributed locking,
background deletion and detecting arbitrary external readers remain outside
this proposal. Consumer adoption and a smaller proven contract come first.
