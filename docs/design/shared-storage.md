# Shared storage referenced by several consumers (#95)

Status: investigation. Recommendation: don't build this until a second consumer
needs it.

## What exists

Every bundle, archive and materialization store holds its own bundles, and
`prune_bundles` (#91) deletes old bundles within one store. Nothing is shared
between stores: two stores that need the same multi-GB file each hold a copy.

PyEnsembl solves this for reference DNA on its own. It keeps one canonical copy
of each DNA file, records which Ensembl releases use it, and deletes a copy only
when no release refers to it. Reads take no locks and write nothing. That code
works today and isn't blocked by datacache.

## Why wait

A shared-object framework built from one consumer would encode pyensembl's
choices (biological identity, directory layout, reference bookkeeping) as
datacache's. Before building it, find a second consumer that needs shared
objects plus reference-aware cleanup, and show which duplicated code both could
delete. The digest-addressed objects other OpenVax packages keep
(`<root>/objects/sha256/`) are a related shape, but nothing yet shows they need
reference counting.

## If a second consumer appears

The smallest design that seems to fit, built from the bundle store that exists:

- **Objects** are ordinary bundle stores, placed where the caller says and
  identified by a caller-declared identity (see
  [source identity](source-identity.md)) or a SHA-256, each keeping its own
  trust meaning.
- **References** are small files, one per consumer and object, written inside
  the object's store under its lock before the lock is released, so pruning can
  never delete an object between install and registration.
- **Pruning** is explicit: list every reference first, refuse to delete if any
  reference file can't be read, skip objects whose lock is busy, never follow
  links, and offer a dry run that reports what would go.
- **Readers** never register or lock; a program reading an object nobody
  references can still lose it to a prune, as with `prune_bundles` today.
- **Migration** never moves existing pyensembl data automatically.

Distributed coordination, catalogue lookups and background deletion stay out of
scope.
