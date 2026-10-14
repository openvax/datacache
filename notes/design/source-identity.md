# Caller-declared source identity (#92)

Status: design only, not implemented.

## Problem

Without a trusted SHA-256, a bundle is reused only when each asset's full URL
matches the URL it was installed from. PyEnsembl serves the same reference DNA
under a different URL for every Ensembl release, so an unchanged file still
looks like a new one and is downloaded again. PyEnsembl can tell the files
are the same from Ensembl's own metadata (provider, species, versioned
assembly, coverage, masking, Ensembl's Unix checksum, compressed size), but
datacache has nowhere to record that.

## Proposal

Let an asset (and a `materialize` source) carry an optional `identity`: any
JSON value the caller builds from its own metadata. For example:

```python
assets = {"dna.fa.gz": {
    "url": release_url,
    "size": compressed_size,
    "identity": {"provider": "ensembl", "species": "homo_sapiens",
                 "assembly": "GRCh38.p14", "coverage": "primary_assembly",
                 "masking": "soft", "unix_sum": "12345 67890"},
}}
```

- **Recording.** The manifest stores the identity exactly as given (canonical
  JSON), next to the redacted URL and URL fingerprint it records today.
- **Reuse.** When an asset declares an identity and has no trusted SHA-256, the
  bundle is reused if the declared identities and the decompression setting are
  equal, whatever the URL. A different identity means a new download, even if
  labels such as the assembly name match. Assets without an identity keep
  today's URL rule.
- **Trust.** An identity is the caller's claim, not verification: it never sets
  `verified=True`. Trusted SHA-256 expectations still decide verification, and
  full inspection still checks every recorded hash, so same-size corruption is
  still caught. Ensembl's checksum stays caller data; datacache never treats it
  as a SHA-256.
- **Old manifests.** A manifest without a recorded identity never satisfies a
  request that declares one; that install needs `force=True` once.

## Open questions

- Should a declared identity also be required to match on every reuse when a
  trusted SHA-256 is present, or is the hash enough? (Proposed: the hash is
  enough; the identity is still recorded.)
- Size limits and secrets: identities are stored in plain manifests, so
  callers must not put credentials in them. Document this, and cap their size.
