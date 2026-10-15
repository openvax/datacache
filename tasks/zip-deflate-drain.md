# Bounded deflate output draining — #123

A complete compressed input can leave output buffered after zlib reaches the
maximum output length. Continue bounded empty-input decompression until the
output falls below that limit. Preserve all existing expansion, CRC, stream-end
and trailing-input checks. Do not use an unbounded flush.

- [ ] Reproduce with the actual HTTP ZIP installer at output-boundary lengths.
- [ ] Fix the draining loop; retain failure/publication contracts.
- [ ] Run lint and all tests, bump patch version, review final CI.
- [ ] Merge and deploy from clean master; verify public package hashes.
