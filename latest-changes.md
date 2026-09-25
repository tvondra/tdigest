Changes for v1.4.7:
- Keep percentile interpolation monotone for closely spaced means and ranks.
- Use consistent inverse-percentile ranks at centroid boundaries with large counts.
- Disable FP contraction for LLVM bitcode as well as for native code.
- Reject text input that underflows to zero, while accepting nonzero subnormals.
- Improve uninstall to cleanup this extension's control and SQL files.
- Mark scalar functions operating on a tdigest value as parallel safe, to
allow parallel queries in more cases.
- Avoid division by zero when rebalancing and compacting digests (benign with IEEE-754).
- Preserve reusable tdigest states during finalization (addresses `FINALFUNC_MODIFY = READ_ONLY`).
- Improve error messages for arrays containing NULL values.
- Fix duplicate keys in tdigest_to_json() output.
- Reject NULL compression in the aggregate transition functions.
- Reduce memory usage (grow/shrink aggregate state, eagerly free copies, read/write float8 arrays directly).
- Corrections and clarifications of assorted comments and docs.
- Various cleanups of stale boilerplate in the Makefile.
