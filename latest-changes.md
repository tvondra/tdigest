Changes for v2.0.0-dev:
- Collapse the ~20 aggregate variants into a single tdigest() builder
aggregate plus plain functions operating on a tdigest value. See the
"Upgrading to 2.0.0" section in README.md.
- INCOMPATIBLE: tdigest_percentile(tdigest, ...), tdigest_percentile_of
(tdigest, ...), tdigest_sum(tdigest, ...) and tdigest_avg(tdigest, ...)
are now plain functions rather than aggregates. The signatures are
unchanged, so existing queries keep parsing but change meaning - wrap
the digest in tdigest() to keep aggregating.
- INCOMPATIBLE: the aggregates taking raw values and a percentile (e.g.
tdigest_percentile(double precision, int, double precision)) are gone.
Build a digest with tdigest() and pass it to the plain function.
- Remove the percentiles/values/trim thresholds from the aggregate state,
and keep the centroids in a separately allocated buffer that grows with
repalloc, so the state pointer stays stable.
- Trimmed sum/avg computed over a digest built by the tdigest() aggregate
are now estimates rather than exact values - the aggregate compacts the
digest, while the removed tdigest_sum/tdigest_avg aggregates calculated
from the raw, possibly uncompacted aggregate state.
- Queries rewritten from the trimmed aggregates now consume a digest
compacted by tdigest(), so estimates may differ from previously
uncompacted states, and can be noticeably worse when merging several
pre-aggregated digests. tdigest_sum()/tdigest_avg() themselves do not
compact, they only sort the input and convert the old on-disk format.
- Fix tdigest_percentile() and tdigest_percentile_of() to compact the
digest before computing, which also sorts the centroids and converts
the old on-disk format. The interpolation assumes each centroid covers
a distinct contiguous range of the input, which sorting alone does not
guarantee.
- Give all the functions operating on a digest named parameters with the
p_ prefix, so that they may be called using named arguments.
- On PostgreSQL 13 and later, switch the tdigest type's default to
extended storage, allowing TOAST compression when it saves enough
space. Existing columns retain their storage policy; use ALTER TABLE
... ALTER COLUMN ... SET STORAGE EXTENDED to enable compression.
PostgreSQL 11 and 12 retain the external type default, but support the
same per-column setting. Existing values need a real rewrite, not a
no-op UPDATE, to apply the new policy. See "Upgrading to 2.0.0" in
README.md.
