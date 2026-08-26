-- Use terse verbosity, so that the expected output does not depend on the
-- error context, which differs between PostgreSQL versions (particularly
-- for the COPY ... FROM statements below).
\set VERBOSITY terse

-- check tdigest_compact() makes progress even for extreme digests
--
-- tdigest_compact() walks the sorted centroids and merges each one into the
-- current output centroid while the merged count stays below the size limit
-- derived from the quantile position (tdigest.c, tdigest_compact around line
-- 440). If a single centroid holds nearly all the weight of the digest, then
-- q0 is ~1 for everything that follows it, the size limit q0*(1-q0)*... is
-- effectively zero, and no two centroids can ever be merged. Compaction then
-- returns with ncentroids unchanged, i.e. still equal to BUFFER_SIZE.
--
-- Note that nothing here overflows - all the counts are perfectly valid and
-- the digests are well-formed. But code expecting the compaction to free
-- some space in the buffer may write beyond the end of the buffer, etc.
--
-- 1) small number of values - no crash, but already trigger OOB writer
SELECT tdigest_count(tdigest(d)) FROM (
    SELECT ('flags 1 count ' || c || ' compression 10 centroids 1 (' || i || ', ' || c || ')')::tdigest AS d
      FROM generate_series(1, 500) s(i),
           LATERAL (SELECT CASE WHEN i = 1 THEN 9000000000000000000::bigint ELSE 1 END AS c) x
) q;

-- 2) more values - enough to crash the backend with a segmentation fault.
SELECT tdigest_count(tdigest(d)) FROM (
    SELECT ('flags 1 count ' || c || ' compression 10 centroids 1 (' || i || ', ' || c || ')')::tdigest AS d
      FROM generate_series(1, 2000) s(i),
           LATERAL (SELECT CASE WHEN i = 1 THEN 9000000000000000000::bigint ELSE 1 END AS c) x
) q;
