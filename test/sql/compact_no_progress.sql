-- Use terse verbosity, so that the expected output does not depend on the
-- error context, which differs between PostgreSQL versions.
\set VERBOSITY terse

-- check tdigest_compact() makes progress even for extreme digests
--
-- tdigest_compact() walks the sorted centroids and merges each one into the
-- current output centroid while the merged count stays below the size limit
-- derived from the quantile position. With the former subtraction-based
-- calculation of (1-q), a centroid holding nearly all the weight could make
-- q0 round to 1 for everything following it. The size limit then became zero
-- and compaction returned with ncentroids unchanged, still at BUFFER_SIZE.
-- The current code uses exact integer remainders for these factors, and
-- raises an error if a compaction still leaves the buffer full.
--
-- Note that nothing here overflows - all the counts are perfectly valid and
-- the digests are well-formed. The old code assumed compaction always freed
-- some space. These regressions check progress and preservation of the count.
--
-- 1) small number of values
SELECT tdigest_count(tdigest(d)) FROM (
    SELECT ('flags 1 count ' || c || ' compression 10 centroids 1 (' || i || ', ' || c || ')')::tdigest AS d
      FROM generate_series(1, 500) s(i),
           LATERAL (SELECT CASE WHEN i = 1 THEN 9000000000000000000::bigint ELSE 1 END AS c) x
) q;

-- 2) more values, requiring additional compactions
SELECT tdigest_count(tdigest(d)) FROM (
    SELECT ('flags 1 count ' || c || ' compression 10 centroids 1 (' || i || ', ' || c || ')')::tdigest AS d
      FROM generate_series(1, 2000) s(i),
           LATERAL (SELECT CASE WHEN i = 1 THEN 9000000000000000000::bigint ELSE 1 END AS c) x
) q;
