-- Regression test for check_compression(), which rejects compression
-- values outside the [MIN_COMPRESSION, MAX_COMPRESSION] = [10, 10000]
-- range accepted by the t-digest implementation.

\set VERBOSITY terse

-- compression below the minimum
SELECT tdigest_percentile(tdigest(v, 9), 0.5) FROM (VALUES (1.0)) AS t(v);

-- compression above the maximum
SELECT tdigest_percentile(tdigest(v, 10001), 0.5) FROM (VALUES (1.0)) AS t(v);

-- the incremental API checks the compression too
SELECT tdigest_add(NULL::tdigest, 1.0, 9);
SELECT tdigest_add(NULL::tdigest, ARRAY[1.0], 10001);

-- and so does the aggregate adding values with a count
SELECT tdigest(v, 1, 9) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest(v, 1, 10001) FROM (VALUES (1.0)) AS t(v);

-- both ends of the accepted range have to work
SELECT tdigest_percentile(tdigest(v, 10), 0.5) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest_percentile(tdigest(v, 10000), 0.5) FROM (VALUES (1.0)) AS t(v);

-- the transition functions are not strict, so they have to reject a NULL
-- compression themselves (once there's a non-NULL value to add)
SELECT tdigest(v, NULL) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest(v, 1, NULL) FROM (VALUES (1.0)) AS t(v);
