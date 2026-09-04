-- Regression test for the count check in tdigest_add_double_array_count()
-- and tdigest_add_double_array_values_count(), the transition functions of
-- the aggregates accepting an explicit number of occurrences of a value and
-- an array of percentiles (or values).
--
-- The count says how many times the value is added to the digest, so it has
-- to be a positive value.

\set VERBOSITY terse

-- zero count
SELECT tdigest_percentile(v, 0::bigint, 100, ARRAY[0.5]) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest_percentile_of(v, 0::bigint, 100, ARRAY[1.0]) FROM (VALUES (1.0)) AS t(v);

-- negative count
SELECT tdigest_percentile(v, -1::bigint, 100, ARRAY[0.5]) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest_percentile_of(v, -1::bigint, 100, ARRAY[1.0]) FROM (VALUES (1.0)) AS t(v);

-- the smallest valid count has to work
SELECT tdigest_percentile(v, 1::bigint, 100, ARRAY[0.5]) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest_percentile_of(v, 1::bigint, 100, ARRAY[1.0]) FROM (VALUES (1.0)) AS t(v);

-- NULL count means a single occurrence, so it's valid too
SELECT tdigest_percentile(v, NULL::bigint, 100, ARRAY[0.5]) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest_percentile_of(v, NULL::bigint, 100, ARRAY[1.0]) FROM (VALUES (1.0)) AS t(v);
