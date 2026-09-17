-- Regression test for the count check in tdigest_add_double_values_count(),
-- the transition function of the tdigest_percentile_of() aggregates accepting
-- an explicit number of occurrences of a value.
--
-- A non-NULL count must be positive. A NULL count means one occurrence
-- of the input value.

\set VERBOSITY terse

-- zero count
SELECT tdigest_percentile_of(v, 0::bigint, 100, 1.0) FROM (VALUES (1.0)) AS t(v);

-- negative count
SELECT tdigest_percentile_of(v, -1::bigint, 100, 1.0) FROM (VALUES (1.0)) AS t(v);

-- the smallest valid count has to work
SELECT tdigest_percentile_of(v, 1::bigint, 100, 1.0) FROM (VALUES (1.0)) AS t(v);

-- NULL count means a single occurrence, so it's valid too
SELECT tdigest_percentile_of(v, NULL::bigint, 100, 1.0) FROM (VALUES (1.0)) AS t(v);
