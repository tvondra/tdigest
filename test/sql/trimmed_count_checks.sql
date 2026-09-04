-- Regression test for the count check in tdigest_add_double_count_trimmed,
-- the transition function of the trimmed aggregates accepting an explicit
-- number of occurrences of a value.
--
-- The count says how many times the value is added to the digest, so it
-- has to be a positive value.

\set VERBOSITY terse

-- zero count
SELECT tdigest_avg(v, 0::bigint, 100, 0.0, 1.0) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest_sum(v, 0::bigint, 100, 0.0, 1.0) FROM (VALUES (1.0)) AS t(v);

-- negative count
SELECT tdigest_avg(v, -1::bigint, 100, 0.0, 1.0) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest_sum(v, -1::bigint, 100, 0.0, 1.0) FROM (VALUES (1.0)) AS t(v);

-- the smallest valid count has to work
SELECT tdigest_avg(v, 1::bigint, 100, 0.0, 1.0) FROM (VALUES (1.0)) AS t(v);
SELECT tdigest_sum(v, 1::bigint, 100, 0.0, 1.0) FROM (VALUES (1.0)) AS t(v);
