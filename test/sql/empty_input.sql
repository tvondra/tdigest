-- Regression test for the NULL handling around an empty input.
--
-- tdigest_digest, the only final function left, is not strict, so an
-- aggregate over an empty input calls it with a NULL state and it has to
-- return NULL rather than fail. The functions consuming a digest then have
-- to accept that NULL and return NULL in turn.

-- the final function gets a NULL state and returns NULL
SELECT tdigest(v, 100) IS NULL FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;

-- the same for the other two aggregates sharing the final function
SELECT tdigest(v, 1::bigint, 100) IS NULL FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest(d) IS NULL FROM (VALUES (NULL::tdigest)) AS t(d) WHERE d IS NOT NULL;

-- every function consuming a digest returns NULL for a NULL digest
SELECT tdigest_percentile(tdigest(v, 100), 0.5) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest_percentile(tdigest(v, 100), ARRAY[0.5]) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest_percentile_of(tdigest(v, 100), 1.0) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest_percentile_of(tdigest(v, 100), ARRAY[1.0]) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest_sum(tdigest(v, 100), 0.0, 1.0) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest_avg(tdigest(v, 100), 0.0, 1.0) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest_count(tdigest(v, 100)) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest_json(tdigest(v, 100)) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest_double_array(tdigest(v, 100)) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
SELECT tdigest_is_valid(tdigest(v, 100)) FROM (VALUES (1.0)) AS t(v) WHERE v > 2.0;
