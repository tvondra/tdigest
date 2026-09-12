DO $$
DECLARE
    v_version numeric;
BEGIN

    SELECT substring(setting from '\d+')::numeric INTO v_version FROM pg_settings WHERE name = 'server_version';

    -- GUCs common for all versions
    PERFORM set_config('extra_float_digits', '0', false);
    PERFORM set_config('parallel_setup_cost', '0', false);
    PERFORM set_config('parallel_tuple_cost', '0', false);
    PERFORM set_config('max_parallel_workers_per_gather', '2', false);

    -- 9.6 used somewhat different GUC name for relation size
    IF v_version < 10 THEN
        PERFORM set_config('min_parallel_relation_size', '1kB', false);
    ELSE
        PERFORM set_config('min_parallel_table_scan_size', '1kB', false);
    END IF;

    -- in 14 disable Memoize nodes, to make explain more consistent
    IF v_version >= 14 THEN
        PERFORM set_config('enable_memoize', 'off', false);
    END IF;

END;
$$ LANGUAGE plpgsql;

-- the user-facing functions must all be parallel safe, otherwise queries
-- using them lose parallelism (the aggregate support functions take or
-- return "internal" and cannot be called from SQL, so they are exempt)
SELECT p.proname, p.proparallel
  FROM pg_proc p JOIN pg_depend d ON (d.objid = p.oid AND d.deptype = 'e')
       JOIN pg_extension e ON (e.oid = d.refobjid)
 WHERE e.extname = 'tdigest'
   AND p.proargtypes::oid[] && ARRAY['internal'::regtype]::oid[] IS NOT TRUE
   AND p.prorettype <> 'internal'::regtype
   AND p.proparallel <> 's'
 ORDER BY 1;

-- test parallel query
CREATE TABLE test_parallel (v double precision, c int, d int);
INSERT INTO test_parallel SELECT 1000 * random(), 1 + mod(i,7), mod(i,113) FROM generate_series(1,100000) s(i);
ANALYZE test_parallel;

CREATE TABLE test_parallel_2 (d tdigest);
INSERT INTO test_parallel_2 SELECT tdigest(v, 100) FROM test_parallel GROUP BY d;
ANALYZE test_parallel_2;

-- individual values
EXPLAIN (COSTS OFF)
WITH x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM test_parallel)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(v, 100, 0.95) AS b
  FROM test_parallel) foo;

WITH x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM test_parallel)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(v, 100, 0.95) AS b
  FROM test_parallel) foo;


EXPLAIN (COSTS OFF)
SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    0.95 AS a,
    tdigest_percentile_of(v, 100, 950) AS b
  FROM test_parallel) foo;

SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    0.95 AS a,
    tdigest_percentile_of(v, 100, 950) AS b
  FROM test_parallel) foo;


EXPLAIN (COSTS OFF)
WITH x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM test_parallel)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(d, 0.95) AS b
  FROM test_parallel_2) foo;

WITH x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM test_parallel)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(d, 0.95) AS b
  FROM test_parallel_2) foo;


EXPLAIN (COSTS OFF)
SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    0.95 AS a,
    tdigest_percentile_of(d, 950) AS b
  FROM test_parallel_2) foo;

SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    0.95 AS a,
    tdigest_percentile_of(d, 950) AS b
  FROM test_parallel_2) foo;


-- array of percentiles / values
EXPLAIN (COSTS OFF)
WITH x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM test_parallel)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(v, 100, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM test_parallel) foo;

WITH x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM test_parallel)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(v, 100, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM test_parallel) foo;


EXPLAIN (COSTS OFF)
WITH x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) FROM test_parallel)) AS p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile_of(v, 100, ARRAY[950, 990])) AS b
  FROM test_parallel) foo;

WITH x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) FROM test_parallel)) AS p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile_of(v, 100, ARRAY[950, 990])) AS b
  FROM test_parallel) foo;


EXPLAIN (COSTS OFF)
WITH x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM test_parallel)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(d, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM test_parallel_2) foo;

WITH x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM test_parallel)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(d, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM test_parallel_2) foo;


EXPLAIN (COSTS OFF)
WITH x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) FROM test_parallel)) AS p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile_of(d, ARRAY[950, 990])) AS b
  FROM test_parallel_2) foo;

WITH x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) FROM test_parallel)) AS p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile_of(d, ARRAY[950, 990])) AS b
  FROM test_parallel_2) foo;

-- trimmed aggregates
EXPLAIN (COSTS OFF)
SELECT tdigest_sum(v, 100, 0.05, 0.95) FROM test_parallel;

-- trimming nothing has to reproduce the exact sum
SELECT abs(a - b) / a < 0.01
FROM (
  SELECT
    (SELECT sum(v) FROM test_parallel) AS a,
    tdigest_sum(v, 100, 0.0, 1.0) AS b
  FROM test_parallel) foo;


EXPLAIN (COSTS OFF)
SELECT tdigest_avg(v, 100, 0.05, 0.95) FROM test_parallel;

SELECT abs(a - b) / a < 0.01
FROM (
  SELECT
    (SELECT avg(v) FROM test_parallel) AS a,
    tdigest_avg(v, 100, 0.0, 1.0) AS b
  FROM test_parallel) foo;


EXPLAIN (COSTS OFF)
SELECT tdigest_sum(d, 0.05, 0.95) FROM test_parallel_2;

SELECT abs(a - b) / a < 0.01
FROM (
  SELECT
    (SELECT sum(v) FROM test_parallel) AS a,
    tdigest_sum(d, 0.0, 1.0) AS b
  FROM test_parallel_2) foo;


EXPLAIN (COSTS OFF)
SELECT tdigest_avg(d, 0.05, 0.95) FROM test_parallel_2;

SELECT abs(a - b) / a < 0.01
FROM (
  SELECT
    (SELECT avg(v) FROM test_parallel) AS a,
    tdigest_avg(d, 0.0, 1.0) AS b
  FROM test_parallel_2) foo;


-- casting a digest to text must not force a serial plan
EXPLAIN (COSTS OFF)
SELECT count(*) FROM test_parallel_2 WHERE length(d::text) > 5;

DROP TABLE test_parallel;
DROP TABLE test_parallel_2;
