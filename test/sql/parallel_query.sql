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

-- test parallel query
DROP TABLE t;
CREATE TABLE t (v double precision, c int, d int);
INSERT INTO t SELECT 1000 * random(), 1 + mod(i,7), mod(i,113) FROM generate_series(1,100000) s(i);
ANALYZE t;

CREATE TABLE t2 (d tdigest);
INSERT INTO t2 SELECT tdigest(v, 100) FROM t GROUP BY d;
ANALYZE t2;

-- individual values
EXPLAIN (COSTS OFF)
WITH x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM t)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(v, 100, 0.95) AS b
  FROM t) foo;

WITH x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM t)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(v, 100, 0.95) AS b
  FROM t) foo;


EXPLAIN (COSTS OFF)
SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    0.95 AS a,
    tdigest_percentile_of(v, 100, 950) AS b
  FROM t) foo;

SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    0.95 AS a,
    tdigest_percentile_of(v, 100, 950) AS b
  FROM t) foo;


EXPLAIN (COSTS OFF)
WITH x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM t)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(d, 0.95) AS b
  FROM t2) foo;

WITH x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM t)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(d, 0.95) AS b
  FROM t2) foo;


EXPLAIN (COSTS OFF)
SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    0.95 AS a,
    tdigest_percentile_of(d, 950) AS b
  FROM t2) foo;

SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    0.95 AS a,
    tdigest_percentile_of(d, 950) AS b
  FROM t2) foo;


-- array of percentiles / values
EXPLAIN (COSTS OFF)
WITH x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM t)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(v, 100, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM t) foo;

WITH x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM t)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(v, 100, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM t) foo;


EXPLAIN (COSTS OFF)
WITH x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) FROM t)) AS p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile_of(v, 100, ARRAY[950, 990])) AS b
  FROM t) foo;

WITH x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) FROM t)) AS p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile_of(v, 100, ARRAY[950, 990])) AS b
  FROM t) foo;


EXPLAIN (COSTS OFF)
WITH x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM t)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(d, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM t2) foo;

WITH x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM t)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(d, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM t2) foo;


EXPLAIN (COSTS OFF)
WITH x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) FROM t)) AS p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile_of(d, ARRAY[950, 990])) AS b
  FROM t2) foo;

WITH x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) FROM t)) AS p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile_of(d, ARRAY[950, 990])) AS b
  FROM t2) foo;
