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

-- check trimmed mean (from raw data)
-- we compare the result to a range, to deal with the randomness
WITH data AS (SELECT random() AS r FROM generate_series(1,10000) AS x)
SELECT
    tdigest_avg(data.r, 50, 0.1, 0.9) between 0.45 and 0.55 AS mean_10_90,
    tdigest_avg(data.r, 50, 0.25, 0.75) between 0.45 and 0.55 AS mean_25_75,
    tdigest_avg(data.r, 50, 0.0, 0.5) between 0.2 and 0.3 AS mean_0_50,
    tdigest_avg(data.r, 50, 0.5, 1.0) between 0.7 and 0.8 AS mean_50_100
FROM data;

WITH data AS (SELECT random() AS r, (1 + (3 * random())::int) AS c FROM generate_series(1,10000) AS x)
SELECT
    tdigest_avg(data.r, data.c, 100, 0.1, 0.9) between 0.45 and 0.55 AS mean_10_90,
    tdigest_avg(data.r, data.c, 100, 0.25, 0.75) between 0.45 and 0.55 AS mean_25_75,
    tdigest_avg(data.r, data.c, 100, 0.0, 0.5) between 0.2 and 0.3 AS mean_0_50,
    tdigest_avg(data.r, data.c, 100, 0.5, 1.0) between 0.7 and 0.8 AS mean_50_100
FROM data;

-- check trimmed mean (from pracalculated tdigest)
-- we compare the result to a range, to deal with the randomness
WITH data AS (SELECT tdigest(random(), 50) AS d FROM generate_series(1,10000) AS x)
SELECT
    tdigest_avg(data.d, 0.1, 0.9) between 0.45 and 0.55 AS mean_10_90,
    tdigest_avg(data.d, 0.25, 0.75) between 0.45 and 0.55 AS mean_25_75,
    tdigest_avg(data.d, 0.0, 0.5) between 0.2 and 0.3 AS mean_0_50,
    tdigest_avg(data.d, 0.5, 1.0) between 0.7 and 0.8 AS mean_50_100
FROM data;

-- check trimmed sum (from raw data)
-- we compare the result to a range, to deal with the randomness
WITH data AS (SELECT random() AS r FROM generate_series(1,10000) AS x)
SELECT
    tdigest_sum(data.r, 50, 0.1, 0.9) between 8000 * 0.45 and 8000 * 0.55 AS sum_10_90,
    tdigest_sum(data.r, 50, 0.25, 0.75) between 5000 * 0.45 and 5000 * 0.55 AS sum_25_75,
    tdigest_sum(data.r, 50, 0.0, 0.5) between 5000 * 0.2 and 5000 * 0.3 AS sum_0_50,
    tdigest_sum(data.r, 50, 0.5, 1.0) between 5000 * 0.7 and 5000 * 0.8 AS sum_50_100
FROM data;

WITH data AS (SELECT random() AS r, (1 + (3 * random())::int) AS c FROM generate_series(1,10000) AS x)
SELECT
    tdigest_sum(data.r, data.c, 100, 0.1, 0.9) between 20000 * 0.45 and 20000 * 0.55 AS sum_10_90,
    tdigest_sum(data.r, data.c, 100, 0.25, 0.75) between 12500 * 0.45 and 12500 * 0.55 AS sum_25_75,
    tdigest_sum(data.r, data.c, 100, 0.0, 0.5) between 12500 * 0.2 and 12500 * 0.3 AS sum_0_50,
    tdigest_sum(data.r, data.c, 100, 0.5, 1.0) between 12500 * 0.7 and 12500 * 0.8 AS sum_50_100
FROM data;

-- check trimmed sum (from pracalculated tdigest)
-- we compare the result to a range, to deal with the randomness
WITH data AS (SELECT tdigest(random(), 50) AS d FROM generate_series(1,10000) AS x)
SELECT
    tdigest_sum(data.d, 0.1, 0.9) between 8000 * 0.45 and 8000 * 0.55 AS sum_10_90,
    tdigest_sum(data.d, 0.25, 0.75) between 5000 * 0.45 and 5000 * 0.55 AS sum_25_75,
    tdigest_sum(data.d, 0.0, 0.5) between 5000 * 0.2 and 5000 * 0.3 AS sum_0_50,
    tdigest_sum(data.d, 0.5, 1.0) between 5000 * 0.7 and 5000 * 0.8 AS sum_50_100
FROM data;

WITH data AS (SELECT tdigest(random(), 50) AS d FROM generate_series(1,10000) AS x)
SELECT
    tdigest_digest_sum(data.d, 0.05, 0.95) between 9000 * 0.45 and 9000 * 0.55 AS sum_05_95,
    tdigest_digest_avg(data.d, 0.05, 0.95) between 0.45 and 0.55 AS mean_05_95
FROM data;
