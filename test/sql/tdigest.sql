\set ECHO none

-- disable the notices for the create script (shell types etc.)
SET client_min_messages = 'WARNING';
\i tdigest--1.0.0.sql
\i tdigest--1.0.0--1.0.1.sql
\i tdigest--1.0.1--1.2.0.sql
\i tdigest--1.2.0--1.3.0.sql
\i tdigest--1.3.0--1.4.0-dev.sql
SET client_min_messages = 'NOTICE';
SET extra_float_digits = 0;

\set ECHO all

-- SRF function implementing a simple deterministict PRNG

CREATE OR REPLACE FUNCTION prng(nrows int, seed int = 23982, p1 bigint = 16807, p2 bigint = 0, n bigint = 2147483647) RETURNS SETOF double precision AS $$
DECLARE
    val INT := seed;
BEGIN
    FOR i IN 1..nrows LOOP
        val := (val * p1 + p2) % n;

        RETURN NEXT (val::double precision / n);
    END LOOP;

    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION random_normal(nrows int, mean double precision = 0.5, stddev double precision = 0.1, minval double precision = 0.0, maxval double precision = 1.0, seed int = 23982, p1 bigint = 16807, p2 bigint = 0, n bigint = 2147483647) RETURNS SETOF double precision AS $$
DECLARE
    v BIGINT := seed;
    x DOUBLE PRECISION;
    y DOUBLE PRECISION;
    s DOUBLE PRECISION;
    r INT := nrows;
BEGIN

    WHILE true LOOP

        -- random x
        v := (v * p1 + p2) % n;
        x := 2 * v / n::double precision - 1.0;

        -- random y
        v := (v * p1 + p2) % n;
        y := 2 * v / n::double precision - 1.0;

        s := x^2 + y^2;

        IF s != 0.0 AND s < 1.0 THEN

            s = sqrt(-2 * ln(s) / s);

            x := mean + stddev * s * x;

            IF x >= minval AND x <= maxval THEN
                RETURN NEXT x;
                r := r - 1;
            END IF;

            EXIT WHEN r = 0;

            y := mean + stddev * s * y;

            IF y >= minval AND y <= maxval THEN
                RETURN NEXT y;
                r := r - 1;
            END IF;

            EXIT WHEN r = 0;

        END IF;

    END LOOP;

END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    v_version numeric;
BEGIN

    SELECT substring(setting from '\d+')::numeric INTO v_version FROM pg_settings WHERE name = 'server_version';

    -- GUCs common for all versions
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


-----------------------------------------------------------
-- nice data set with ordered (asc) / evenly-spaced data --
-----------------------------------------------------------

-- 10 centroids (tiny)
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(1,100000) s(i))
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(1,100000) s(i)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 10, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 100 centroids (okay-ish)
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(1,100000) s(i))
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(1,100000) s(i)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 100, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 1000 centroids (very accurate)
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(1,100000) s(i))
SELECT
    p,
    abs(a - b) < 0.001, -- arbitrary threshold of 0.1%
    (CASE WHEN abs(a - b) < 0.001 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(1,100000) s(i)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 1000, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

------------------------------------------------------------
-- nice data set with ordered (desc) / evenly-spaced data --
------------------------------------------------------------

-- 10 centroids (tiny)
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(100000,1,-1) s(i))
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(100000,1,-1) s(i)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 10, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 100 centroids (okay-ish)
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(100000,1,-1) s(i))
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(100000,1,-1) s(i)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 100, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 1000 centroids (very accurate)
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(100000,1,-1) s(i))
SELECT
    p,
    abs(a - b) < 0.001, -- arbitrary threshold of 0.1%
    (CASE WHEN abs(a - b) < 0.001 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(100000,1,-1) s(i)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 1000, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

----------------------------------------------------
-- nice data set with random / evenly-spaced data --
----------------------------------------------------

-- 10 centroids (tiny)
WITH data AS (SELECT i / 100000.0 AS x FROM (SELECT generate_series(1,100000) AS i, prng(100000, 49979693) AS x ORDER BY x) foo)
SELECT
    p,
    abs(a - b) < 0.1, -- arbitrary threshold of 10%
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT i / 100000.0 AS x FROM (SELECT generate_series(1,100000) AS i, prng(100000, 49979693) AS x ORDER BY x) foo),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 10, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 100 centroids (okay-ish)
WITH data AS (SELECT i / 100000.0 AS x FROM (SELECT generate_series(1,100000) AS i, prng(100000, 49979693) AS x ORDER BY x) foo)
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT i / 100000.0 AS x FROM (SELECT generate_series(1,100000) AS i, prng(100000, 49979693) AS x ORDER BY x) foo),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 100, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 1000 centroids (very accurate)
WITH data AS (SELECT i / 100000.0 AS x FROM (SELECT generate_series(1,100000) AS i, prng(100000, 49979693) AS x ORDER BY x) foo)
SELECT
    p,
    abs(a - b) < 0.001, -- arbitrary threshold of 0.1%
    (CASE WHEN abs(a - b) < 0.001 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT i / 100000.0 AS x FROM (SELECT generate_series(1,100000) AS i, prng(100000, 49979693) AS x ORDER BY x) foo),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 1000, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

----------------------------------------------
-- nice data set with random data (uniform) --
----------------------------------------------

-- 10 centroids (tiny)
WITH data AS (SELECT x FROM prng(100000) s(x))
SELECT
    p,
    abs(a - b) < 0.1, -- arbitrary threshold of 10%
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT x FROM prng(100000) s(x)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 10, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 100 centroids (okay-ish)
WITH data AS (SELECT x FROM prng(100000) s(x))
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT x FROM prng(100000) s(x)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 100, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 1000 centroids (very accurate)
WITH data AS (SELECT x FROM prng(100000) s(x))
SELECT
    p,
    abs(a - b) < 0.001, -- arbitrary threshold of 0.1%
    (CASE WHEN abs(a - b) < 0.001 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT x FROM prng(100000) s(x)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 1000, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

--------------------------------------------------
-- nice data set with random data (skewed sqrt) --
--------------------------------------------------

-- 10 centroids (tiny)
WITH data AS (SELECT sqrt(z) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.1, -- arbitrary threshold of 10%
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT sqrt(z) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 10, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 100 centroids (okay-ish)
WITH data AS (SELECT sqrt(z) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT sqrt(z) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 100, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 1000 centroids (very accurate)
WITH data AS (SELECT sqrt(z) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.001, -- arbitrary threshold of 0.1%
    (CASE WHEN abs(a - b) < 0.001 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT sqrt(z) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 1000, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-------------------------------------------------------
-- nice data set with random data (skewed sqrt+sqrt) --
-------------------------------------------------------

-- 10 centroids (tiny)
WITH data AS (SELECT sqrt(sqrt(z)) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.1, -- arbitrary threshold of 10%
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT sqrt(sqrt(z)) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 10, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 100 centroids (okay-ish)
WITH data AS (SELECT sqrt(sqrt(z)) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT sqrt(sqrt(z)) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 100, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 1000 centroids (very accurate)
WITH data AS (SELECT sqrt(sqrt(z)) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.001, -- arbitrary threshold of 0.1%
    (CASE WHEN abs(a - b) < 0.001 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT sqrt(sqrt(z)) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 1000, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-------------------------------------------------
-- nice data set with random data (skewed pow) --
-------------------------------------------------

-- 10 centroids (tiny)
WITH data AS (SELECT pow(z, 2) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.1, -- arbitrary threshold of 10%
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT pow(z, 2) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 10, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 100 centroids (okay-ish)
WITH data AS (SELECT pow(z, 2) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.005, -- arbitrary threshold of 0.5%
    (CASE WHEN abs(a - b) < 0.005 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT pow(z, 2) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 100, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 1000 centroids (very accurate)
WITH data AS (SELECT pow(z, 2) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.001, -- arbitrary threshold of 0.1%
    (CASE WHEN abs(a - b) < 0.001 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT pow(z, 2) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 1000, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;


-----------------------------------------------------
-- nice data set with random data (skewed pow+pow) --
-----------------------------------------------------

-- 10 centroids (tiny)
WITH data AS (SELECT pow(z, 4) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.1, -- arbitrary threshold of 10%
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT pow(z, 4) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 10, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 100 centroids (okay-ish)
WITH data AS (SELECT pow(z, 4) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT pow(z, 4) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 100, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 1000 centroids (very accurate)
WITH data AS (SELECT pow(z, 4) AS x FROM prng(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.001, -- arbitrary threshold of 0.1%
    (CASE WHEN abs(a - b) < 0.001 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT pow(z, 4) AS x FROM prng(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 1000, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

----------------------------------------------------------
-- nice data set with random data (normal distribution) --
----------------------------------------------------------

-- 10 centroids (tiny)
WITH data AS (SELECT pow(z, 4) AS x FROM random_normal(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.025, -- arbitrary threshold of 2.5%
    (CASE WHEN abs(a - b) < 0.025 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT pow(z, 4) AS x FROM random_normal(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 10, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 100 centroids (okay-ish)
WITH data AS (SELECT pow(z, 4) AS x FROM random_normal(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT pow(z, 4) AS x FROM random_normal(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 100, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- 1000 centroids (very accurate)
WITH data AS (SELECT pow(z, 4) AS x FROM random_normal(100000) s(z))
SELECT
    p,
    abs(a - b) < 0.001, -- arbitrary threshold of 0.1%
    (CASE WHEN abs(a - b) < 0.001 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(tdigest_percentile(x, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- make sure the resulting percentiles are in the right order
WITH data AS (SELECT pow(z, 4) AS x FROM random_normal(100000) s(z)),
     perc AS (SELECT array_agg((i/100.0)::double precision) AS p FROM generate_series(1,99) s(i))
SELECT * FROM (
    SELECT
        p,
        a,
        LAG(a) OVER (ORDER BY p) AS b
    FROM (
        SELECT
            unnest((SELECT p FROM perc)) AS p,
            unnest(tdigest_percentile(x, 1000, (SELECT p FROM perc))) AS a
        FROM data
    ) foo ) bar WHERE a <= b;

-- some basic tests to verify transforming from and to text work
-- 10 centroids (tiny)
WITH data AS (SELECT i / 100000.0 AS x FROM generate_series(1,100000) s(i)),
     intermediate AS (SELECT tdigest(x, 10)::text AS intermediate_x FROM data),
     tdigest_parsed AS (SELECT tdigest_percentile(intermediate_x::tdigest, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS a FROM intermediate),
     pg_percentile AS (SELECT percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x) AS b FROM data)
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(a) AS a,
        unnest(b) AS b
    FROM tdigest_parsed,
         pg_percentile
) foo;

-- verify we can store tdigest in a summary table
CREATE TABLE intermediate_tdigest (grouping int, summary tdigest);

WITH data AS (SELECT row_number() OVER () AS i, pow(z, 4) AS x FROM random_normal(100000) s(z))
INSERT INTO intermediate_tdigest
SELECT
    i % 10 AS grouping,
    tdigest(x, 100) AS summary
FROM data
GROUP BY i % 10;

WITH data AS (SELECT pow(z, 4) AS x FROM random_normal(100000) s(z)),
     intermediate AS (SELECT tdigest_percentile(summary, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS a FROM intermediate_tdigest),
     pg_percentile AS (SELECT percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x) AS b FROM data)
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.01 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(a) AS a,
        unnest(b) AS b
    FROM intermediate,
         pg_percentile
) foo;

-- verify 'extreme' percentiles for the dataset would not read out of bounds on the centroids
WITH data AS (SELECT x FROM generate_series(1,10) AS x)
SELECT
    p,
    abs(a - b) < 0.1, -- arbitrary threshold of 10% given the small dataset and extreme percentiles it is not very accurate
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.99]) AS p,
        unnest(tdigest_percentile(x, 10, ARRAY[0.01, 0.99])) AS a,
        unnest(percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY x)) AS b
    FROM data
) foo;

-- check that the computed percentiles are perfectly correlated (don't decrease for higher p values)
-- first test on a tiny t-digest with all centroids having count = 1
WITH
-- percentiles to compute
perc AS (SELECT array_agg((i / 100.0)::double precision) AS percentiles FROM generate_series(1,99) s(i)),
-- input data (just 15 points)
input_data AS (select i::double precision AS val FROM generate_series(1,15) s(i))
SELECT * FROM (
    SELECT p, v AS v1, lag(v, 1) OVER (ORDER BY p) v2 FROM (
        SELECT
            unnest(perc.percentiles) p,
            unnest(tdigest_percentile(input_data.val, 100, perc.percentiles)) v
        FROM perc, input_data
        GROUP BY perc.percentiles
    ) foo
) bar where v2 > v1;

-- test casting to json
SELECT cast(tdigest(i / 1000.0, 10) as json) from generate_series(1,1000) s(i);
SELECT cast(tdigest(i / 1000.0, 25) as json) from generate_series(1,1000) s(i);
SELECT cast(tdigest(i / 1000.0, 100) as json) from generate_series(1,1000) s(i);

-- test casting to double precision array
SELECT array_agg(round(v::numeric,3)) FROM (
  SELECT unnest(cast(tdigest(i / 1000.0, 10) as double precision[])) AS v from generate_series(1,1000) s(i)
) foo;

SELECT array_agg(round(v::numeric,3)) FROM (
  SELECT unnest(cast(tdigest(i / 1000.0, 25) as double precision[])) AS v from generate_series(1,1000) s(i)
) foo;

SELECT array_agg(round(v::numeric,3)) FROM (
  SELECT unnest(cast(tdigest(i / 1000.0, 100) as double precision[])) AS v from generate_series(1,1000) s(i)
) foo;

-- <value,count> API
select tdigest_percentile(value, count, 100, 0.95)
from (values
  (47325940488,1),
  (15457695432,2),
  (6889790700,3),
  (4188763788,4),
  (2882932224,5),
  (2114815860,6),
  (1615194324,7),
  (2342114568,9),
  (1626471924,11),
  (1660755408,14),
  (1143728292,17),
  (1082582424,21),
  (911488284,26),
  (728863908,32),
  (654898692,40),
  (530198076,50),
  (417883440,62),
  (341452344,77),
  (274579584,95),
  (231921120,118),
  (184091820,146),
  (152469828,181),
  (125634972,224),
  (107059704,278),
  (88746120,345),
  (73135668,428),
  (61035756,531),
  (50683320,658),
  (42331824,816),
  (35234400,1012),
  (29341356,1255),
  (24290928,1556),
  (20284668,1929),
  (17215908,2391),
  (14737488,2964),
  (12692772,3674),
  (11220732,4555),
  (9787584,5647),
  (8148420,7000),
  (6918612,8678),
  (6015000,10758),
  (5480316,13336),
  (5443356,16532),
  (4535616,20494),
  (3962316,25406),
  (3914484,31495),
  (3828108,39043),
  (3583536,48400),
  (4104120,60000),
  (166024740,2147483647)) foo (count, value);

----------------------------------------------
-- nice data set with random data (uniform) --
----------------------------------------------

-- 10 centroids (tiny)
WITH
 data AS (SELECT prng(1000) x, prng(1000, 29823218) cnt),
 data_expanded AS (SELECT x FROM (SELECT x, generate_series(1, (10 + 100 * cnt)::int) FROM data) foo ORDER BY random())
SELECT
    p,
    abs(a - b) < 0.1, -- arbitrary threshold of 10%
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(a) AS a,
        unnest(b) AS b
    FROM
       (SELECT percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x) a FROM data_expanded) foo,
       (SELECT tdigest_percentile(x, (10 + 100 * cnt)::int, 10, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) b FROM data) bar
) baz;

-- 100 centroids (okay-ish)
WITH
 data AS (SELECT prng(1000) x, prng(1000, 29823218) cnt),
 data_expanded AS (SELECT x FROM (SELECT x, generate_series(1, (10 + 100 * cnt)::int) FROM data) foo ORDER BY random())
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(a) AS a,
        unnest(b) AS b
    FROM
       (SELECT percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x) a FROM data_expanded) foo,
       (SELECT tdigest_percentile(x, (10 + 100 * cnt)::int, 100, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) b FROM data) bar
) baz;

-- 1000 centroids (very accurate)
WITH
 data AS (SELECT prng(1000) x, prng(1000, 29823218) cnt),
 data_expanded AS (SELECT x FROM (SELECT x, generate_series(1, (10 + 100 * cnt)::int) FROM data) foo ORDER BY random())
SELECT
    p,
    abs(a - b) < 0.01, -- arbitrary threshold of 1%
    (CASE WHEN abs(a - b) < 0.1 THEN NULL ELSE (a - b) END) AS err
FROM (
    SELECT
        unnest(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) AS p,
        unnest(a) AS a,
        unnest(b) AS b
    FROM
       (SELECT percentile_cont(ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) WITHIN GROUP (ORDER BY x) a FROM data_expanded) foo,
       (SELECT tdigest_percentile(x, (10 + 100 * cnt)::int, 1000, ARRAY[0.01, 0.05, 0.1, 0.9, 0.95, 0.99]) b FROM data) bar
) baz;

-- test incremental API (adding values one by one)
CREATE TABLE t (d tdigest);
INSERT INTO t VALUES (NULL);

-- check this produces the same result building the tdigest at once, but we
-- need to be careful about feeding the data in the same order, and we must
-- not compactify the t-digest after each increment
DO LANGUAGE plpgsql $$
DECLARE
  r RECORD;
BEGIN
    FOR r IN (SELECT i FROM generate_series(1,1000) s(i) ORDER BY md5(i::text)) LOOP
        UPDATE t SET d = tdigest_add(d, r.i, 100, false);
    END LOOP;
END$$;

-- compare the results, but do force a compaction of the incremental result
WITH x AS (SELECT i FROM generate_series(1,1000) s(i) ORDER BY md5(i::text))
SELECT (SELECT tdigest(d)::text FROM t) = (SELECT tdigest(x.i, 100)::text FROM x) AS match;

-- now try the same thing with bulk incremental update (using arrays)
TRUNCATE t;
INSERT INTO t VALUES (NULL);

DO LANGUAGE plpgsql $$
DECLARE
  r RECORD;
BEGIN
    FOR r IN (SELECT a, array_agg(i::double precision) AS v FROM (SELECT mod(i,5) AS a, i FROM generate_series(1,1000) s(i) ORDER BY mod(i,5), md5(i::text)) foo GROUP BY a ORDER BY a) LOOP
        UPDATE t SET d = tdigest_add(d, r.v, 100, false);
    END LOOP;
END$$;

-- compare the results, but do force a compaction of the incremental result
WITH x AS (SELECT mod(i,5) AS a, i::double precision AS d FROM generate_series(1,1000) s(i) ORDER BY mod(i,5), i)
SELECT (SELECT tdigest(d)::text FROM t) = (SELECT tdigest(x.d, 100)::text FROM x);

-- now try the same thing with bulk incremental update (using t-digests)
TRUNCATE t;
INSERT INTO t VALUES (NULL);

DO LANGUAGE plpgsql $$
DECLARE
  r RECORD;
BEGIN
    FOR r IN (SELECT a, tdigest(i,100) AS d FROM (SELECT mod(i,5) AS a, i FROM generate_series(1,1000) s(i) ORDER BY mod(i,5), md5(i::text)) foo GROUP BY a ORDER BY a) LOOP
        UPDATE t SET d = tdigest_union(d, r.d, false);
    END LOOP;
END$$;

-- compare the results, but do force a compaction of the incremental result
WITH x AS (SELECT a, tdigest(i,100) AS d FROM (SELECT mod(i,5) AS a, i FROM generate_series(1,1000) s(i) ORDER BY mod(i,5), md5(i::text)) foo GROUP BY a ORDER BY a)
SELECT (SELECT tdigest(d)::text FROM t) = (SELECT tdigest(x.d)::text FROM x);

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


-- <value,count> API

EXPLAIN (COSTS OFF)
WITH
  d AS (SELECT t.* FROM t, LATERAL generate_series(1,t.c)),
  x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM d)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(v, c, 100, 0.95) AS b
  FROM t) foo;

WITH
  d AS (SELECT t.* FROM t, LATERAL generate_series(1,t.c)),
  x AS (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY v) AS p FROM d)
SELECT
  0.95,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile(v, c, 100, 0.95) AS b
  FROM t) foo;


EXPLAIN (COSTS OFF)
WITH
  d AS (SELECT t.* FROM t, LATERAL generate_series(1,t.c)),
  x AS (SELECT percent_rank(950) WITHIN GROUP (ORDER BY v) AS p FROM d)
SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile_of(v, c, 100, 950) AS b
  FROM t) foo;

WITH
  d AS (SELECT t.* FROM t, LATERAL generate_series(1,t.c)),
  x AS (SELECT percent_rank(950) WITHIN GROUP (ORDER BY v) AS p FROM d)
SELECT
  950,
  abs(a - b) < 0.01
FROM (
  SELECT
    (SELECT p FROM x) AS a,
    tdigest_percentile_of(v, c, 100, 950) AS b
  FROM t) foo;



-- array of percentiles / values
EXPLAIN (COSTS OFF)
WITH
  d AS (SELECT t.* FROM t, LATERAL generate_series(1,t.c)),
  x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM d)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(v, c, 100, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM t) foo;

WITH
  d AS (SELECT t.* FROM t, LATERAL generate_series(1,t.c)),
  x AS (SELECT percentile_disc(ARRAY[0.0, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY v) AS p FROM d)
SELECT
  p,
  abs(a - b) / 1000 < 0.01
FROM (
  SELECT
    unnest(ARRAY[0.0, 0.95, 0.99, 1.0]) p,
    unnest((SELECT p FROM x)) AS a,
    unnest(tdigest_percentile(v, c, 100, ARRAY[0.0, 0.95, 0.99, 1.0])) AS b
  FROM t) foo;


EXPLAIN (COSTS OFF)
WITH
  d AS (SELECT t.* FROM t, LATERAL generate_series(1,t.c)),
  x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) AS p FROM d)) p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((select x.p from x)) AS a,
    unnest(tdigest_percentile_of(v, c, 100, ARRAY[950, 990])) AS b
  FROM t) foo;

WITH
  d AS (SELECT t.* FROM t, LATERAL generate_series(1,t.c)),
  x AS (SELECT array_agg((SELECT percent_rank(f) WITHIN GROUP (ORDER BY v) AS p FROM d)) p FROM unnest(ARRAY[950, 990]) f)
SELECT
  p,
  abs(a - b) < 0.01
FROM (
  SELECT
    unnest(ARRAY[950, 990]) AS p,
    unnest((select x.p from x)) AS a,
    unnest(tdigest_percentile_of(v, c, 100, ARRAY[950, 990])) AS b
  FROM t) foo;

-- test input function, and conversion from old to new format
SELECT 'flags 0 count 20 compression 10 centroids 8 (1000.000000, 1) (2000.000000, 1) (7000.000000, 2) (26000.000000, 4) (84000.000000, 7) (51000.000000, 3) (19000.000000, 1) (20000.000000, 1)'::tdigest;

-- test input of invalid data
-- negative count
SELECT 'flags 0 count -20 compression 10 centroids 8 (1000.000000, 1) (2000.000000, 1) (7000.000000, 2) (26000.000000, 4) (84000.000000, 7) (51000.000000, 3) (19000.000000, 1) (20000.000000, 1)'::tdigest;
-- mismatching count
SELECT 'flags 0 count 21 compression 10 centroids 8 (1000.000000, 1) (2000.000000, 1) (7000.000000, 2) (26000.000000, 4) (84000.000000, 7) (51000.000000, 3) (19000.000000, 1) (20000.000000, 1)'::tdigest;
-- incorrectly sorted centroids
SELECT 'flags 0 count 20 compression 10 centroids 8 (1000.000000, 1) (2000.000000, 1) (1000.000000, 2) (26000.000000, 4) (84000.000000, 7) (51000.000000, 3) (19000.000000, 1) (20000.000000, 1)'::tdigest;

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

RESET extra_float_digits;
