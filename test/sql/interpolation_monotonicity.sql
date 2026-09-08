\set ECHO none
SET max_parallel_workers_per_gather = 0;

DO $$
DECLARE
    d tdigest;
    percentiles double precision[];
    probes double precision[];
    results double precision[];
    raw_results double precision[];
    weighted_results double precision[];
    scalar_result double precision;
    bounds double precision[];
    i integer;
BEGIN
    SELECT array_agg(n / 10000.0::double precision ORDER BY n)
      INTO percentiles FROM generate_series(0, 10000) AS s(n);

    FOREACH d IN ARRAY ARRAY[
        'flags 1 count 3 compression 10000 centroids 2 (1, 1) (1.000000000000001, 2)'::tdigest,
        'flags 1 count 3 compression 10000 centroids 2 (-1.000000000000001, 2) (-1, 1)'::tdigest,
        'flags 1 count 2 compression 10000 centroids 2 (-18014398509481984, 1) (-1, 1)'::tdigest,
        'flags 1 count 2 compression 10000 centroids 2 (-1.7e308, 1) (-5e-324, 1)'::tdigest,
        'flags 1 count 2 compression 10000 centroids 2 (-1.7e308, 1) (1.7e308, 1)'::tdigest,
        'flags 1 count 2 compression 10000 centroids 2 (5e-324, 1) (1e-323, 1)'::tdigest
    ] LOOP
        SELECT tdigest_percentile(d, percentiles) INTO results;
        bounds := tdigest_double_array(d);

        IF array_length(results, 1) IS DISTINCT FROM array_length(percentiles, 1)
           OR EXISTS (
               SELECT 1 FROM (
                   SELECT v, lag(v) OVER (ORDER BY ord) AS prev
                     FROM unnest(results) WITH ORDINALITY AS u(v, ord)
               ) AS steps
               WHERE v IS NULL OR v < prev
                  OR NOT (v BETWEEN bounds[5] AND bounds[array_length(bounds, 1) - 1])
           ) THEN
            RAISE EXCEPTION 'Nonmonotone or out-of-range percentiles for %', d;
        END IF;

        RAISE NOTICE 'Percentiles are monotone and in range';

        FOREACH i IN ARRAY ARRAY[1, 2018, 2019, 5001, 10001] LOOP
            SELECT tdigest_percentile(d, percentiles[i]) INTO scalar_result;
            IF scalar_result IS DISTINCT FROM results[i] THEN
                RAISE EXCEPTION 'Scalar and array percentiles disagree';
            END IF;
        END LOOP;

        RAISE NOTICE 'Scalar and array percentiles agree';

    END LOOP;

    SELECT tdigest_percentile(tdigest(v, 10000), percentiles),
           tdigest_percentile(tdigest(v, 1::bigint, 10000), percentiles)
      INTO raw_results, weighted_results
      FROM (VALUES (1::double precision),
                   (1.000000000000001::double precision),
                   (1.000000000000001::double precision)) AS t(v);

    FOREACH results SLICE 1 IN ARRAY ARRAY[raw_results, weighted_results] LOOP
        IF EXISTS (
            SELECT 1 FROM (
                SELECT v, lag(v) OVER (ORDER BY ord) AS prev
                  FROM unnest(results) WITH ORDINALITY AS u(v, ord)
            ) AS steps WHERE v IS NULL OR v < prev
        ) THEN
            RAISE EXCEPTION 'Nonmonotone raw or weighted percentiles';
        END IF;

        RAISE NOTICE 'Raw and weighted percentiles are monotone';

    END LOOP;

    d := 'flags 1 count 4503599627370498 compression 10000 centroids 3 (0, 4503599627370496) (1, 1) (2, 1)'::tdigest;
    SELECT array_agg(1 + n / 10000.0::double precision ORDER BY n)
      INTO probes FROM generate_series(1, 9999) AS s(n);
    SELECT tdigest_percentile_of(d, probes) INTO results;

    IF EXISTS (
        SELECT 1 FROM (
            SELECT v, lag(v) OVER (ORDER BY ord) AS prev
              FROM unnest(results) WITH ORDINALITY AS u(v, ord)
        ) AS steps WHERE v IS NULL OR v < prev OR NOT (v BETWEEN 0 AND 1)
    ) THEN
        RAISE EXCEPTION 'Nonmonotone inverse interpolation';
    END IF;

    RAISE NOTICE 'Inverse interpolation is monotone';

    FOREACH i IN ARRAY ARRAY[1, 1874, 1875, 9999] LOOP
        SELECT tdigest_percentile_of(d, probes[i]) INTO scalar_result;
        IF scalar_result IS DISTINCT FROM results[i] THEN
            RAISE EXCEPTION 'Scalar and array inverse percentiles disagree';
        END IF;
    END LOOP;

    RAISE NOTICE 'Scalar and array inverse percentiles agree';

END
$$;

\set ECHO all

-- At p = 0.75 - 2^-53, q = 1 - 2^-52 and the result rounds to -5.
-- Subtracting nearly equal magnitudes used to return -4 instead.
SELECT tdigest_percentile(tdigest(v, 10000), 0.7499999999999999::double precision) AS raw_scalar,
       tdigest_percentile(tdigest(v, 10000), ARRAY[0.7499999999999999]::double precision[]) AS raw_array,
       tdigest_percentile(tdigest(v, 1::bigint, 10000), 0.7499999999999999::double precision) AS weighted_scalar,
       tdigest_percentile(tdigest(v, 1::bigint, 10000),
                          ARRAY[0.7499999999999999]::double precision[]) AS weighted_array
FROM (VALUES (-18014398509481984::double precision),
             (-1::double precision)) AS input(v);

-- Also cover stored digests and count rounding that makes q exactly 1.
-- The endpoint case used to return -4, inside the bracket but not equal to -3.
WITH inputs(label, d, p) AS (
    VALUES (
        'precision',
        'flags 1 count 2 compression 10000 centroids 2 (-18014398509481984, 1) (-1, 1)'::tdigest,
        0.7499999999999999::double precision
    ), (
        'endpoint',
        'flags 1 count 9007199254743615 compression 10000 centroids 3 (-10000000000000000, 9007199254740839) (-3, 290) (-1, 2486)'::tdigest,
        0.9999999999997077::double precision
    )
)
SELECT label, tdigest_percentile(tdigest(d), p) AS scalar_result,
       tdigest_percentile(tdigest(d), ARRAY[p]) AS array_result
FROM inputs
GROUP BY label, p
ORDER BY label;
