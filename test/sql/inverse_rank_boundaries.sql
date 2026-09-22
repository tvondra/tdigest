\set ECHO none
SET max_parallel_workers_per_gather = 0;

DO $$
DECLARE
    d tdigest;
    probes double precision[] := ARRAY[
        -1.0, 0.0, 0.9999999999999999, 1.0, 1.0000000000000002,
        1.9999999999999998, 2.0, 3.0
    ];
    results double precision[];
    scalar_result double precision;
    i integer;
BEGIN
    FOREACH d IN ARRAY ARRAY[
        'flags 1 count 18014398509481990 compression 10000 centroids 3 (0, 1) (1, 18014398509481987) (2, 2)'::tdigest,
        'flags 1 count 19140298416324609 compression 10000 centroids 3 (0, 1) (1, 18014398509481981) (2, 1125899906842627)'::tdigest,
        'flags 1 count 22517998136852481 compression 10000 centroids 3 (0, 1) (1, 18014398509481981) (2, 4503599627370499)'::tdigest,
        'flags 1 count 9223372036854775807 compression 10000 centroids 3 (0, 9223372036854775805) (1, 1) (2, 1)'::tdigest
    ] LOOP
        SELECT tdigest_percentile_of(d, probes) INTO results;

        IF array_length(results, 1) IS DISTINCT FROM array_length(probes, 1)
           OR EXISTS (
               SELECT 1 FROM (
                   SELECT v, lag(v) OVER (ORDER BY ord) AS prev
                     FROM unnest(results) WITH ORDINALITY AS u(v, ord)
               ) AS steps
               WHERE v IS NULL OR v < prev OR NOT (v BETWEEN 0 AND 1)
           ) THEN
            RAISE EXCEPTION 'Inverse ranks decrease at a centroid boundary: %', results;
        END IF;

        RAISE NOTICE 'Inverse ranks increase at a centroid boundary';

        FOR i IN 1..array_length(probes, 1) LOOP
            SELECT tdigest_percentile_of(d, probes[i]) INTO scalar_result;
            IF scalar_result IS DISTINCT FROM results[i] THEN
                RAISE EXCEPTION 'Scalar and array inverse percentiles disagree';
            END IF;
        END LOOP;

        RAISE NOTICE 'Scalar and array inverse percentiles agree';

    END LOOP;

    -- Exact equality still counts half of the entire equal-mean run.
    SELECT tdigest_percentile_of(
        'flags 1 count 18014398509481990 compression 10000 centroids 3 (1, 1) (1, 18014398509481987) (1, 2)'::tdigest,
        1.0) INTO scalar_result;
    IF scalar_result IS DISTINCT FROM 0.5::double precision THEN
        RAISE EXCEPTION 'Equal-mean ranks lost their half-weight convention';
    END IF;

    RAISE NOTICE 'Equal-mean ranks follow the half-weight convention';
END
$$;

\set ECHO all

SET extra_float_digits = 2;

-- The CDF used to decrease from 0.375 to 0.37499999999999994 just above 1.
-- Both equal-mean centroids survive compaction at this compression.
WITH ranks AS (
    SELECT tdigest_percentile_of(d, 1::double precision) AS at_mean,
           tdigest_percentile_of(d, 1.0000000000000002::double precision) AS above_mean,
           tdigest_percentile_of(d, ARRAY[1, 1.0000000000000002]::double precision[]) AS array_ranks
    FROM (VALUES (
        'flags 1 count 36028797018963981 compression 10000 centroids 4 (0, 9007199254740995) (1, 2) (1, 9007199254740993) (10, 18014398509481991)'::tdigest
    )) AS input(d)
)
SELECT at_mean, above_mean, array_ranks,
       at_mean <= above_mean AS monotone,
       array_ranks = ARRAY[at_mean, above_mean] AS scalar_array_agree
FROM ranks;

-- Preserve half-items without rounding the integer part and half separately.
-- In particular, (double) (18014398509481987 / 2) + 0.5 double-rounds.
WITH counts(n) AS (
    VALUES (1::bigint), (3), (18014398509481987), (9223372036854775807)
)
SELECT n, tdigest_percentile_of(
           format('flags 1 count %s compression 10000 centroids 1 (1, %s)', n, n)::tdigest,
           1::double precision) AS rank
FROM counts
GROUP BY n
ORDER BY n;
