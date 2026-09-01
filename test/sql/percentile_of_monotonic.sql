-- tdigest_percentile_of() evaluates the CDF of the digest, so for a given
-- digest the result has to be a non-decreasing function of the value, and it
-- always has to fall into the [0, 1] range. Neither of that holds right now.
--
-- tdigest_compute_quantiles_of() interpolates between two centroids, and to
-- do that it first moves from the boundary of the previous centroid to its
-- mean, which sits in the middle of the centroid:
--
--     count -= (prev->count / 2);
--
-- That is an int64 division, even though "count" is a double and the very
-- next statement halves the same value as (prev->count / 2.0). For centroids
-- with an odd count the two halves therefore disagree by 0.5 items, and the
-- interpolated CDF ends up shifted by 0.5/count with respect to the exact
-- value calculated in the (value == curr->mean) branch just above.
--
-- The result is a saw-tooth: the CDF jumps up by 0.5/count right after the
-- mean of every odd-sized centroid, and drops back at the next centroid. For
-- the (0, 7) (100, 3) digest used below:
--
--     value       now         expected
--     0           0.35        0.35
--     0.0001      0.4000005   0.3500005
--     50          0.65        0.6
--     99.9999     0.8999995   0.8499995
--     100         0.85        0.85
--
-- i.e. the CDF at 99.9999 is higher than the CDF at 100.

\set VERBOSITY terse

SET extra_float_digits = 0;

CREATE TABLE tdigest_monotonic_digests (id int, descr text, d tdigest);

-- Hand-built digests. All of them use centroid means from the [0, 100] range,
-- so that a single set of probes works for all of them. What matters are the
-- centroid counts - the discontinuity shows up at the mean of every centroid
-- with an odd count.
INSERT INTO tdigest_monotonic_digests VALUES
    (1, 'two centroids, odd + odd',
        'flags 1 count 10 compression 10000 centroids 2 (0, 7) (100, 3)'),
    (2, 'two centroids, odd + even',
        'flags 1 count 11 compression 10000 centroids 2 (0, 7) (100, 4)'),
    (3, 'two centroids, even + odd',
        'flags 1 count 11 compression 10000 centroids 2 (0, 8) (100, 3)'),
    (4, 'two centroids, even + even',
        'flags 1 count 4 compression 10000 centroids 2 (0, 2) (100, 2)'),
    (5, 'two centroids, single item each',
        'flags 1 count 2 compression 10000 centroids 2 (0, 1) (100, 1)'),
    (6, 'three centroids, single item each',
        'flags 1 count 3 compression 10000 centroids 3 (0, 1) (50, 1) (100, 1)'),
    (7, 'three centroids, mixed counts',
        'flags 1 count 12 compression 10000 centroids 3 (0, 3) (50, 4) (100, 5)'),
    (8, 'a single centroid',
        'flags 1 count 10 compression 10000 centroids 1 (50, 10)'),
    (9, 'repeated means',
        'flags 1 count 6 compression 10000 centroids 4 (0, 1) (50, 2) (50, 2) (100, 1)');

-- And a couple of digests built by the aggregate from actual data. The first
-- one is small enough not to be compacted at all (so every centroid holds a
-- single item), the other two are compacted.
INSERT INTO tdigest_monotonic_digests
SELECT 10, 'built from 51 distinct values, no compaction', tdigest((2 * i)::double precision, 10000)
  FROM generate_series(0, 50) s(i);

INSERT INTO tdigest_monotonic_digests
SELECT 11, 'built from 10000 rows, compression 10', tdigest((i % 101)::double precision, 10)
  FROM generate_series(1, 10000) s(i);

INSERT INTO tdigest_monotonic_digests
SELECT 12, 'built from 10000 rows, compression 100', tdigest((i % 101)::double precision, 100)
  FROM generate_series(1, 10000) s(i);

-- The values to probe the digests with: a coarse grid reaching outside the
-- range of the digests, and then the centroid means used above with values
-- immediately below and above them. The interpolation switches from one pair
-- of centroids to the next exactly at the means, so that is where it is
-- discontinuous.
CREATE TABLE tdigest_monotonic_probes (v double precision);

INSERT INTO tdigest_monotonic_probes
SELECT DISTINCT v FROM (
    SELECT i / 2.0::double precision FROM generate_series(-20, 220) s(i)
    UNION ALL
    SELECT m + o FROM generate_series(0, 100, 2) s(m),
                      unnest(ARRAY[-1e-6, -1e-9, 0, 1e-9, 1e-6]::double precision[]) o
) s(v);

-- The CDF must never decrease. Report one row per digest that breaks it, so
-- that a failure stays readable.
WITH q AS (
    SELECT g.id, g.descr, p.arr, tdigest_percentile_of(g.d, p.arr) AS res
      FROM tdigest_monotonic_digests g,
           (SELECT array_agg(v ORDER BY v) AS arr FROM tdigest_monotonic_probes) p
     GROUP BY g.id, g.descr, p.arr
), u AS (
    SELECT id, descr, ord, v, r
      FROM q, unnest(q.arr, q.res) WITH ORDINALITY AS x(v, r, ord)
), s AS (
    SELECT id, descr, v, r, lag(v) OVER w AS prev_v, lag(r) OVER w AS prev_r
      FROM u WINDOW w AS (PARTITION BY id ORDER BY ord)
)
SELECT id, descr, count(*) AS decreasing_steps, max(prev_r - r) AS max_drop
  FROM s WHERE r < prev_r
 GROUP BY id, descr ORDER BY id;

-- And the result must always be a probability.
WITH q AS (
    SELECT g.id, g.descr, p.arr, tdigest_percentile_of(g.d, p.arr) AS res
      FROM tdigest_monotonic_digests g,
           (SELECT array_agg(v ORDER BY v) AS arr FROM tdigest_monotonic_probes) p
     GROUP BY g.id, g.descr, p.arr
)
SELECT id, descr, count(*) AS out_of_range
  FROM q, unnest(q.res) AS r
 WHERE NOT (r BETWEEN 0 AND 1)
 GROUP BY id, descr ORDER BY id;

-- The concrete case from the comment at the top, spelled out. The CDF has to
-- grow (or at least stay the same) on each of the four steps.
SELECT r[1] <= r[2] AS "0 -> 0.0001",
       r[2] <= r[3] AS "0.0001 -> 50",
       r[3] <= r[4] AS "50 -> 99.9999",
       r[4] <= r[5] AS "99.9999 -> 100"
  FROM (SELECT tdigest_percentile_of('flags 1 count 10 compression 10000 centroids 2 (0, 7) (100, 3)'::tdigest,
                                     ARRAY[0.0, 0.0001, 50.0, 99.9999, 100.0]::double precision[]) AS r) x;

-- The same thing through the two aggregates building the digest from data,
-- to show this is not an artifact of a hand-written digest. Three items with
-- the value 0 and a single one with the value 100.
SELECT r[1] <= r[2] AS "0 -> 0.0001",
       r[2] <= r[3] AS "0.0001 -> 50",
       r[3] <= r[4] AS "50 -> 99.9999",
       r[4] <= r[5] AS "99.9999 -> 100"
  FROM (SELECT tdigest_percentile_of(v, 10000, ARRAY[0.0, 0.0001, 50.0, 99.9999, 100.0]::double precision[]) AS r
          FROM (VALUES (0.0::double precision), (0.0::double precision),
                       (0.0::double precision), (100.0::double precision)) t(v)) x;

SELECT r[1] <= r[2] AS "0 -> 0.0001",
       r[2] <= r[3] AS "0.0001 -> 50",
       r[3] <= r[4] AS "50 -> 99.9999",
       r[4] <= r[5] AS "99.9999 -> 100"
  FROM (SELECT tdigest_percentile_of(v, c, 10000, ARRAY[0.0, 0.0001, 50.0, 99.9999, 100.0]::double precision[]) AS r
          FROM (VALUES (0.0::double precision, 3::bigint),
                       (100.0::double precision, 1::bigint)) t(v, c)) x;

DROP TABLE tdigest_monotonic_digests;
DROP TABLE tdigest_monotonic_probes;
