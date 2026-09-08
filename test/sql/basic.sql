-- validation of percentile values

-- correct values
SELECT tdigest_percentile(tdigest(1.0, 10), 0.0);
SELECT tdigest_percentile(tdigest(1.0, 10), 0.1);
SELECT tdigest_percentile(tdigest(1.0, 10), 0.5);
SELECT tdigest_percentile(tdigest(1.0, 10), 0.9);
SELECT tdigest_percentile(tdigest(1.0, 10), 1.0);
SELECT tdigest_percentile(tdigest(1.0, 10), ARRAY[0.0, 0.1, 0.5, 0.9, 1.0]);

-- out of range values
SELECT tdigest_percentile(tdigest(1.0, 10), -1.0);
SELECT tdigest_percentile(tdigest(1.0, 10), 1.5);
SELECT tdigest_percentile(tdigest(1.0, 10), ARRAY[0.0, -1.0, 0.5, 0.9, 1.0]);
SELECT tdigest_percentile(tdigest(1.0, 10), ARRAY[0.0, 0.1, 0.5, 0.9, 1.5]);

-- NaN
SELECT tdigest_percentile(tdigest(1.0, 10), 'NaN'::double precision);
SELECT tdigest_percentile(tdigest(1.0, 10), ARRAY[0.0, 'NaN'::double precision, 0.5, 0.9, 1.0]);

-- infinite values
SELECT tdigest_percentile(tdigest(1.0, 10), 'infinity'::double precision);
SELECT tdigest_percentile(tdigest(1.0, 10), '-infinity'::double precision);
SELECT tdigest_percentile(tdigest(1.0, 10), ARRAY[0.0, 'infinity'::double precision, 0.5, 0.9, 1.0]);
SELECT tdigest_percentile(tdigest(1.0, 10), ARRAY[0.0, '-infinity'::double precision, 0.5, 0.9, 1.0]);

-- more tests
SELECT tdigest_percentile(tdigest(1.0::double precision, 10), 'NaN'::double precision);
SELECT tdigest_percentile(tdigest(1.0::double precision, 10), '-infinity'::double precision);
SELECT tdigest_percentile(tdigest(1.0::double precision, 10), 'infinity'::double precision);

SELECT tdigest_percentile(tdigest(1.0::double precision, 10), ARRAY[0.5, 'NaN']::double precision[]);
SELECT tdigest_percentile(tdigest(1.0::double precision, 10), ARRAY[0.5, '-infinity']::double precision[]);
SELECT tdigest_percentile(tdigest(1.0::double precision, 10), ARRAY[0.5, 'infinity']::double precision[]);

SELECT tdigest_percentile('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest, 'NaN'::double precision);
SELECT tdigest_percentile('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest, '-infinity'::double precision);
SELECT tdigest_percentile('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest, 'infinity'::double precision);

SELECT tdigest_percentile('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest,
                          ARRAY[0.5, 'NaN']::double precision[]);
SELECT tdigest_percentile('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest,
                          ARRAY[0.5, '-infinity']::double precision[]);
SELECT tdigest_percentile('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest,
                          ARRAY[0.5, 'infinity']::double precision[]);

-- trim thresholds are checked too
SELECT tdigest_avg(tdigest(1.0::double precision, 10), 'NaN'::double precision, 1.0);
SELECT tdigest_avg(tdigest(1.0::double precision, 10), '-infinity'::double precision, 1.0);
SELECT tdigest_avg(tdigest(1.0::double precision, 10), 'infinity'::double precision, 1.0);

SELECT tdigest_sum('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest, 0.0, 'NaN'::double precision);
SELECT tdigest_sum('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest, 0.0, '-infinity'::double precision);
SELECT tdigest_sum('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest, 0.0, 'infinity'::double precision);

-- validation of input values
-- input functions must reject NaN / infinity means in various places
SELECT 'flags 1 count 1 compression 10 centroids 1 (NaN, 1)'::tdigest;
SELECT 'flags 1 count 1 compression 10 centroids 1 (infinity, 1)'::tdigest;
SELECT 'flags 1 count 1 compression 10 centroids 1 (-infinity, 1)'::tdigest;

-- the aggregates building the digest have to reject NaN / infinity too
SELECT tdigest('NaN'::float8, 10);
SELECT tdigest('infinity'::float8, 10);
SELECT tdigest('-infinity'::float8, 10);

-- same for the value/count API
SELECT tdigest('NaN'::float8, 50::bigint, 10);
SELECT tdigest('NaN'::float8, 200::bigint, 10);
SELECT tdigest('Infinity'::float8, 200::bigint, 10);
SELECT tdigest('-Infinity'::float8, 200::bigint, 10);

-- t-digest extreme enough for the percentile to fall to the right of the last centroid
SELECT tdigest_percentile('flags 1 count 4611686018427388928 compression 10 centroids 3 (1, 4611686018427387904) (2, 512) (3, 512)'::tdigest,
                          0.9999999999999998::double precision);

-- same centroid, but the percentile falls in between the second and third centroid
SELECT tdigest_percentile('flags 1 count 4611686018427388928 compression 10 centroids 3 (1, 4611686018427387904) (2, 512) (3, 512)'::tdigest,
                          ARRAY[0.0, 0.5, 0.9999999999999996, 1.0]::double precision[]);

-- the same digest with other compressions, to show the compression does not affect this
-- the compaction leaves the digest alone for all compression values
SELECT c AS compression,
       tdigest_percentile(('flags 1 count 4611686018427388928 compression ' || c  || ' centroids 3 (1, 4611686018427387904) (2, 512) (3, 512)')::tdigest,
                          0.9999999999999998::double precision)
  FROM (VALUES (10), (100), (1000), (10000)) v(c)
 GROUP BY c ORDER BY c;

-- make sure finite inputs do not not produce infinite centroids
SELECT tdigest_count(tdigest(v, 10))
FROM (SELECT (CASE WHEN i % 2 = 0 THEN 1e308 ELSE 1e307 END)::float8 AS v
      FROM generate_series(1,200) i) x;

-- make sure finite inputs do not result in NaN means
WITH x AS (SELECT (CASE i % 4 WHEN 0 THEN -1e308
                              WHEN 1 THEN -1e307
                              WHEN 2 THEN 1e307
                              ELSE 1e308 END)::float8 AS v
           FROM generate_series(1,400) i)
SELECT tdigest_percentile(tdigest(v,10),0.5) FROM x;

-- extreme digest - make sure we can read the output we produced
SELECT tdigest_count((SELECT tdigest(v, c, 10)
        FROM (VALUES (1e307::float8, 1000000::bigint),
                     (1e308::float8, 1000000::bigint)) x(v, c))::text::tdigest);

-- extreme digest - overflow/rounding issue in tdigest_compute_quantiles
SELECT tdigest_percentile('flags 1 count 204745659738676291 compression 100 centroids 3 (5360.6421513780951, 96854112897858161) (5670.4376627340216, 36597233557612361) (5670.4376627340216, 71294313283205769)'::tdigest, 0.56241841621276667);

-- extreme digest - overflow/rounding issue in tdigest_compute_quantiles_of
SELECT tdigest_percentile_of('flags 1 count 467525031770889061 compression 100 centroids 5 (-714.32532319527991, 372151463885287745) (-649.22210009079686, 12983637447676689) (-613.86327584091737, 82389930437924625) (-554.7460856469786, 1) (-497.74083276256061, 1)'::tdigest, -517.61603495911652);
