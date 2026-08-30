-- validation of percentile values

-- correct values
SELECT tdigest_percentile(1.0, 10, 0.0);
SELECT tdigest_percentile(1.0, 10, 0.1);
SELECT tdigest_percentile(1.0, 10, 0.5);
SELECT tdigest_percentile(1.0, 10, 0.9);
SELECT tdigest_percentile(1.0, 10, 1.0);
SELECT tdigest_percentile(1.0, 10, ARRAY[0.0, 0.1, 0.5, 0.9, 1.0]);

-- out of range values
SELECT tdigest_percentile(1.0, 10, -1.0);
SELECT tdigest_percentile(1.0, 10, 1.5);
SELECT tdigest_percentile(1.0, 10, ARRAY[0.0, -1.0, 0.5, 0.9, 1.0]);
SELECT tdigest_percentile(1.0, 10, ARRAY[0.0, 0.1, 0.5, 0.9, 1.5]);

-- NaN
SELECT tdigest_percentile(1.0, 10, 'NaN'::double precision);
SELECT tdigest_percentile(1.0, 10, ARRAY[0.0, 'NaN'::double precision, 0.5, 0.9, 1.0]);

-- infinite values
SELECT tdigest_percentile(1.0, 10, 'infinity'::double precision);
SELECT tdigest_percentile(1.0, 10, '-infinity'::double precision);
SELECT tdigest_percentile(1.0, 10, ARRAY[0.0, 'infinity'::double precision, 0.5, 0.9, 1.0]);
SELECT tdigest_percentile(1.0, 10, ARRAY[0.0, '-infinity'::double precision, 0.5, 0.9, 1.0]);

-- more tests
SELECT tdigest_percentile(1.0::double precision, 10, 'NaN'::double precision);
SELECT tdigest_percentile(1.0::double precision, 10, '-infinity'::double precision);
SELECT tdigest_percentile(1.0::double precision, 10, 'infinity'::double precision);

SELECT tdigest_percentile(1.0::double precision, 10, ARRAY[0.5, 'NaN']::double precision[]);
SELECT tdigest_percentile(1.0::double precision, 10, ARRAY[0.5, '-infinity']::double precision[]);
SELECT tdigest_percentile(1.0::double precision, 10, ARRAY[0.5, 'infinity']::double precision[]);

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
SELECT tdigest_avg(1.0::double precision, 10, 'NaN'::double precision, 1.0);
SELECT tdigest_avg(1.0::double precision, 10, '-infinity'::double precision, 1.0);
SELECT tdigest_avg(1.0::double precision, 10, 'infinity'::double precision, 1.0);

SELECT tdigest_digest_sum('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest, 0.0, 'NaN'::double precision);
SELECT tdigest_digest_sum('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest, 0.0, '-infinity'::double precision);
SELECT tdigest_digest_sum('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest, 0.0, 'infinity'::double precision);

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
