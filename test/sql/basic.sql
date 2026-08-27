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
