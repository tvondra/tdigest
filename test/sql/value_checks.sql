-- unlike percentile values passed to tdigest_percentile(), values passed to
-- the tdigest_percentile_of() functions are not validated and instead return
-- hard-coded values for NaN/infinity

\set VERBOSITY terse

-- values are not checked anywhere, instead we just return NaN/-infinity/infinity

SELECT tdigest_percentile_of(tdigest(1.0::double precision, 10), 'NaN'::double precision);
SELECT tdigest_percentile_of(tdigest(1.0::double precision, 10), '-infinity'::double precision);
SELECT tdigest_percentile_of(tdigest(1.0::double precision, 10), 'infinity'::double precision);

SELECT tdigest_percentile_of(tdigest(1.0::double precision, 10), ARRAY['NaN', '-infinity', 'infinity']::double precision[]);
SELECT tdigest_percentile_of('flags 1 count 3 compression 10 centroids 3 (1, 1) (2, 1) (3, 1)'::tdigest,
                             'NaN'::double precision);
SELECT tdigest_percentile_of('flags 1 count 3 compression 10 centroids 3 (1, 1) (2, 1) (3, 1)'::tdigest,
                             '-infinity'::double precision);
SELECT tdigest_percentile_of('flags 1 count 3 compression 10 centroids 3 (1, 1) (2, 1) (3, 1)'::tdigest,
                             'infinity'::double precision);
SELECT tdigest_percentile_of('flags 1 count 3 compression 10 centroids 3 (1, 1) (2, 1) (3, 1)'::tdigest,
                             ARRAY['NaN', '-infinity', 'infinity']::double precision[]);

-- the value/count API does not check the values either
SELECT tdigest_percentile_of(tdigest(1.0::double precision, 2::bigint, 10), 'NaN'::double precision);
SELECT tdigest_percentile_of(tdigest(1.0::double precision, 2::bigint, 10), '-infinity'::double precision);
SELECT tdigest_percentile_of(tdigest(1.0::double precision, 2::bigint, 10), 'infinity'::double precision);

SELECT tdigest_percentile_of(tdigest(1.0::double precision, 2::bigint, 10), ARRAY['NaN']::double precision[]);
SELECT tdigest_percentile_of(tdigest(1.0::double precision, 2::bigint, 10), ARRAY['-infinity']::double precision[]);
SELECT tdigest_percentile_of(tdigest(1.0::double precision, 2::bigint, 10), ARRAY['infinity']::double precision[]);
