-- Regression test for the compression check in tdigest_add_double_increment()
-- and tdigest_add_double_array_increment(), the functions of the incremental
-- API.
--
-- The compression is only optional when there already is a t-digest to add
-- the value to, because that determines the compression. Without a digest
-- there is nothing to derive the compression from, so it has to be supplied.

\set VERBOSITY terse

-- NULL digest and NULL compression (which is the default)
SELECT tdigest_add(NULL::tdigest, 1.0::double precision);
SELECT tdigest_add(NULL::tdigest, 1.0::double precision, NULL::int);

SELECT tdigest_add(NULL::tdigest, ARRAY[1.0]::double precision[]);
SELECT tdigest_add(NULL::tdigest, ARRAY[1.0]::double precision[], NULL::int);

-- with the compression supplied it has to work
SELECT tdigest_add(NULL::tdigest, 1.0::double precision, 100);
SELECT tdigest_add(NULL::tdigest, ARRAY[1.0]::double precision[], 100);

-- with an existing digest the compression is not needed
SELECT tdigest_add('flags 1 count 1 compression 100 centroids 1 (1, 1)'::tdigest,
                   2.0::double precision);
SELECT tdigest_add('flags 1 count 1 compression 100 centroids 1 (1, 1)'::tdigest,
                   ARRAY[2.0]::double precision[]);
