-- Regression test for the NULL handling in the transition functions.
--
-- None of the transition functions is strict, so NULL rows do reach them
-- and have to be ignored. Each function is exercised twice: once with all
-- input rows NULL (so the state stays NULL and the transition function
-- returns NULL), and once with a NULL row following a non-NULL one (so the
-- transition function returns the existing state unmodified).

-- tdigest_add_double
SELECT tdigest_percentile(v, 100, 0.5) FROM (VALUES (NULL::double precision), (NULL)) AS t(v);
SELECT tdigest_percentile(v, 100, 0.5) FROM (VALUES (1.0), (NULL)) AS t(v);

-- tdigest_add_double_count
SELECT tdigest(v, 1::bigint, 100) FROM (VALUES (NULL::double precision), (NULL)) AS t(v);
SELECT tdigest(v, 1::bigint, 100) FROM (VALUES (1.0), (NULL)) AS t(v);

-- tdigest_add_double_values
SELECT tdigest_percentile_of(v, 100, 1.0) FROM (VALUES (NULL::double precision), (NULL)) AS t(v);
SELECT tdigest_percentile_of(v, 100, 1.0) FROM (VALUES (1.0), (NULL)) AS t(v);

-- tdigest_add_double_values_count
SELECT tdigest_percentile_of(v, 1::bigint, 100, 1.0) FROM (VALUES (NULL::double precision), (NULL)) AS t(v);
SELECT tdigest_percentile_of(v, 1::bigint, 100, 1.0) FROM (VALUES (1.0), (NULL)) AS t(v);

-- tdigest_add_digest
SELECT tdigest_percentile(d, 0.5) FROM (VALUES (NULL::tdigest), (NULL)) AS t(d);
SELECT tdigest_percentile(d, 0.5) FROM (VALUES ('flags 1 count 1 compression 100 centroids 1 (1, 1)'::tdigest), (NULL)) AS t(d);

-- tdigest_add_digest_values
SELECT tdigest_percentile_of(d, 1.0) FROM (VALUES (NULL::tdigest), (NULL)) AS t(d);
SELECT tdigest_percentile_of(d, 1.0) FROM (VALUES ('flags 1 count 1 compression 100 centroids 1 (1, 1)'::tdigest), (NULL)) AS t(d);

-- tdigest_add_double_array
SELECT tdigest_percentile(v, 100, ARRAY[0.5]) FROM (VALUES (NULL::double precision), (NULL)) AS t(v);
SELECT tdigest_percentile(v, 100, ARRAY[0.5]) FROM (VALUES (1.0), (NULL)) AS t(v);

-- tdigest_add_double_array_count
SELECT tdigest_percentile(v, 1::bigint, 100, ARRAY[0.5]) FROM (VALUES (NULL::double precision), (NULL)) AS t(v);
SELECT tdigest_percentile(v, 1::bigint, 100, ARRAY[0.5]) FROM (VALUES (1.0), (NULL)) AS t(v);

-- tdigest_add_double_array_values
SELECT tdigest_percentile_of(v, 100, ARRAY[1.0]) FROM (VALUES (NULL::double precision), (NULL)) AS t(v);
SELECT tdigest_percentile_of(v, 100, ARRAY[1.0]) FROM (VALUES (1.0), (NULL)) AS t(v);

-- tdigest_add_double_array_values_count
SELECT tdigest_percentile_of(v, 1::bigint, 100, ARRAY[1.0]) FROM (VALUES (NULL::double precision), (NULL)) AS t(v);
SELECT tdigest_percentile_of(v, 1::bigint, 100, ARRAY[1.0]) FROM (VALUES (1.0), (NULL)) AS t(v);

-- tdigest_add_digest_array
SELECT tdigest_percentile(d, ARRAY[0.5]) FROM (VALUES (NULL::tdigest), (NULL)) AS t(d);
SELECT tdigest_percentile(d, ARRAY[0.5]) FROM (VALUES ('flags 1 count 1 compression 100 centroids 1 (1, 1)'::tdigest), (NULL)) AS t(d);

-- tdigest_add_digest_array_values
SELECT tdigest_percentile_of(d, ARRAY[1.0]) FROM (VALUES (NULL::tdigest), (NULL)) AS t(d);
SELECT tdigest_percentile_of(d, ARRAY[1.0]) FROM (VALUES ('flags 1 count 1 compression 100 centroids 1 (1, 1)'::tdigest), (NULL)) AS t(d);
