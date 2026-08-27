-- Use terse verbosity, so that the expected output does not depend on the
-- error context, which differs between PostgreSQL versions (particularly
-- for the COPY ... FROM statements below).
\set VERBOSITY terse

CREATE TABLE tdigest_overflow_test (d tdigest);

-- Regression test for the count overflow in tdigest_in() and tdigest_recv().
--
-- Both input functions sum the centroid counts and compare the result to the
-- total count stored in the header. Both the centroid counts and the total
-- are int64, so the sum may overflow - and if it happens to wrap around to
-- exactly the value stored in the header, the "total count does not match"
-- check passes and a corrupted t-digest gets accepted.
--
-- Five centroids with count 2^62 are the smallest such case: the centroids
-- weigh 5 * 2^62 in total, which is 2^62 modulo 2^64. So a digest claiming
-- "count 2^62" passes the check, while the centroids actually weigh five
-- times as much.

-- tdigest_in() has to reject the wrapping value
SELECT 'flags 1 count 4611686018427387904 compression 10 centroids 5 (1, 4611686018427387904) (2, 4611686018427387904) (3, 4611686018427387904) (4, 4611686018427387904) (5, 4611686018427387904)'::tdigest;

-- tdigest_recv() has to reject it too. There's no way to call the receive
-- function directly, so go through a binary COPY. A binary COPY file with a
-- single bytea column is byte-for-byte the same as a file with a single
-- tdigest column holding the same bytes, so we can build the input with the
-- send functions of the individual header/centroid fields.
COPY (SELECT int4send(1)                                  -- flags
          || int8send(4611686018427387904::bigint)        -- count
          || int4send(10)                                 -- compression
          || int4send(5)                                  -- ncentroids
          || float8send(1) || int8send(4611686018427387904::bigint)
          || float8send(2) || int8send(4611686018427387904::bigint)
          || float8send(3) || int8send(4611686018427387904::bigint)
          || float8send(4) || int8send(4611686018427387904::bigint)
          || float8send(5) || int8send(4611686018427387904::bigint))
  TO '/tmp/tdigest_count_overflow.bin' WITH (FORMAT binary);

COPY tdigest_overflow_test FROM '/tmp/tdigest_count_overflow.bin' WITH (FORMAT binary);

-- a t-digest with a total count of exactly INT64_MAX is still valid, though,
-- and has to be accepted by both input functions
SELECT 'flags 1 count 9223372036854775807 compression 10 centroids 2 (1, 4611686018427387903) (2, 4611686018427387904)'::tdigest;

COPY (SELECT int4send(1)
          || int8send(9223372036854775807::bigint)
          || int4send(10)
          || int4send(2)
          || float8send(1) || int8send(4611686018427387903::bigint)
          || float8send(2) || int8send(4611686018427387904::bigint))
  TO '/tmp/tdigest_count_overflow.bin' WITH (FORMAT binary);

COPY tdigest_overflow_test FROM '/tmp/tdigest_count_overflow.bin' WITH (FORMAT binary);

SELECT * FROM tdigest_overflow_test;

-- Regression test for the count overflow in tdigest_add() and tdigest_add_centroid().
-- A couple examples triggering overflows of total count when adding values
-- or centroids to a digest (or aggregate state)
WITH source(value) AS (
    SELECT * FROM
        (VALUES ('flags 1 count 9223372036854775807 compression 10 centroids 1 (-1000, 9223372036854775807)'::tdigest),
                ('flags 1 count 1 compression 10 centroids 1 (-1000, 1)'::tdigest)) AS t(v)
)
SELECT tdigest(value)
FROM source;

WITH source(value) AS (
    SELECT * FROM
        (VALUES ('flags 1 count 4611686018427387904 compression 10 centroids 1 (-1000, 4611686018427387904)'::tdigest),
                ('flags 1 count 4611686018427387904 compression 10 centroids 1 (-1000, 4611686018427387904)'::tdigest)) AS t(v)
)
SELECT tdigest(value)
FROM source;

WITH source(value) AS (
    SELECT
        'flags 1 count 5000000000000000000 compression 10 centroids 1 (-1000, 5000000000000000000)'::tdigest
    FROM generate_series(1, 2)
)
SELECT tdigest(value)
FROM source;

-- the negative cases (no overflows, hitting INT64_MAX exactly) should still pass
WITH source(value) AS (
    SELECT * FROM
        (VALUES ('flags 1 count 9223372036854775806 compression 10 centroids 1 (-1000, 9223372036854775806)'::tdigest),
                ('flags 1 count 1 compression 10 centroids 1 (-1000, 1)'::tdigest)) AS t(v)
)
SELECT tdigest(value)
FROM source;

WITH source(value) AS (
    SELECT * FROM
        (VALUES ('flags 1 count 4611686018427387903 compression 10 centroids 1 (-1000, 4611686018427387903)'::tdigest),
                ('flags 1 count 4611686018427387904 compression 10 centroids 1 (-1000, 4611686018427387904)'::tdigest)) AS t(v)
)
SELECT tdigest(value)
FROM source;

WITH source(value) AS (
    SELECT
        'flags 1 count 4611686018427387903 compression 10 centroids 1 (-1000, 4611686018427387903)'::tdigest
    FROM generate_series(1, 2)
    UNION ALL
    SELECT
        'flags 1 count 1 compression 10 centroids 1 (-1000, 1)'::tdigest
)
SELECT tdigest(value)
FROM source;

DROP TABLE tdigest_overflow_test;
