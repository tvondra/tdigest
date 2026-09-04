-- Regression test for the receive function tdigest_recv(), which parses the
-- binary representation of a t-digest.
--
-- The input is user supplied, so it has to be validated just like the text
-- input. The queries exercise all the error checks in the receive function,
-- one by one.
--
-- Test through a binary COPY, which goes through the receive function. We
-- build the input with the send functions of the individual fields.

\set VERBOSITY terse

CREATE TABLE tdigest_dst (s tdigest);

-- a valid value, to make sure the checks don't reject correct input
COPY (SELECT int4send(1)                            -- flags
          || int8send(3::bigint)                    -- count
          || int4send(10)                           -- compression
          || int4send(2)                            -- ncentroids
          || float8send(1) || int8send(1::bigint)
          || float8send(2) || int8send(2::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

--
-- sanity checks on the header
--

-- unknown flags
COPY (SELECT int4send(2)
          || int8send(1::bigint)
          || int4send(10)
          || int4send(1)
          || float8send(1) || int8send(1::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- compression out of the supported range
COPY (SELECT int4send(1)
          || int8send(1::bigint)
          || int4send(9)
          || int4send(1)
          || float8send(1) || int8send(1::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY (SELECT int4send(1)
          || int8send(1::bigint)
          || int4send(10001)
          || int4send(1)
          || float8send(1) || int8send(1::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- non-positive total count
COPY (SELECT int4send(1)
          || int8send(0::bigint)
          || int4send(10)
          || int4send(1)
          || float8send(1) || int8send(1::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- non-positive number of centroids
COPY (SELECT int4send(1)
          || int8send(1::bigint)
          || int4send(10)
          || int4send(0))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- more centroids than fit into the buffer (10 * compression)
COPY (SELECT int4send(1)
          || int8send(1::bigint)
          || int4send(10)
          || int4send(101))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

--
-- sanity checks on the centroids
--

-- centroid mean that is not a valid number
COPY (SELECT int4send(1)
          || int8send(1::bigint)
          || int4send(10)
          || int4send(1)
          || float8send('NaN') || int8send(1::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY (SELECT int4send(1)
          || int8send(1::bigint)
          || int4send(10)
          || int4send(1)
          || float8send('Infinity') || int8send(1::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- non-positive centroid count
COPY (SELECT int4send(1)
          || int8send(1::bigint)
          || int4send(10)
          || int4send(1)
          || float8send(1) || int8send(0::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- centroid count exceeding the total count
COPY (SELECT int4send(1)
          || int8send(1::bigint)
          || int4send(10)
          || int4send(1)
          || float8send(1) || int8send(2::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- centroids not sorted by mean
COPY (SELECT int4send(1)
          || int8send(3::bigint)
          || int4send(10)
          || int4send(2)
          || float8send(2) || int8send(2::bigint)
          || float8send(1) || int8send(1::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- centroids not sorted by mean, in the old on-disk format storing sums
-- instead of means (the sums are sorted, but the means are not)
COPY (SELECT int4send(0)
          || int8send(3::bigint)
          || int4send(10)
          || int4send(2)
          || float8send(4) || int8send(1::bigint)
          || float8send(6) || int8send(2::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- centroid counts overflowing the total count
COPY (SELECT int4send(1)
          || int8send(9223372036854775807::bigint)
          || int4send(10)
          || int4send(2)
          || float8send(1) || int8send(9223372036854775807::bigint)
          || float8send(2) || int8send(9223372036854775807::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

--
-- sanity checks on the value as a whole
--

-- centroid counts not adding up to the total count
COPY (SELECT int4send(1)
          || int8send(3::bigint)
          || int4send(10)
          || int4send(1)
          || float8send(1) || int8send(1::bigint))
  TO '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

COPY tdigest_dst FROM '/tmp/tdigest_recv_checks.bin' WITH (FORMAT binary);

-- only the valid value should have made it into the table
SELECT s FROM tdigest_dst;

DROP TABLE tdigest_dst;
