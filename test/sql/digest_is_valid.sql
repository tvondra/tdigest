-- Regression test for tdigest_is_valid(), which performs the same checks as
-- the input functions (tdigest_in and tdigest_recv), but on an existing value
-- and without raising an error.
--
-- Digests written by older versions of the extension (which did not have all
-- of the checks) may be broken in various ways, and nothing re-validates the
-- values when reading them back from disk. This function allows finding such
-- digests.
--
-- The input functions reject invalid values, so we can't use them to build
-- the test data. Instead we build the on-disk representation by hand, and use
-- a binary-coercible cast to turn it into a t-digest. Note the forged values
-- must not be passed to anything except tdigest_is_valid() - the whole point
-- is that they are broken, so e.g. printing them might read past the end of
-- the value.

\set VERBOSITY terse

-- The fields of the on-disk representation use the host byte order, while
-- the send functions produce network byte order (big endian), so we may need
-- to reverse the bytes. Determine the host byte order by looking at the flags
-- of a digest we know has flags set to 1.
CREATE FUNCTION is_little_endian() RETURNS bool AS $$
    SELECT substring('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest::bytea from 1 for 4)
           = '\x01000000'::bytea;
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION swap_bytes(b bytea) RETURNS bytea AS $$
    SELECT CASE WHEN is_little_endian()
                THEN (SELECT string_agg(substring(b from i for 1), ''::bytea ORDER BY i DESC)
                        FROM generate_series(1, length(b)) AS i)
                ELSE b
           END;
$$ LANGUAGE sql IMMUTABLE;

-- Transform values to big endian (if needed)
CREATE FUNCTION be_int4(v int) RETURNS bytea AS $$
    SELECT swap_bytes(int4send(v));
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION be_int8(v bigint) RETURNS bytea AS $$
    SELECT swap_bytes(int8send(v));
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION be_float8(v double precision) RETURNS bytea AS $$
    SELECT swap_bytes(float8send(v));
$$ LANGUAGE sql IMMUTABLE;

-- the layout has to be (flags, count, compression, ncentroids) followed by
-- the centroids, each stored as (mean, count)
SELECT length('flags 1 count 3 compression 10 centroids 2 (1, 1) (2, 2)'::tdigest::bytea) AS bytes;

-- a forged value has to be the same as the parsed one (sanity check of the
-- forging, not of the validation)
SELECT (be_int4(1)
     || be_int8(3::bigint)
     || be_int4(10)
     || be_int4(2)
     || be_float8(1) || be_int8(1::bigint)
     || be_float8(2) || be_int8(2::bigint))::tdigest;

--
-- valid values, to make sure the checks don't reject correct digests
--

SELECT tdigest_is_valid('flags 1 count 3 compression 10 centroids 2 (1, 1) (2, 2)'::tdigest);

-- the old on-disk format, storing sums instead of means
SELECT tdigest_is_valid('flags 0 count 3 compression 10 centroids 2 (1, 1) (4, 2)'::tdigest);

-- centroids not sorted by mean (produced by the incremental API)
SELECT tdigest_is_valid('flags 1 count 3 compression 10 centroids 2 (2, 2) (1, 1)'::tdigest);

-- digests marked as sorted (and compacted)
SELECT tdigest_is_valid('flags 3 count 3 compression 10 centroids 2 (1, 1) (2, 2)'::tdigest);
SELECT tdigest_is_valid('flags 7 count 3 compression 10 centroids 2 (1, 1) (2, 2)'::tdigest);

-- centroids with the same mean may be in any order (zeros with a different
-- sign are equal too)
SELECT tdigest_is_valid((be_int4(7)
                      || be_int8(7::bigint)
                      || be_int4(10)
                      || be_int4(4)
                      || be_float8(0) || be_int8(1::bigint)
                      || be_float8(1) || be_int8(2::bigint)
                      || be_float8(1) || be_int8(3::bigint)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(7)
                      || be_int8(3::bigint)
                      || be_int4(10)
                      || be_int4(2)
                      || be_float8(0) || be_int8(1::bigint)
                      || be_float8('-0') || be_int8(2::bigint))::tdigest);

-- the extreme compression values
SELECT tdigest_is_valid('flags 1 count 1 compression 10 centroids 1 (1, 1)'::tdigest);
SELECT tdigest_is_valid('flags 1 count 1 compression 10000 centroids 1 (1, 1)'::tdigest);

-- exactly as many centroids as fit into the buffer (10 * compression)
SELECT tdigest_is_valid(('flags 1 count 100 compression 10 centroids 100 ' ||
                         (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
                            FROM generate_series(1, 100) AS s(i)))::tdigest);

-- the same for a sorted digest, while a compacted one may have all centroids
-- but one
SELECT tdigest_is_valid(('flags 3 count 100 compression 10 centroids 100 ' ||
                         (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
                            FROM generate_series(1, 100) AS s(i)))::tdigest);

SELECT tdigest_is_valid(('flags 7 count 99 compression 10 centroids 99 ' ||
                         (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
                            FROM generate_series(1, 99) AS s(i)))::tdigest);

-- digests built by the extension itself
SELECT tdigest_is_valid(tdigest(i::double precision, 10)) FROM generate_series(1, 1000) AS s(i);
SELECT tdigest_is_valid(tdigest(i::double precision, 100)) FROM generate_series(1, 1000) AS s(i);
SELECT tdigest_is_valid(tdigest_add(NULL::tdigest, ARRAY[1.0, 2.0, 3.0], 10, false));
SELECT tdigest_is_valid(tdigest_union(tdigest_add(NULL::tdigest, 1.0, 10),
                                      tdigest_add(NULL::tdigest, 2.0, 10)));

--
-- sanity checks on the header
--

-- unknown flags
SELECT tdigest_is_valid((be_int4(8)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

-- the SORTED (2) and COMPACTED (4) flags require the STORES_MEAN (1) flag
SELECT tdigest_is_valid((be_int4(2)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(4)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(6)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

-- and COMPACTED requires SORTED
SELECT tdigest_is_valid((be_int4(5)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

-- compression out of the supported range
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(9)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10001)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

-- non-positive total count
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(0::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(1)
                      || be_int8((-1)::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

-- non-positive number of centroids
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(0))::tdigest);

SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(-1))::tdigest);

-- more centroids than fit into the buffer (10 * compression)
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(101)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

--
-- sanity checks on the length of the value
--
-- The input functions build the digest from the parsed header, so the length
-- always matches. For an existing value we have to check it really is long
-- enough for the centroids the header promises.
--

-- value too short to even contain the header
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10))::tdigest);

-- fewer centroids than the header says
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(3::bigint)
                      || be_int4(10)
                      || be_int4(2)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

-- more centroids than the header says
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint)
                      || be_float8(2) || be_int8(2::bigint))::tdigest);

-- incomplete centroid at the end of the value
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1))::tdigest);

--
-- sanity checks on the centroids
--

-- centroid mean that is not a valid number
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8('NaN') || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8('Infinity') || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(3::bigint)
                      || be_int4(10)
                      || be_int4(2)
                      || be_float8(1) || be_int8(1::bigint)
                      || be_float8('-Infinity') || be_int8(2::bigint))::tdigest);

-- the same in a sorted digest, where the check of mean order would not notice
-- (all comparisons with NaN are false, and the infinities are in order)
SELECT tdigest_is_valid((be_int4(3)
                      || be_int8(3::bigint)
                      || be_int4(10)
                      || be_int4(3)
                      || be_float8(1) || be_int8(1::bigint)
                      || be_float8('NaN') || be_int8(1::bigint)
                      || be_float8(2) || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(7)
                      || be_int8(3::bigint)
                      || be_int4(10)
                      || be_int4(3)
                      || be_float8(1) || be_int8(1::bigint)
                      || be_float8(2) || be_int8(1::bigint)
                      || be_float8('Infinity') || be_int8(1::bigint))::tdigest);

-- non-positive centroid count
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(0::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(2)
                      || be_float8(1) || be_int8(1::bigint)
                      || be_float8(2) || be_int8((-1)::bigint))::tdigest);

-- zero centroid count, even if the counts add up to the total count
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(2)
                      || be_float8(1) || be_int8(1::bigint)
                      || be_float8(2) || be_int8(0::bigint))::tdigest);

-- centroid count exceeding the total count
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(1::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(2::bigint))::tdigest);

-- centroid counts overflowing the total count
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(9223372036854775807::bigint)
                      || be_int4(10)
                      || be_int4(2)
                      || be_float8(1) || be_int8(9223372036854775807::bigint)
                      || be_float8(2) || be_int8(9223372036854775807::bigint))::tdigest);

--
-- sanity checks on the value as a whole
--

-- centroid counts not adding up to the total count
SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(3::bigint)
                      || be_int4(10)
                      || be_int4(1)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(1)
                      || be_int8(3::bigint)
                      || be_int4(10)
                      || be_int4(2)
                      || be_float8(1) || be_int8(1::bigint)
                      || be_float8(2) || be_int8(3::bigint))::tdigest);

-- centroids not sorted by mean, in digests marked as sorted
SELECT tdigest_is_valid((be_int4(3)
                      || be_int8(3::bigint)
                      || be_int4(10)
                      || be_int4(2)
                      || be_float8(2) || be_int8(2::bigint)
                      || be_float8(1) || be_int8(1::bigint))::tdigest);

SELECT tdigest_is_valid((be_int4(7)
                      || be_int8(4::bigint)
                      || be_int4(10)
                      || be_int4(3)
                      || be_float8(1) || be_int8(1::bigint)
                      || be_float8(3) || be_int8(1::bigint)
                      || be_float8(2) || be_int8(2::bigint))::tdigest);

-- a compacted digest can't have a full buffer
SELECT tdigest_is_valid((be_int4(7)
                      || be_int8(100::bigint)
                      || be_int4(10)
                      || be_int4(100)
                      || (SELECT string_agg(be_float8(i) || be_int8(1::bigint), ''::bytea ORDER BY i)
                            FROM generate_series(1, 100) AS s(i)))::tdigest);

--
-- the checks have to work on values read back from a table too (which may
-- use a different varlena header, or be TOASTed)
--

CREATE TABLE tdigest_values (id int, d tdigest);

INSERT INTO tdigest_values
SELECT 1, 'flags 1 count 3 compression 10 centroids 2 (1, 1) (2, 2)'::tdigest;

INSERT INTO tdigest_values
SELECT 2, (be_int4(1)
        || be_int8(3::bigint)
        || be_int4(10)
        || be_int4(1)
        || be_float8(1) || be_int8(1::bigint))::tdigest;

INSERT INTO tdigest_values
SELECT 3, tdigest(i::double precision, 1000) FROM generate_series(1, 100000) AS s(i);

SELECT id, tdigest_is_valid(d) FROM tdigest_values ORDER BY id;

-- NULL input produces NULL
SELECT tdigest_is_valid(NULL::tdigest) IS NULL;

DROP TABLE tdigest_values;

DROP FUNCTION be_float8(double precision);
DROP FUNCTION be_int8(bigint);
DROP FUNCTION be_int4(int);
DROP FUNCTION swap_bytes(bytea);
DROP FUNCTION is_little_endian();
