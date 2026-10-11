-- Regression test for the input function tdigest_in(), which parses the
-- textual representation of a t-digest.
--
-- The input is user supplied, so it has to be validated carefully - both
-- the syntax of the value and the sanity of the parsed data. The queries
-- exercise all the error checks in the input function, one by one.

\set VERBOSITY terse

-- a valid value, to make sure the checks don't reject correct input
SELECT 'flags 1 count 3 compression 10 centroids 2 (1, 1) (2, 2)'::tdigest;

--
-- syntax errors detected while parsing the individual fields
--

-- unexpected space (the value must not start with whitespace)
SELECT ' flags 1 count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- unexpected space (only a single space may separate the fields)
SELECT 'flags 1  count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- missing space (the fields have to be separated)
SELECT 'flags 1count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- unexpected keyword
SELECT 'flgs 1 count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- integer field that is not a number at all
SELECT 'flags x count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- integer field that does not fit into bigint
SELECT 'flags 1 count 99999999999999999999 compression 10 centroids 1 (1, 3)'::tdigest;

-- integer fields that fit into bigint but not into integer, both above
-- and below the integer range
SELECT 'flags 1 count 3 compression 3000000000 centroids 1 (1, 3)'::tdigest;
SELECT 'flags -3000000000 count 3 compression 10 centroids 1 (1, 3)'::tdigest;
SELECT 'flags 1 count 3 compression -3000000000 centroids 1 (1, 3)'::tdigest;
SELECT 'flags 1 count 3 compression 10 centroids -3000000000 (1, 3)'::tdigest;

-- the boundaries of the integer range pass the range check, the values are
-- then rejected by the checks of the individual fields
SELECT 'flags 2147483647 count 3 compression 10 centroids 1 (1, 3)'::tdigest;
SELECT 'flags -2147483648 count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- floating point field that is not a number at all
SELECT 'flags 1 count 3 compression 10 centroids 1 (x, 3)'::tdigest;

-- floating point field that does not fit into double precision
SELECT 'flags 1 count 3 compression 10 centroids 1 (1e400, 3)'::tdigest;

-- complete underflow must not silently turn a nonzero input into zero
SELECT 'flags 1 count 3 compression 10 centroids 1 (1e-400, 3)'::tdigest;
SELECT 'flags 1 count 3 compression 10 centroids 1 (-1e-400, 3)'::tdigest;
SELECT 'flags 0 count 3 compression 10 centroids 1 (1e-400, 3)'::tdigest;
SELECT 'flags 0 count 3 compression 10 centroids 1 (-1e-400, 3)'::tdigest;

--
-- sanity checks on the parsed header
--

-- unknown flags
SELECT 'flags 8 count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- known flags combined with an unknown one
SELECT 'flags 15 count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- the SORTED (2) and COMPACTED (4) flags require the STORES_MEAN (1) flag,
-- and COMPACTED requires SORTED
SELECT 'flags 2 count 3 compression 10 centroids 1 (1, 3)'::tdigest;
SELECT 'flags 4 count 3 compression 10 centroids 1 (1, 3)'::tdigest;
SELECT 'flags 6 count 3 compression 10 centroids 1 (1, 3)'::tdigest;
SELECT 'flags 5 count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- all valid combinations of flags (no flags means the legacy format, storing
-- sums instead of means, which gets converted to the current one)
SELECT 'flags 0 count 3 compression 10 centroids 1 (3, 3)'::tdigest;
SELECT 'flags 1 count 3 compression 10 centroids 1 (1, 3)'::tdigest;
SELECT 'flags 3 count 3 compression 10 centroids 1 (1, 3)'::tdigest;
SELECT 'flags 7 count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- compression out of the supported range
SELECT 'flags 1 count 3 compression 9 centroids 1 (1, 3)'::tdigest;
SELECT 'flags 1 count 3 compression 10001 centroids 1 (1, 3)'::tdigest;

-- non-positive total count
SELECT 'flags 1 count 0 compression 10 centroids 1 (1, 1)'::tdigest;

-- non-positive number of centroids
SELECT 'flags 1 count 3 compression 10 centroids 0'::tdigest;

-- more centroids than fit into the buffer (10 * compression)
SELECT 'flags 1 count 3 compression 10 centroids 101'::tdigest;

-- exactly as many centroids as fit into the buffer is fine
SELECT tdigest_count(('flags 1 count 100 compression 10 centroids 100 ' ||
                      (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
                         FROM generate_series(1, 100) AS s(i)))::tdigest);

--
-- sanity checks on the parsed centroids
--

-- centroid mean that is not a valid number
SELECT 'flags 1 count 3 compression 10 centroids 1 (NaN, 3)'::tdigest;
SELECT 'flags 1 count 3 compression 10 centroids 1 (Infinity, 3)'::tdigest;

-- the same in a sorted digest, where the check of mean order would not notice
-- (all comparisons with NaN are false, and the infinities are in order)
SELECT 'flags 3 count 3 compression 10 centroids 3 (1, 1) (NaN, 1) (2, 1)'::tdigest;
SELECT 'flags 3 count 3 compression 10 centroids 3 (-Infinity, 1) (1, 1) (2, 1)'::tdigest;
SELECT 'flags 7 count 3 compression 10 centroids 3 (1, 1) (2, 1) (Infinity, 1)'::tdigest;

-- non-positive centroid count
SELECT 'flags 1 count 3 compression 10 centroids 1 (1, 0)'::tdigest;

-- centroid count exceeding the total count
SELECT 'flags 1 count 3 compression 10 centroids 1 (1, 4)'::tdigest;

-- centroids not sorted by mean
SELECT 'flags 1 count 3 compression 10 centroids 2 (2, 1) (1, 2)'::tdigest;

-- that is not allowed for digests marked as sorted (or compacted), no matter
-- where the out-of-order pair is
SELECT 'flags 3 count 3 compression 10 centroids 2 (2, 1) (1, 2)'::tdigest;
SELECT 'flags 7 count 3 compression 10 centroids 2 (2, 1) (1, 2)'::tdigest;
SELECT 'flags 3 count 4 compression 10 centroids 3 (1, 1) (3, 1) (2, 2)'::tdigest;
SELECT 'flags 7 count 4 compression 10 centroids 3 (1, 1) (3, 1) (2, 2)'::tdigest;
SELECT 'flags 3 count 5 compression 10 centroids 4 (-1, 1) (-2, 1) (1, 1) (2, 2)'::tdigest;

-- centroids with the same mean may be in any order (the order by count is not
-- checked), and that includes zeros with a different sign
SELECT 'flags 3 count 6 compression 10 centroids 3 (1, 3) (1, 1) (1, 2)'::tdigest;
SELECT 'flags 7 count 7 compression 10 centroids 4 (0, 1) (1, 2) (1, 3) (1, 1)'::tdigest;
SELECT 'flags 7 count 3 compression 10 centroids 2 (0, 1) (-0, 2)'::tdigest;
SELECT 'flags 7 count 3 compression 10 centroids 2 (-0, 1) (0, 2)'::tdigest;

-- centroid counts overflowing the total count
SELECT 'flags 1 count 9223372036854775807 compression 10 centroids 2 (1, 9223372036854775807) (2, 9223372036854775807)'::tdigest;

--
-- sanity checks on the value as a whole
--

-- more centroids than the header says
SELECT 'flags 1 count 1 compression 10 centroids 1 (1, 1) (2, 1)'::tdigest;

-- fewer centroids than the header says
SELECT 'flags 1 count 3 compression 10 centroids 3 (1, 1)'::tdigest;

-- centroid counts not adding up to the total count
SELECT 'flags 1 count 3 compression 10 centroids 1 (1, 1)'::tdigest;

-- that is checked before the order of centroids in a sorted digest
SELECT 'flags 3 count 4 compression 10 centroids 2 (2, 1) (1, 2)'::tdigest;

-- a compacted digest can't have a full buffer (compaction always frees some
-- space, or fails), but a sorted one can, and a compacted one can have all
-- centroids but one
SELECT ('flags 7 count 100 compression 10 centroids 100 ' ||
        (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
           FROM generate_series(1, 100) AS s(i)))::tdigest;

SELECT tdigest_count(('flags 3 count 100 compression 10 centroids 100 ' ||
                      (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
                         FROM generate_series(1, 100) AS s(i)))::tdigest);

SELECT tdigest_count(('flags 7 count 99 compression 10 centroids 99 ' ||
                      (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
                         FROM generate_series(1, 99) AS s(i)))::tdigest);

-- the same for the maximum compression (the full buffer is checked after
-- parsing all the centroids)
SELECT ('flags 7 count 100000 compression 10000 centroids 100000 ' ||
        (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
           FROM generate_series(1, 100000) AS s(i)))::tdigest;

SELECT tdigest_count(('flags 3 count 100000 compression 10000 centroids 100000 ' ||
                      (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
                         FROM generate_series(1, 100000) AS s(i)))::tdigest);

SELECT tdigest_count(('flags 7 count 99999 compression 10000 centroids 99999 ' ||
                      (SELECT string_agg(format('(%s, 1)', i), ' ' ORDER BY i)
                         FROM generate_series(1, 99999) AS s(i)))::tdigest);
