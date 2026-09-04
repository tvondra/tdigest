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

-- integer field that fits into bigint but not into integer
SELECT 'flags 1 count 3 compression 3000000000 centroids 1 (1, 3)'::tdigest;

-- floating point field that is not a number at all
SELECT 'flags 1 count 3 compression 10 centroids 1 (x, 3)'::tdigest;

-- floating point field that does not fit into double precision
SELECT 'flags 1 count 3 compression 10 centroids 1 (1e400, 3)'::tdigest;

--
-- sanity checks on the parsed header
--

-- unknown flags
SELECT 'flags 2 count 3 compression 10 centroids 1 (1, 3)'::tdigest;

-- compression out of the supported range
SELECT 'flags 1 count 3 compression 9 centroids 1 (1, 3)'::tdigest;
SELECT 'flags 1 count 3 compression 10001 centroids 1 (1, 3)'::tdigest;

-- non-positive total count
SELECT 'flags 1 count 0 compression 10 centroids 1 (1, 1)'::tdigest;

-- non-positive number of centroids
SELECT 'flags 1 count 3 compression 10 centroids 0'::tdigest;

-- more centroids than fit into the buffer (10 * compression)
SELECT 'flags 1 count 3 compression 10 centroids 101'::tdigest;

--
-- sanity checks on the parsed centroids
--

-- centroid mean that is not a valid number
SELECT 'flags 1 count 3 compression 10 centroids 1 (NaN, 3)'::tdigest;
SELECT 'flags 1 count 3 compression 10 centroids 1 (Infinity, 3)'::tdigest;

-- non-positive centroid count
SELECT 'flags 1 count 3 compression 10 centroids 1 (1, 0)'::tdigest;

-- centroid count exceeding the total count
SELECT 'flags 1 count 3 compression 10 centroids 1 (1, 4)'::tdigest;

-- centroids not sorted by mean
SELECT 'flags 1 count 3 compression 10 centroids 2 (2, 1) (1, 2)'::tdigest;

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
