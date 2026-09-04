-- Regression test for the count overflow in tdigest_add(), which adds a
-- single value (a centroid with count 1) to the aggregate state.
--
-- The total count is int64, so adding one more value to a t-digest with
-- count INT64_MAX has to fail instead of wrapping around to a negative
-- total. The incremental API is the simplest way to get a state with such
-- a total count.

\set VERBOSITY terse

-- adding a single value overflows the total count
SELECT tdigest_add('flags 1 count 9223372036854775807 compression 100 centroids 1 (1, 9223372036854775807)'::tdigest,
                   2.0);

-- the same thing when adding an array of values
SELECT tdigest_add('flags 1 count 9223372036854775807 compression 100 centroids 1 (1, 9223372036854775807)'::tdigest,
                   ARRAY[2.0]);

-- the last value that still fits must not fail, though
SELECT tdigest_add('flags 1 count 9223372036854775806 compression 100 centroids 1 (1, 9223372036854775806)'::tdigest,
                   2.0);
