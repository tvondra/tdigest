-- partitioned table: two partitions, each containing a t-digest with vastly
-- different compression values
CREATE TABLE digest_combine_test(k int, d tdigest) PARTITION BY LIST (k);
CREATE TABLE digest_combine_test_1 PARTITION OF digest_combine_test FOR VALUES IN (1);
CREATE TABLE digest_combine_test_2 PARTITION OF digest_combine_test FOR VALUES IN (2);

-- comp=10: BUFFER_SIZE=100 slots
INSERT INTO digest_combine_test SELECT 1, tdigest(v::float8, 10) FROM generate_series(1,100) v;

-- comp=10000: BUFFER_SIZE=10000 slots
INSERT INTO digest_combine_test SELECT 2, tdigest(v::float8, 10000) FROM generate_series(1,10000) v;

-- enough data to reliably trigger partitionwise aggregate
INSERT INTO digest_combine_test SELECT * FROM digest_combine_test;
INSERT INTO digest_combine_test SELECT * FROM digest_combine_test;
INSERT INTO digest_combine_test SELECT * FROM digest_combine_test;

ANALYZE digest_combine_test;

-- force partitionwise parallel aggregate
SET enable_partitionwise_aggregate = on;
SET max_parallel_workers_per_gather = 2;
SET parallel_leader_participation = off;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;

-- tdigest_combine(small, huge)
EXPLAIN (COSTS OFF)
SELECT tdigest(d) FROM digest_combine_test;

SELECT tdigest(d) FROM digest_combine_test;

DROP TABLE digest_combine_test;
