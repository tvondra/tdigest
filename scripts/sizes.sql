-- script to generate a table of digest sizes

DROP EXTENSION IF EXISTS tdigest CASCADE;
CREATE EXTENSION tdigest;

CREATE CAST (tdigest AS bytea) WITHOUT FUNCTION;

DROP TABLE IF EXISTS t_random;
DROP TABLE IF EXISTS t_int;

CREATE TABLE t_random (c int, d_external tdigest, d_extended tdigest);
CREATE TABLE t_int    (c int, d_external tdigest, d_extended tdigest);

ALTER TABLE t_random ALTER COLUMN d_extended SET STORAGE extended;
ALTER TABLE t_int    ALTER COLUMN d_extended SET STORAGE extended;

INSERT INTO t_int SELECT    10, d FROM (SELECT mod(i,10), tdigest(i, 10) d, NULL     FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_int SELECT    50, d FROM (SELECT mod(i,10), tdigest(i, 50) d, NULL     FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_int SELECT   100, d FROM (SELECT mod(i,10), tdigest(i, 100) d, NULL    FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_int SELECT   200, d FROM (SELECT mod(i,10), tdigest(i, 200) d, NULL    FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_int SELECT   500, d FROM (SELECT mod(i,10), tdigest(i, 500) d, NULL    FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_int SELECT  1000, d FROM (SELECT mod(i,10),  tdigest(i, 1000) d, NULL  FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_int SELECT  2000, d FROM (SELECT mod(i,10),  tdigest(i, 2000) d, NULL  FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_int SELECT  5000, d FROM (SELECT mod(i,10),  tdigest(i, 5000) d, NULL  FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_int SELECT 10000, d FROM (SELECT mod(i,10),  tdigest(i, 10000) d, NULL FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));

INSERT INTO t_random SELECT    10, d FROM (SELECT mod(i,10), tdigest(random(), 10) d, NULL     FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_random SELECT    50, d FROM (SELECT mod(i,10), tdigest(random(), 50) d, NULL     FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_random SELECT   100, d FROM (SELECT mod(i,10), tdigest(random(), 100) d, NULL    FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_random SELECT   200, d FROM (SELECT mod(i,10), tdigest(random(), 200) d, NULL    FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_random SELECT   500, d FROM (SELECT mod(i,10), tdigest(random(), 500) d, NULL    FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_random SELECT  1000, d FROM (SELECT mod(i,10),  tdigest(random(), 1000) d, NULL  FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_random SELECT  2000, d FROM (SELECT mod(i,10),  tdigest(random(), 2000) d, NULL  FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_random SELECT  5000, d FROM (SELECT mod(i,10),  tdigest(random(), 5000) d, NULL  FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));
INSERT INTO t_random SELECT 10000, d FROM (SELECT mod(i,10),  tdigest(random(), 10000) d, NULL FROM generate_series(1, 1000000) s(i) GROUP BY mod(i, 10));

UPDATE t_random SET d_extended = d_external;
UPDATE t_int SET d_extended = d_external;

SELECT
  c AS compression,
  AVG(tdigest_count(d_external))::int AS items,
  AVG(regexp_replace(d_external::text, '.*centroids ([0-9]*) .*', '\1')::int) ::int AS centroids,
  AVG(LENGTH(CAST(d_external AS bytea)))::int AS raw_length,
  AVG(pg_column_size(d_external))::int AS external_length,
  AVG(pg_column_size(d_extended))::int AS extended_length
FROM t_int GROUP BY c ORDER BY c;

SELECT
  c AS compression,
  AVG(tdigest_count(d_external))::int AS items,
  AVG(regexp_replace(d_external::text, '.*centroids ([0-9]*) .*', '\1')::int) ::int AS centroids,
  AVG(LENGTH(CAST(d_external AS bytea)))::int AS raw_length,
  AVG(pg_column_size(d_external))::int AS external_length,
  AVG(pg_column_size(d_extended))::int AS extended_length
FROM t_random GROUP BY c ORDER BY c;
