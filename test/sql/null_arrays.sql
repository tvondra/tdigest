-- Non-STRICT transition functions must reject NULL percentile or hypothetical
-- value arguments when a non-NULL input creates the aggregate state. NULL
-- input values themselves are skipped instead.

\set VERBOSITY terse

-- The requested percentile/value is only read while the aggregate state is
-- still NULL, so the first non-NULL input is enough to exercise each check.

-- tdigest_add_double(internal, double precision, int, double precision)
SELECT tdigest_percentile(v, 100, NULL::double precision)
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_double_array(internal, double precision, int, double precision[])
SELECT tdigest_percentile(v, 100, NULL::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_double_count(internal, double precision, bigint, int, double precision)
SELECT tdigest_percentile(v, 2, 100, NULL::double precision)
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_double_array_count(internal, double precision, bigint, int, double precision[])
SELECT tdigest_percentile(v, 2, 100, NULL::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_digest(internal, tdigest, double precision)
SELECT tdigest_percentile(d, NULL::double precision)
  FROM (SELECT tdigest(v, 100) AS d
          FROM (VALUES (1.0::double precision), (2.0)) s(v)) t;

-- tdigest_add_digest_array(internal, tdigest, double precision[])
SELECT tdigest_percentile(d, NULL::double precision[])
  FROM (SELECT tdigest(v, 100) AS d
          FROM (VALUES (1.0::double precision), (2.0)) s(v)) t;

-- tdigest_add_double_values(internal, double precision, int, double precision)
SELECT tdigest_percentile_of(v, 100, NULL::double precision)
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_double_array_values(internal, double precision, int, double precision[])
SELECT tdigest_percentile_of(v, 100, NULL::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_double_values_count(internal, double precision, bigint, int, double precision)
SELECT tdigest_percentile_of(v, 2, 100, NULL::double precision)
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_double_array_values_count(internal, double precision, bigint, int, double precision[])
SELECT tdigest_percentile_of(v, 2, 100, NULL::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_digest_values(internal, tdigest, double precision)
SELECT tdigest_percentile_of(d, NULL::double precision)
  FROM (SELECT tdigest(v, 100) AS d
          FROM (VALUES (1.0::double precision), (2.0)) s(v)) t;

-- tdigest_add_digest_array_values(internal, tdigest, double precision[])
SELECT tdigest_percentile_of(d, NULL::double precision[])
  FROM (SELECT tdigest(v, 100) AS d
          FROM (VALUES (1.0::double precision), (2.0)) s(v)) t;

-- A NULL array on a later row is harmless (the state already exists), and
-- tdigest_add_double_array_increment() guards the same call correctly. Both
-- work before and after the fix.
SELECT tdigest_percentile(v, 100, CASE WHEN v = 1.0 THEN ARRAY[0.5] END)
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_count(tdigest_add(NULL::tdigest, NULL::double precision[], 100));

-- The arrays however should not be empty {}.

-- tdigest_add_double_array(internal, double precision, int, double precision[])
SELECT tdigest_percentile(v, 100, '{}'::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_double_array_count(internal, double precision, bigint, int, double precision[])
SELECT tdigest_percentile(v, 2, 100, '{}'::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_digest_array(internal, tdigest, double precision[])
SELECT tdigest_percentile(d, '{}'::double precision[])
  FROM (SELECT tdigest(v, 100) AS d
          FROM (VALUES (1.0::double precision), (2.0)) s(v)) t;

-- tdigest_add_double_array_values(internal, double precision, int, double precision[])
SELECT tdigest_percentile_of(v, 100, '{}'::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_double_array_values_count(internal, double precision, bigint, int, double precision[])
SELECT tdigest_percentile_of(v, 2, 100, '{}'::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- tdigest_add_digest_array_values(internal, tdigest, double precision[])
SELECT tdigest_percentile_of(d, '{}'::double precision[])
  FROM (SELECT tdigest(v, 100) AS d
          FROM (VALUES (1.0::double precision), (2.0)) s(v)) t;

-- a NULL element inside an array is rejected too, and the message has to name
-- the argument the array was passed as, not always a percentile
SELECT tdigest_percentile(v, 100, ARRAY[0.5, NULL]::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_percentile_of(v, 100, ARRAY[1.0, NULL]::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_count(tdigest_add(NULL::tdigest, ARRAY[1.0, NULL]::double precision[], 100));
