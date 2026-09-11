-- Make sure the functions consuming a digest handle NULL and empty array
-- arguments.
--
-- The functions are STRICT, so a NULL digest or a NULL percentile/value
-- yields a NULL result. An empty array is not NULL though, and there is
-- nothing sensible to return for it, so it is rejected with an error.

\set VERBOSITY terse

-- a NULL percentile/value gives a NULL result, for both the scalar and the
-- array form
SELECT tdigest_percentile(tdigest(v, 100), NULL::double precision)
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_percentile(tdigest(v, 100), NULL::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_percentile_of(tdigest(v, 100), NULL::double precision)
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_percentile_of(tdigest(v, 100), NULL::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- the same for the trimmed functions, where either threshold may be NULL
SELECT tdigest_sum(tdigest(v, 100), NULL::double precision, 1.0)
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_avg(tdigest(v, 100), 0.0, NULL::double precision)
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- adding a NULL array to a digest adds no values
SELECT tdigest_count(tdigest_add(NULL::tdigest, NULL::double precision[], 100));

-- an empty array is rejected
SELECT tdigest_percentile(tdigest(v, 100), '{}'::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_percentile_of(tdigest(v, 100), '{}'::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

-- a NULL element inside an array is rejected too, and the message has to name
-- the argument the array was passed as, not always a percentile
SELECT tdigest_percentile(tdigest(v, 100), ARRAY[0.5, NULL]::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_percentile_of(tdigest(v, 100), ARRAY[1.0, NULL]::double precision[])
  FROM (VALUES (1.0::double precision), (2.0)) s(v);

SELECT tdigest_count(tdigest_add(NULL::tdigest, ARRAY[1.0, NULL]::double precision[], 100));
