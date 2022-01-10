-- test casting to json
SELECT cast(tdigest(i / 1000.0, 10) as json) from generate_series(1,1000) s(i);

SELECT cast(tdigest(i / 1000.0, 25) as json) from generate_series(1,1000) s(i);

SELECT cast(tdigest(i / 1000.0, 100) as json) from generate_series(1,1000) s(i);

-- test casting to double precision array
SELECT array_agg(round(v::numeric,3)) FROM (
  SELECT unnest(cast(tdigest(i / 1000.0, 10) as double precision[])) AS v from generate_series(1,1000) s(i)
) foo;

SELECT array_agg(round(v::numeric,3)) FROM (
  SELECT unnest(cast(tdigest(i / 1000.0, 25) as double precision[])) AS v from generate_series(1,1000) s(i)
) foo;

SELECT array_agg(round(v::numeric,3)) FROM (
  SELECT unnest(cast(tdigest(i / 1000.0, 100) as double precision[])) AS v from generate_series(1,1000) s(i)
) foo;
