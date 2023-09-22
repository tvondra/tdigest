# t-digest extension

[![make installcheck](https://github.com/tvondra/tdigest/actions/workflows/ci.yml/badge.svg)](https://github.com/tvondra/tdigest/actions/workflows/ci.yml)

This PostgreSQL extension implements t-digest, a data structure for on-line
accumulation of rank-based statistics such as quantiles and trimmed means.
The algorithm is also very friendly to parallel programs.

The t-digest data structure was introduced by Ted Dunning in 2013, and more
detailed description and example implementation is available in his github
repository [1]. In particular, see the paper [2] explaining the idea. Some
of the code was inspired by tdigestc [3] and tdigest [4] by ajwerner.

The accuracy of estimates produced by t-digests can be orders of magnitude
more accurate than those produced by previous digest algorithms in spite of
the fact that t-digests are much more compact when stored on disk.


## Basic usage

The extension provides two functions, which you can see as a replacement of
`percentile_cont` aggregate:

* `tdigest_percentile(value double precision, compression int,
                      quantile double precision)`

* `tdigest_percentile(value double precision, compression int,
                      quantiles double precision[])`

* `tdigest_percentile_of(value double precision, compression int,
                         value double precision)`

* `tdigest_percentile_of(value double precision, compression int,
                         values double precision[])`

That is, instead of running

```
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY a) FROM t
```

you might now run

```
SELECT tdigest_percentile(a, 100, 0.95) FROM t
```

and similarly for the variants with array of percentiles. This should run
much faster, as the t-digest does not require sort of all the data and can
be parallelized. Also, the memory usage is very limited, depending on the
compression parameter.


## Accuracy

All functions building the t-digest summaries accept `accuracy` parameter
that determines how detailed the histogram approximating the CDF is. The
value essentially limits the number of "buckets" in the t-digest, so the
higher the value the larger the digest.

Each bucket is represented by two `double precision` values (i.e. 16B per
bucket), so 10000 buckets means the largest possible t-digest is ~160kB.
That is however before the transparent compression all varlena types go
through, so the on-disk footprint may be much smaller.

It's hard to say what is a good accuracy value, as it very much depends on
the data set (how non-uniform the data distribution is, etc.), but given a
t-digest with N buckets, the error is roughly 1/N. So t-digests build with
accuracy set to 100 have roughly 1% error (with respect to the total range
of data), which is more than enough for most use cases.

This however ignores that t-digests don't have uniform bucket size. Buckets
close to 0.0 and 1.0 are much smaller (thus providing more accurate results)
while buckets close to the median are much bigger. That's consistent with
the purpose of the t-digest, i.e. estimating percentiles close to extremes.


## Advanced usage

The extension also provides a `tdigest` data type, which makes it possible
to precompute digests for subsets of data, and then quickly combine those
"partial" digest into a digest representing the whole data set. The prebuilt
digests should be much smaller compared to the original data set, allowing
significantly faster response times.

To compute the `t-digest` use `tdigest` aggregate function. The digests can
then be stored on disk and later summarized using the `tdigest_percentile`
functions (with `tdigest` as the first argument).

* `tdigest(value double precision, compression int)`

* `tdigest_percentile(digest tdigest,
                      quantile double precision)`

* `tdigest_percentile(digest tdigest,
                      quantiles double precision[])`

* `tdigest_percentile_of(digest tdigest,
                         value double precision)`

* `tdigest_percentile_of(digest tdigest,
                         values double precision[])`

So for example you may do this:

```
-- table with some random source data
CREATE TABLE t (a int, b int, c double precision);

INSERT INTO t SELECT 10 * random(), 10 * random(), random()
                FROM generate_series(1,10000000);

-- table with pre-aggregated digests into table "p"
CREATE TABLE p AS SELECT a, b, tdigest(c, 100) AS d FROM t GROUP BY a, b;

-- summarize the data from "p" (compute the 95-th percentile)
SELECT a, tdigest_percentile(d, 0.95) FROM p GROUP BY a ORDER BY a;
```

The pre-aggregated table is indeed much smaller:

~~~
db=# \d+
                         List of relations
 Schema | Name | Type  | Owner | Persistence |  Size  | Description 
--------+------+-------+-------+-------------+--------+-------------
 public | p    | table | user  | permanent   | 120 kB | 
 public | t    | table | user  | permanent   | 422 MB | 
(2 rows)
~~~

And on my machine the last query takes ~1.5ms. Compare that to queries on
the source data:

~~~
\timing on

-- exact results
SELECT a, percentile_cont(0.95) WITHIN GROUP (ORDER BY c)
  FROM t GROUP BY a ORDER BY a;
  ...
Time: 6956.566 ms (00:06.957)

-- tdigest estimate (no parallelism)
SET max_parallel_workers_per_gather = 0;
SELECT a, tdigest_percentile(c, 100, 0.95) FROM t GROUP BY a ORDER BY a;
  ...
Time: 2873.116 ms (00:02.873)

-- tdigest estimate (4 workers)
SET max_parallel_workers_per_gather = 4;
SELECT a, tdigest_percentile(c, 100, 0.95) FROM t GROUP BY a ORDER BY a;
  ...
Time: 893.538 ms
~~~

This shows how much more efficient the t-digest estimate is compared to the
exact query with `percentile_cont` (the difference would increase for larger
data sets, due to increased overhead for spilling to disk).

It also shows how effective the pre-aggregation can be. There are 121 rows
in table `p` so with 120kB disk space that's ~1kB per row, each representing
about 80k values. With 8B per value, that's ~640kB, i.e. a compression ratio
of 640:1. As the digest size is not tied to the number of items, this will
only improve for larger data set.


## Pre-aggregated data

When dealing with data sets with a lot of redundancy (values repeating
many times), it may be more efficient to partially pre-aggregate the data
and use functions that allow specifying the number of occurrences for each
value. This reduces the number of SQL-function calls.

There are five such aggregate functions:

* `tdigest_percentile(value double precision, count bigint, compression int,
                      quantile double precision)`

* `tdigest_percentile(value double precision, count bigint, compression int,
                      quantiles double precision[])`

* `tdigest_percentile_of(value double precision, count bigint, compression int,
                         value double precision)`

* `tdigest_percentile_of(value double precision, count bigint, compression int,
                         values double precision[])`

* `tdigest(value double precision, count bigint, compression int)`


## Incremental updates

An existing t-digest may be updated incrementally, either by adding a single
value, or by merging-in a whole t-digest. For example, it's possible to add
1000 random values to the t-digest like this:

```
DO LANGUAGE plpgsql $$
DECLARE
  r record;
BEGIN
  FOR r IN (SELECT random() AS v FROM generate_series(1,1000)) LOOP
    UPDATE t SET d = tdigest_add(d, r.v);
  END LOOP;
END $$;
```

The overhead of doing this is fairly high, though - the t-digest has to be
deserialized and serialized over and over, for each value we're adding.
That overhead may be reduced by pre-aggregating data, either into an array
or a t-digest.

```
DO LANGUAGE plpgsql $$
DECLARE
  a double precision[];
BEGIN
  SELECT array_agg(random()) INTO a FROM generate_series(1,1000);
  UPDATE t SET d = tdigest_add(d, a);
END $$;
```

Alternatively, it's possible to use pre-aggregated t-digest values instead
of the arrays:

```
DO LANGUAGE plpgsql $$
DECLARE
  r record;
BEGIN
  FOR r IN (SELECT mod(i,3) AS a, tdigest(random(),100) AS d FROM generate_series(1,1000) s(i) GROUP BY mod(i,3)) LOOP
    UPDATE t SET d = tdigest_union(d, r.d);
  END LOOP;
END $$;
```

It may be undesirable to perform compaction after every incremental update
(esp. when adding the values one by one).  All functions in the incremental
API allow disabling compaction by setting the `compact` parameter to `false`.
The disadvantage is that without the compaction, the resulting digests may
be somewhat larger (by a factor of 10). It's advisable to use either the
multi-value functions (with compaction after each batch) if possible, or
force compaction, e.g. by doing something like this:

```
UPDATE t SET d = tdigest_union(NULL, d);
```


## Trimmed aggregates

The extension provides two aggregate functions allowing to calculate trimmed
(truncted) sum and average.

* `tdigest_sum(digest tdigest, low double precision, high double precision)`

* `tdigest_avg(digest tdigest, low double precision, high double precision)`

The `low` and `high` parameters specify where to truncte the data.


## Functions

### `tdigest_percentile(value, accuracy, percentile)`

Computes a requested percentile from the data, using a t-digest with the
specified accuracy.

#### Synopsis

```
SELECT tdigest_percentile(t.c, 100, 0.95) FROM t
```

#### Parameters

- `value` - values to aggregate
- `accuracy` - accuracy of the t-digest
- `percentile` - value in [0, 1] specifying the percentile


### `tdigest_percentile(value, count, accuracy, percentile)`

Computes a requested percentile from the data, using a t-digest with the
specified accuracy.

#### Synopsis

```
SELECT tdigest_percentile(t.c, t.a, 100, 0.95) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences of the value
- `accuracy` - accuracy of the t-digest
- `percentile` - value in [0, 1] specifying the percentile


### `tdigest_percentile(value, accuracy, percentile[])`

Computes requested percentiles from the data, using a t-digest with the
specified accuracy.

#### Synopsis

```
SELECT tdigest_percentile(t.c, 100, ARRAY[0.95, 0.99]) FROM t
```

#### Parameters

- `value` - values to aggregate
- `accuracy` - accuracy of the t-digest
- `percentile[]` - array of values in [0, 1] specifying the percentiles


### `tdigest_percentile(value, count, accuracy, percentile[])`

Computes requested percentiles from the data, using a t-digest with the
specified accuracy.

#### Synopsis

```
SELECT tdigest_percentile(t.c, t.a, 100, ARRAY[0.95, 0.99]) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences of the value
- `accuracy` - accuracy of the t-digest
- `percentile[]` - array of values in [0, 1] specifying the percentiles


### `tdigest_percentile_of(value, accuracy, hypothetical_value)`

Computes relative rank of a hypothetical value, using a t-digest with the
specified accuracy.

#### Synopsis

```
SELECT tdigest_percentile_of(t.c, 100, 139832.3) FROM t
```

#### Parameters

- `value` - values to aggregate
- `accuracy` - accuracy of the t-digest
- `hypothetical_value` - hypothetical value


### `tdigest_percentile_of(value, count, accuracy, hypothetical_value)`

Computes relative rank of a hypothetical value, using a t-digest with the
specified accuracy.

#### Synopsis

```
SELECT tdigest_percentile_of(t.c, t.a, 100, 139832.3) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences of the value
- `accuracy` - accuracy of the t-digest
- `hypothetical_value` - hypothetical value


### `tdigest_percentile_of(value, accuracy, hypothetical_value[])`

Computes relative ranks of a hypothetical values, using a t-digest with
the specified accuracy.

#### Synopsis

```
SELECT tdigest_percentile_of(t.c, 100, ARRAY[6343.43, 139832.3]) FROM t
```

#### Parameters

- `value` - values to aggregate
- `accuracy` - accuracy of the t-digest
- `hypothetical_value` - hypothetical values


### `tdigest_percentile_of(value, count, accuracy, hypothetical_value[])`

Computes relative ranks of a hypothetical values, using a t-digest with
the specified accuracy.

#### Synopsis

```
SELECT tdigest_percentile_of(t.c, t.a, 100, ARRAY[6343.43, 139832.3]) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences of the value
- `accuracy` - accuracy of the t-digest
- `hypothetical_value` - hypothetical values


### `tdigest(value, accuracy)`

Computes t-digest with the specified accuracy.

#### Synopsis

```
SELECT tdigest(t.c, 100) FROM t
```

#### Parameters

- `value` - values to aggregate
- `accuracy` - accuracy of the t-digest


### `tdigest(value, count, accuracy)`

Computes t-digest with the specified accuracy. The values are added with
as many occurrences as determined by the count parameter.

#### Synopsis

```
SELECT tdigest(t.c, t.a, 100) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences for each value
- `accuracy` - accuracy of the t-digest


### `tdigest_count(tdigest)`

Returns number of items represented by the t-digest.

#### Synopsis

```
SELECT tdigest_count(d) FROM (
    SELECT tdigest(t.c, 100) FROM t
) foo
```


### `tdigest_percentile(tdigest, percentile)`

Computes requested percentile from the pre-computed t-digests.

#### Synopsis

```
SELECT tdigest_percentile(d, 0.99) FROM (
    SELECT tdigest(t.c, 100) FROM t
) foo
```

#### Parameters

- `tdigest` - t-digest to aggregate and process
- `percentile` - value in [0, 1] specifying the percentile


### `tdigest_percentile(tdigest, percentile[])`

Computes requested percentiles from the pre-computed t-digests.

#### Synopsis

```
SELECT tdigest_percentile(d, ARRAY[0.95, 0.99]) FROM (
    SELECT tdigest(t.c, 100) FROM t
) foo
```

#### Parameters

- `tdigest` - t-digest to aggregate and process
- `percentile` - values in [0, 1] specifying the percentiles


### `tdigest_percentile_of(tdigest, hypothetical_value)`

Computes relative rank of a hypothetical value, using a pre-computed t-digest.

#### Synopsis

```
SELECT tdigest_percentile_of(d, 349834.1) FROM (
    SELECT tdigest(t.c, 100) FROM t
) foo
```

#### Parameters

- `tdigest` - t-digest to aggregate and process
- `hypothetical_value` - hypothetical value


### `tdigest_percentile_of(tdigest, hypothetical_value[])`

Computes relative ranks of hypothetical values, using a pre-computed t-digest.

#### Synopsis

```
SELECT tdigest_percentile_of(d, ARRAY[438.256, 349834.1]) FROM (
    SELECT tdigest(t.c, 100) FROM t
) foo
```

#### Parameters

- `tdigest` - t-digest to aggregate and process
- `hypothetical_value` - hypothetical values


### `tdigest_add(tdigest, double precision)`

Performs incremental update of the t-digest by adding a single value.

#### Synopsis

```
UPDATE t SET d = tdigest_add(d, random());
```

#### Parameters

- `tdigest` - t-digest to update
- `element` - value to add to the digest
- `compression` - compression t (used when t-digest is `NULL`)
- `compact` - force compaction (default: true)


### `tdigest_add(tdigest, double precision[])`

Performs incremental update of the t-digest by adding values from an array.

#### Synopsis

```
UPDATE t SET d = tdigest_add(d, ARRAY[random(), random(), random()]);
```

#### Parameters

- `tdigest` - t-digest to update
- `elements` - array of values to add to the digest
- `compression` - compression t (used when t-digest is `NULL`)
- `compact` - force compaction (default: true)


### `tdigest_union(tdigest, tdigest)`

Performs incremental update of the t-digest by merging-in another digest.

#### Synopsis

```
WITH x AS (SELECT tdigest(random(), 100) AS d FROM generate_series(1,1000))
UPDATE t SET d = tdigest_union(t.d, x.d) FROM x;
```

#### Parameters

- `tdigest` - t-digest to update
- `tdigest_add` - t-digest to merge into `tdigest`
- `compression` - compression t (used when t-digest is `NULL`)
- `compact` - force compaction (default: true)


### `tdigest_json(tdigest)`

Returns the t-digest as a JSON value. The function is also exposed as a
cast from `tdigest` to `json`.

#### Synopsis

```
SELECT tdigest_json(d) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;

SELECT CAST(d AS json) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `tdigest` - t-digest to cast to a `json` value


### `tdigest_double_array(tdigest)`

Returns the t-digest as a `double precision[]` array. The function is also
exposed as a cast from `tdigest` to `double precision[]`.

#### Synopsis

```
SELECT tdigest_double_array(d) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;

SELECT CAST(d AS double precision[]) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `tdigest` - t-digest to cast to a `double precision[]` value


### `tdigest_avg(value, count, accuracy, low, high)`

Computes trimmed mean of values, discarding values at the low and high end.
The `low` and `high` values specify which part of the sample should be
included in the mean, so e.g. `low = 0.1` and `high = 0.9` means 10% low
and high values will be discarded.

#### Synopsis

```
SELECT tdigest_avg(t.v, t.c, 100, 0.1, 0.9) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences of the value
- `accuracy` - accuracy of the t-digest
- `low` - low threshold percentile (values below are discarded)
- `high` - high threshold percentile (values above are discarded)v


### `tdigest_avg(tdigest, low, high)`

Computes trimmed mean of values, discarding values at the low and high end.
The `low` and `high` values specify which part of the sample should be
included in the mean, so e.g. `low = 0.1` and `high = 0.9` means 10% low
and high values will be discarded.

#### Synopsis

```
SELECT tdigest_avg(d, 0.05, 0.95) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `tdigest` - tdigest to calculate mean from
- `low` - low threshold percentile (values below are discarded)
- `high` - high threshold percentile (values above are discarded)


### `tdigest_sum(value, accuracy, low, high)`

Computes trimmed sum of values, discarding values at the low and high end.
The `low` and `high` values specify which part of the sample should be
included in the sum, so e.g. `low = 0.1` and `high = 0.9` means 10% low
and high values will be discarded.

#### Synopsis

```
SELECT tdigest_sum(t.v, 100, 0.1, 0.9) FROM t
```

#### Parameters

- `value` - values to aggregate
- `accuracy` - accuracy of the t-digest
- `low` - low threshold percentile (values below are discarded)
- `high` - high threshold percentile (values above are discarded)


### `tdigest_sum(value, count, accuracy, low, high)`

Computes trimmed sum of values, discarding values at the low and high end.
The `low` and `high` values specify which part of the sample should be
included in the sum, so e.g. `low = 0.1` and `high = 0.9` means 10% low
and high values will be discarded.

#### Synopsis

```
SELECT tdigest_sum(t.v, t.c, 100, 0.1, 0.9) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences of the value
- `accuracy` - accuracy of the t-digest
- `low` - low threshold percentile (values below are discarded)
- `high` - high threshold percentile (values above are discarded)


### `tdigest_sum(tdigest, low, high)`

Computes trimmed sum of values, discarding values at the low and high end.
The `low` and `high` values specify which part of the sample should be
included in the sum, so e.g. `low = 0.1` and `high = 0.9` means 10% low
and high values will be discarded.

#### Synopsis

```
SELECT tdigest_sum(d, 0.05, 0.95) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `tdigest` - tdigest to calculate sum from
- `low` - low threshold percentile (values below are discarded)
- `high` - high threshold percentile (values above are discarded)


### `tdigest_avg(tdigest, double precision, double precision)`

Calculates average of values between the low and high threshold.

#### Synopsis

```
SELECT tdigest_avg(tdigest(v, 100), 0.25, 0.75) FROM generate_series(1,10000)
```

#### Parameters

- `tdigest` - t-digest to calculate average for
- `low` - low threshold (truncate values below)
- `high` - high threshold (truncate values above)


### `tdigest_sum(tdigest, double precision, double precision)`

Calculates sum of values between the low and high threshold.

#### Synopsis

```
SELECT tdigest_sum(tdigest(v, 100), 0.25, 0.75) FROM generate_series(1,10000)
```

#### Parameters

- `tdigest` - t-digest to calculate sum for
- `low` - low threshold (truncate values below)
- `high` - high threshold (truncate values above)


Notes
-----

At the moment, the extension only supports `double precision` values, but
it should not be very difficult to extend it to other numeric types (both
integer and/or floating point, including `numeric`). Ultimately, it could
support any data type with a concept of ordering and mean.

The estimates do depend on the order of incoming data, and so may differ
between runs. This applies especially to parallel queries, for which the
workers generally see different subsets of data for each run (and build
different digests, which are then combined together).


License
-------
This software is distributed under the terms of PostgreSQL license.
See LICENSE or http://www.opensource.org/licenses/bsd-license.php for
more details.


[1] https://github.com/tdunning/t-digest

[2] https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf

[3] https://github.com/ajwerner/tdigestc

[4] https://github.com/ajwerner/tdigest
