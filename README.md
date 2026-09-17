# t-digest extension

[![make installcheck](https://github.com/tvondra/tdigest/actions/workflows/ci.yml/badge.svg)](https://github.com/tvondra/tdigest/actions/workflows/ci.yml)

This PostgreSQL extension implements t-digest, a data structure for on-line
accumulation of rank-based statistics such as quantiles and trimmed means.
The algorithm is also very friendly to parallel programs.

The t-digest data structure was introduced by Ted Dunning in 2013, and a more
detailed description and an example implementation are available in his GitHub
repository [1]. In particular, see the paper [2] explaining the idea. Some
of the code was inspired by tdigestc [3] and tdigest [4] by ajwerner.

The accuracy of estimates produced by t-digests can be orders of magnitude
more accurate than those produced by previous digest algorithms in spite of
the fact that t-digests are much more compact when stored on disk.


## Basic usage

For the basic use case the extension provides four aggregate functions. The
`tdigest_percentile` ones can be seen as a replacement for the
`percentile_cont` aggregate, while the `tdigest_percentile_of` ones perform
the inverse operation, estimating the relative rank of a given value:

* `tdigest_percentile(value double precision, compression int,
                      quantile double precision)`

* `tdigest_percentile(value double precision, compression int,
                      quantiles double precision[])`

* `tdigest_percentile_of(value double precision, compression int,
                         hypothetical_value double precision)`

* `tdigest_percentile_of(value double precision, compression int,
                         hypothetical_values double precision[])`

That is, instead of running

```
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY a) FROM t
```

you might now run

```
SELECT tdigest_percentile(a, 100, 0.95) FROM t
```

and similarly for the variants with an array of percentiles. This should run
much faster, as the t-digest does not require sorting all the data and can
be parallelized. Also, the memory usage is very limited, depending on the
compression parameter.


## Accuracy

Functions building t-digests accept a `compression` parameter that controls
the trade-off between accuracy, digest size, memory use and processing cost.
Larger values generally retain more, smaller centroids. The accepted range is
`[10, 10000]`; values outside this range are rejected with an error.

Compression is neither the number of centroids nor an error bound. Accuracy
depends on the data distribution, input order and history of merging digests.
There is no general `1/N` error guarantee for a digest with `N` centroids, and
compression 100 does not promise 1% error relative to the range of data values.
Values such as 100, used in the examples, are starting points to evaluate
against exact results on representative data.

The algorithm allows smaller centroid weights near quantiles 0.0 and 1.0
than near the median. This concentrates resolution in the tails, but does
not impose a fixed error bound in the units of the input values.

### Digest size and storage

The centroid buffer holds at most `10 * compression` centroids, including
uncompacted input. A compacted digest is usually much smaller. Each centroid
stores an 8-byte `double precision` mean and an 8-byte integer count. With
the 24-byte full header, a digest uses `24 + 16 * ncentroids` bytes before
any TOAST processing. The largest permitted value therefore has 100000
centroids and occupies 1,600,024 bytes (about 1.53 MiB).

The on-disk digests are typically much smaller than the centroid buffer,
due to compaction which merges centroids depending on how close to the
median of the dataset they lie.

The type uses PostgreSQL's `EXTERNAL` storage policy by default. This allows
large values to be stored out of line using TOAST, but does not compress
them. A column can use `EXTENDED` storage to permit TOAST compression;
changing that setting does not itself rewrite existing values. Tuple and
TOAST overhead are additional to the size of the digest.


## Advanced usage

The extension also provides a `tdigest` data type, which makes it possible
to precompute digests for subsets of data, and then quickly combine those
"partial" digests into a digest representing the whole data set. The prebuilt
digests should be much smaller compared to the original data set, allowing
significantly faster response times.

To compute a `t-digest`, use the `tdigest` aggregate function. The digests can
then be stored on disk and later summarized using the `tdigest_percentile`
functions (with `tdigest` as the first argument).

* `tdigest(value double precision, compression int)`

* `tdigest(digest tdigest)`

* `tdigest_percentile(digest tdigest,
                      quantile double precision)`

* `tdigest_percentile(digest tdigest,
                      quantiles double precision[])`

* `tdigest_percentile_of(digest tdigest,
                         hypothetical_value double precision)`

* `tdigest_percentile_of(digest tdigest,
                         hypothetical_values double precision[])`

The `tdigest(digest tdigest)` variant is an aggregate merging multiple
pre-computed digests into a single digest, which can be stored again.

Digest-input aggregates accept digests with different compression settings.
Each aggregate state takes its compression from its first non-`NULL` digest;
with parallel aggregation, the final choice can depend on worker and combine
order. Use a consistent compression across input digests when that choice
matters. Merging cannot recover detail already lost by compaction.

So for example you may do this:

```
-- table with some random source data, with "a" usable as a count of
-- occurrences (so it has to be positive)
CREATE TABLE t (a int, b int, c double precision);

INSERT INTO t SELECT 1 + 10 * random(), 10 * random(), random()
                FROM generate_series(1,10000000);

-- table with pre-aggregated digests
CREATE TABLE p AS SELECT a, b, tdigest(c, 100) AS d FROM t GROUP BY a, b;

-- summarize the data from "p" (compute the 95th percentile)
SELECT a, tdigest_percentile(d, 0.95) FROM p GROUP BY a ORDER BY a;
```

An example run produced a much smaller pre-aggregated table:

~~~
db=# \d+
                         List of relations
 Schema | Name | Type  | Owner | Persistence |  Size  | Description 
--------+------+-------+-------+-------------+--------+-------------
 public | p    | table | user  | permanent   | 120 kB | 
 public | t    | table | user  | permanent   | 422 MB | 
(2 rows)
~~~

On the same machine, the last query took about 1.5 ms. Compare that to the
following example timings on the source data; sizes and timings will vary
with the data, PostgreSQL version and hardware:

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

This illustrates how much faster the t-digest estimate can be than the
exact query with `percentile_cont`. The difference can increase when sorting
larger data sets requires spilling to disk.

It also shows how effective the pre-aggregation can be. In this example,
there are 121 rows in table `p`, so with 120kB disk space that's ~1kB per row,
each representing about 80k values. With 8B per value, that's ~640kB, or a
compression ratio of about 640:1. For a fixed number of groups and a fixed
compression, digest storage is bounded while the raw data grows.


## Pre-aggregated data

When dealing with data sets with a lot of redundancy (values repeating
many times), it may be more efficient to partially pre-aggregate the data
and use functions that allow specifying the number of occurrences for each
value. This reduces the number of SQL-function calls.

There are seven such aggregate functions:

* `tdigest(value double precision, count bigint, compression int)`

* `tdigest_percentile(value double precision, count bigint, compression int,
                      quantile double precision)`

* `tdigest_percentile(value double precision, count bigint, compression int,
                      quantiles double precision[])`

* `tdigest_percentile_of(value double precision, count bigint, compression int,
                         hypothetical_value double precision)`

* `tdigest_percentile_of(value double precision, count bigint, compression int,
                         hypothetical_values double precision[])`

* `tdigest_avg(value double precision, count bigint, compression int,
               low double precision, high double precision)`

* `tdigest_sum(value double precision, count bigint, compression int,
               low double precision, high double precision)`

A non-`NULL` `count` must be positive and determines how many times the value
is added to the digest. A `NULL` count means one occurrence. The total count
in a digest must fit in a `bigint`; exceeding 9223372036854775807 raises an
error. See the "trimmed aggregates" section for the `low` and `high` parameters.


## Incremental updates

An existing t-digest may be updated incrementally, either by adding a single
value, or by merging-in a whole t-digest. The following examples use the
table `p` with the pre-aggregated digests (in column `d`), built in
[Advanced usage](#advanced-usage). Each example adds the same new values to
every row of `p`; use a `WHERE` clause when updating only selected groups.

For example, it's possible to add 1000 random values to the t-digests like
this:

```
DO LANGUAGE plpgsql $$
DECLARE
  r record;
BEGIN
  FOR r IN (SELECT random() AS v FROM generate_series(1,1000)) LOOP
    UPDATE p SET d = tdigest_add(d, r.v);
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
  vals double precision[];
BEGIN
  SELECT array_agg(random()) INTO vals FROM generate_series(1,1000);
  UPDATE p SET d = tdigest_add(d, vals);
END $$;
```

Alternatively, it's possible to use pre-aggregated t-digest values instead
of the arrays:

```
WITH batch AS (
    SELECT tdigest(random(), 100) AS d FROM generate_series(1,1000)
)
UPDATE p SET d = tdigest_union(p.d, batch.d) FROM batch;
```

It may be undesirable to perform compaction after every incremental update,
especially when adding values one by one. Setting `compact` to `false` skips
compaction at the end of the call; compaction still occurs when adding to a
full centroid buffer. The result may be unsorted and larger than a compacted
digest, but remains subject to the `10 * compression` centroid limit.

Use the multi-value functions with compaction after each batch when possible,
or compact a stored digest by re-aggregating it:

```
UPDATE p SET d = (SELECT tdigest(x) FROM (SELECT p.d) s(x));
```

Adding a `NULL` value or a `NULL` array with `tdigest_add` returns the original
digest unchanged. A non-`NULL` value or array with a `NULL` digest instead
creates a new digest and requires a compression value.

When either input to `tdigest_union` is `NULL`, it returns the other digest
unchanged, without compaction; two `NULL` digests produce `NULL`. In all
incremental functions, the `compact` flag itself must not be `NULL`, even
for calls that otherwise do nothing.


## Trimmed aggregates

The extension provides aggregate functions allowing to calculate trimmed
(truncated) sum and average, either directly from the values or from a
pre-computed digest:

* `tdigest_sum(value double precision, compression int,
               low double precision, high double precision)`

* `tdigest_sum(value double precision, count bigint, compression int,
               low double precision, high double precision)`

* `tdigest_sum(digest tdigest, low double precision, high double precision)`

* `tdigest_avg(value double precision, compression int,
               low double precision, high double precision)`

* `tdigest_avg(value double precision, count bigint, compression int,
               low double precision, high double precision)`

* `tdigest_avg(digest tdigest, low double precision, high double precision)`

The `low` and `high` parameters specify where to truncate the data. They are
percentiles (not values), so both have to be in `[0.0, 1.0]` with
`low <= high`, otherwise an error is raised. For example `low = 0.1` and
`high = 0.9` means the lowest and highest 10% of the values are discarded.

There are also two non-aggregate functions, calculating the trimmed sum and
average for a single `tdigest` value:

* `tdigest_digest_sum(digest tdigest, low double precision DEFAULT 0.0,
                      high double precision DEFAULT 1.0)`

* `tdigest_digest_avg(digest tdigest, low double precision DEFAULT 0.0,
                      high double precision DEFAULT 1.0)`

The difference between `tdigest_sum(digest, low, high)` and
`tdigest_digest_sum(digest, low, high)` is that the former is an aggregate
(combining all the digests in a group first), while the latter is a plain
function processing a single digest value (and thus may be combined with
other columns without a `GROUP BY` clause).


## Functions

The following list covers the aggregates and utility functions provided
by this extension. Type I/O functions and internal aggregate support
functions are not listed. The `accuracy` parameter in these descriptions
is the compression used when building the t-digest, as described in the
[Accuracy](#accuracy) section.

The `tdigest`, `tdigest_percentile`, `tdigest_percentile_of`, `tdigest_avg`
and `tdigest_sum` functions are aggregates (all of them parallel safe), while
`tdigest_count`, `tdigest_add`, `tdigest_union`, `tdigest_json`,
`tdigest_double_array`, `tdigest_digest_sum`, `tdigest_digest_avg` and
`tdigest_is_valid` are plain functions operating on `tdigest` values.

The examples use a table `t` with the values in column `c`, and - for the
variants with a `count` parameter - the number of occurrences of each value
in column `a`. Non-`NULL` counts must be positive; a `NULL` count means one
occurrence.

### Common argument rules

Aggregates ignore `NULL` input values or digests and return SQL `NULL` when
there are no non-`NULL` inputs. This also applies to the array-returning
aggregates: empty input produces `NULL`, not an empty array. Values added to
a digest must be finite; `NaN` and positive or negative infinity are rejected.

Compression, requested percentiles or hypothetical values, and trim
thresholds must be non-`NULL` when the aggregate state is initialized. Keep
these arguments constant within each group. The implementation captures
them on the first non-`NULL` input of each state, rather than checking them
on every row; later changes are ignored and can give order-dependent results,
especially in parallel queries. The input value and its count may vary
between rows.

Requested percentiles must be in `[0, 1]`. Arrays of percentiles or
hypothetical values must be nonempty, one-dimensional, and contain no `NULL`
elements. Array results follow the order of the requested elements and have
the usual lower bound of 1, regardless of the input array's lower bound.

All `tdigest_percentile_of` variants estimate a smoothed relative rank,
counting half of an equal-mean centroid group's weight at that mean. This
is not an exact count of smaller values. Hypothetical values may be
non-finite: `-Infinity`, `Infinity` and `NaN` return 0, 1 and `NaN`,
respectively, when the aggregate has non-`NULL` input.

The non-incremental scalar functions return `NULL` if any argument is `NULL`.
The incremental functions have the initialization and no-op rules described
in [Incremental updates](#incremental-updates).

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

Computes relative ranks of hypothetical values, using a t-digest with
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

Computes relative ranks of hypothetical values, using a t-digest with
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


### `tdigest(digest)`

Merges pre-computed t-digests into a single t-digest. This is also the way
to force compaction of a digest built with `compact = false`.

#### Synopsis

```
SELECT tdigest(d) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t GROUP BY t.a
) foo
```

#### Parameters

- `digest` - t-digests to merge


### `tdigest_count(tdigest)`

Returns the number of items represented by the t-digest. This is a plain
function, not an aggregate.

#### Synopsis

```
SELECT tdigest_count(d) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo
```

#### Parameters

- `tdigest` - t-digest to inspect


### `tdigest_percentile(tdigest, percentile)`

Computes requested percentile from the pre-computed t-digests.

#### Synopsis

```
SELECT tdigest_percentile(d, 0.99) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
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
    SELECT tdigest(t.c, 100) AS d FROM t
) foo
```

#### Parameters

- `tdigest` - t-digest to aggregate and process
- `percentile` - values in [0, 1] specifying the percentiles


### `tdigest_percentile_of(tdigest, hypothetical_value)`

Estimates the relative rank of a hypothetical value using a pre-computed
t-digest.

At an exact centroid mean, half of the total weight of all centroids with
that mean is counted. A digest containing only copies of one value therefore
returns `0.5` at that value, not the fraction of rows strictly below it
(`0.0`). This is a smoothed rank estimate, not an exact count of smaller
values.

#### Synopsis

```
SELECT tdigest_percentile_of(d, 349834.1) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo
```

#### Parameters

- `tdigest` - t-digest to aggregate and process
- `hypothetical_value` - hypothetical value


### `tdigest_percentile_of(tdigest, hypothetical_value[])`

Estimates relative ranks of hypothetical values using a pre-computed
t-digest, with the same half-weight convention at centroid means as the
scalar form.

#### Synopsis

```
SELECT tdigest_percentile_of(d, ARRAY[438.256, 349834.1]) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo
```

#### Parameters

- `tdigest` - t-digest to aggregate and process
- `hypothetical_value` - hypothetical values


### `tdigest_add(tdigest, double precision, compression = NULL, compact = true)`

Performs incremental update of the t-digest by adding a single value.

#### Synopsis

```
UPDATE p SET d = tdigest_add(d, random());
```

#### Parameters

- `tdigest` - t-digest to update (may be `NULL`)
- `element` - value to add; `NULL` leaves the digest unchanged
- `compression` - required to initialize a digest from a non-`NULL` value;
  ignored for an existing digest (default: `NULL`)
- `compact` - compact at the end of the call (default: true; must not be `NULL`)


### `tdigest_add(tdigest, double precision[], compression = NULL, compact = true)`

Performs incremental update of the t-digest by adding values from an array.

#### Synopsis

```
UPDATE p SET d = tdigest_add(d, ARRAY[random(), random(), random()]);
```

#### Parameters

- `tdigest` - t-digest to update (may be `NULL`)
- `elements` - nonempty, one-dimensional array of non-`NULL` values;
  a `NULL` array leaves the digest unchanged
- `compression` - required to initialize a digest from a non-`NULL` array;
  ignored for an existing digest (default: `NULL`)
- `compact` - compact at the end of the call (default: true; must not be `NULL`)


### `tdigest_union(tdigest, tdigest, compact = true)`

Performs incremental update of the t-digest by merging-in another digest.
When either of the digests is `NULL`, the other one is returned unchanged
(without compaction). When both are non-`NULL`, the result uses the
compression of `digest1`, even if `digest2` has a different compression.

#### Synopsis

```
WITH x AS (SELECT tdigest(random(), 100) AS d FROM generate_series(1,1000))
UPDATE p SET d = tdigest_union(p.d, x.d) FROM x;
```

#### Parameters

- `digest1` - t-digest to update
- `digest2` - t-digest to merge into `digest1`
- `compact` - compact at the end of the call (default: true; must not be `NULL`)


### `tdigest_json(tdigest)`

Returns the t-digest as a JSON value. The function is also exposed as a
cast from `tdigest` to `json`.

The document has the flags, the total number of items (`count`), the
compression and the number of centroids, followed by the per-centroid
`means` and `counts` arrays.

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
exposed as a cast from `tdigest` to `double precision[]`. The array contains
the flags, the total number of items, the compression and the number of
centroids, followed by a `(mean, count)` pair for each centroid.

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


### `tdigest_avg(value, accuracy, low, high)`

Computes trimmed mean of values, discarding values at the low and high end.
The `low` and `high` values are percentiles in [0, 1] (with `low <= high`)
specifying which part of the sample should be included in the mean, so e.g.
`low = 0.1` and `high = 0.9` means 10% low and high values will be
discarded.

#### Synopsis

```
SELECT tdigest_avg(t.c, 100, 0.1, 0.9) FROM t
```

#### Parameters

- `value` - values to aggregate
- `accuracy` - accuracy of the t-digest
- `low` - low threshold percentile (values below are discarded)
- `high` - high threshold percentile (values above are discarded)


### `tdigest_avg(value, count, accuracy, low, high)`

Computes trimmed mean of values, discarding values at the low and high end.
The `low` and `high` values are percentiles in [0, 1] (with `low <= high`)
specifying which part of the sample should be included in the mean, so e.g.
`low = 0.1` and `high = 0.9` means 10% low and high values will be
discarded.

#### Synopsis

```
SELECT tdigest_avg(t.c, t.a, 100, 0.1, 0.9) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences of the value
- `accuracy` - accuracy of the t-digest
- `low` - low threshold percentile (values below are discarded)
- `high` - high threshold percentile (values above are discarded)


### `tdigest_avg(tdigest, low, high)`

Computes trimmed mean of values, discarding values at the low and high end.
The `low` and `high` values are percentiles in [0, 1] (with `low <= high`)
specifying which part of the sample should be included in the mean, so e.g.
`low = 0.1` and `high = 0.9` means 10% low and high values will be
discarded.

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
The `low` and `high` values are percentiles in [0, 1] (with `low <= high`)
specifying which part of the sample should be included in the sum, so e.g.
`low = 0.1` and `high = 0.9` means 10% low and high values will be
discarded.

#### Synopsis

```
SELECT tdigest_sum(t.c, 100, 0.1, 0.9) FROM t
```

#### Parameters

- `value` - values to aggregate
- `accuracy` - accuracy of the t-digest
- `low` - low threshold percentile (values below are discarded)
- `high` - high threshold percentile (values above are discarded)


### `tdigest_sum(value, count, accuracy, low, high)`

Computes trimmed sum of values, discarding values at the low and high end.
The `low` and `high` values are percentiles in [0, 1] (with `low <= high`)
specifying which part of the sample should be included in the sum, so e.g.
`low = 0.1` and `high = 0.9` means 10% low and high values will be
discarded.

#### Synopsis

```
SELECT tdigest_sum(t.c, t.a, 100, 0.1, 0.9) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences of the value
- `accuracy` - accuracy of the t-digest
- `low` - low threshold percentile (values below are discarded)
- `high` - high threshold percentile (values above are discarded)


### `tdigest_sum(tdigest, low, high)`

Computes trimmed sum of values, discarding values at the low and high end.
The `low` and `high` values are percentiles in [0, 1] (with `low <= high`)
specifying which part of the sample should be included in the sum, so e.g.
`low = 0.1` and `high = 0.9` means 10% low and high values will be
discarded.

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


### `tdigest_digest_avg(tdigest, low, high)`

Calculates trimmed mean for a single t-digest value. Unlike `tdigest_avg`,
this is a plain function, not an aggregate.

#### Synopsis

```
SELECT tdigest_digest_avg(d, 0.25, 0.75) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `tdigest` - t-digest to calculate the mean for
- `low` - low threshold percentile (values below are discarded, default: 0.0)
- `high` - high threshold percentile (values above are discarded, default: 1.0)


### `tdigest_digest_sum(tdigest, low, high)`

Calculates trimmed sum for a single t-digest value. Unlike `tdigest_sum`,
this is a plain function, not an aggregate.

#### Synopsis

```
SELECT tdigest_digest_sum(d, 0.25, 0.75) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `tdigest` - t-digest to calculate the sum for
- `low` - low threshold percentile (values below are discarded, default: 0.0)
- `high` - high threshold percentile (values above are discarded, default: 1.0)


### `tdigest_is_valid(tdigest)`

Checks whether the t-digest is valid, i.e. that it passes the same sanity checks
as the input functions (parsing the text or binary representation). Returns
`true` for valid digests, `false` otherwise.

Digests produced by the extension are always valid, and it's not possible
to construct an invalid one through the input functions. But digests stored
by older versions of the extension (which did not have all the checks) may
be broken in various ways, and the values are not re-validated when read
back. This function makes it possible to find such digests.

#### Synopsis

```
SELECT a, b FROM p WHERE NOT tdigest_is_valid(p.d);
```

#### Parameters

- `tdigest` - t-digest to check


Notes
-----

Input values and centroid means use `double precision`. PostgreSQL can
convert other numeric types to it, as in the integer-valued examples, but
those conversions can lose precision. The digest does not retain the native
precision of `bigint` or `numeric` inputs.

The estimates do depend on the order of incoming data, and so may differ
between runs. This applies especially to parallel queries, for which the
workers generally see different subsets of data for each run (and build
different digests, which are then combined together).


Known issues
------------

## incorrect alignment

The SQL data type is defined without specifying the `ALIGNMENT` parameter,
so it uses the default 4-byte alignment. Its C representation contains
`double` and `int64` fields that can require 8-byte alignment. Accessing
misaligned fields may incur a performance penalty on amd64/arm64 and can
cause `SIGBUS` crashes on platforms with strict alignment requirements.

The implementation handles this in `tdigest_detoast()` by making an aligned
copy when necessary. Detoasting out-of-line values, compressed values or
values with a short varlena header already produces an aligned allocation.
Inline values with a 4-byte header need no copy during ordinary detoasting,
so they may need the additional alignment copy.

Whether a digest stays inline depends on its actual centroid count, the
other columns in the tuple, and the column's storage settings. There is no
compression-parameter threshold that guarantees out-of-line storage, and
the default `EXTERNAL` policy does not permit TOAST compression.

The extra copy requires an allocation and a single `memcpy()` of the digest.
For small inline values, this overhead is usually modest.

The SQL data type retains its original 4-byte alignment for compatibility
with existing on-disk values.


## FINALFUNC_MODIFY = READ_ONLY

The final functions mutate the aggregate state (they sort it, and most of
them also compact it), which means `FINALFUNC_MODIFY` should not be
`READ_ONLY`. It is, though, because that's what the aggregates were created
with, and changing it would break upgrades of existing installations.

Instead, each final function that would damage the state checks
`AggStateIsShared()`, and works on a copy when the state may be needed
again - that is, when the aggregate is used as a window function, or when
several aggregates share a single transition state. So the results are
correct in those cases, at the cost of copying the state.

The two exceptions are the final functions of the trimmed `tdigest_sum()`
and `tdigest_avg()` aggregates, which only sort the state. Sorting is just
a permutation of the centroids, and the state is sorted anyway before it's
used, so there's nothing to protect and no copy is made.


## fused multiply-add (FMA)

Various places in the code use expressions of the form `a * b + c` (e.g.
when calculating the mean of two merged centroids, or when interpolating
between two centroid means). Compilers are allowed to contract such
expressions into a single fused multiply-add (FMA) instruction, which
rounds only once, and so produces slightly different results than a
separate multiplication and addition.

Whether that happens depends on the platform and on the compiler flags.
FMA is part of the baseline instruction set on aarch64, so `gcc` contracts
by default there (at `-O2` and higher - the contraction happens in a pass
enabled only by `-O2`), while on x86-64 it does not, because FMA requires
`-mfma` or a sufficiently recent `-march`. The results then differ in the
last couple of digits, and the regression tests - which compare the exact
float8 output - fail.

For now, the `Makefile` builds with `-ffp-contract=off`, if the compiler
understands the option, so that the results do not depend on which
instructions happen to be available. Compilers spelling the option
differently (or not having it at all) may still produce digests that
differ in the last digit or two.

This is merely a workaround to make the tests pass. Ideally, we want to
allow FMA, because it's expected to be faster and give more precise
results (thanks to a single rounding).


License
-------
This software is distributed under the terms of the PostgreSQL license.
See LICENSE or https://www.postgresql.org/about/licence/ for
more details.


[1] https://github.com/tdunning/t-digest

[2] https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf

[3] https://github.com/ajwerner/tdigestc

[4] https://github.com/ajwerner/tdigest
