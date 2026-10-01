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

For the basic use case the extension provides an aggregate function building
the `tdigest` sketch from source data

* `tdigest(value double precision, compression int) -> tdigest`

And then several functions processing the digests. The `tdigest_percentile`
ones can be seen as a replacement of the `percentile_cont` aggregate, while
the `tdigest_percentile_of` ones perform the inverse operation,
estimating the relative rank of a given value:

* `tdigest_percentile(digest tdigest, percentile double precision) -> double precision`

* `tdigest_percentile(digest tdigest, percentile double precision[]) -> double precision[]`

* `tdigest_percentile_of(digest tdigest, value double precision) -> double precision`

* `tdigest_percentile_of(digest tdigest, values double precision[]) -> double precision[]`

That is, instead of running

```sql
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY a) FROM t
```

you might now run

```sql
SELECT tdigest_percentile(tdigest(a, 100), 0.95) FROM t
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

Since 2.0.0 the type uses PostgreSQL's `extended` storage policy by default
on PostgreSQL 13 and later, so large values can be compressed and, if that
is not enough, stored out of line using TOAST. On PostgreSQL 11 and 12 the
type keeps the `external` policy it always had, which stores large values
out of line without compressing them.

Either way a column can override the setting with

```sql
ALTER TABLE ... ALTER COLUMN ... SET STORAGE
```

and changing it does not itself rewrite existing values - see
[Upgrading to 2.0.0](#upgrading-to-200). Tuple and TOAST overhead are
additional to the size of the digest.

The results are estimates computed from the centroids the digest happens to
hold. That is worth spelling out because it changed in 2.0.0 for the trimmed
functions: `tdigest_sum` and `tdigest_avg` used to be aggregates computing
from the raw aggregate state, which buffers up to ten times `compression`
values before compacting. Setting `compression` above the number of input
rows therefore meant no compaction ever happened and the trimmed results came
out exact. Building the digest is a separate step now, and the `tdigest()`
aggregate compacts before returning, so `tdigest_sum(tdigest(v, 100), ...)`
sees a compacted digest and returns an estimate like every other function.
Queries relying on the old behaviour will see their results shift.

Note this is a property of how the digest was built, not of the trimmed
functions - those summarize the centroids the digest happens to have. A
digest built through the incremental API with `p_compact := false` reaches
them uncompacted, and the fewer compactions happened while building it, the
closer to exact the trimmed results are. The percentile functions do compact
the digest they are given, so those always return estimates matching the
compression level. See [Incremental updates](#incremental-updates).

Here is a table of sizes for digests with different compression values,
built on random data:

| compression   | centroids | length (B) | external (B) | extended (B) |
|--------------:|----------:|-----------:|-------------:|-------------:|
|            10 |        18 |        305 |          309 |          308 |
|            50 |        40 |        655 |          659 |          658 |
|           100 |        61 |        993 |          997 |          997 |
|           200 |       100 |       1616 |         1620 |         1624 |
|           500 |       203 |       3275 |         3275 |         2238 |
|          1000 |       357 |       5732 |         5732 |         3765 |
|          2000 |       627 |      10058 |        10058 |         6432 |
|          5000 |      1318 |      21113 |        21113 |        12646 |
|         10000 |      2265 |      36260 |        36260 |        20177 |

Where `centroids` is the number of centroids in a compacted digest, `length`
is the "raw" size of the digest. `external` and `extended` are the on-disk
sizes of centroid, depending on the storage policy set for the column. It's
clear that `external` is almost the same as `length`, while `extended` is
often much smaller thanks to compression.

This is merely an example - the actual values depend on the data. For example
digests on integer values tend to be much more compressible, cutting the
`extended` size about in half.


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

* `tdigest_percentile(p_digest tdigest,
                      p_percentile double precision)`

* `tdigest_percentile(p_digest tdigest,
                      p_percentiles double precision[])`

* `tdigest_percentile_of(p_digest tdigest,
                         p_value double precision)`

* `tdigest_percentile_of(p_digest tdigest,
                         p_values double precision[])`

The `tdigest(digest tdigest)` variant is an aggregate merging multiple
pre-computed digests into a single digest, which can be stored again.

Digest-input aggregates accept digests with different compression settings.
Each aggregate state takes its compression from its first non-`NULL` digest;
with parallel aggregation, the final choice can depend on worker and combine
order. Use a consistent compression across input digests when that choice
matters. Merging cannot recover detail already lost by compaction.

So for example you may do this:

```sql
-- table with some random source data, with "a" usable as a count of
-- occurrences (so it has to be positive)
CREATE TABLE t (a int, b int, c double precision);

INSERT INTO t SELECT 1 + 10 * random(), 10 * random(), random()
                FROM generate_series(1,10000000);

-- table with pre-aggregated digests
CREATE TABLE p AS SELECT a, b, tdigest(c, 100) AS d FROM t GROUP BY a, b;

-- summarize the data from "p" (compute the 95th percentile)
SELECT a, tdigest_percentile(tdigest(d), 0.95) FROM p GROUP BY a ORDER BY a;
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

```sql
\timing on

-- exact results
SELECT a, percentile_cont(0.95) WITHIN GROUP (ORDER BY c)
  FROM t GROUP BY a ORDER BY a;
  ...
Time: 6956.566 ms (00:06.957)

-- tdigest estimate (no parallelism)
SET max_parallel_workers_per_gather = 0;
SELECT a, tdigest_percentile(tdigest(c, 100), 0.95) FROM t GROUP BY a ORDER BY a;
  ...
Time: 2873.116 ms (00:02.873)

-- tdigest estimate (4 workers)
SET max_parallel_workers_per_gather = 4;
SELECT a, tdigest_percentile(tdigest(c, 100), 0.95) FROM t GROUP BY a ORDER BY a;
  ...
Time: 893.538 ms
```

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
and use an aggregate function that allows specifying the number of
occurrences for each value. This reduces the number of SQL-function calls.

* `tdigest(value double precision, count bigint, compression int)`

For a non-NULL input value, `count` determines how many times the value is
added to the digest. A supplied count must be positive; a NULL count means
one occurrence. NULL input values are skipped.


## Incremental updates

An existing t-digest may be updated incrementally, either by adding a single
value, or by merging-in a whole t-digest. The following examples use the
table `p` with the pre-aggregated digests (in column `d`), built in
[Advanced usage](#advanced-usage). Each example adds the same new values to
every row of `p`; use a `WHERE` clause when updating only selected groups.

For example, it's possible to add 1000 random values to the t-digests like
this:

```sql
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

```sql
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

```sql
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

```sql
UPDATE p SET d = (SELECT tdigest(x) FROM (SELECT p.d) s(x));
```

Adding a `NULL` value or a `NULL` array with `tdigest_add` returns the original
digest unchanged. A non-`NULL` value or array with a `NULL` digest instead
creates a new digest and requires a compression value.

When either input to `tdigest_union` is `NULL`, it returns the other digest
unchanged, without compaction; two `NULL` digests produce `NULL`. In all
incremental functions, the `compact` flag itself must not be `NULL`, even
for calls that otherwise do nothing.

The functions taking a digest name their arguments with a `p_` prefix, so
they may also be called using named arguments. (The `tdigest()` aggregates
are the exception - their arguments have no names and have to be passed
positionally.) To start a new digest while postponing final compaction, for
example:

```sql
SELECT tdigest_add(NULL::tdigest, 42.0,
                   p_compression => 100, p_compact => false);
```

`tdigest_union` returns the other digest unchanged when one is NULL, so
`tdigest_union(NULL, d)` does *not* compact it. `tdigest_add(d, NULL)` likewise
returns `d` unchanged. In contrast, `tdigest_add(NULL, value, compression)`
creates a new digest for a non-NULL value and requires a compression value.


## Trimmed statistics

The extension provides functions allowing to calculate trimmed (truncated)
sum and average, from a digest:

* `tdigest_sum(p_digest tdigest, p_low double precision, p_high double precision)`

* `tdigest_avg(p_digest tdigest, p_low double precision, p_high double precision)`

The `p_low` and `p_high` parameters specify where to truncate the data. They
are percentiles (not values), so both have to be in `[0.0, 1.0]` with
`p_low <= p_high`, otherwise an error is raised. For example `p_low = 0.1`
and `p_high = 0.9` means the lowest and highest 10% of the values are
discarded. The thresholds are optional, defaulting to `p_low = 0.0` and
`p_high = 1.0`.


## Functions

The following list covers the aggregates and utility functions provided
by this extension. Type I/O functions and internal aggregate support
functions are not listed. The `accuracy` parameter in these descriptions
is the compression used when building the t-digest, as described in the
[Accuracy](#accuracy) section.

The `tdigest` is an aggregate (a parallel safe one), while `tdigest_percentile`,
`tdigest_percentile_of`, `tdigest_avg`, `tdigest_sum`, `tdigest_count`,
`tdigest_add`, `tdigest_union`, `tdigest_json`, `tdigest_double_array` and
`tdigest_is_valid` are plain functions operating on a single `tdigest` value.
All of them are parallel safe.

The examples use a table `t` with the values in column `c`, and - for the
variants with a `count` parameter - the number of occurrences of each value
in column `a`. Supplied counts must be positive; NULL counts mean one
occurrence. The incremental-update examples use the digest column `p.d`
from [Advanced usage](#advanced-usage).


### `tdigest(value, accuracy)`

Computes t-digest with the specified accuracy.

#### Synopsis

```sql
SELECT tdigest(t.c, 100) FROM t
```

#### Parameters

- `value` - values to aggregate
- `accuracy` - accuracy of the t-digest


### `tdigest(value, count, accuracy)`

Computes t-digest with the specified accuracy. The values are added with
as many occurrences as determined by the count parameter.

#### Synopsis

```sql
SELECT tdigest(t.c, t.a, 100) FROM t
```

#### Parameters

- `value` - values to aggregate
- `count` - number of occurrences for each value (NULL means one)
- `accuracy` - accuracy of the t-digest


### `tdigest(digest)`

Merges pre-computed t-digests into a single t-digest. This is also the way
to force compaction of a digest built with `p_compact = false`.

#### Synopsis

```sql
SELECT tdigest(d) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t GROUP BY t.a
) foo
```

#### Parameters

- `digest` - t-digests to merge


### `tdigest_count(p_digest tdigest)`

Returns the number of items represented by the t-digest. This is a plain
function, not an aggregate.

#### Synopsis

```sql
SELECT tdigest_count(d) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo
```

#### Parameters

- `p_digest` - t-digest to inspect


### `tdigest_percentile(p_digest tdigest, p_percentile double precision)`

Computes the requested percentile from a pre-computed t-digest.

#### Synopsis

```sql
SELECT tdigest_percentile(d, 0.99) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo
```

#### Parameters

- `p_digest` - t-digest to process
- `p_percentile` - value in [0, 1] specifying the percentile


### `tdigest_percentile(p_digest tdigest, p_percentiles double precision[])`

Computes the requested percentiles from a pre-computed t-digest.

#### Synopsis

```sql
SELECT tdigest_percentile(d, ARRAY[0.95, 0.99]) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo
```

#### Parameters

- `p_digest` - t-digest to process
- `p_percentiles` - values in [0, 1] specifying the percentiles


### `tdigest_percentile_of(p_digest tdigest, p_value double precision)`

Estimates the relative rank of a hypothetical value using a pre-computed
t-digest.

At an exact centroid mean, half of the total weight of all centroids with
that mean is counted. A digest containing only copies of one value therefore
returns `0.5` at that value, not the fraction of rows strictly below it
(`0.0`). This is a smoothed rank estimate, not an exact count of smaller
values.

#### Synopsis

```sql
SELECT tdigest_percentile_of(d, 349834.1) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo
```

#### Parameters

- `p_digest` - t-digest to process
- `p_value` - hypothetical value


### `tdigest_percentile_of(p_digest tdigest, p_values double precision[])`

Estimates relative ranks of hypothetical values using a pre-computed
t-digest, with the same half-weight convention at centroid means as the
scalar form.

#### Synopsis

```sql
SELECT tdigest_percentile_of(d, ARRAY[438.256, 349834.1]) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo
```

#### Parameters

- `p_digest` - t-digest to process
- `p_values` - hypothetical values


### `tdigest_add(p_digest tdigest, p_element double precision, p_compression int, p_compact bool)`

Performs incremental update of the t-digest by adding a single value.

#### Synopsis

```sql
UPDATE p SET d = tdigest_add(d, random());
```

#### Parameters

- `p_digest` - t-digest to update (may be `NULL`)
- `p_element` - value to add; `NULL` leaves the digest unchanged
- `p_compression` - required to initialize a digest from a non-`NULL` value;
  ignored for an existing digest (default: `NULL`)
- `p_compact` - compact at the end of the call (default: true; must not be `NULL`)


### `tdigest_add(p_digest tdigest, p_elements double precision[], p_compression int, p_compact bool)`

Performs incremental update of the t-digest by adding values from an array.

#### Synopsis

```sql
UPDATE p SET d = tdigest_add(d, ARRAY[random(), random(), random()]);
```

#### Parameters

- `p_digest` - t-digest to update (may be `NULL`)
- `p_elements` - nonempty, one-dimensional array of non-`NULL` values;
  a `NULL` array leaves the digest unchanged
- `p_compression` - required to initialize a digest from a non-`NULL` array;
  ignored for an existing digest (default: `NULL`)
- `p_compact` - compact at the end of the call (default: true; must not be `NULL`)


### `tdigest_union(p_digest1 tdigest, p_digest2 tdigest, p_compact bool)`

Performs incremental update of the t-digest by merging-in another digest.
When either of the digests is `NULL`, the other one is returned unchanged
(without compaction). When both are non-`NULL`, the result uses the
compression of `digest1`, even if `digest2` has a different compression.

#### Synopsis

```sql
WITH x AS (SELECT tdigest(random(), 100) AS d FROM generate_series(1,1000))
UPDATE p SET d = tdigest_union(p.d, x.d) FROM x;
```

#### Parameters

- `p_digest1` - t-digest to update
- `p_digest2` - t-digest to merge into `digest1`
- `p_compact` - compact at the end of the call (default: true; must not be `NULL`)


### `tdigest_json(p_digest tdigest)`

Returns the t-digest as a JSON value. The function is also exposed as a
cast from `tdigest` to `json`.

The document has the flags, the total number of items (`count`), the
compression and the number of centroids, followed by the per-centroid
`means` and `counts` arrays.

#### Synopsis

```sql
SELECT tdigest_json(d) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;

SELECT CAST(d AS json) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `p_digest` - t-digest to cast to a `json` value


### `tdigest_double_array(p_digest tdigest) -> double precision[]`

Returns the t-digest as a `double precision[]` array. The function is also
exposed as a cast from `tdigest` to `double precision[]`. The array contains
the flags, the total number of items, the compression and the number of
centroids, followed by a `(mean, count)` pair for each centroid.

#### Synopsis

```sql
SELECT tdigest_double_array(d) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;

SELECT CAST(d AS double precision[]) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `p_digest` - t-digest to cast to a `double precision[]` value


### `tdigest_avg(p_digest tdigest, p_low double precision, p_high double precision)`

Computes trimmed mean of values, discarding values at the low and high end.
The `p_low` and `p_high` values are percentiles in [0, 1] (with
`p_low <= p_high`) specifying which part of the sample should be included in
the mean, so e.g. `p_low = 0.1` and `p_high = 0.9` means 10% low and high
values will be discarded.

#### Synopsis

```sql
SELECT tdigest_avg(d, 0.05, 0.95) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `p_digest` - t-digest to calculate mean from
- `p_low` - low threshold percentile (default: 0.0)
- `p_high` - high threshold percentile (default: 1.0)


### `tdigest_sum(p_digest tdigest, p_low double precision, p_high double precision)`

Computes trimmed sum of values, discarding values at the low and high end.
The `p_low` and `p_high` values are percentiles in [0, 1] (with
`p_low <= p_high`) specifying which part of the sample should be included in
the sum, so e.g. `p_low = 0.1` and `p_high = 0.9` means 10% low and high
values will be discarded.

#### Synopsis

```sql
SELECT tdigest_sum(d, 0.05, 0.95) FROM (
    SELECT tdigest(t.c, 100) AS d FROM t
) foo;
```

#### Parameters

- `p_digest` - t-digest to calculate sum from
- `p_low` - low threshold percentile (default: 0.0)
- `p_high` - high threshold percentile (default: 1.0)


### `tdigest_is_valid(p_digest tdigest)`

Checks whether the t-digest is valid, i.e. that it passes the same sanity checks
as the input functions (parsing the text or binary representation). Returns
`true` for valid digests, `false` otherwise.

Digests produced by the extension are always valid, and it's not possible
to construct an invalid one through the input functions. But digests stored
by older versions of the extension (which did not have all the checks) may
be broken in various ways, and the values are not re-validated when read
back. This function makes it possible to find such digests.

#### Synopsis

```sql
SELECT a, b FROM p WHERE NOT tdigest_is_valid(p.d);
```

#### Parameters

- `p_digest` - t-digest to check


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


Security
--------

If you believe you have found a security vulnerability in this repository,
please report [this form](https://github.com/tvondra/tdigest/security/advisories/new)
of this GitHub project. This creates a private communication channel
between the reporter and the maintainers.

If you are absolutely unable to or have strong reasons not to use GitHub's
vulnerability reporting workflow, please reach out to the maintainer at
[tomas@vondra.me](mailto:tomas@vondra.me).

Notes:

* The code assumes digests stored on-disk are valid and not corrupted.
  If the suspected vulnerability requires a corrupted digest, without a way
  to create such digests (using the current version), it's not a security
  issue. This is in line with general assumptions in the Postgres code.

* A valid vulnerability must not require superuser privileges. A superuser
  can do almost anything (ultimately can read/write memory) and does not
  need to bother with vulnerabilities.


Known issues
------------

### incorrect alignment

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
compression-parameter threshold that guarantees out-of-line storage. With
the `extended` policy the type uses since 2.0.0 (on PostgreSQL 13 and
later), a large digest is more likely to be compressed than moved out of
line, and a compressed value is detoasted into an aligned allocation just
like an out-of-line one.

The extra copy requires an allocation and a single `memcpy()` of the digest.
For small inline values, this overhead is usually modest.

The SQL data type retains its original 4-byte alignment for compatibility
with existing on-disk values.


### FINALFUNC_MODIFY = READ_ONLY

All three aggregates share a single final function, `tdigest_digest()`, and
it mutates the aggregate state - it sorts and compacts it before turning it
into a digest. That means `FINALFUNC_MODIFY` should not be `READ_ONLY`. It
is, though, because that's what the aggregates were created with, and
changing it would break upgrades of existing installations.

Instead, the final function checks `AggStateIsShared()`, and works on a copy
when the state may be needed again - that is, when the aggregate is used as
a window function, or when several aggregates share a single transition
state. So the results are correct in those cases, at the cost of copying the
state.

Before 2.0.0 there were two more aggregates, for the trimmed `tdigest_sum()`
and `tdigest_avg()`, whose final functions only sorted the state and did not
need the copy. They are plain functions taking a digest now, so they have no
aggregate state to protect - they sort a copy of the digest, like every
other function consuming one.


### fused multiply-add (FMA)

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
instructions happen to be available. The option is added to both `CFLAGS`
and `BITCODE_CFLAGS`, because the LLVM bitcode used for JIT inlining is
compiled separately and does not inherit `CFLAGS`. Compilers spelling the
option differently (or not having it at all) may still produce digests that
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

[5] https://github.com/tvondra/tdigest/security/advisories/new
