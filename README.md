t-digest extension
==================

This PostgreSQL extension implements t-digest, a data structure for on-line
accumulation of rank-based statistics such as quantiles and trimmed means.
The algorithm is also very friendly to parallel programs.

The t-digest data structure was introduced by Ted Dunning in 2013, and more
detailed description and example implementation is available in his github
repository [1]. In particular, see the paper [2] explaining the idea. Some
of the code was inspired by tdigestc [3] available on github.

The accuracy of estimates produced by t-digests can be orders of magnitude
more accurate than those produced by previous digest algorithms in spite of
the fact that t-digests are much more compact when stored on disk.


Basic usage
-----------

The extension provides two functions, which you can see as a replacement of
`percentile_cont` aggregate:

* `tdigest_percentile(value double precision, compression int,
                      quantile double precision)`

* `tdigest_percentile(value double precision, compression int,
                      quantiles double precision[])`

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


Advanced usage
--------------

The extension also provides a `tdigest` data type, which makes it possible
to precompute digests for subsets of data, and then quickly combine those
"partial" digest into a digest representing the whole data set. The prebuilt
digests should be much smaller compared to the original data set, allowing
significantly faster response times.

To compute the `t-digest` use `tdigest` aggregate function. The digests can
then be stored on disk and later summarized using the `tdigest_percentile`
functions (with `tdigest` as the first argument).

* `tdigest(value double precision, compression int)`

* `tdigest_percentile(digest tdigest, compression int,
                      quantile double precision)`

* `tdigest_percentile(digest tdigest, compression int,
                      quantiles double precision[])`

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
