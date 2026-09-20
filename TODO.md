TODO
====

This is a simple list of possible improvements and enhancements, in mostly
random order. So if you're thinking about contributing to the extension,
this might provide inspiration. Of course, if you can think of yet another
improvement, add it to this list.

* Support other data types, not just "double precision". Supporting "numeric"
  seems natural, maybe we could support integer types too (possibly with
  rounding of the interpolated value).

* Explore adding a "discrete" variant, similar to percentile_disc. I'm not
  sure this is actually possible, considering we're not keeping all the
  source data, forcing us to interpolate.

* Maybe tdigest_compute_quantiles/tdigest_compute_quantiles_of should sum
  the counts of centroids matching the prev/next mean, and use that for
  the interpolation? See the comment at tdigest_compute_quantiles_of for
  more details.

* When calculating trimmed aggregates, the code should interpolate between
  centroids just like quantiles/quantiles-of, instead of assuming all the
  items are located in the mean. See the comment at tdigest_trimmed_agg for
  additional details.
