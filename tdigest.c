/*
 * tdigest.c - implementation of t-digest for PostgreSQL, useful for estimation
 * of quantiles, percentiles, trimmed means, and various similar metrics.
 *
 * Copyright (C) Tomas Vondra, 2019
 */

#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <limits.h>
#include <ctype.h>
#include <errno.h>

#include "postgres.h"
#include "common/int.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#if PG_VERSION_NUM >= 120000
#include "utils/float.h"	/* float8out_internal */
#endif

#include "catalog/pg_type.h"

PG_MODULE_MAGIC;

/*
 * A centroid, used both for in-memory and on-disk storage.
 */
typedef struct centroid_t {
	double	mean;
	int64	count;
} centroid_t;

/*
 * On-disk representation of the t-digest.
 */
typedef struct tdigest_t {
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		flags;			/* reserved for future use (versioning, ...) */
	int64		count;			/* number of items added to the t-digest */
	int			compression;	/* compression used to build the digest */
	int			ncentroids;		/* number of cetroids in the array */
	centroid_t	centroids[FLEXIBLE_ARRAY_MEMBER];
} tdigest_t;

/*
 * Centroids used to store (sum,count), but we want to store (mean,count)
 * because that allows us to prevent rounding errors e.g. when merging
 * centroids with the same mean, or adding the same value to the centroid.
 *
 * To handle existing tdigest data in backwards-compatible way, we have
 * a flag marking the new ones with mean, and we convert the old values.
 */
#define	TDIGEST_STORES_MEAN		0x0001

/* All valid flags, OR-ed. */
#define	TDIGEST_VALID_FLAGS		(TDIGEST_STORES_MEAN)

/*
 * An aggregate state, representing the t-digest and some additional info
 * (requested percentiles, ...).
 *
 * When adding new values to the t-digest, we add them as centroids into a
 * separate "uncompacted" part of the array. While centroids need more space
 * than plain points (24B vs. 8B), making the aggregate state quite a bit
 * larger, it does simplify the code quite a bit as it only needs to deal
 * with single struct type instead of two (centroids + points). But maybe
 * we should separate those two things in the future.
 *
 * XXX We only ever use one of values/percentiles, never both at the same
 * time. In the future the values may use a different data types than double
 * (e.g. numeric), so we keep both fields.
 */
typedef struct tdigest_aggstate_t {
	/* basic t-digest fields (centroids at the end) */
	int64		count;			/* number of samples in the digest */
	int			ncompactions;	/* number of merges/compactions */
	int			compression;	/* compression algorithm */
	int			ncentroids;		/* number of centroids */
	int			ncompacted;		/* compacted part */
	/* array of requested percentiles and values */
	int			npercentiles;	/* number of percentiles */
	int			nvalues;		/* number of values */
	double		trim_low;		/* low threshold (for trimmed aggs) */
	double		trim_high;		/* high threshold (for trimmed aggs) */
	double	   *percentiles;	/* array of percentiles (if any) */
	double	   *values;			/* array of values (if any) */
	centroid_t *centroids;		/* centroids for the digest */
} tdigest_aggstate_t;

static int  centroid_cmp(const void *a, const void *b);

#define PG_GETARG_TDIGEST(x)	(tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(x))

/*
 * Size of buffer for incoming data, as a multiple of the compression value.
 * Quoting from the t-digest paper:
 *
 * The constant of proportionality should be determined by experiment, but
 * micro-benchmarks indicate that C2/C1 is in the range from 5 to 20 for
 * a single core of an Intel i7 processor. In these micro-benchmarks,
 * increasing the buffer size to (10 * delta) dramatically improves the
 * average speed but further buffer size increases have much less effect.
 *
 * XXX Maybe make the coefficient user-defined, with some reasonable limits
 * (say 2 - 20), so that users can pick the right trade-off between speed
 * and memory usage.
 */
#define	BUFFER_SIZE(compression)	(10 * (compression))
#define AssertBounds(index, length) Assert((index) >= 0 && (index) < (length))

#define MIN_COMPRESSION		10
#define MAX_COMPRESSION		10000

/* prototypes */
PG_FUNCTION_INFO_V1(tdigest_add_double_array);
PG_FUNCTION_INFO_V1(tdigest_add_double_array_count);
PG_FUNCTION_INFO_V1(tdigest_add_double_array_values);
PG_FUNCTION_INFO_V1(tdigest_add_double_array_values_count);
PG_FUNCTION_INFO_V1(tdigest_add_double);
PG_FUNCTION_INFO_V1(tdigest_add_double_count);
PG_FUNCTION_INFO_V1(tdigest_add_double_values);
PG_FUNCTION_INFO_V1(tdigest_add_double_values_count);

PG_FUNCTION_INFO_V1(tdigest_add_digest_array);
PG_FUNCTION_INFO_V1(tdigest_add_digest_array_values);
PG_FUNCTION_INFO_V1(tdigest_add_digest);
PG_FUNCTION_INFO_V1(tdigest_add_digest_values);

PG_FUNCTION_INFO_V1(tdigest_array_percentiles);
PG_FUNCTION_INFO_V1(tdigest_array_percentiles_of);
PG_FUNCTION_INFO_V1(tdigest_percentiles);
PG_FUNCTION_INFO_V1(tdigest_percentiles_of);
PG_FUNCTION_INFO_V1(tdigest_digest);

PG_FUNCTION_INFO_V1(tdigest_serial);
PG_FUNCTION_INFO_V1(tdigest_deserial);
PG_FUNCTION_INFO_V1(tdigest_combine);

PG_FUNCTION_INFO_V1(tdigest_in);
PG_FUNCTION_INFO_V1(tdigest_out);
PG_FUNCTION_INFO_V1(tdigest_send);
PG_FUNCTION_INFO_V1(tdigest_recv);

PG_FUNCTION_INFO_V1(tdigest_is_valid);

PG_FUNCTION_INFO_V1(tdigest_count);
PG_FUNCTION_INFO_V1(tdigest_to_json);
PG_FUNCTION_INFO_V1(tdigest_to_array);

PG_FUNCTION_INFO_V1(tdigest_add_double_increment);
PG_FUNCTION_INFO_V1(tdigest_add_double_array_increment);
PG_FUNCTION_INFO_V1(tdigest_union_double_increment);

PG_FUNCTION_INFO_V1(tdigest_add_double_trimmed);
PG_FUNCTION_INFO_V1(tdigest_add_double_count_trimmed);
PG_FUNCTION_INFO_V1(tdigest_add_digest_trimmed);
PG_FUNCTION_INFO_V1(tdigest_trimmed_avg);
PG_FUNCTION_INFO_V1(tdigest_trimmed_sum);

PG_FUNCTION_INFO_V1(tdigest_digest_sum);
PG_FUNCTION_INFO_V1(tdigest_digest_avg);

Datum tdigest_add_double_array(PG_FUNCTION_ARGS);
Datum tdigest_add_double_array_count(PG_FUNCTION_ARGS);
Datum tdigest_add_double_array_values(PG_FUNCTION_ARGS);
Datum tdigest_add_double_array_values_count(PG_FUNCTION_ARGS);
Datum tdigest_add_double(PG_FUNCTION_ARGS);
Datum tdigest_add_double_count(PG_FUNCTION_ARGS);
Datum tdigest_add_double_values(PG_FUNCTION_ARGS);
Datum tdigest_add_double_values_count(PG_FUNCTION_ARGS);

Datum tdigest_add_digest_array(PG_FUNCTION_ARGS);
Datum tdigest_add_digest_array_values(PG_FUNCTION_ARGS);
Datum tdigest_add_digest(PG_FUNCTION_ARGS);
Datum tdigest_add_digest_values(PG_FUNCTION_ARGS);

Datum tdigest_array_percentiles(PG_FUNCTION_ARGS);
Datum tdigest_array_percentiles_of(PG_FUNCTION_ARGS);
Datum tdigest_percentiles(PG_FUNCTION_ARGS);
Datum tdigest_percentiles_of(PG_FUNCTION_ARGS);

Datum tdigest_digest(PG_FUNCTION_ARGS);

Datum tdigest_serial(PG_FUNCTION_ARGS);
Datum tdigest_deserial(PG_FUNCTION_ARGS);
Datum tdigest_combine(PG_FUNCTION_ARGS);

Datum tdigest_in(PG_FUNCTION_ARGS);
Datum tdigest_out(PG_FUNCTION_ARGS);
Datum tdigest_send(PG_FUNCTION_ARGS);
Datum tdigest_recv(PG_FUNCTION_ARGS);

Datum tdigest_is_valid(PG_FUNCTION_ARGS);

Datum tdigest_count(PG_FUNCTION_ARGS);

Datum tdigest_add_double_increment(PG_FUNCTION_ARGS);
Datum tdigest_add_double_array_increment(PG_FUNCTION_ARGS);
Datum tdigest_union_double_increment(PG_FUNCTION_ARGS);

Datum tdigest_to_json(PG_FUNCTION_ARGS);
Datum tdigest_to_array(PG_FUNCTION_ARGS);

Datum tdigest_add_double_trimmed(PG_FUNCTION_ARGS);
Datum tdigest_add_double_count_trimmed(PG_FUNCTION_ARGS);
Datum tdigest_add_digest_trimmed(PG_FUNCTION_ARGS);
Datum tdigest_trimmed_avg(PG_FUNCTION_ARGS);
Datum tdigest_trimmed_sum(PG_FUNCTION_ARGS);

Datum tdigest_digest_sum(PG_FUNCTION_ARGS);
Datum tdigest_digest_avg(PG_FUNCTION_ARGS);

static Datum double_to_array(FunctionCallInfo fcinfo, double * d, int len);
static double *array_to_double(FunctionCallInfo fcinfo, ArrayType *v, int * len);
static int64 double_to_int64(double value, int64 maxvalue);

#if PG_VERSION_NUM < 150000
/*
 * Thin wrappers that convert strings to exactly 64-bit integers, matching our
 * definition of int64.  (For the naming, compare that POSIX has
 * strtoimax()/strtoumax() which return intmax_t/uintmax_t.)
 *
 * XXX Backward compatibility
 */
#if SIZEOF_LONG == 8
#define strtoi64(str, endptr, base) ((int64) strtol(str, endptr, base))
#elif SIZEOF_LONG_LONG == 8
#define strtoi64(str, endptr, base) ((int64) strtoll(str, endptr, base))
#endif

#endif	/* PG_VERSION_NUM < 150000 */

/* basic checks on the t-digest (proper sum of counts, ...) */
static void
AssertCheckTDigest(tdigest_t *digest)
{
#ifdef USE_ASSERT_CHECKING
	int	i;
	int64	cnt;

	Assert(digest->flags == 0 || digest->flags == TDIGEST_STORES_MEAN);

	Assert((digest->compression >= MIN_COMPRESSION) &&
		   (digest->compression <= MAX_COMPRESSION));

	Assert(digest->count >= 0);

	Assert(digest->ncentroids >= 0);
	Assert(digest->ncentroids <= BUFFER_SIZE(digest->compression));

	cnt = 0;
	for (i = 0; i < digest->ncentroids; i++)
	{
		Assert(digest->centroids[i].count > 0);
		Assert(isfinite(digest->centroids[i].mean));
		cnt += digest->centroids[i].count;
		/* FIXME also check this does work with the scale function */
	}

	Assert(VARSIZE_ANY(digest) == offsetof(tdigest_t, centroids) +
		   digest->ncentroids * sizeof(centroid_t));

	Assert(digest->count == cnt);
#endif
}

static void
AssertCheckTDigestAggState(tdigest_aggstate_t *state)
{
#ifdef USE_ASSERT_CHECKING
	int	i;
	int64	cnt;

	Assert(state->npercentiles >= 0);

	Assert(((state->npercentiles == 0) && (state->percentiles == NULL)) ||
		   ((state->npercentiles > 0) && (state->percentiles != NULL)));

	for (i = 0; i < state->npercentiles; i++)
		Assert((state->percentiles[i] >= 0.0) &&
			   (state->percentiles[i] <= 1.0));

	Assert((state->compression >= MIN_COMPRESSION) &&
		   (state->compression <= MAX_COMPRESSION));

	Assert(state->count >= 0);

	Assert(state->ncentroids >= 0);
	Assert(state->ncentroids <= BUFFER_SIZE(state->compression));

	cnt = 0;
	for (i = 0; i < state->ncentroids; i++)
	{
		Assert(state->centroids[i].count > 0);
		Assert(isfinite(state->centroids[i].mean));
		cnt += state->centroids[i].count;

		/* XXX maybe check this does work with the scale function */
	}

	Assert(state->count == cnt);
#endif
}

static void
reverse_centroids(centroid_t *centroids, int ncentroids)
{
	int	start = 0,
		end = (ncentroids - 1);

	while (start < end)
	{
		centroid_t	tmp = centroids[start];
		centroids[start] = centroids[end];
		centroids[end] = tmp;

		start++;
		end--;
	}
}

static void
rebalance_centroids(centroid_t *centroids, int ncentroids,
					int64 weight_before, int64 weight_after)
{
	double	ratio = weight_before / (double) weight_after;
	int64	count_before = 0;
	int64	count_after = 0;
	int		start = 0;
	int		end = (ncentroids - 1);
	int		i;

	centroid_t *scratch = palloc(sizeof(centroid_t) * ncentroids);

	i = 0;
	while (i < ncentroids)
	{
		while (i < ncentroids)
		{
			scratch[start] = centroids[i];
			count_before += centroids[i].count;
			i++;
			start++;

			if (count_before > count_after * ratio)
				break;
		}

		while (i < ncentroids)
		{
			scratch[end] = centroids[i];
			count_after += centroids[i].count;
			i++;
			end--;

			if (count_before < count_after * ratio)
				break;
		}
	}

	memcpy(centroids, scratch, sizeof(centroid_t) * ncentroids);
	pfree(scratch);
}


/*
 * Sort an array of centroids, with the given total count.
 *
 * We have to sort the whole array, because we don't just simply sort the
 * centroids - we do the rebalancing of items with the same mean too.
 */
static void
tdigest_sort_centroids(centroid_t *centroids, int ncentroids, int64 count)
{
	int		i;
	int64	count_so_far;
	int64	next_group;
	int64	median_count;

	/* do qsort on the non-sorted part */
	pg_qsort(centroids,
			 ncentroids,
			 sizeof(centroid_t), centroid_cmp);

	/*
	 * The centroids are sorted by (mean,count). That's fine for centroids up
	 * to median, but above median this ordering is incorrect for centroids
	 * with the same mean (or for groups crossing the median boundary). To fix
	 * this we 'rebalance' those groups. Those entirely above median can be
	 * simply sorted in the opposite order, while those crossing the median
	 * need to be rebalanced depending on what part is below/above median.
	 */
	count_so_far = 0;
	next_group = 0;	/* includes count_so_far */
	median_count = (count / 2);

	/*
	 * Split the centroids into groups with the same mean, process each group
	 * depending on whether it falls before/after median.
	 */
	i = 0;
	while (i < ncentroids)
	{
		int	j = i;
		int	group_size = 0;

		/* determine the end of the group */
		while ((j < ncentroids) &&
			   (centroids[i].mean == centroids[j].mean))
		{
			next_group += centroids[j].count;
			group_size++;
			j++;
		}

		/*
		 * We can ignore groups of size 1 (number of centroids, not counts), as
		 * those are trivially sorted.
		 */
		if (group_size > 1)
		{
			if (count_so_far >= median_count)
			{
				/* group fully above median - reverse the order */
				reverse_centroids(&centroids[i], group_size);
			}
			else if (next_group >= median_count)	/* group split by median */
			{
				rebalance_centroids(&centroids[i], group_size,
									median_count - count_so_far,
									next_group - median_count);
			}
		}

		i = j;
		count_so_far = next_group;
	}
}

/*
 * Sort centroids in the aggregate state.
 */
static void
tdigest_sort(tdigest_aggstate_t *state)
{
	tdigest_sort_centroids(state->centroids, state->ncentroids, state->count);
}

/*
 * Fallback compaction, merging adjacent centroids irrespective of the size
 * limits, until the digest fits into the requested compression.
 *
 * The regular size-based compaction is not guaranteed to make any progress.
 * This is a last resort for such cases, trading accuracy for a guarantee
 * that the digest never grows past the end of the centroid array. It only
 * applies to digests that are already severely skewed, so it does not really
 * cause much harm.
 *
 * The centroids are simply split into groups of the same size (number of
 * centroids), and each group is merged into a single centroid. That needs
 * just a single pass over the array, so the cost is linear even for many
 * centroids.
 *
 * Expects the centroids to be already sorted. We only call this from
 * tdigest_compact(), which does tdigest_sort() at the beginning.
 *
 * XXX At this point this is likely dead code, because the regular compaction
 * in tdigest_compact should always make progress and free some space. But
 * that needs more consideration/verification.
 */
static void
tdigest_compact_forced(tdigest_aggstate_t *state)
{
	int			cur = 0;		/* current output centroid */
	int			group_size;		/* input centroids per output centroid */

	Assert(state->ncentroids > state->compression);

	group_size = (state->ncentroids + state->compression - 1) / state->compression;

	/*
	 * Groups need to be large enough for the compacted digest to fit into
	 * the requested compression.
	 */
	Assert(group_size * state->compression >= state->ncentroids);

	/* process groups of input centrois */
	for (;;)
	{
		int		i;
		int64	group_count = 0;
		double	mean = 0;

		/* range of indexes of input centroids */
		int		start = cur * group_size;
		int		end = Min(start + group_size, state->ncentroids);

		/* stop after processing all input centroids */
		if (start >= end)
			break;

		/*
		 * total count of the range of input centroids
		 *
		 * This can't overflow - the counts add up to the total count of the
		 * digest, which is known not to overflow. So no need to check for
		 * overflows here.
		 */
		for (i = start; i < end; i++)
		{
			group_count += state->centroids[i].count;
		}

		/*
		 * calculate the group mean using the overflow-resistant approach
		 *
		 * XXX We could detect "same mean" case, similar to tdigest_compact,
		 * and furthermore we could find runs of the same mean in the group,
		 * and only average when the mean changes. Doesn't seem worth it,
		 * this is a fallback anyway.
		 *
		 * XXX Maybe this is not entirely overflow-free? The weights are
		 * calculated in double, so can't that lose precision and sum to a
		 * total > 1.0? Then the result might "drift" above the valid means.
		 * And consider two centroids with means close to DBL_MAX, with one
		 * centroid having very high count value. Could it happen that
		 * (mean * 0.9999 > mean) for a positive mean? Maybe it could even
		 * overflow to +/- infinity.
		 */
		for (i = start; i < end; i++)
		{
			mean += state->centroids[i].mean * (state->centroids[i].count / (double) group_count);
		}

		/*
		 * XXX It should not be possible to get a NaN mean. That would require
		 * adding up -infinity and +infinity in the loop above, but the input
		 * means should be finite (or we have bigger problem earlier). And for
		 * the multiplication to overflow, the weight needs to be close to 1.0,
		 * but that can happen only for a single centroid.
		 */
		Assert(!isnan(mean));

		/*
		 * Handle a possible overflow in the mean calculation above, by clamping
		 * it by the min/max mean of the group we're compacting.
		 *
		 * XXX I'm not convinced it can happen, but better safe than sorry. We
		 * don't want to end up storing digests with bogus means.
		 */
		mean = Max(Min(state->centroids[end - 1].mean, mean),
				   state->centroids[start].mean);

		state->centroids[cur].count = group_count;
		state->centroids[cur].mean = mean;
		cur++;
	}

	state->ncentroids = cur;
	state->ncompacted = state->ncentroids;

	Assert(state->ncentroids <= state->compression);
}

/*
 * Perform compaction of the t-digest, i.e. merge the centroids as required
 * by the compression parameter.
 *
 * We always keep the data sorted in ascending order. This way we can reuse
 * the sort between compactions, and also when computing the quantiles.
 *
 * The regular compaction is not guaranteed not make any progress. The size
 * limits are calculated in double, may end up too low to allow merging any
 * centroids. If that happens, we force a compaction that simply merges
 * neighbor centroids.
 *
 * XXX Switch the direction regularly, to eliminate possible bias and improve
 * accuracy, as mentioned in the paper.
 *
 * XXX This initially used the k1 scale function, but the implementation was
 * not limiting the number of centroids for some reason (it might have been
 * a bug in the implementation, of course). The current code is a modified
 * copy from ajwerner [1], and AFAIK it's the k2 function, it's much simpler
 * and generally works quite nicely.
 *
 * [1] https://github.com/ajwerner/tdigestc/blob/master/go/tdigest.c
 */
static void
tdigest_compact(tdigest_aggstate_t *state)
{
	int			i;

	int			cur;	/* current centroid */
	int64		count_so_far;
	int64		total_count;
	double		denom;
	double		normalizer;
	int			start;
	int			step;
	int			n;

	AssertCheckTDigestAggState(state);

	/* if the digest is fully compacted, it's been already compacted */
	if (state->ncompacted == state->ncentroids)
		return;

	tdigest_sort(state);

	state->ncompactions++;

	if (state->ncompactions % 2 == 0)
	{
		start = 0;
		step = 1;
	}
	else
	{
		start = state->ncentroids - 1;
		step = -1;
	}

	total_count = state->count;
	denom = 2 * M_PI * total_count * log(total_count);
	normalizer = state->compression / denom;

	cur = start;
	count_so_far = 0;
	n = 1;

	for (i = start + step; (i >= 0) && (i < state->ncentroids); i += step)
	{
		int64	proposed_count;
		double	q0;
		double	q2;
		double	z;
		bool	should_add;

		proposed_count = state->centroids[cur].count + state->centroids[i].count;

		z = proposed_count * normalizer;
		q0 = count_so_far / (double) total_count;
		q2 = (count_so_far + proposed_count) / (double) total_count;

		/*
		 * Calculate the (1 - q) factors from the exact integer remainders,
		 * instead of subtracting the quotients from 1.
		 *
		 * The two are equivalent with exact arithmetic, but not in double. If
		 * a single centroid holds almost the whole weight of the digest, the
		 * quotient rounds to exactly 1.0, and (1 - q) cancels to exactly 0.
		 * The size limit then says no two centroids may be merged, even
		 * though the exact limit is small but positive - and the compaction
		 * ends up not making any progress at all.
		 */
		should_add =
			(z <= (q0 * ((double) (total_count - count_so_far) / (double) total_count))) &&
			(z <= (q2 * ((double) (total_count - count_so_far - proposed_count) / (double) total_count)));

		if (should_add)
		{
			/*
			 * If both centroids have the same mean, don't calculate it again.
			 * The recaulculation may cause rounding errors, so that the means
			 * would drift apart over time. We want to keep them equal for as
			 * long as possible.
			 */
			if (state->centroids[cur].mean != state->centroids[i].mean)
			{
				double	mean;
				int64	count;

				/* count can't overflow int64 (total is within INT64_MAX) */
				count = state->centroids[i].count;
				count += state->centroids[cur].count;

				/* calculate the mean in a way that should not overflow */
				mean = state->centroids[i].mean * (state->centroids[i].count / (double) count);
				mean += state->centroids[cur].mean * (state->centroids[cur].count / (double) count);

				/* should not happen for finite inputs */
				Assert(!isnan(mean));

				/*
				 * paranoia: clamp to not underflow/overflow the inputs
				 *
				 * We may be walking the centroids forward or backwards, which
				 * determines whether (cur < i) or (cur > i).
				 */
				if (step > 0)
				{
					Assert(cur < i);

					mean = Min(Max(state->centroids[cur].mean, mean),
							   state->centroids[i].mean);
				}
				else
				{
					Assert(cur > i);

					mean = Min(Max(state->centroids[i].mean, mean),
							   state->centroids[cur].mean);
				}

				state->centroids[cur].mean = mean;
			}

			/* XXX Do this after possibly recalculating the mean. */
			state->centroids[cur].count += state->centroids[i].count;
		}
		else
		{
			count_so_far += state->centroids[cur].count;
			cur += step;
			n++;
			state->centroids[cur] = state->centroids[i];
		}

		if (cur != i)
		{
			state->centroids[i].count = 0;
			state->centroids[i].mean = 0;
		}
	}

	state->ncentroids = n;
	state->ncompacted = state->ncentroids;

	if (step < 0)
		memmove(state->centroids, &state->centroids[cur], n * sizeof(centroid_t));

	/*
	 * The compaction above is not guaranteed to make any progress, so if it
	 * did not free up any space, fall back to merging the centroids without
	 * regard for the size limits.
	 */
	if (state->ncentroids == BUFFER_SIZE(state->compression))
		tdigest_compact_forced(state);

	AssertCheckTDigestAggState(state);

	/* Must have freed some space in the buffer. */
	Assert(state->ncentroids < BUFFER_SIZE(state->compression));
}

/*
 * Estimate requested quantiles from the t-digest agg state.
 */
static void
tdigest_compute_quantiles(tdigest_aggstate_t *state, double *result)
{
	int			i, j;

	AssertCheckTDigestAggState(state);

	/*
	 * Trigger a compaction, which also sorts the data.
	 *
	 * XXX maybe just do a sort here, which should give us a bit more accurate
	 * results, probably.
	 */
	tdigest_compact(state);

	/*
	 * Determine the two centroids the quantile lies between, and calculate the
	 * estimate using linear interpolation.
	 *
	 * XXX All of this works fine for t-digests with non-extreme counts, up to
	 * about 2^52. At that point the double precision ULP gets > 1.0, and some
	 * of the calculations here start misbehaving a little. For example the
	 * (count * 0.9999...) can get higher than count, etc. We try to prevent
	 * obviously bogus results, but it's futile to try to fix this perfectly.
	 * The cases are extremely rare, and we're calculating estimates anyway.
	 * If we wanted to fix this properly, we'd need to use some sort of large
	 * float data type (there seems to be "long double" and binary128).
	 *
	 * XXX The rounding/precision issues affect only accuracy of results, not
	 * correctness of the code. For example, it must not result in OOB access
	 * to bogus centroids etc.
	 */
	for (i = 0; i < state->npercentiles; i++)
	{
		int64	count;
		double	goal = (state->count * state->percentiles[i]);
		bool	is_before = false;

		centroid_t *c = NULL,
				   *prev,
				   *next;

		/* integer and fractional parts of half-centroids before/after */
		double	distance,
				total_distance,
				q;

		/* first centroid for percentile 1.0 */
		if (state->percentiles[i] == 0.0)
		{
			c = &state->centroids[0];
			result[i] = c->mean;
			continue;
		}

		/* last centroid for percentile 1.0 */
		if (state->percentiles[i] == 1.0)
		{
			c = &state->centroids[state->ncentroids - 1];
			result[i] = c->mean;
			continue;
		}

		/*
		 * Walk the centroids and calculate running sum of counts. Stop before
		 * adding a centroid that would exceed the goal - we don't know if the
		 * goal falls before/after the mean yet.
		 *
		 * FIXME There can be multiple centroids with the same mean, in which
		 * case we should use the total count for all of them. Not sure how
		 * likely it's to have centroids with exactly the same mean. But it
		 * might affect the interpolation later.
		 */
		count = 0;
		for (j = 0; j < state->ncentroids; j++)
		{
			c = &state->centroids[j];

			/* Adding the centroid would exceeded the goal, so stop. */
			if (count + c->count >= goal)
				break;

			count += c->count;
		}

		/*
		 * Adding the whole entroid would exceed the goal, but we don't know
		 * on which side of the mean the value lies yet. We might have also
		 * hit the mean exactly. Let's figure that out.
		 *
		 * This will determine which centroids we'll look at for linear
		 * interpolation (previous/following one) later.
		 *
		 * We know centroid "c" exceeds the goal, but did we hit the mean,
		 * or are we to the left/right? We assume half the items is before
		 * the mean, half after.
		 */
		is_before = goal < (count + c->count / 2.0);

		/*
		 * Pick centroids for linear interpolation, depending on which side
		 * of the "current" centroid we fell on. Either use the previous or
		 * the following centroid.
		 *
		 * For extreme percentile values (or somehow weird digests) we can
		 * end up before/after the last centroid, in which case we need to
		 * be careful to not access OOB.
		 */
		if (is_before)
		{
			/* no previous centroid, use the current (first) one */
			if (j == 0)
			{
				result[i] = c->mean;
				continue;
			}

			prev = &state->centroids[j - 1];
			next = &state->centroids[j];

			Assert(next == c);

			/*
			 * Undo the centroid already added above (count is integer,
			 * so we can't undo half of it without possibly losing half
			 * of the count). We'll deal with that later.
			 */
			count -= prev->count;
		}
		else
		{
			/* no following centroid, use the current (last) one */
			if (j == (state->ncentroids - 1))
			{
				result[i] = c->mean;
				continue;
			}

			prev = &state->centroids[j];
			next = &state->centroids[j + 1];

			Assert(prev == c);
		}

		/* paranoia: make sure the prev/next centroids are valid */
		Assert(prev >= &state->centroids[0]);
		Assert(next <= &state->centroids[state->ncentroids - 1]);
		Assert((prev + 1) == next);

		/*
		 * Now we know the quantile lies somewhere between the centroids,
		 * we need to calculate the correct value. (We know it's not at
		 * either mean, that's what the above branches are for.)
		 *
		 * We will calculate the distance from the first mean, the total
		 * distance between the means. And we'll do linear interpolation.
		 */

		/* distance to the first mean (of the previous centroid) */
		distance = (double) (goal - count) - prev->count / 2.0;

		/* distance between the means of the two centroids */
		total_distance = (prev->count / 2.0) + (next->count / 2.0);

		/*
		 * We should be "to the right" the first centroid, and should not
		 * be so far ahead to exceed the next one. So in printiple, this
		 * should be true:
		 *
		 * Assert((distance >= 0) && (distance <= total_distance));
		 *
		 * But, it's tricky due to precision and rounding. We're switching
		 * from int64 to double, and double has much lower precision close
		 * to INT64_MAX (ULP >> 1.0). With high goal and/or count values we
		 * can end up with distance outside the [0, total_distance] range,
		 * or just hit the centroids exactly.
		 *
		 * XXX Try uncommenting the assert, there's a test triggering it.
		 */

		/*
		 * Clamp distance to [0, total_distance], to mitigate unexpected
		 * rouding / precision errors.
		 */
		distance = Max(0.0, Min(total_distance, distance));

		/*
		 * the actual linear interpolation, using the formula
		 *
		 *   (1 - q) * v1 + q * v2
		 *
		 * XXX The "q" should not overflow/underflow or misbehave in other
		 * ways, as distance is in [0.0, total_distance]. But clamp anyway,
		 * to deal with unexpected rounding / precision errors.
		 *
		 * XXX Not sure this is needed with the clamped distance.
		 */
		q = Max(0.0, Min(1.0, distance / total_distance));

		result[i] = (1 - q) * prev->mean + q * next->mean;
	}
}

/*
 * Estimate inverse of quantile given a value from the t-digest agg state.
 *
 * Essentially an inverse to tdigest_compute_quantiles.
 */
static void
tdigest_compute_quantiles_of(tdigest_aggstate_t *state, double *result)
{
	int			i;

	AssertCheckTDigestAggState(state);

	/*
	 * Trigger a compaction, which also sorts the data.
	 *
	 * XXX maybe just do a sort here, which should give us a bit more accurate
	 * results, probably.
	 */
	tdigest_compact(state);

	for (i = 0; i < state->nvalues; i++)
	{
		int			j;
		double		count;
		double		value = state->values[i];
		double		c, d, q, q1, q2, r;

		/* next and previous centroids */
		centroid_t *curr = NULL;
		centroid_t *prev = NULL;

		/* handle infinity/NaN values by mapping them to 0.0, 1.0 and NaN */
		if (!isfinite(value))
		{
			if (isnan(value))
				result[i] = NAN;
			else if (value < 0)	/* -infinity */
				result[i] = 0.0;
			else				/* infinity */
				result[i] = 1.0;

			continue;
		}

		/*
		 * Find the first centroid with (mean >= value), and remember the
		 * last centroid before that - if the value is in between, we will
		 * be calculating the percentile by linear approximation.
		 */
		count = 0;
		for (j = 0; j < state->ncentroids; j++)
		{
			/* remember the previous centroid, grab the next one */
			prev = curr;
			curr = &state->centroids[j];

			if (curr->mean >= value)
				break;

			count += curr->count;
		}

		/*
		 * If the value exactly matches the mean of the current centroid,
		 * we're almost there. There may be multiple centroids with the same
		 * mean, so we just need to advance past those.
		 */
		if (value == curr->mean)
		{
			int64	count_at_value = 0;

			/*
			 * There may be multiple centroids with this mean (i.e. containing
			 * this value), so find all of them and sum their weights.
			 */
			while ((j < state->ncentroids) && (state->centroids[j].mean == value))
			{
				count_at_value += state->centroids[j].count;
				j++;
			}

			result[i] = (count + (count_at_value / 2.0)) / state->count;

			/* the next centroid has a higher mean, so we're done */
			continue;
		}

		/*
		 * If (value > curr->mean), it means we went through all centroids
		 * without finding one with a larger mean. So the value is above
		 * all centroids, and so it's 1.0 percentile.
		 */
		if (value > curr->mean)	/* past the largest centroid */
		{
			result[i] = 1;
			continue;
		}

		/*
		 * It's also possible even the first centroid has a higher mean, in
		 * which case the value is 0.0 percentile.
		 */
		if (prev == NULL)		/* before the smallest centroid */
		{
			result[i] = 0;
			continue;
		}

		/* we have two distinct centroids */
		Assert((prev != NULL) && (curr != NULL) && (prev != curr));
		Assert(prev->mean < curr->mean);

		/*
		 * The value lies somewhere between two centroids. We want to figure out
		 * where along the line from the prev node to this node the value is.
		 *
		 * FIXME What if there are multiple centroids with the same mean as the
		 * prev/curr centroid? This probably needs to lookup all of them and sum
		 * their counts, just like we did in case of the exact mean equality, no?
		 * Both for the current and previous centroids, so that the approximation
		 * works well.
		 */

		count -= (prev->count / 2.0);

		/*
		 * We assume for both prev/curr centroid, half the count is on left/righ,
		 * so between them we have (prev->count/2 + curr->count/2). At zero we
		 * are in prev->mean and at (prev->count/2 + curr->count/2) we're at
		 * curr->mean.
		 *
		 * XXX Because (count >= 1), each centroid contributes at least 0.5, so
		 * we know (c >= 1.0). It can get a bit imprecise for extreme values, due
		 * to (int64 -> double) conversion. The double ULP is ~512.
		 */
		c = (curr->count / 2.0 + prev->count / 2.0);
		d = (curr->mean - prev->mean);

		/* quantiles for the prev/next mean */
		q1 = count / (double) state->count;
		q2 = (count + c) / (double) state->count;

		Assert(q1 <= q2);

		/*
		 * Calculate the linear interpolation of q1/q2 percentiles.
		 *
		 * We need to be careful about infinity/NaN during calculation. The
		 * means may be so close to +/- DBL_MAX, that with "d" gets infinite.
		 * That's equivalent to 0 slope, but we can do a bit better - if this
		 * happens, we halve the values, which makes the difference finite
		 * again (in exchange for loss of precision, but that's acceptable).
		 */
		if (isfinite(d))
			q = (value - prev->mean) / d;
		else
		{
			/* trick - halve the means, so the difference can't overflow */
			q = (value / 2.0 - prev->mean / 2.0) / (curr->mean / 2.0 - prev->mean / 2.0);
		}

		/* calculate the linear interpolation */
		r = (1 - q) * q1 + q * q2;

		/*
		 * In principle, the result should be in between the percentiles for
		 * the two centroids (we're between them)
		 *
		 * Assert((q1 <= r) && (r <= q2));
		 *
		 * But for extreme values (close to 1.0, which can happen for values
		 * on the right tail of a massive digest), we can end up rounding to
		 * a value outside the [q1,q2] range. So clamp the value to defend
		 * against that.
		 *
		 * XXX Try uncommenting the assert, there's a test triggering it.
		 */

		result[i] = Max(q1, Min(q2, r));
	}
}


/* add a value to the t-digest, trigger a compaction if full */
static void
tdigest_add(tdigest_aggstate_t *state, double v)
{
	int	compression = state->compression;

	/* make sure we're not adding bogus NaN/infinity values as centroids */
	if (!isfinite(v))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("all values added to t-digest must be finite")));

	/*
	 * If the buffer is full, trigger compaction here so that we have
	 * free space for the new value.
	 */
	if (state->ncentroids == BUFFER_SIZE(compression))
		tdigest_compact(state);

	/* make sure we have space for the value */
	Assert(state->ncentroids < BUFFER_SIZE(compression));

	/* for a single point, the value is both sum and mean */
	state->centroids[state->ncentroids].count = 1;
	state->centroids[state->ncentroids].mean = v;
	state->ncentroids++;

	/* make sure the total does not overflow */
	if (pg_add_s64_overflow(state->count, 1, &state->count))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("tdigest count overflow")));
}

/*
 * Add a centroid (possibly with count not equal to 1) to the t-digest,
 * triggers a compaction when buffer full.
 */
static void
tdigest_add_centroid(tdigest_aggstate_t *state, double mean, int64 count)
{
	int	compression = state->compression;

	/* we should not have an infinite/NaN mean in a digest */
	Assert(isfinite(mean));

	/*
	 * If the buffer is full, trigger compaction here so that we have
	 * free space for the new value.
	 */
	if (state->ncentroids == BUFFER_SIZE(compression))
		tdigest_compact(state);

	/* make sure we have space for the value */
	Assert(state->ncentroids < BUFFER_SIZE(compression));

	/* for a single point, the value is both sum and mean */
	state->centroids[state->ncentroids].count = count;
	state->centroids[state->ncentroids].mean = mean;
	state->ncentroids++;

	/* make sure the total does not overflow */
	if (pg_add_s64_overflow(state->count, count, &state->count))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("tdigest count overflow")));
}

/* allocate t-digest with enough space for a requested number of centroids */
static tdigest_t *
tdigest_allocate(int ncentroids)
{
	Size		len;
	tdigest_t  *digest;
	char	   *ptr;

	len = offsetof(tdigest_t, centroids) + ncentroids * sizeof(centroid_t);

	/* we pre-allocate the array for all centroids and also the buffer for incoming data */
	ptr = palloc(len);
	SET_VARSIZE(ptr, len);

	digest = (tdigest_t *) ptr;

	digest->flags = 0;
	digest->ncentroids = 0;
	digest->count = 0;
	digest->compression = 0;

	/* new tdigest are automatically storing mean */
	digest->flags |= TDIGEST_STORES_MEAN;

	return digest;
}

/*
 * tdigest_update_format
 *		Update t-digest format to represent centroids as (mean,count).
 *
 * Switches the centroids from (sum,count) to (mean,count), so that all
 * the places processing centroids can use just the new format.
 *
 * If the digest already uses the new format, this is a no-op. Otherwise
 * a modified copy of the digest is returned.
 *
 * XXX This does not affect on-disk representation of existing digests,
 * we create just an in-memory version of the digest. Only when the
 * digest gets modified a new format will be written back.
 */
static tdigest_t *
tdigest_update_format(tdigest_t *digest)
{
	int		i;
	int		s;
	char   *ptr;

	/* if already new format, we're done */
	if (digest->flags & TDIGEST_STORES_MEAN)
		return digest;

	/*
	 * We'll convert the digest so that centroids use means, but we must
	 * not modify the input digest - it might be just a pointer to data
	 * buffer, or something like that. So we have to create a copy first.
	 */
	s = VARSIZE_ANY(digest);
	ptr = palloc(s);
	memcpy(ptr, digest, s);

	digest = (tdigest_t *) ptr;

	/* And now tweak the contents of the copy. */
	for (i = 0; i < digest->ncentroids; i++)
	{
		digest->centroids[i].mean
			= digest->centroids[i].mean / digest->centroids[i].count;
	}

	digest->flags |= TDIGEST_STORES_MEAN;

	return digest;
}

/*
 * tdigest_sort_digest
 *		Make sure the centroids of the digest are sorted by mean.
 *
 * Digests with the centroids in an arbitrary order are perfectly valid - the
 * incremental API keeps the digests uncompacted (and thus unsorted), and the
 * input functions accept such digests too. So the places walking the centroids
 * in the order of means have to do the sort themselves.
 *
 * If the digest is already sorted, this is a no-op. Otherwise a sorted copy of
 * the digest is returned - we must not sort the digest in place, it might be
 * just a pointer to a data buffer, or something like that.
 *
 * Expects a digest in the new format, i.e. with centroids storing means (see
 * tdigest_update_format).
 *
 * XXX It's a bit wasteful to do the sort over and over, even for on-disk digests
 * that are perfectly sorted. It should be possible to have a TDIGEST_SORTED flag
 * tracking when a digest is already sorted, and skip the sort.
 */
static tdigest_t *
tdigest_sort_digest(tdigest_t *digest)
{
	int		i;
	int		s;
	char   *ptr;

	Assert(digest->flags & TDIGEST_STORES_MEAN);

	/* if the centroids are already sorted, we're done */
	for (i = 1; i < digest->ncentroids; i++)
	{
		/*
		 * XXX Not quite right, it needs to consider the count too, if
		 * the centroids have the same mean (and whether we're below or
		 * above the mean of the whole digest.
		 */
		if (digest->centroids[i - 1].mean > digest->centroids[i].mean)
			break;
	}

	/* if the digest is already sorted, bail out */
	if (i >= digest->ncentroids)
		return digest;

	/*
	 * Create a fresh copy of the digest, not to break the current one (which
	 * may even be persistent on disk.
	 */
	s = VARSIZE_ANY(digest);
	ptr = palloc(s);
	memcpy(ptr, digest, s);

	digest = (tdigest_t *) ptr;

	tdigest_sort_centroids(digest->centroids, digest->ncentroids,
						   digest->count);

	return digest;
}

/*
 * allocate a tdigest aggregate state, along with space for percentile(s)
 * and value(s) requested when calling the aggregate function
 */
static tdigest_aggstate_t *
tdigest_aggstate_allocate(int npercentiles, int nvalues, int compression)
{
	Size				len;
	tdigest_aggstate_t *state;
	char			   *ptr;

	/* at least one of those values is 0 */
	Assert(nvalues == 0 || npercentiles == 0);

	/*
	 * We allocate a single chunk for the struct including percentiles and
	 * centroids (including extra buffer for new data).
	 */
	len = MAXALIGN(sizeof(tdigest_aggstate_t)) +
		  MAXALIGN(sizeof(double) * npercentiles) +
		  MAXALIGN(sizeof(double) * nvalues) +
		  (BUFFER_SIZE(compression) * sizeof(centroid_t));

	ptr = palloc0(len);

	state = (tdigest_aggstate_t *) ptr;
	ptr += MAXALIGN(sizeof(tdigest_aggstate_t));

	state->nvalues = nvalues;
	state->npercentiles = npercentiles;
	state->compression = compression;

	if (npercentiles > 0)
	{
		state->percentiles = (double *) ptr;
		ptr += MAXALIGN(sizeof(double) * npercentiles);
	}

	if (nvalues > 0)
	{
		state->values = (double *) ptr;
		ptr += MAXALIGN(sizeof(double) * nvalues);
	}

	state->centroids = (centroid_t *) ptr;
	ptr += (BUFFER_SIZE(compression) * sizeof(centroid_t));

	Assert(ptr == (char *) state + len);

	return state;
}

static tdigest_t *
tdigest_aggstate_to_digest(tdigest_aggstate_t *state, bool compact)
{
	int			i;
	tdigest_t  *digest;

	if (compact)
		tdigest_compact(state);

	digest = tdigest_allocate(state->ncentroids);

	digest->count = state->count;
	digest->ncentroids = state->ncentroids;
	digest->compression = state->compression;

	for (i = 0; i < state->ncentroids; i++)
	{
		digest->centroids[i].mean = state->centroids[i].mean;
		digest->centroids[i].count = state->centroids[i].count;
	}

	return digest;
}

/* check that the requested percentiles are valid */
static void
check_percentiles(double *percentiles, int npercentiles)
{
	int i;

	for (i = 0; i < npercentiles; i++)
	{
		if (!((percentiles[i] >= 0.0) && (percentiles[i] <= 1.0)))
			elog(ERROR, "invalid percentile value %f, should be in [0.0, 1.0]",
				 percentiles[i]);
	}
}

static void
check_compression(int compression)
{
	if (compression < MIN_COMPRESSION || compression > MAX_COMPRESSION)
		elog(ERROR, "invalid compression value %d", compression);
}

static void
check_trim_values(double low, double high)
{
	if (!((low >= 0.0) && (low <= 1.0)))
		elog(ERROR, "invalid low percentile value %f, should be in [0.0, 1.0]",
			 low);

	if (!((high >= 0.0) && (high <= 1.0)))
		elog(ERROR, "invalid high percentile value %f, should be in [0.0, 1.0]",
			 high);

	if (low > high)
		elog(ERROR, "invalid low/high percentile values %f/%f, should be low <= high",
			 low, high);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with a single percentile.
 */
Datum
tdigest_add_double(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t *state;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int		compression = PG_GETARG_INT32(2);
		double *percentiles = NULL;
		int		npercentiles = 0;
		MemoryContext	oldcontext;

		check_compression(compression);

		oldcontext = MemoryContextSwitchTo(aggcontext);

		if (PG_NARGS() >= 4)
		{
			percentiles = (double *) palloc(sizeof(double));
			percentiles[0] = PG_GETARG_FLOAT8(3);
			npercentiles = 1;

			check_percentiles(percentiles, npercentiles);
		}

		state = tdigest_aggstate_allocate(npercentiles, 0, compression);

		if (percentiles)
		{
			memcpy(state->percentiles, percentiles, sizeof(double) * npercentiles);
			pfree(percentiles);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value with a given count to the t-digest, as a sequence of properly
 * sized centroids.
 *
 * This is an alternative to adding a single centroid, representing all the
 * points with the same value. It follows all the rules on centroid sizes,
 * etc.
 *
 * The centroids are handed over to the aggregate state as they are computed,
 * instead of building a t-digest first. The number of centroids the loop
 * produces is not bounded by the compression (on the tails the calculated
 * size drops below 1, and gets clamped), so there is no size of a centroid
 * array that would be guaranteed to be sufficient.
 */
static void
tdigest_add_generated(tdigest_aggstate_t *state, double value, int64 count)
{
	int64		count_so_far;
	int64		count_remaining;
	double		denom;
	double		normalizer;
	int			compression = state->compression;

	/* make sure we're not adding bogus NaN/infinity values as centroids */
	if (!isfinite(value))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("all values added to t-digest must be finite")));

	denom = 2 * M_PI * count * log(count);
	normalizer = compression / denom;

	count_so_far = 0;	/* does not include current centroid */
	count_remaining = count;

	/*
	 * Create largest possible centroids, until we run out of items. In each
	 * step we need to find the largest possible well-formed centroid, i.e. one
	 * that matches the two conditions:
	 *
	 *	z <= q0 * (1 - q0)    where q0 = (count_so_far / count)
	 *
	 *	z <= q2 * (1 - q2)    where q2 = (count_so_far + X) / count;
	 *
	 * with z = (X * normalizer). X being the value we need to determine. Solving
	 * q0 is trivial, while q2 leads to a quadratic equation with two roots.
	 */
	while (count_remaining > 0)
	{
		int64	proposed_count;
		double	q0;
		double	b, c, d;
		double	r1, r2;

		/*
		 * Solving z <= q0 * (1 - q0) is trivial.
		 *
		 * Just like in tdigest_compact, we must not calculate (1 - q0) by
		 * subtracting the two doubles - for q0 close to 1 that cancels out all
		 * the significant digits. We already have count_remaining, which is
		 * exactly (count - count_so_far), so use that remainder instead.
		 */
		q0 = count_so_far / (double) count;
		r1 = (q0 * (count_remaining / (double) count) / normalizer);

		/*
		 * Solve z <= q2 * (1 - q2) as a quadratic equation. The inequatily we
		 * need to solve is
		 *
		 *	0 <= a * x^2 + b * x + c
		 *
		 * with (a = -1) and the following coefficients.
		 *
		 * XXX The counts may be very high values (int64), so we need to be
		 * careful to prevent overflows by doing everything with double.
		 *
		 * XXX c is mathematically (count_so_far * (count - count_so_far)), so
		 * calculate it as a plain product of the two exact integers. The
		 * expanded form (count_so_far * count - count_so_far * count_so_far)
		 * is a difference of two huge and nearly equal values, which loses
		 * almost all the precision.
		 */
		b = ((double) count - 2 * (double) count_so_far - (double) count * (double) count * normalizer);
		c = ((double) count_so_far * (double) count_remaining);

		/*
		 * As this is an "upside down" parabola, the values between the roots
		 * are positive - we're looking for the larger of the two roots, which
		 * for a = -1 is (b + sqrt(b*b + 4*c)) / 2.
		 *
		 * XXX Evaluating that expression directly is only safe for b >= 0.
		 * For b < 0 the sqrt is very close to -b, so the addition cancels out
		 * all the significant digits (and often yields exactly zero, forcing
		 * us to emit a single-item centroid). Use the equivalent "conjugate"
		 * form 2*c / (sqrt(b*b + 4*c) - b) in that case, which only ever adds
		 * values of the same sign. Both branches are hit in practice - b is
		 * positive whenever compression < 2*pi*ln(count).
		 *
		 * XXX c is never negative, so the discriminant is a sum of two
		 * non-negative values and the sqrt is always well defined.
		 */
		d = sqrt(b * b + 4 * c);

		if (b >= 0)
			r2 = (b + d) / 2;
		else
			r2 = (2 * c) / (d - b);

		/*
		 * paranoia: We should not be dealing withh NaN values here. Crash in
		 * debug build, double_to_int64 will mitigate it in regular builds.
		 */
		Assert(isfinite(r1) && isfinite(r2));

		/*
		 * We need to meet both conditions, so use the smaller solution. The
		 * value may be large (or NaN), so clamp it - we must not add more
		 * than what remains anyway.
		 */
		proposed_count = double_to_int64(floor(Min(r1, r2)), count_remaining);

		/*
		 * It's possible to get very low values on the tails, but we must add
		 * at least something, otherwise we'd get infinite loops.
		 */
		proposed_count = Max(proposed_count, 1);

		tdigest_add_centroid(state, value, proposed_count);

		count_so_far += proposed_count;
		count_remaining -= proposed_count;
	}
}

/*
 * Add a value with count to the tdigest (create one if needed). Transition
 * function for tdigest aggregate with a single percentile.
 */
Datum
tdigest_add_double_count(PG_FUNCTION_ARGS)
{
	int64				i;
	int64				count;
	tdigest_aggstate_t *state;
	MemoryContext		aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double_count called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int		compression = PG_GETARG_INT32(3);
		double *percentiles = NULL;
		int		npercentiles = 0;
		MemoryContext	oldcontext;

		check_compression(compression);

		oldcontext = MemoryContextSwitchTo(aggcontext);

		if (PG_NARGS() >= 5)
		{
			percentiles = (double *) palloc(sizeof(double));
			percentiles[0] = PG_GETARG_FLOAT8(4);
			npercentiles = 1;
			check_percentiles(percentiles, npercentiles);
		}

		state = tdigest_aggstate_allocate(npercentiles, 0, compression);

		if (percentiles)
		{
			memcpy(state->percentiles, percentiles, sizeof(double) * npercentiles);
			pfree(percentiles);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	if (PG_ARGISNULL(2))
	{
		count = 1;
	}
	else
		count = PG_GETARG_INT64(2);

	/* can't add values with non-positive counts */
	if (count <= 0)
		elog(ERROR, "invalid count value %lld, must be a positive value",
			 (long long) count);

	/*
	 * When adding too many values (than would fit into an empty buffer, and
	 * thus likely causing too many compactions), we instead add them as
	 * properly sized centroids.
	 *
	 * This is much faster, because the centroids can be generated in one go,
	 * so there are only very few compactions.
	 */
	if (count > BUFFER_SIZE(state->compression))
	{
		tdigest_add_generated(state, PG_GETARG_FLOAT8(1), count);

		count = 0;
	}

	/*
	 * If there are only a couple values, just add them one by one, so that
	 * we do proper compaction and sizing of centroids. Otherwise we might end
	 * up with oversized centroid on the tails etc.
	 */
	for (i = 0; i < count; i++)
		tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with a single value.
 */
Datum
tdigest_add_double_values(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t *state;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double_values called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int		compression = PG_GETARG_INT32(2);
		double *values = NULL;
		int		nvalues = 0;
		MemoryContext	oldcontext;

		check_compression(compression);

		oldcontext = MemoryContextSwitchTo(aggcontext);

		if (PG_NARGS() >= 4)
		{
			values = (double *) palloc(sizeof(double));
			values[0] = PG_GETARG_FLOAT8(3);
			nvalues = 1;
		}

		state = tdigest_aggstate_allocate(0, nvalues, compression);

		if (values)
		{
			memcpy(state->values, values, sizeof(double) * nvalues);
			pfree(values);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with a single value.
 */
Datum
tdigest_add_double_values_count(PG_FUNCTION_ARGS)
{
	int64				i;
	int64				count;
	tdigest_aggstate_t *state;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double_values_count called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int		compression = PG_GETARG_INT32(3);
		double *values = NULL;
		int		nvalues = 0;
		MemoryContext	oldcontext;

		check_compression(compression);

		oldcontext = MemoryContextSwitchTo(aggcontext);

		if (PG_NARGS() >= 5)
		{
			values = (double *) palloc(sizeof(double));
			values[0] = PG_GETARG_FLOAT8(4);
			nvalues = 1;
		}

		state = tdigest_aggstate_allocate(0, nvalues, compression);

		if (values)
		{
			memcpy(state->values, values, sizeof(double) * nvalues);
			pfree(values);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	if (PG_ARGISNULL(2))
	{
		count = 1;
	}
	else
		count = PG_GETARG_INT64(2);

	/* can't add values with non-positive counts */
	if (count <= 0)
		elog(ERROR, "invalid count value %lld, must be a positive value",
			 (long long) count);

	/*
	 * When adding too many values (than would fit into an empty buffer, and
	 * thus likely causing too many compactions), we instead add them as
	 * properly sized centroids.
	 *
	 * This is much faster, because the centroids can be generated in one go,
	 * so there are only very few compactions.
	 */
	if (count > BUFFER_SIZE(state->compression))
	{
		tdigest_add_generated(state, PG_GETARG_FLOAT8(1), count);

		count = 0;
	}

	/*
	 * If there are only a couple values, just add them one by one, so that
	 * we do proper compaction and sizing of centroids. Otherwise we might end
	 * up with oversized centroid on the tails etc.
	 */
	for (i = 0; i < count; i++)
		tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with a single percentile.
 */
Datum
tdigest_add_digest(PG_FUNCTION_ARGS)
{
	int					i;
	tdigest_aggstate_t *state;
	tdigest_t		   *digest;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_digest called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

	/* make sure we get digest with the new format */
	digest = tdigest_update_format(digest);

	/* if there's no aggregate state allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		double *percentiles = NULL;
		int		npercentiles = 0;

		MemoryContext	oldcontext;

		oldcontext = MemoryContextSwitchTo(aggcontext);

		if (PG_NARGS() >= 3)
		{
			percentiles = (double *) palloc(sizeof(double));
			percentiles[0] = PG_GETARG_FLOAT8(2);
			npercentiles = 1;

			check_percentiles(percentiles, npercentiles);
		}

		state = tdigest_aggstate_allocate(npercentiles, 0, digest->compression);

		if (percentiles)
		{
			memcpy(state->percentiles, percentiles, sizeof(double) * npercentiles);
			pfree(percentiles);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	/*
	 * XXX should it be allowed to add digest to a state with a different
	 * compression value? Will it produce a "good" t-digest or does it break
	 * the assumptions and produce much worse estimates?
	 */

	/* copy data from the tdigest into the aggstate */
	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with a single value.
 */
Datum
tdigest_add_digest_values(PG_FUNCTION_ARGS)
{
	int					i;
	tdigest_aggstate_t *state;
	tdigest_t		   *digest;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_digest_values called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

	/* make sure we get digest with the new format */
	digest = tdigest_update_format(digest);

	/* if there's no aggregate state allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		double *values = NULL;
		int		nvalues = 0;

		MemoryContext	oldcontext;

		oldcontext = MemoryContextSwitchTo(aggcontext);

		if (PG_NARGS() >= 3)
		{
			values = (double *) palloc(sizeof(double));
			values[0] = PG_GETARG_FLOAT8(2);
			nvalues = 1;
		}

		state = tdigest_aggstate_allocate(0, nvalues, digest->compression);

		if (values)
		{
			memcpy(state->values, values, sizeof(double) * nvalues);
			pfree(values);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	/*
	 * XXX should it be allowed to add digest to a state with a different
	 * compression value? Will it produce a "good" t-digest or does it break
	 * the assumptions and produce much worse estimates?
	 */

	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with an array of percentiles.
 */
Datum
tdigest_add_double_array(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t *state;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double_array called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int compression = PG_GETARG_INT32(2);
		double *percentiles;
		int		npercentiles;
		MemoryContext	oldcontext;

		check_compression(compression);

		/* Percentiles are required in order to create the aggregate state. */
		if (PG_ARGISNULL(3))
			elog(ERROR, "percentiles must not be NULL");

		oldcontext = MemoryContextSwitchTo(aggcontext);

		percentiles = array_to_double(fcinfo,
									  PG_GETARG_ARRAYTYPE_P(3),
									  &npercentiles);

		check_percentiles(percentiles, npercentiles);

		state = tdigest_aggstate_allocate(npercentiles, 0, compression);

		memcpy(state->percentiles, percentiles, sizeof(double) * npercentiles);

		pfree(percentiles);

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with an array of percentiles.
 */
Datum
tdigest_add_double_array_count(PG_FUNCTION_ARGS)
{
	int64				i;
	int64				count;
	tdigest_aggstate_t *state;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double_array_count called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int compression = PG_GETARG_INT32(3);
		double *percentiles;
		int		npercentiles;
		MemoryContext	oldcontext;

		check_compression(compression);

		/* Percentiles are required in order to create the aggregate state. */
		if (PG_ARGISNULL(4))
			elog(ERROR, "percentiles must not be NULL");

		oldcontext = MemoryContextSwitchTo(aggcontext);

		percentiles = array_to_double(fcinfo,
									  PG_GETARG_ARRAYTYPE_P(4),
									  &npercentiles);

		check_percentiles(percentiles, npercentiles);

		state = tdigest_aggstate_allocate(npercentiles, 0, compression);

		memcpy(state->percentiles, percentiles, sizeof(double) * npercentiles);

		pfree(percentiles);

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	if (PG_ARGISNULL(2))
	{
		count = 1;
	}
	else
		count = PG_GETARG_INT64(2);

	/* can't add values with non-positive counts */
	if (count <= 0)
		elog(ERROR, "invalid count value %lld, must be a positive value",
			 (long long) count);

	/*
	 * When adding too many values (than would fit into an empty buffer, and
	 * thus likely causing too many compactions), we instead add them as
	 * properly sized centroids.
	 *
	 * This is much faster, because the centroids can be generated in one go,
	 * so there are only very few compactions.
	 */
	if (count > BUFFER_SIZE(state->compression))
	{
		tdigest_add_generated(state, PG_GETARG_FLOAT8(1), count);

		count = 0;
	}

	/*
	 * Add the values one by one, not as one large centroid with the count.
	 * We do it like this to allow proper compaction and sizing of centroids,
	 * otherwise we might end up with oversized centroid on the tails etc.
	 *
	 * XXX If this turns out a bit too expensive, we may try determining the
	 * size by looking for the smallest centroid covering this value.
	 */
	for (i = 0; i < count; i++)
		tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with an array of values.
 */
Datum
tdigest_add_double_array_values(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t *state;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double_array called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int compression = PG_GETARG_INT32(2);
		double *values;
		int		nvalues;
		MemoryContext	oldcontext;

		check_compression(compression);

		/* Values are required in order to create the aggregate state. */
		if (PG_ARGISNULL(3))
			elog(ERROR, "values must not be NULL");

		oldcontext = MemoryContextSwitchTo(aggcontext);

		values = array_to_double(fcinfo,
								 PG_GETARG_ARRAYTYPE_P(3),
								 &nvalues);

		state = tdigest_aggstate_allocate(0, nvalues, compression);

		memcpy(state->values, values, sizeof(double) * nvalues);

		pfree(values);

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with an array of values.
 */
Datum
tdigest_add_double_array_values_count(PG_FUNCTION_ARGS)
{
	int64				i;
	int64				count;
	tdigest_aggstate_t *state;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double_array_values_count called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int compression = PG_GETARG_INT32(3);
		double *values;
		int		nvalues;
		MemoryContext	oldcontext;

		check_compression(compression);

		/* Values are required in order to create the aggregate state. */
		if (PG_ARGISNULL(4))
			elog(ERROR, "values must not be NULL");

		oldcontext = MemoryContextSwitchTo(aggcontext);

		values = array_to_double(fcinfo,
								 PG_GETARG_ARRAYTYPE_P(4),
								 &nvalues);

		state = tdigest_aggstate_allocate(0, nvalues, compression);

		memcpy(state->values, values, sizeof(double) * nvalues);

		pfree(values);

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	if (PG_ARGISNULL(2))
	{
		count = 1;
	}
	else
		count = PG_GETARG_INT64(2);

	/* can't add values with non-positive counts */
	if (count <= 0)
		elog(ERROR, "invalid count value %lld, must be a positive value",
			 (long long) count);

	/*
	 * When adding too many values (than would fit into an empty buffer, and
	 * thus likely causing too many compactions), we instead add them as
	 * properly sized centroids.
	 *
	 * This is much faster, because the centroids can be generated in one go,
	 * so there are only very few compactions.
	 */
	if (count > BUFFER_SIZE(state->compression))
	{
		tdigest_add_generated(state, PG_GETARG_FLOAT8(1), count);

		count = 0;
	}

	/*
	 * Add the values one by one, not as one large centroid with the count.
	 * We do it like this to allow proper compaction and sizing of centroids,
	 * otherwise we might end up with oversized centroid on the tails etc.
	 *
	 * XXX If this turns out a bit too expensive, we may try determining the
	 * size by looking for the smallest centroid covering this value.
	 */
	for (i = 0; i < count; i++)
		tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a digest to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with an array of percentiles.
 */
Datum
tdigest_add_digest_array(PG_FUNCTION_ARGS)
{
	int					i;
	tdigest_aggstate_t *state;
	tdigest_t		   *digest;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_digest_array called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

	/* make sure we get digest with the new format */
	digest = tdigest_update_format(digest);

	/* if there's no aggregate state allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		double *percentiles;
		int		npercentiles;
		MemoryContext	oldcontext;

		/* Percentiles are required in order to create the aggregate state. */
		if (PG_ARGISNULL(2))
			elog(ERROR, "percentiles must not be NULL");

		oldcontext = MemoryContextSwitchTo(aggcontext);

		percentiles = array_to_double(fcinfo,
									  PG_GETARG_ARRAYTYPE_P(2),
									  &npercentiles);

		check_percentiles(percentiles, npercentiles);

		state = tdigest_aggstate_allocate(npercentiles, 0, digest->compression);

		memcpy(state->percentiles, percentiles, sizeof(double) * npercentiles);

		pfree(percentiles);

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	/*
	 * XXX should it be allowed to add digest to a state with a different
	 * compression value? Will it produce a "good" t-digest or does it break
	 * the assumptions and produce much worse estimates?
	 */

	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a digest to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with an array of values.
 */
Datum
tdigest_add_digest_array_values(PG_FUNCTION_ARGS)
{
	int					i;
	tdigest_aggstate_t *state;
	tdigest_t		   *digest;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_digest_array_values called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

	/* make sure we get digest with the new format */
	digest = tdigest_update_format(digest);

	/* if there's no aggregate state allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		double *values;
		int		nvalues;
		MemoryContext	oldcontext;

		/* Values are required in order to create the aggregate state. */
		if (PG_ARGISNULL(2))
			elog(ERROR, "values must not be NULL");

		oldcontext = MemoryContextSwitchTo(aggcontext);

		values = array_to_double(fcinfo,
								 PG_GETARG_ARRAYTYPE_P(2),
								 &nvalues);

		state = tdigest_aggstate_allocate(0, nvalues, digest->compression);

		memcpy(state->values, values, sizeof(double) * nvalues);

		pfree(values);

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	/*
	 * XXX should it be allowed to add digest to a state with a different
	 * compression value? Will it produce a "good" t-digest or does it break
	 * the assumptions and produce much worse estimates?
	 */

	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Compute percentile from a tdigest. Final function for tdigest aggregate
 * with a single percentile.
 */
Datum
tdigest_percentiles(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t	   *state;
	MemoryContext	aggcontext;
	double			ret;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_percentiles called in non-aggregate context");

	/* if there's no digest, return NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	tdigest_compute_quantiles(state, &ret);

	PG_RETURN_FLOAT8(ret);
}

/*
 * Compute percentile from a tdigest. Final function for tdigest aggregate
 * with a single percentile.
 */
Datum
tdigest_percentiles_of(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t	   *state;
	MemoryContext	aggcontext;
	double			ret;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_percentiles_of called in non-aggregate context");

	/* if there's no digest, return NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	tdigest_compute_quantiles_of(state, &ret);

	PG_RETURN_FLOAT8(ret);
}

/*
 * Build a t-digest varlena value from the aggegate state.
 */
Datum
tdigest_digest(PG_FUNCTION_ARGS)
{
	tdigest_t			   *digest;
	tdigest_aggstate_t	   *state;
	MemoryContext	aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_digest called in non-aggregate context");

	/* if there's no digest, return NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	digest = tdigest_aggstate_to_digest(state, true);

	PG_RETURN_POINTER(digest);
}

/*
 * Compute percentiles from a tdigest. Final function for tdigest aggregate
 * with an array of percentiles.
 */
Datum
tdigest_array_percentiles(PG_FUNCTION_ARGS)
{
	double	*result;
	MemoryContext aggcontext;

	tdigest_aggstate_t *state;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_array_percentiles called in non-aggregate context");

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	result = palloc(state->npercentiles * sizeof(double));

	tdigest_compute_quantiles(state, result);

	return double_to_array(fcinfo, result, state->npercentiles);
}

/*
 * Compute percentiles from a tdigest. Final function for tdigest aggregate
 * with an array of values.
 */
Datum
tdigest_array_percentiles_of(PG_FUNCTION_ARGS)
{
	double	*result;
	MemoryContext aggcontext;

	tdigest_aggstate_t *state;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_array_percentiles_of called in non-aggregate context");

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	result = palloc(state->nvalues * sizeof(double));

	tdigest_compute_quantiles_of(state, result);

	return double_to_array(fcinfo, result, state->nvalues);
}

Datum
tdigest_serial(PG_FUNCTION_ARGS)
{
	bytea	   *v;
	tdigest_aggstate_t  *state;
	Size		len;
	char	   *ptr;

	state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	len = offsetof(tdigest_aggstate_t, percentiles) +
		  state->npercentiles * sizeof(double) +
		  state->nvalues * sizeof(double) +
		  state->ncentroids * sizeof(centroid_t);

	v = palloc(len + VARHDRSZ);

	SET_VARSIZE(v, len + VARHDRSZ);
	ptr = VARDATA(v);

	memcpy(ptr, state, offsetof(tdigest_aggstate_t, percentiles));
	ptr += offsetof(tdigest_aggstate_t, percentiles);

	if (state->npercentiles > 0)
	{
		memcpy(ptr, state->percentiles, sizeof(double) * state->npercentiles);
		ptr += sizeof(double) * state->npercentiles;
	}

	if (state->nvalues > 0)
	{
		memcpy(ptr, state->values, sizeof(double) * state->nvalues);
		ptr += sizeof(double) * state->nvalues;
	}

	/* FIXME maybe don't serialize full centroids, but just sum/count */
	memcpy(ptr, state->centroids,
		   sizeof(centroid_t) * state->ncentroids);
	ptr += sizeof(centroid_t) * state->ncentroids;

	Assert(VARDATA(v) + len == ptr);

	PG_RETURN_POINTER(v);
}

/*
 * XXX Unlike the other "input" functions (tdigest_in/tdigest_recv), this
 * does not validate the digest at all. We assume this function is used only
 * on data we created in the same process (possibly in a parallel worker),
 * and not on untrusted values controlled by the user (which is why the other
 * input functions need the validation).
 */
Datum
tdigest_deserial(PG_FUNCTION_ARGS)
{
	bytea  *v = (bytea *) PG_GETARG_POINTER(0);
	char   *ptr = VARDATA_ANY(v);
	tdigest_aggstate_t	tmp;
	tdigest_aggstate_t *state;
	double			   *percentiles = NULL;
	double			   *values = NULL;

	/* copy aggstate header into a local variable */
	memcpy(&tmp, ptr, offsetof(tdigest_aggstate_t, percentiles));
	ptr += offsetof(tdigest_aggstate_t, percentiles);

	/* allocate and copy percentiles */
	if (tmp.npercentiles > 0)
	{
		percentiles = palloc(tmp.npercentiles * sizeof(double));
		memcpy(percentiles, ptr, tmp.npercentiles * sizeof(double));
		ptr += tmp.npercentiles * sizeof(double);
	}

	/* allocate and copy values */
	if (tmp.nvalues > 0)
	{
		values = palloc(tmp.nvalues * sizeof(double));
		memcpy(values, ptr, tmp.nvalues * sizeof(double));
		ptr += tmp.nvalues * sizeof(double);
	}

	state = tdigest_aggstate_allocate(tmp.npercentiles, tmp.nvalues,
									  tmp.compression);

	if (tmp.npercentiles > 0)
	{
		memcpy(state->percentiles, percentiles, tmp.npercentiles * sizeof(double));
		pfree(percentiles);
	}

	if (tmp.nvalues > 0)
	{
		memcpy(state->values, values, tmp.nvalues * sizeof(double));
		pfree(values);
	}

	/* copy the data into the newly-allocated state */
	memcpy(state, &tmp, offsetof(tdigest_aggstate_t, percentiles));
	/* we don't need to move the pointer */

	/* copy the centroids back */
	memcpy(state->centroids, ptr,
		   sizeof(centroid_t) * state->ncentroids);
	ptr += sizeof(centroid_t) * state->ncentroids;

	PG_RETURN_POINTER(state);
}

static tdigest_aggstate_t *
tdigest_copy(tdigest_aggstate_t *state)
{
	tdigest_aggstate_t *copy;

	copy = tdigest_aggstate_allocate(state->npercentiles, state->nvalues,
									 state->compression);

	memcpy(copy, state, offsetof(tdigest_aggstate_t, percentiles));

	if (state->nvalues > 0)
		memcpy(copy->values, state->values,
			   sizeof(double) * state->nvalues);

	if (state->npercentiles > 0)
		memcpy(copy->percentiles, state->percentiles,
			   sizeof(double) * state->npercentiles);

	memcpy(copy->centroids, state->centroids,
		   state->ncentroids * sizeof(centroid_t));

	return copy;
}

Datum
tdigest_combine(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t	 *src;
	tdigest_aggstate_t	 *dst;
	MemoryContext aggcontext;
	MemoryContext oldcontext;
	int	i;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_combine called in non-aggregate context");

	/* if no "merged" state yet, try creating it */
	if (PG_ARGISNULL(0))
	{
		/* nope, the second argument is NULL to, so return NULL */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();

		/* the second argument is not NULL, so copy it */
		src = (tdigest_aggstate_t *) PG_GETARG_POINTER(1);

		/* copy the digest into the right long-lived memory context */
		oldcontext = MemoryContextSwitchTo(aggcontext);
		src = tdigest_copy(src);
		MemoryContextSwitchTo(oldcontext);

		PG_RETURN_POINTER(src);
	}

	/*
	 * If the second argument is NULL, just return the first one (we know
	 * it's not NULL at this point).
	 */
	if (PG_ARGISNULL(1))
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));

	/* Now we know neither argument is NULL, so merge them. */
	src = (tdigest_aggstate_t *) PG_GETARG_POINTER(1);
	dst = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	AssertCheckTDigestAggState(dst);
	AssertCheckTDigestAggState(src);

	/*
	 * XXX should it be allowed to add digest to a state with a different
	 * compression value? Will it produce a "good" t-digest or does it break
	 * the assumptions and produce much worse estimates?
	 */

	/* copy data from the tdigest into the aggstate */
	for (i = 0; i < src->ncentroids; i++)
		tdigest_add_centroid(dst, src->centroids[i].mean,
								  src->centroids[i].count);

	AssertCheckTDigestAggState(dst);

	PG_RETURN_POINTER(dst);
}

/* API for incremental updates */

/*
 * expand the t-digest into an in-memory aggregate state
 */
static tdigest_aggstate_t *
tdigest_digest_to_aggstate(tdigest_t *digest)
{
	int					i;
	tdigest_aggstate_t *state;

	/* make sure we get digest with the new format */
	digest = tdigest_update_format(digest);

	state = tdigest_aggstate_allocate(0, 0, digest->compression);

	/* copy data from the tdigest into the aggstate */
	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state,
							 digest->centroids[i].mean,
							 digest->centroids[i].count);

	AssertCheckTDigestAggState(state);

	return state;
}

/*
 * Add a single value to the t-digest. This is not very efficient, as it has
 * to deserialize the t-digest into the in-memory aggstate representation
 * and serialize it back for each call, but it's convenient and acceptable
 * for some use cases.
 *
 * When efficiency is important, it may be possible to use the batch variant
 * with first aggregating the updates into a t-digest, and then merge that
 * into an existing t-digest in one step using tdigest_union_double_increment
 *
 * This is similar to hll_add, while the "union" is more like hll_union.
 */
Datum
tdigest_add_double_increment(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t *state;
	bool				compact = PG_GETARG_BOOL(3);

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int		compression;

		/*
		 * We don't require compression, but only when there is an existing
		 * t-digest value. Make sure the value was supplied.
		 */
		if (PG_ARGISNULL(2))
			elog(ERROR, "compression value not supplied, but t-digest is NULL");

		compression = PG_GETARG_INT32(2);

		check_compression(compression);

		state = tdigest_aggstate_allocate(0, 0, compression);
	}
	else
		state = tdigest_digest_to_aggstate(PG_GETARG_TDIGEST(0));

	tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(tdigest_aggstate_to_digest(state, compact));
}

/*
 * Add an array of values to the t-digest. This amortizes the overhead of
 * deserializing and serializing the t-digest, compared to the per-value
 * version.
 *
 * When efficiency is important, it may be possible to use the batch variant
 * with first aggregating the updates into a t-digest, and then merge that
 * into an existing t-digest in one step using tdigest_union_double_increment
 *
 * This is similar to hll_add, while the "union" is more like hll_union.
 */
Datum
tdigest_add_double_array_increment(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t *state;
	bool				compact = PG_GETARG_BOOL(3);
	double			   *values;
	int					nvalues;
	int					i;

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		int		compression;

		/*
		 * We don't require compression, but only when there is an existing
		 * t-digest value. Make sure the value was supplied.
		 */
		if (PG_ARGISNULL(2))
			elog(ERROR, "compression value not supplied, but t-digest is NULL");

		compression = PG_GETARG_INT32(2);

		check_compression(compression);

		state = tdigest_aggstate_allocate(0, 0, compression);
	}
	else
		state = tdigest_digest_to_aggstate(PG_GETARG_TDIGEST(0));

	values = array_to_double(fcinfo,
							 PG_GETARG_ARRAYTYPE_P(1),
							 &nvalues);

	for (i = 0; i < nvalues; i++)
		tdigest_add(state, values[i]);

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(tdigest_aggstate_to_digest(state, compact));
}

/*
 * Merge a t-digest into another t-digest. This is somewaht inefficient, as
 * it has to deserialize the t-digests into the in-memory aggstate values,
 * and serialize it back for each call, but it's better than doing it for
 * each individual value (like tdigest_union_double_increment).
 *
 * This is similar to hll_union.
 */
Datum
tdigest_union_double_increment(PG_FUNCTION_ARGS)
{
	int					i;
	tdigest_aggstate_t *state;
	tdigest_t		   *digest;
	bool				compact = PG_GETARG_BOOL(2);

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();
	else if (PG_ARGISNULL(0))
		PG_RETURN_POINTER(PG_GETARG_POINTER(1));
	else if (PG_ARGISNULL(1))
		PG_RETURN_POINTER(PG_GETARG_POINTER(0));

	/* now we know both arguments are non-null */

	/* parse the first digest (we'll merge the other one into this) */
	state = tdigest_digest_to_aggstate(PG_GETARG_TDIGEST(0));
	AssertCheckTDigestAggState(state);

	/* parse the second digest */
	digest = PG_GETARG_TDIGEST(1);

	/* make sure we get a digest with the new format */
	digest = tdigest_update_format(digest);

	AssertCheckTDigest(digest);

	/* copy data from the tdigest into the aggstate */
	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(tdigest_aggstate_to_digest(state, compact));
}


/*
 * Comparator, ordering the centroids by mean value.
 *
 * When the mean is the same, we try ordering the centroids by count.
 *
 * In principle, centroids with the same mean represent the same value,
 * but we still need to care about the count to allow rebalancing the
 * centroids later.
 */
static int
centroid_cmp(const void *a, const void *b)
{
	double	ma, mb;

	centroid_t *ca = (centroid_t *) a;
	centroid_t *cb = (centroid_t *) b;

	ma = ca->mean;
	mb = cb->mean;

	if (ma < mb)
		return -1;
	else if (ma > mb)
		return 1;

	if (ca->count < cb->count)
		return -1;
	else if (ca->count > cb->count)
		return 1;

	return 0;
}

/*
 * Parsing of the textual t-digest representation.
 *
 * We can't use sscanf, because it does not report overflows in any way - the
 * value simply saturates to the maximum for the data type. That's a problem
 * for the count, where the saturated value is a perfectly valid count, so we
 * can't detect it after the fact. Use strtoll/strtod, which do set errno.
 *
 * All of these advance the pointer past the parsed part on success, and never
 * return on failure.
 */

/*
 * Match a literal string, after skipping (optional) leading space.
 */
static void
parse_str(char **ptr, const char *value, bool space)
{
	char   *str = *ptr;
	size_t	len = strlen(value);

	/* if requested, skip the one initial space character */
	if (space)
	{
		if (isspace((unsigned char) *str))
			str++;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("failed to parse t-digest value, missing space")));
	}

	/* at this point there must be no whitespace */
	if (isspace((unsigned char) *str))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("failed to parse t-digest value, unexpected space")));

	/* the prefix should match our string */
	if (strncmp(str, value, len) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("failed to parse t-digest value, expected \"%s\"",
						value)));

	*ptr = str + len;
}

/*
 * Parse an int64 value, and make sure it's in range.
 */
static int64
parse_int64(char **ptr, const char *field)
{
	char   *endptr;
	int64	value;

	errno = 0;
	value = strtoi64(*ptr, &endptr, 10);

	if (endptr == *ptr)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("failed to parse %s of a t-digest", field)));

	if (errno == ERANGE)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("%s of a t-digest is out of range for bigint", field)));

	*ptr = endptr;

	return value;
}

/*
 * Parse an int32 value, and make sure it's in range.
 *
 * Parse it as int64 first, so that we can range check it before narrowing it
 * down, instead of relying on the (undefined) conversion.
 */
static int32
parse_int32(char **ptr, const char *field)
{
	int64	value = parse_int64(ptr, field);

	if (value < PG_INT32_MIN || value > PG_INT32_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("%s of a t-digest is out of range for integer", field)));

	return (int32) value;
}

/*
 * Parse a double value, and make sure it's in range.
 */
static double
parse_double(char **ptr, const char *field)
{
	char   *endptr;
	double	value;

	errno = 0;
	value = strtod(*ptr, &endptr);

	if (endptr == *ptr)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("failed to parse %s of a t-digest", field)));

	if ((errno == ERANGE) && !isfinite(value))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("%s of a t-digest is out of range for double precision",
						field)));

	*ptr = endptr;

	return value;
}

Datum
tdigest_in(PG_FUNCTION_ARGS)
{
	int			i;
	char	   *str = PG_GETARG_CSTRING(0);
	tdigest_t  *digest = NULL;
	size_t		slen;

	/* t-digest header fields */
	int32       flags;
	int64		count,
				total_count;
	int			compression;
	int			ncentroids;
	char	   *ptr;

	slen = strlen(str);

	ptr = str;

	parse_str(&ptr, "flags", false);
	flags = parse_int32(&ptr, "flags");

	parse_str(&ptr, "count", true);
	count = parse_int64(&ptr, "count");

	parse_str(&ptr, "compression", true);
	compression = parse_int32(&ptr, "compression");

	parse_str(&ptr, "centroids", true);
	ncentroids = parse_int32(&ptr, "number of centroids");

	if ((flags & ~TDIGEST_VALID_FLAGS) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid flags for t-digest")));

	if ((compression < MIN_COMPRESSION) || (compression > MAX_COMPRESSION))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("compression for t-digest must be in [%d, %d]",
						MIN_COMPRESSION, MAX_COMPRESSION)));

	if (count <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("count value for the t-digest must be positive")));

	if (ncentroids <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("number of centroids for the t-digest must be positive")));

	if (ncentroids > BUFFER_SIZE(compression))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("number of centroids for the t-digest exceeds buffer size")));

	digest = tdigest_allocate(ncentroids);

	digest->flags = flags;
	digest->count = count;
	digest->ncentroids = ncentroids;
	digest->compression = compression;

	total_count = 0;
	ncentroids = 0;
	for (i = 0; i < digest->ncentroids; i++)
	{
		double	mean;

		parse_str(&ptr, "(", true);
		mean = parse_double(&ptr, "mean of a centroid");
		parse_str(&ptr, ",", false);
		count = parse_int64(&ptr, "count of a centroid");
		parse_str(&ptr, ")", false);

		/*
		 * Not sure if this can happen with text input, but better to keep the
		 * checks the same as in tdigest_recv.
		 */
		if (!isfinite(mean))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("mean value for all centroids in a t-digest must be valid")));

		if (count <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("count value for all centroids in a t-digest must be positive")));
		else if (count > digest->count)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("count value of a centroid exceeds total count")));

		digest->centroids[i].count = count;
		digest->centroids[i].mean = mean;

		/*
		 * track the total count so that we can check later
		 *
		 * Make sure the count does not overflow at any point. It could
		 * overflow and then wrap around to the expected total, but it would
		 * still cause an issue.
		 */
		if (pg_add_s64_overflow(total_count, count, &total_count))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("tdigest count overflow")));

		/*
		 * This can't overflow - each centroid has a positive count, and if
		 * the total_count does not overflow, this can't either.
		 */
		ncentroids++;

		/*
		 * The parsing already moved the pointer past the closing parenthesis.
		 * If this is the end of the string, stop parsing, even if we failed to
		 * parse the right number of centroids.
		 */
		if (*ptr == '\0')
			break;

		/* must not scan past the end of the input string */
		Assert(ptr <= str + slen);
	}

	/*
	 * Malformed inputs may have the wrong number of centroids, in which case
	 * we either don't consume the whole input (ncentroids too high), or we
	 * don't get all the expected centroids (ncentroids too high).
	 */
	if (ptr < str + slen)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input t-digest value too long")));

	if (ncentroids != digest->ncentroids)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input t-digest value too short")));

	/*
	 * If we consumed just the right number of centroids, we must have read
	 * the whole input value exactly.
	 */
	Assert(ptr == str + slen);

	/* check that the total matches */
	if (total_count != digest->count)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("total count does not match the data (%lld != %lld)",
						(long long) total_count, (long long) digest->count)));

	/*
	 * Make sure we return digest with the new format (it might be the
	 * old format, in which case "mean" fields actually store "sum").
	 */
	digest = tdigest_update_format(digest);

	AssertCheckTDigest(digest);

	PG_RETURN_POINTER(digest);
}

Datum
tdigest_out(PG_FUNCTION_ARGS)
{
	int			i;
	tdigest_t  *digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	StringInfoData	str;

	AssertCheckTDigest(digest);

	initStringInfo(&str);

	appendStringInfo(&str, "flags %d count " INT64_FORMAT " compression %d centroids %d",
					 digest->flags, digest->count, digest->compression,
					 digest->ncentroids);

	/*
	 * If this is an old tdigest with sum values, we'll send those, and
	 * it's up to the reader to fix it. It'll be indicated by not having
	 * the TDIGEST_STORES_MEAN flag.
	 */
	for (i = 0; i < digest->ncentroids; i++)
	{
		char *tmp = float8out_internal(digest->centroids[i].mean);

		appendStringInfo(&str, " (%s, " INT64_FORMAT ")",
						 tmp, digest->centroids[i].count);
		pfree(tmp);
	}

	PG_RETURN_CSTRING(str.data);
}

Datum
tdigest_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	tdigest_t  *digest;
	int			i;
	int64		count;
	int64		total_count;
	int32		flags;
	int32		compression;
	int32		ncentroids;

	flags = pq_getmsgint(buf, sizeof(int32));

	/* make sure the t-digest format is supported */
	if ((flags != 0) && (flags != TDIGEST_STORES_MEAN))
		elog(ERROR, "unsupported t-digest on-disk format");

	count = pq_getmsgint64(buf);
	compression = pq_getmsgint(buf, sizeof(int32));
	ncentroids = pq_getmsgint(buf, sizeof(int32));

	if ((compression < MIN_COMPRESSION) || (compression > MAX_COMPRESSION))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("compression for t-digest must be in [%d, %d]",
						MIN_COMPRESSION, MAX_COMPRESSION)));

	if (count <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("count value for the t-digest must be positive")));

	if (ncentroids <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("number of centroids for the t-digest must be positive")));

	if (ncentroids > BUFFER_SIZE(compression))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("number of centroids for the t-digest exceeds buffer size")));

	digest = tdigest_allocate(ncentroids);

	digest->flags = flags;
	digest->count = count;
	digest->compression = compression;
	digest->ncentroids = ncentroids;

	total_count = 0;
	for (i = 0; i < digest->ncentroids; i++)
	{
		digest->centroids[i].mean = pq_getmsgfloat8(buf);
		digest->centroids[i].count = pq_getmsgint64(buf);

		if (!isfinite(digest->centroids[i].mean))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("mean value for all centroids in a t-digest must be valid")));

		if (digest->centroids[i].count <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("count value for all centroids in a t-digest must be positive")));
		else if (digest->centroids[i].count > digest->count)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("count value of a centroid exceeds total count")));

		/*
		 * track the total count so that we can check later
		 *
		 * Make sure the count does not overflow at any point. It could
		 * overflow and then wrap around to the expected total, but it would
		 * still cause an issue.
		 */
		if (pg_add_s64_overflow(total_count, digest->centroids[i].count,
								&total_count))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("tdigest count overflow")));
	}

	/* check that the total matches */
	if (total_count != digest->count)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("total count does not match the data (%lld != %lld)",
						(long long) total_count, (long long) digest->count)));

	/*
	 * Make sure we return digest with the new format (it might be the
	 * old format, in which case "mean" fields actually store "sum").
	 */
	digest = tdigest_update_format(digest);

	AssertCheckTDigest(digest);

	PG_RETURN_POINTER(digest);
}

Datum
tdigest_send(PG_FUNCTION_ARGS)
{
	tdigest_t  *digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	StringInfoData buf;
	int			i;

	pq_begintypsend(&buf);

	pq_sendint(&buf, digest->flags, 4);
	pq_sendint64(&buf, digest->count);
	pq_sendint(&buf, digest->compression, 4);
	pq_sendint(&buf, digest->ncentroids, 4);

	for (i = 0; i < digest->ncentroids; i++)
	{
		pq_sendfloat8(&buf, digest->centroids[i].mean);
		pq_sendint64(&buf, digest->centroids[i].count);
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * tdigest_is_valid
 *		Check the t-digest passes the same sanity checks as the input funcs.
 *
 * The input functions (tdigest_in and tdigest_recv) validate the values, so
 * it's not possible to construct an invalid digest through them. But digests
 * stored by older versions of the extension (which did not have all those
 * checks) are not re-validated when read back, so they may be broken in
 * various ways - bogus flags, compression out of range, centroid counts not
 * adding up to the total count, and so on. This function performs the same
 * checks on an existing value, allowing users to find such digests.
 *
 * Returns true if the digest passes all the checks, false otherwise. Never
 * raises an error for an invalid digest (that's the whole point).
 *
 * The one check the input functions don't need to do explicitly is on the
 * length of the value - they build the digest from the parsed header, so it
 * always matches. For an existing value we have to check the value really is
 * long enough for the centroids the header promises, otherwise we'd read past
 * the end of it.
 *
 * XXX Keep this in sync with the checks in tdigest_in and tdigest_recv.
 */
Datum
tdigest_is_valid(PG_FUNCTION_ARGS)
{
	int			i;
	int64		total_count;
	Size		vlen;
	Size		expected;
	tdigest_t  *digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	vlen = VARSIZE_ANY(digest);

	/*
	 * We need at least the header, otherwise we can't even look at the fields
	 * describing the rest of the value.
	 */
	if (vlen < offsetof(tdigest_t, centroids))
		PG_RETURN_BOOL(false);

	/* make sure the t-digest format is supported */
	if ((digest->flags & ~TDIGEST_VALID_FLAGS) != 0)
		PG_RETURN_BOOL(false);

	if ((digest->compression < MIN_COMPRESSION) ||
		(digest->compression > MAX_COMPRESSION))
		PG_RETURN_BOOL(false);

	if (digest->count <= 0)
		PG_RETURN_BOOL(false);

	if (digest->ncentroids <= 0)
		PG_RETURN_BOOL(false);

	if (digest->ncentroids > BUFFER_SIZE(digest->compression))
		PG_RETURN_BOOL(false);

	/*
	 * The header determines how long the value has to be, so make sure it
	 * really is that long before reading any of the centroids.
	 *
	 * The number of centroids is limited by the buffer size (checked above),
	 * so this can't overflow.
	 */
	expected = offsetof(tdigest_t, centroids) +
			   digest->ncentroids * sizeof(centroid_t);

	if (vlen != expected)
		PG_RETURN_BOOL(false);

	total_count = 0;
	for (i = 0; i < digest->ncentroids; i++)
	{
		if (!isfinite(digest->centroids[i].mean))
			PG_RETURN_BOOL(false);

		if (digest->centroids[i].count <= 0)
			PG_RETURN_BOOL(false);
		else if (digest->centroids[i].count > digest->count)
			PG_RETURN_BOOL(false);

		/*
		 * track the total count so that we can check later
		 *
		 * Make sure the count does not overflow at any point. It could
		 * overflow and then wrap around to the expected total, but it would
		 * still cause an issue.
		 */
		if (pg_add_s64_overflow(total_count, digest->centroids[i].count,
								&total_count))
			PG_RETURN_BOOL(false);
	}

	/* check that the total matches */
	if (total_count != digest->count)
		PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(true);
}

Datum
tdigest_count(PG_FUNCTION_ARGS)
{
	tdigest_t  *digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	PG_RETURN_INT64(digest->count);
}

/*
 * tdigest_to_json
 *		Transform the tdigest into a JSON value.
 *
 * We make sure to always print mean, even for tdigests in the older format
 * storing sum for centroids. Otherwise the "mean" key would be confusing.
 * But we don't call tdigest_update_format, and instead we simply update the
 * flags and convert the sum/mean values.
 *
 * The centroids are stored in two separate arrays - one for means, one for
 * counts. That makes it easier to process, because it's clear the i-th
 * in each array is for i-th centroid. We might store it in a single array,
 * but then we'd have to walk it in pairs. And it'd mix float and int
 * values in the same array.
 */
Datum
tdigest_to_json(PG_FUNCTION_ARGS)
{
	int				i;
	StringInfoData	str;
	tdigest_t	   *digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	int32			flags = digest->flags;

	initStringInfo(&str);

	appendStringInfoChar(&str, '{');

	flags |= TDIGEST_STORES_MEAN;

	appendStringInfo(&str, "\"flags\": %d, ", flags);
	appendStringInfo(&str, "\"count\": " INT64_FORMAT ", ", digest->count);
	appendStringInfo(&str, "\"compression\": %d, ", digest->compression);
	appendStringInfo(&str, "\"centroids\": %d, ", digest->ncentroids);

	appendStringInfoString(&str, "\"mean\": [");

	for (i = 0; i < digest->ncentroids; i++)
	{
		double	mean = digest->centroids[i].mean;
		char *tmp;

		if (i > 0)
			appendStringInfoString(&str, ", ");

		/*
		 * When the TDIGEST_STORES_MEAN flags is not set, the value is
		 * actually a sum, so convert it to mean now. We have to check the
		 * diget->flags, not the local variable.
		 */
		if (! (digest->flags & TDIGEST_STORES_MEAN))
			mean = mean / digest->centroids[i].count;

		tmp = float8out_internal(mean);

		/* don't print insignificant zeroes to the right of decimal point */
		appendStringInfo(&str, "%s", tmp);
		pfree(tmp);
	}

	appendStringInfoString(&str, "], ");

	appendStringInfoString(&str, "\"count\": [");

	for (i = 0; i < digest->ncentroids; i++)
	{
		if (i > 0)
			appendStringInfoString(&str, ", ");

		appendStringInfo(&str, INT64_FORMAT, digest->centroids[i].count);
	}

	appendStringInfoString(&str, "]");

	appendStringInfoChar(&str, '}');

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}

/*
 * tdigest_to_array
 *		Transform the tdigest into an array of double values.
 *
 * The whole digest is stored in a single "double precision" array, which
 * may be a bit confusing and perhaps fragile if more fields need to be
 * added in the future. The initial elements are flags, count (number of
 * items added to the digest), compression (determines the limit on number
 * of centroids) and current number of centroids. Follows stream of values
 * encoding the centroids in pairs of (mean, count).
 *
 * We make sure to always print mean, even for tdigests in the older format
 * storing sum for centroids. Otherwise the "mean" key would be confusing.
 * But we don't call tdigest_update_format, and instead we simply update the
 * flags and convert the sum/mean values.
 */
Datum
tdigest_to_array(PG_FUNCTION_ARGS)
{
	int				i,
					idx;
	tdigest_t	   *digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	int32			flags = digest->flags;
	double		   *values;
	int				nvalues;

	flags |= TDIGEST_STORES_MEAN;

	/* number of values to store in the array */
	nvalues = 4 + (digest->ncentroids * 2);
	values = (double *) palloc(sizeof(double) * nvalues);

	idx = 0;
	values[idx++] = flags;
	values[idx++] = digest->count;
	values[idx++] = digest->compression;
	values[idx++] = digest->ncentroids;

	for (i = 0; i < digest->ncentroids; i++)
	{
		double	mean = digest->centroids[i].mean;

		/*
		 * When the TDIGEST_STORES_MEAN flags is not set, the value is
		 * actually a sum, so convert it to mean now. We have to check the
		 * diget->flags, not the local variable.
		 */
		if (! (digest->flags & TDIGEST_STORES_MEAN))
			mean = mean / digest->centroids[i].count;

		/* don't print insignificant zeroes to the right of decimal point */
		values[idx++] = mean;
		values[idx++] = digest->centroids[i].count;
	}

	Assert(idx == nvalues);

	return double_to_array(fcinfo, values, nvalues);
}

Datum
tdigest_add_double_trimmed(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t *state;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double_trimmed called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext;
		int		compression = PG_GETARG_INT32(2);
		double	low = PG_GETARG_FLOAT8(3);
		double	high = PG_GETARG_FLOAT8(4);

		check_compression(compression);

		check_trim_values(low, high);

		oldcontext = MemoryContextSwitchTo(aggcontext);

		state = tdigest_aggstate_allocate(0, 0, compression);
		state->trim_low = low;
		state->trim_high = high;

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

Datum
tdigest_add_double_count_trimmed(PG_FUNCTION_ARGS)
{
	int64	i;
	int64	count;
	tdigest_aggstate_t *state;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_double_count_trimmed called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	/* if there's no digest allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext;
		int		compression = PG_GETARG_INT32(3);
		double	low = PG_GETARG_FLOAT8(4);
		double	high = PG_GETARG_FLOAT8(5);

		check_compression(compression);

		check_trim_values(low, high);

		oldcontext = MemoryContextSwitchTo(aggcontext);

		state = tdigest_aggstate_allocate(0, 0, compression);
		state->trim_low = low;
		state->trim_high = high;

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	if (PG_ARGISNULL(2))
		count = 1;
	else
		count = PG_GETARG_INT64(2);

	/* can't add values with non-positive counts */
	if (count <= 0)
		elog(ERROR, "invalid count value %lld, must be a positive value",
			 (long long) count);

	/*
	 * When adding too many values (than would fit into an empty buffer, and
	 * thus likely causing too many compactions), we instead add them as
	 * properly sized centroids.
	 *
	 * This is much faster, because the centroids can be generated in one go,
	 * so there are only very few compactions.
	 */
	if (count > BUFFER_SIZE(state->compression))
	{
		tdigest_add_generated(state, PG_GETARG_FLOAT8(1), count);

		count = 0;
	}

	/*
	 * If there are only a couple values, just add them one by one, so that
	 * we do proper compaction and sizing of centroids. Otherwise we might end
	 * up with oversized centroid on the tails etc.
	 */
	for (i = 0; i < count; i++)
		tdigest_add(state, PG_GETARG_FLOAT8(1));

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Add a value to the tdigest (create one if needed). Transition function
 * for tdigest aggregate with a single value.
 */
Datum
tdigest_add_digest_trimmed(PG_FUNCTION_ARGS)
{
	int					i;
	tdigest_aggstate_t *state;
	tdigest_t		   *digest;

	MemoryContext aggcontext;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_add_digest_trimmed called in non-aggregate context");

	/*
	 * We want to skip NULL values altogether - we return either the existing
	 * t-digest (if it already exists) or NULL.
	 */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();

		/* if there already is a state accumulated, don't forget it */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

	/* make sure we get digest with the new format */
	digest = tdigest_update_format(digest);

	/* if there's no aggregate state allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext;
		double	low = PG_GETARG_FLOAT8(2);
		double	high = PG_GETARG_FLOAT8(3);

		check_trim_values(low, high);

		oldcontext = MemoryContextSwitchTo(aggcontext);
		state = tdigest_aggstate_allocate(0, 0, digest->compression);
		state->trim_low = low;
		state->trim_high = high;

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	/*
	 * XXX should it be allowed to add digest to a state with a different
	 * compression value? Will it produce a "good" t-digest or does it break
	 * the assumptions and produce much worse estimates?
	 */

	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

	AssertCheckTDigestAggState(state);

	PG_RETURN_POINTER(state);
}

/*
 * Convert a (count * fraction) product back to a count.
 *
 * The product is calculated in double, so for very high counts it may end up
 * outside the int64 range - even for frac = 1.0. For example
 *
 * 1.000000 * 9223372036854775296 = 9223372036854775808.000000
 *
 * which is above both the count and INT64_MAX (9223372036854775807). That
 * makes the conversion undefined. It can't lead to underflow/overflow, as
 * we're not using this to access memory, but it might lead to bogus results
 * (e.g. NULL instead of the correct result).
 *
 * Clamp it to the [0, maxvalue] range, which is the only range that makes
 * sense anyway.
 *
 * Note: We know the frac value is in [0.0, 1.0], but we don't rely on that
 * here. We'll clamp it to [0, maxvalue].
 */
static int64
double_to_int64(double value, int64 maxvalue)
{
	/* paranoia: we should not get NaN values here */
	if (isnan(value))
		return 0;

	/* clamp it to the [0, count] range */
	if (value < 0)
		return 0;

	/*
	 * The comparison is done in double on purpose. If we did it as int64,
	 * it might already overflow and wrap. Converting count to double may
	 * round it up to 2^63, but that should be tine - the comparison is
	 * still correct, and we return count for anything that large.
	 */
	if (value >= (double) maxvalue)
		return maxvalue;

	/* ok, should be safe to count */
	return (int64) value;
}

/*
 * How many items of a centroid to use for the aggregate?
 *
 * Calculates the number of items of a centroid that fall into the
 * [count_low, count_high) range of items, with count_done items preceding
 * the centroid.
 *
 * The centroids in the middle of the range are included as a whole, but the
 * first and last one may be cut in half by the boundary, in which case only
 * part of the centroid is included.
 */
static int64
tdigest_trimmed_count(centroid_t *centroid, int64 count_done,
					  int64 count_low, int64 count_high)
{
	int64	count_add;

	/* Assume the whole centroid falls into the range. */
	count_add = centroid->count;

	/*
	 * If we haven't reached the low threshold yet, skip appropriate
	 * part of the centroid.
	 *
	 * (count_low - count_done) is how far we're from the low threshold, so a
	 * positive value is how many items we still need to "skip" (capped to
	 * size of the centroid). A negative value means we're past the low
	 * threshold, and the centroid may be in the range (unless it's past the
	 * high threshold too).
	 */
	count_add -= Min(Max(0, count_low - count_done),
					 count_add);

	/*
	 * If we have reached the upper threshold, ignore the overflowing
	 * part of the centroid.
	 *
	 * The items we still have start at count_low (or at the beginning of
	 * the centroid, whichever comes later), not at count_done - the part
	 * below count_low was already removed by the preceding step. Don't
	 * count that part a second time, i.e. don't start at the beginning
	 * of the centroid.
	 */
	count_add = Min(Max(0, count_high - Max(count_done, count_low)),
					count_add);

	return count_add;
}

/*
 * Calculate trimmed aggregates from centroids.
 *
 * Returns the trimmed mean, the trimmed sum, and the number of items in the
 * trimmed range.
 *
 * The obvious way to calculate the mean is to add (mean * count) for all the
 * centroids in the range, and then divide the sum by the number of items.
 * That however may overflow to infinity, even when the mean is perfectly
 * representable - the average of 1e307 values is 1e307, but the sum of many
 * such values is not. And once one partial sum saturates to +Infinity and
 * another one to -Infinity, the accumulator turns into NaN.
 *
 * The mean of a digest is always within the range of the centroid means, so
 * it should never overflow. We just have to calculate it in a way that does
 * not overflow either, i.e. as a weighted average of the centroid means,
 * similarly to how tdigest_compact() does when merging centroids
 *
 *     mean += centroids[i].mean * (count_add / total_count)
 *
 * The weights are non-negative and add up to 1.0 (albeit maybe not perfectly,
 * due to limited precision of float8), so the running mean stays within the
 * range of the centroid means.
 *
 * This needs the total number of items in the range up front, so we walk the
 * centroids twice. The first pass determines the first and last centroid of
 * the range, and adds up the (int64) counts - that can't overflow, as the
 * counts add up to the total count of the digest.
 *
 * The sum, on the other hand, may legitimately exceed the float8 range, so we
 * keep accumulating it the simple way (which is also exact), and leave it to
 * the caller to complain about the overflow.
 */
static void
tdigest_trimmed_agg(centroid_t *centroids, int ncentroids,
					int64 count, double low, double high,
					double *meanp, double *sump, int64 *countp)
{
	int		i;
	int		first = 0,
			last = -1;
	double	mean = 0,
			sum = 0;
	int64	count_done = 0,
			count_first = 0,
			count_total = 0,
			count_low,
			count_high;

	/* translate the percentiles to counts */
	count_low = double_to_int64(floor(count * low), count);
	count_high = double_to_int64(ceil(count * high), count);

	/* verify sane range */
	Assert((count_low <= count_high) && (0 <= count_low)  && (count_high <= count));

	*meanp = 0;
	*sump = 0;
	*countp = 0;

	/*
	 * Find the first and last centroid of the range, and the number of items
	 * the range contains.
	 */
	for (i = 0; i < ncentroids; i++)
	{
		int64	count_add = tdigest_trimmed_count(&centroids[i], count_done,
												  count_low, count_high);

		if (count_add > 0)
		{
			/* remember where the range starts, including the item offset */
			if (last == -1)
			{
				first = i;
				count_first = count_done;
			}

			last = i;
			count_total += count_add;
		}

		/* consider the whole centroid processed */
		count_done += centroids[i].count;

		/* break once we cross the high threshold */
		if (count_done >= count_high)
			break;
	}

	/* no items in the range, the callers return NULL in that case */
	if (count_total == 0)
		return;

	/* now walk just the centroids in the range, and calculate mean / sum */
	count_done = count_first;

	for (i = first; i <= last; i++)
	{
		int64	count_add = tdigest_trimmed_count(&centroids[i], count_done,
												  count_low, count_high);

		/* consider the whole centroid processed */
		count_done += centroids[i].count;

		/* increment the mean / sum */
		mean += centroids[i].mean * (count_add / (double) count_total);
		sum += centroids[i].mean * count_add;

		Assert(!isnan(mean));
	}

	// Assert((centroids[first].mean <= mean) && (mean <= centroids[last].mean));

	/*
	 * paranoia: handle possible overflow of the mean by clamping it to the
	 * valid range using the mean of the first/last centroid
	 *
	 * Maybe overflow is not the right term, but what can happen easily is
	 * the value "drifting" outside the valid range with very high counts,
	 * even if the centroids have the exact same mean. For example with
	 * centroids like (1, 22522773787004704) and (1, 22627252715671912) the
	 * mean will drift to 1.0000000000000002.
	 *
	 * To confirm, uncomment the assert above, and rerun tests. There's a
	 * query that triggers it.
	 */
	mean = Max(Min(centroids[last].mean, mean),
			   centroids[first].mean);

	*meanp = mean;
	*sump = sum;
	*countp = count_total;
}

/*
 * Return the trimmed sum, and complain if it overflowed - just like the
 * regular float8 arithmetic does, instead of silently returning infinity.
 *
 * Infinity is a perfectly valid result if some of the input values were
 * infinite, in which case the mean is infinite too.
 *
 * XXX This is what float8_mul() does, but that's only available on PG12+.
 */
static double
tdigest_trimmed_sum_value(double sum, double mean, int64 count)
{
	/*
	 * The accumulator may overflow even when the sum itself is perfectly
	 * representable (e.g. with values of the opposite sign). The mean can't
	 * overflow, so recalculate the sum from that.
	 */
	if (!isfinite(sum) && isfinite(mean))
		sum = mean * (double) count;

	if (isinf(sum) && !isinf(mean))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value out of range: overflow")));

	return sum;
}


/*
 * Compute percentile from a tdigest. Final function for tdigest aggregate
 * with a single percentile.
 */
Datum
tdigest_trimmed_avg(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t	   *state;
	MemoryContext	aggcontext;
	double			mean;
	double			sum;
	int64			count;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_trimmed_avg called in non-aggregate context");

	/* if there's no digest, return NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	/* make sure the centroids are sorted */
	tdigest_sort(state);

	tdigest_trimmed_agg(state->centroids, state->ncentroids,
						state->count, state->trim_low, state->trim_high,
						&mean, &sum, &count);

	if (count > 0)
		PG_RETURN_FLOAT8(mean);

	PG_RETURN_NULL();
}

/*
 * Compute percentile from a tdigest. Final function for tdigest aggregate
 * with a single percentile.
 */
Datum
tdigest_trimmed_sum(PG_FUNCTION_ARGS)
{
	tdigest_aggstate_t	   *state;
	MemoryContext	aggcontext;
	double			mean;
	double			sum;
	int64			count;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_trimmed_sum called in non-aggregate context");

	/* if there's no digest, return NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	/* make sure the centroids are sorted */
	tdigest_sort(state);

	tdigest_trimmed_agg(state->centroids, state->ncentroids,
						state->count, state->trim_low, state->trim_high,
						&mean, &sum, &count);

	if (count > 0)
		PG_RETURN_FLOAT8(tdigest_trimmed_sum_value(sum, mean, count));

	PG_RETURN_NULL();
}

/*
 * Trimmed sum of a single digest (non-aggregate function).
 */
Datum
tdigest_digest_sum(PG_FUNCTION_ARGS)
{
	tdigest_t  *digest = PG_GETARG_TDIGEST(0);
	double		low = PG_GETARG_FLOAT8(1);
	double		high = PG_GETARG_FLOAT8(2);

	double		mean;
	double		sum;
	int64		count;

	AssertCheckTDigest(digest);

	check_trim_values(low, high);

	/* make sure we get digest with the new format */
	digest = tdigest_update_format(digest);

	/* tdigest_trimmed_agg expects the centroids sorted by mean */
	digest = tdigest_sort_digest(digest);

	tdigest_trimmed_agg(digest->centroids, digest->ncentroids,
						digest->count, low, high, &mean, &sum, &count);

	if (count > 0)
		PG_RETURN_FLOAT8(tdigest_trimmed_sum_value(sum, mean, count));

	PG_RETURN_NULL();
}

/*
 * Trimmed average of a single digest (non-aggregate function)
 */
Datum
tdigest_digest_avg(PG_FUNCTION_ARGS)
{
	tdigest_t  *digest = PG_GETARG_TDIGEST(0);
	double		low = PG_GETARG_FLOAT8(1);
	double		high = PG_GETARG_FLOAT8(2);

	double		mean;
	double		sum;
	int64		count;

	AssertCheckTDigest(digest);

	check_trim_values(low, high);

	/* make sure we get digest with the new format */
	digest = tdigest_update_format(digest);

	/* tdigest_trimmed_agg expects the centroids sorted by mean */
	digest = tdigest_sort_digest(digest);

	tdigest_trimmed_agg(digest->centroids, digest->ncentroids,
						digest->count, low, high, &mean, &sum, &count);

	if (count > 0)
		PG_RETURN_FLOAT8(mean);

	PG_RETURN_NULL();
}

/*
 * Transform an input FLOAT8 SQL array to a plain double C array.
 *
 * This expects a single-dimensional float8 array, fails otherwise.
 */
static double *
array_to_double(FunctionCallInfo fcinfo, ArrayType *v, int *len)
{
	double *result;
	int		nitems,
		   *dims,
			ndims;
	Oid		element_type;
	int16	typlen;
	bool	typbyval;
	char	typalign;
	int		i;

	/* deconstruct_array */
	Datum	   *elements;
	bool	   *nulls;
	int			nelements;

	ndims = ARR_NDIM(v);
	dims = ARR_DIMS(v);
	nitems = ArrayGetNItems(ndims, dims);

	/* this is a special-purpose function for single-dimensional arrays */
	if (ndims != 1)
		elog(ERROR, "expected a single-dimensional array (dims = %d)", ndims);

	/*
	 * if there are no elements, set the length to 0 and return NULL
	 *
	 * XXX Can this actually happen? for empty arrays we seem to error out
	 * on the preceding check, i.e. ndims = 0.
	 */
	if (nitems == 0)
	{
		(*len) = 0;
		return NULL;
	}

	element_type = ARR_ELEMTYPE(v);

	/* XXX not sure if really needed (can it actually happen?) */
	if (element_type != FLOAT8OID)
		elog(ERROR, "array_to_double expects FLOAT8 array");

	/* allocate space for enough elements */
	result = (double*) palloc(nitems * sizeof(double));

	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

	deconstruct_array(v, element_type, typlen, typbyval, typalign,
					  &elements, &nulls, &nelements);

	/* we should get the same counts here */
	Assert(nelements == nitems);

	for (i = 0; i < nelements; i++)
	{
		if (nulls[i])
			elog(ERROR, "NULL not allowed as a percentile value");

		result[i] = DatumGetFloat8(elements[i]);
	}

	(*len) = nelements;

	return result;
}

/*
 * construct an SQL array from a simple C double array
 */
static Datum
double_to_array(FunctionCallInfo fcinfo, double *d, int len)
{
	ArrayBuildState *astate = NULL;
	int		 i;

	for (i = 0; i < len; i++)
	{
		/* stash away this field */
		astate = accumArrayResult(astate,
								  Float8GetDatum(d[i]),
								  false,
								  FLOAT8OID,
								  CurrentMemoryContext);
	}

	PG_RETURN_ARRAYTYPE_P(DatumGetPointer(makeArrayResult(astate,
										  CurrentMemoryContext)));
}
