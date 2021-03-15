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

#include "postgres.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
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
 * Centroids used to store (sum,count), but we want to store (mean,count).
 * To handle existing tdigest data in backwards-compatible way, we have
 * a flag marking the new ones with mean, and we convert the old values.
 */
#define	TDIGEST_STORES_MEAN		0x0001

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
	double	   *percentiles;	/* array of percentiles (if any) */
	double	   *values;			/* array of values (if any) */
	centroid_t *centroids;		/* centroids for the digest */
} tdigest_aggstate_t;

static int  centroid_cmp(const void *a, const void *b);

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

PG_FUNCTION_INFO_V1(tdigest_count);

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

Datum tdigest_count(PG_FUNCTION_ARGS);

static Datum double_to_array(FunctionCallInfo fcinfo, double * d, int len);
static double *array_to_double(FunctionCallInfo fcinfo, ArrayType *v, int * len);

/* basic checks on the t-digest (proper sum of counts, ...) */
static void
AssertCheckTDigest(tdigest_t *digest)
{
#ifdef USE_ASSERT_CHECKING
	int	i;
	int64	cnt;

	Assert(digest->flags == TDIGEST_STORES_MEAN);

	Assert((digest->compression >= MIN_COMPRESSION) &&
		   (digest->compression <= MAX_COMPRESSION));

	Assert(digest->ncentroids >= 0);
	Assert(digest->ncentroids <= BUFFER_SIZE(digest->compression));

	cnt = 0;
	for (i = 0; i < digest->ncentroids; i++)
	{
		Assert(digest->centroids[i].count > 0);
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

	Assert(state->ncentroids >= 0);
	Assert(state->ncentroids <= BUFFER_SIZE(state->compression));

	cnt = 0;
	for (i = 0; i < state->ncentroids; i++)
	{
		Assert(state->centroids[i].count > 0);
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
 * Sort centroids in the digest.
 *
 * We have to sort the whole array, because we don't just simply sort the
 * centroids - we do the rebalancing of items with the same mean too.
 */
static void
tdigest_sort(tdigest_aggstate_t *state)
{
	int		i;
	int64	count_so_far;
	int64	next_group;
	int64	median_count;

	/* do qsort on the non-sorted part */
	pg_qsort(state->centroids,
			 state->ncentroids,
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
	median_count = (state->count / 2);

	/*
	 * Split the centroids into groups with the same mean, process each group
	 * depending on whether it falls before/after median.
	 */
	i = 0;
	while (i < state->ncentroids)
	{
		int	j = i;
		int	group_size = 0;

		/* determine the end of the group */
		while ((j < state->ncentroids) &&
			   (state->centroids[i].mean == state->centroids[j].mean))
		{
			next_group += state->centroids[j].count;
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
				reverse_centroids(&state->centroids[i], group_size);
			}
			else if (next_group >= median_count)	/* group split by median */
			{
				rebalance_centroids(&state->centroids[i], group_size,
									median_count - count_so_far,
									next_group - median_count);
			}
		}

		i = j;
		count_so_far = next_group;
	}
}

/*
 * Perform compaction of the t-digest, i.e. merge the centroids as required
 * by the compression parameter.
 *
 * We always keep the data sorted in ascending order. This way we can reuse
 * the sort between compactions, and also when computing the quantiles.
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

		should_add = (z <= (q0 * (1 - q0))) && (z <= (q2 * (1 - q2)));

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
				double	sum;
				int64	count;

				sum = state->centroids[i].count * state->centroids[i].mean;
				sum += state->centroids[cur].count * state->centroids[cur].mean;

				count = state->centroids[i].count;
				count += state->centroids[cur].count;

				state->centroids[cur].mean = (sum / count);
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

	AssertCheckTDigestAggState(state);

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

	for (i = 0; i < state->npercentiles; i++)
	{
		double	count;
		double	delta;
		double	goal = (state->percentiles[i] * state->count);
		bool	on_the_right;
		centroid_t *prev, *next;
		centroid_t *c = NULL;
		double	slope;

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

		/* walk throught the centroids and count number of items */
		count = 0;
		for (j = 0; j < state->ncentroids; j++)
		{
			c = &state->centroids[j];

			/* have we exceeded the expected count? */
			if (count + c->count > goal)
				break;

			/* account for the centroid */
			count += c->count;
		}

		delta = goal - count - (c->count / 2.0);

		/*
		 * double arithmetics, so don't compare to 0.0 direcly, it's enough
		 * to be "close enough"
		 */
		if (fabs(delta) < 0.000000001)
		{
			result[i] = c->mean;
			continue;
		}

		on_the_right = (delta > 0.0);

		/*
		 * for extreme percentiles we might end on the right of the last node or on the
		 * left of the first node, instead of interpolating we return the mean of the node
		 */
		if ((on_the_right && (j+1) >= state->ncentroids) ||
			(!on_the_right && (j-1) < 0))
		{
			result[i] = c->mean;
			continue;
		}

		if (on_the_right)
		{
			prev = &state->centroids[j];
			AssertBounds(j+1, state->ncentroids);
			next = &state->centroids[j+1];
			count += (prev->count / 2.0);
		}
		else
		{
			AssertBounds(j-1, state->ncentroids);
			prev = &state->centroids[j-1];
			next = &state->centroids[j];
			count -= (prev->count / 2.0);
		}

		slope = (next->mean - prev->mean) / (next->count / 2.0 + prev->count / 2.0);

		result[i] = prev->mean + slope * (goal - count);
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
		centroid_t *c = NULL;
		centroid_t *prev;
		double		value = state->values[i];
		double		m, x;

		count = 0;
		for (j = 0; j < state->ncentroids; j++)
		{
			c = &state->centroids[j];

			if (c->mean >= value)
				break;

			count += c->count;
		}

		/* the value exactly matches the mean */
		if (value == c->mean)
		{
			int64	count_at_value = 0;

			/*
			 * There may be multiple centroids with this mean (i.e. containing
			 * this value), so find all of them and sum their weights.
			 */
			while (state->centroids[j].mean == value && j < state->ncentroids)
			{
				count_at_value += state->centroids[j].count;
				j++;
			}

			result[i] = (count + (count_at_value / 2.0)) / state->count;
			continue;
		}
		else if (value > c->mean)	/* past the largest */
		{
			result[i] = 1;
			continue;
		}
		else if (j == 0)			/* past the smallest */
		{
			result[i] = 0;
			continue;
		}

		/*
		 * The value lies somewhere between two centroids. We want to figure out
		 * where along the line from the prev node to this node the value is.
		 *
		 * FIXME What if there are multiple centroids with the same mean as the
		 * prev/curr centroid? This probably needs to lookup all of them and sum
		 * their counts, just like we did in case of the exact match, no?
		 */
		prev = c - 1;
		count -= (prev->count / 2);

		/*
		 * We assume for both prev/curr centroid, half the count is on left/righ,
		 * so between them we have (prev->count/2 + curr->count/2). At zero we
		 * are in prev->mean and at (prev->count/2 + curr->count/2) we're at
		 * curr->mean.
		 */
		m = (c->mean - prev->mean) / (c->count / 2.0 + prev->count / 2.0);
		x = (value - prev->mean) / m;

		result[i] = (double) (count + x) / state->count;
	}
}


/* add a value to the t-digest, trigger a compaction if full */
static void
tdigest_add(tdigest_aggstate_t *state, double v)
{
	int	compression = state->compression;
	int	ncentroids = state->ncentroids;

	AssertCheckTDigestAggState(state);

	/* make sure we have space for the value */
	Assert(state->ncentroids < BUFFER_SIZE(compression));

	/* for a single point, the value is both sum and mean */
	state->centroids[ncentroids].count = 1;
	state->centroids[ncentroids].mean = v;
	state->ncentroids++;
	state->count++;

	Assert(state->ncentroids <= BUFFER_SIZE(compression));

	/* if the buffer got full, trigger compaction here so that next
	 * insert has free space */
	if (state->ncentroids == BUFFER_SIZE(compression))
		tdigest_compact(state);
}

/*
 * Add a centroid (possibly with count not equal to 1) to the t-digest,
 * triggers a compaction when buffer full.
 */
static void
tdigest_add_centroid(tdigest_aggstate_t *state, double mean, int64 count)
{
	int	compression = state->compression;
	int	ncentroids = state->ncentroids;

	AssertCheckTDigestAggState(state);

	/* make sure we have space for the value */
	Assert(state->ncentroids < BUFFER_SIZE(compression));

	/* for a single point, the value is both sum and mean */
	state->centroids[ncentroids].count = count;
	state->centroids[ncentroids].mean = mean;
	state->ncentroids++;
	state->count += count;

	Assert(state->ncentroids <= BUFFER_SIZE(compression));

	/* if the buffer got full, trigger compaction here so that next
	 * insert has free space */
	if (state->ncentroids == BUFFER_SIZE(compression))
		tdigest_compact(state);
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

static void
tdigest_fix_mean(tdigest_t *digest)
{
	int		i;

	/* if already new format, we're done */
	if (digest->flags & TDIGEST_STORES_MEAN)
		return;

	for (i = 0; i < digest->ncentroids; i++)
	{
		digest->centroids[i].mean
			= digest->centroids[i].mean / digest->centroids[i].count;
	}

	digest->flags |= TDIGEST_STORES_MEAN;
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
tdigest_aggstate_to_digest(tdigest_aggstate_t *state)
{
	int			i;
	tdigest_t  *digest;

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
		if ((percentiles[i] < 0.0) || (percentiles[i] > 1.0))
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

	PG_RETURN_POINTER(state);
}

/*
 * Generate a t-digest representing a value with a given count.
 *
 * This is an alternative to using a single centroid, representing all points
 * with the same value. It forms a proper t-diget, following all the rules on
 * centroid sizes, etc.
 */
static tdigest_t *
tdigest_generate(int compression, double value, int64 count)
{
	int64		count_so_far;
	int64		count_remaining;
	double		denom;
	double		normalizer;
	int			i;
	tdigest_t  *result = tdigest_allocate(compression);

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
	 * 
	 */
	while (count_remaining > 0)
	{
		int64	proposed_count;
		double	q0;
		double	a, b, c;
		double	r1, r2;

		/* solving z <= q0 * (1 - q0) is trivial */
		q0 = count_so_far / (double) count;
		r1 = (q0 * (1 - q0) / normalizer);

		/*
		 * Solve z <= q2 * (1 - q2) as a quadratic equation. The inequatily we
		 * need to solve is
		 *
		 *	0 <= a * x^2 + b * x + c
		 *
		 * with these coefficients.
		 *
		 * XXX The counts may be very high values (int64), so we need to be
		 * careful to prevent overflows by doing everything with double.
		 */
		a = -1;
		b = ((double) count - 2 * (double) count_so_far - (double) count * (double) count * normalizer);
		c = ((double) count_so_far * (double) count - (double) count_so_far * (double) count_so_far);

		/*
		 * As this is an "upside down" parabola, the values between the roots
		 * are positive - we're looking for the largest of the two values.
		 *
		 * XXX Tthe first root should be the higher one, because sqrt is
		 * always positive, so (-b - sqrt()) is smaller and negative, and
		 * we're dividing by negative value.
		 */
		r2 = Max((-b - sqrt(b * b - 4 * a * c)) / (2 * a),
				 (-b + sqrt(b * b - 4 * a * c)) / (2 * a));

		/* We need to meet both conditions, so use the smaller solution. */
		proposed_count = floor(Min(r1, r2));

		/*
		 * It's possible to get very low values on the tails, but we must add
		 * at least something, otherwise we'd get infinite loops.
		 */
		proposed_count = Max(proposed_count, 1);

		/* add the centroid and update the added/removed counters */
		result->count += proposed_count;
		result->centroids[result->ncentroids].count = proposed_count;
		result->centroids[result->ncentroids].mean = value;
		result->ncentroids++;

		Assert(result->ncentroids <= compression);

		count_so_far += proposed_count;
		count_remaining -= proposed_count;
	}

	result->count = 0;
	for (i = 0; i < result->ncentroids; i++)
		result->count += result->centroids[i].count;

	return result;
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

	Assert(count > 0);

	/*
	 * When adding too many values (than would fit into an empty buffer, and
	 * thus likely causing too many compactions), we instead build a t-digest
	 * and them merge it into the existing state.
	 *
	 * This is much faster, because the t-digest can be generated in one go,
	 * so there can be only one compaction at most.
	 */
	if (count > BUFFER_SIZE(state->compression))
	{
		int			i;
		tdigest_t  *new;
		double		value = PG_GETARG_FLOAT8(1);

		new = tdigest_generate(state->compression, value, count);

		/* XXX maybe not necessary if there's enough space in the buffer */
		tdigest_compact(state);

		for (i = 0; i < new->ncentroids; i++)
		{
			centroid_t   *s = &new->centroids[i];

			state->centroids[state->ncentroids].count = s->count;
			state->centroids[state->ncentroids].mean = value;
			state->ncentroids++;
			state->count += s->count;
		}

		count = 0;
	}

	/*
	 * If there are only a couple values, just add them one by one, so that
	 * we do proper compaction and sizing of centroids. Otherwise we might end
	 * up with oversized centroid on the tails etc.
	 */
	for (i = 0; i < count; i++)
		tdigest_add(state, PG_GETARG_FLOAT8(1));

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

	Assert(count > 0);

	/*
	 * When adding too many values (than would fit into an empty buffer, and
	 * thus likely causing too many compactions), we instead build a t-digest
	 * and them merge it into the existing state.
	 *
	 * This is much faster, because the t-digest can be generated in one go,
	 * so there can be only one compaction at most.
	 */
	if (count > BUFFER_SIZE(state->compression))
	{
		int			i;
		tdigest_t  *new;
		double		value = PG_GETARG_FLOAT8(1);

		new = tdigest_generate(state->compression, value, count);

		/* XXX maybe not necessary if there's enough space in the buffer */
		tdigest_compact(state);

		for (i = 0; i < new->ncentroids; i++)
		{
			centroid_t   *s = &new->centroids[i];

			state->centroids[state->ncentroids].count = s->count;
			state->centroids[state->ncentroids].mean = value;
			state->ncentroids++;
			state->count += s->count;
		}

		count = 0;
	}

	/*
	 * If there are only a couple values, just add them one by one, so that
	 * we do proper compaction and sizing of centroids. Otherwise we might end
	 * up with oversized centroid on the tails etc.
	 */
	for (i = 0; i < count; i++)
		tdigest_add(state, PG_GETARG_FLOAT8(1));

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

	/* fix old tdigest format */
	tdigest_fix_mean(digest);

	/* make sure the t-digest format is supported */
	if (digest->flags != TDIGEST_STORES_MEAN)
		elog(ERROR, "unsupported t-digest on-disk format");

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

	/* copy data from the tdigest into the aggstate */
	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

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

	/* fix old tdigest format */
	tdigest_fix_mean(digest);

	/* make sure the t-digest format is supported */
	if (digest->flags != TDIGEST_STORES_MEAN)
		elog(ERROR, "unsupported t-digest on-disk format");

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

	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

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
		int compression = PG_GETARG_INT32(3);
		double *percentiles;
		int		npercentiles;
		MemoryContext	oldcontext;

		check_compression(compression);

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

	Assert(count > 0);

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
		int compression = PG_GETARG_INT32(3);
		double *values;
		int		nvalues;
		MemoryContext	oldcontext;

		check_compression(compression);

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

	Assert(count > 0);

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

	/* fix old tdigest format */
	tdigest_fix_mean(digest);

	/* make sure the t-digest format is supported */
	if (digest->flags != TDIGEST_STORES_MEAN)
		elog(ERROR, "unsupported t-digest on-disk format");

	/* if there's no aggregate state allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		double *percentiles;
		int		npercentiles;
		MemoryContext	oldcontext;

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

	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

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

	/* fix old tdigest format */
	tdigest_fix_mean(digest);

	/* make sure the t-digest format is supported */
	if (digest->flags != TDIGEST_STORES_MEAN)
		elog(ERROR, "unsupported t-digest on-disk format");

	/* if there's no aggregate state allocated, create it now */
	if (PG_ARGISNULL(0))
	{
		double *values;
		int		nvalues;
		MemoryContext	oldcontext;

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

	for (i = 0; i < digest->ncentroids; i++)
		tdigest_add_centroid(state, digest->centroids[i].mean,
									digest->centroids[i].count);

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

	digest = tdigest_aggstate_to_digest(state);

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

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "tdigest_combine called in non-aggregate context");

	/* the second parameter must not be NULL */
	Assert(!PG_ARGISNULL(1));

	/* so just grab it */
	src = (tdigest_aggstate_t *) PG_GETARG_POINTER(1);

	/* when NULL in the first parameter, just return a copy of the second one */
	if (PG_ARGISNULL(0))
	{
		/* copy the digest into the right long-lived memory context */
		oldcontext = MemoryContextSwitchTo(aggcontext);
		src = tdigest_copy(src);
		MemoryContextSwitchTo(oldcontext);

		PG_RETURN_POINTER(src);
	}

	dst = (tdigest_aggstate_t *) PG_GETARG_POINTER(0);

	/*
	 * Do a compaction on each digest, to make sure we have enough space.
	 *
	 * XXX Maybe do this only when necessary, i.e. when we can't fit the
	 * data into the dst digest? Also, is it really ensured this gives us
	 * enough free space?
	 */
	tdigest_compact(dst);
	tdigest_compact(src);

	AssertCheckTDigestAggState(dst);
	AssertCheckTDigestAggState(src);

	/* copy the second part */
	memcpy(&dst->centroids[dst->ncentroids],
		   src->centroids,
		   src->ncentroids * sizeof(centroid_t));

	dst->ncentroids += src->ncentroids;
	dst->count += src->count;

	/* mark the digest as not compacted */
	dst->ncompacted = 0;

	AssertCheckTDigestAggState(dst);

	PG_RETURN_POINTER(dst);
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

Datum
tdigest_in(PG_FUNCTION_ARGS)
{
	int			i, r;
	char	   *str = PG_GETARG_CSTRING(0);
	tdigest_t  *digest = NULL;

	/* t-digest header fields */
	int32       flags;
	int64		count;
	int			compression;
	int			ncentroids;
	int			header_length;
	char	   *ptr;

	r = sscanf(str, "flags %d count " INT64_FORMAT " compression %d centroids %d%n",
			   &flags, &count, &compression, &ncentroids, &header_length);

	if (r != 4)
		elog(ERROR, "failed to parse t-digest value");

	if ((compression < 10) || (compression > 10000))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("compression for t-digest must be in [10, 10000]")));

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

	ptr = str + header_length;

	for (i = 0; i < digest->ncentroids; i++)
	{
		double	mean;

		if (sscanf(ptr, " (%lf, " INT64_FORMAT ")", &mean, &count) != 2)
			elog(ERROR, "failed to parse centroid");

		digest->centroids[i].count = count;
		digest->centroids[i].mean = mean;

		if (count <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("count value for all centroids in a t-digest must be positive")));

		/* skip to the end of the centroid */
		ptr = strchr(ptr, ')') + 1;
	}

	Assert(ptr == str + strlen(str));

	/* if we received "old" t-digest storing sum, fix it now */
	tdigest_fix_mean(digest);

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

	/* make sure the t-digest format is supported */
	if (digest->flags != TDIGEST_STORES_MEAN)
		elog(ERROR, "unsupported t-digest on-disk format");

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
		appendStringInfo(&str, " (%lf, " INT64_FORMAT ")",
						 digest->centroids[i].mean,
						 digest->centroids[i].count);

	PG_RETURN_CSTRING(str.data);
}

Datum
tdigest_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	tdigest_t  *digest;
	int			i;
	int64		count;
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

	digest = tdigest_allocate(ncentroids);

	digest->flags = flags;
	digest->count = count;
	digest->compression = compression;
	digest->ncentroids = ncentroids;

	for (i = 0; i < digest->ncentroids; i++)
	{
		digest->centroids[i].mean = pq_getmsgfloat8(buf);
		digest->centroids[i].count = pq_getmsgint64(buf);
	}

	/* if we received "old" t-digest storing sum, fix it now */
	tdigest_fix_mean(digest);

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

Datum
tdigest_count(PG_FUNCTION_ARGS)
{
	tdigest_t  *digest = (tdigest_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	PG_RETURN_INT64(digest->count);
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

	PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate,
										  CurrentMemoryContext));
}
