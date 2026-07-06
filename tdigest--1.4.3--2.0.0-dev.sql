DROP AGGREGATE tdigest_sum(tdigest, double precision, double precision);
DROP AGGREGATE tdigest_sum(double precision, bigint, int, double precision, double precision);
DROP AGGREGATE tdigest_sum(double precision, int, double precision, double precision);
DROP AGGREGATE tdigest_avg(tdigest, double precision, double precision);
DROP AGGREGATE tdigest_avg(double precision, bigint, int, double precision, double precision);
DROP AGGREGATE tdigest_avg(double precision, int, double precision, double precision);

DROP AGGREGATE tdigest_percentile(double precision, bigint, integer, double precision);
DROP AGGREGATE tdigest_percentile(double precision, bigint, integer, double precision[]);
DROP AGGREGATE tdigest_percentile(double precision, integer, double precision);
DROP AGGREGATE tdigest_percentile(double precision, integer, double precision[]);
DROP AGGREGATE tdigest_percentile(tdigest, double precision);
DROP AGGREGATE tdigest_percentile(tdigest, double precision[]);
DROP AGGREGATE tdigest_percentile_of(double precision, bigint, integer, double precision);
DROP AGGREGATE tdigest_percentile_of(double precision, bigint, integer, double precision[]);
DROP AGGREGATE tdigest_percentile_of(double precision, integer, double precision);
DROP AGGREGATE tdigest_percentile_of(double precision, integer, double precision[]);
DROP AGGREGATE tdigest_percentile_of(tdigest, double precision);
DROP AGGREGATE tdigest_percentile_of(tdigest, double precision[]);

DROP FUNCTION tdigest_trimmed_sum(internal);
DROP FUNCTION tdigest_trimmed_avg(internal);
DROP FUNCTION tdigest_add_digest_trimmed(internal, tdigest, double precision, double precision);
DROP FUNCTION tdigest_add_double_count_trimmed(internal, double precision, bigint, int, double precision, double precision);
DROP FUNCTION tdigest_add_double_trimmed(internal, double precision, int, double precision, double precision);

DROP FUNCTION tdigest_add_digest(internal, tdigest, double precision);
DROP FUNCTION tdigest_add_digest_array(internal, tdigest, double precision[]);
DROP FUNCTION tdigest_add_digest_array_values(internal, tdigest, double precision[]);
DROP FUNCTION tdigest_add_digest_values(internal, tdigest, double precision);
DROP FUNCTION tdigest_add_double(internal, double precision, integer, double precision);
DROP FUNCTION tdigest_add_double_array(internal, double precision, integer, double precision[]);
DROP FUNCTION tdigest_add_double_array_count(internal, double precision, bigint, integer, double precision[]);
DROP FUNCTION tdigest_add_double_array_values(internal, double precision, integer, double precision[]);
DROP FUNCTION tdigest_add_double_array_values_count(internal, double precision, bigint, integer, double precision[]);
DROP FUNCTION tdigest_add_double_count(internal, double precision, bigint, integer, double precision);
DROP FUNCTION tdigest_add_double_values(internal, double precision, integer, double precision);
DROP FUNCTION tdigest_add_double_values_count(internal, double precision, bigint, integer, double precision);

DROP FUNCTION tdigest_array_percentiles(internal);
DROP FUNCTION tdigest_array_percentiles_of(internal);
DROP FUNCTION tdigest_percentiles(internal);
DROP FUNCTION tdigest_percentiles_of(internal);

CREATE FUNCTION tdigest_percentile(tdigest, double precision)
    RETURNS double precision
    AS 'tdigest', 'tdigest_percentile'
    LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_percentile(tdigest, double precision[])
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_percentile_array'
    LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_percentile_of(tdigest, double precision)
    RETURNS double precision
    AS 'tdigest', 'tdigest_percentile_of'
    LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_percentile_of(tdigest, double precision[])
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_percentile_of_array'
    LANGUAGE C IMMUTABLE PARALLEL SAFE;

DROP FUNCTION tdigest_digest_sum(tdigest, double precision, double precision);
DROP FUNCTION tdigest_digest_avg(tdigest, double precision, double precision);

CREATE OR REPLACE FUNCTION tdigest_sum(p_digest tdigest, p_low double precision = 0.0, p_high double precision = 1.0)
    RETURNS double precision
    AS 'tdigest', 'tdigest_digest_sum'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tdigest_avg(p_digest tdigest, p_low double precision = 0.0, p_high double precision = 1.0)
    RETURNS double precision
    AS 'tdigest', 'tdigest_digest_avg'
    LANGUAGE C IMMUTABLE STRICT;
