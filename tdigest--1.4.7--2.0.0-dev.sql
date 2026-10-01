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

CREATE FUNCTION tdigest_percentile(p_digest tdigest, p_percentile double precision)
    RETURNS double precision
    AS 'tdigest', 'tdigest_percentile'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION tdigest_percentile(p_digest tdigest, p_percentiles double precision[])
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_percentile_array'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION tdigest_percentile_of(p_digest tdigest, p_value double precision)
    RETURNS double precision
    AS 'tdigest', 'tdigest_percentile_of'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION tdigest_percentile_of(p_digest tdigest, p_values double precision[])
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_percentile_of_array'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- The remaining functions operating on a digest were created without
-- parameter names, so they could only be called positionally. Adding names
-- to unnamed parameters is the one change CREATE OR REPLACE allows, and it
-- keeps the dependent casts intact (unlike DROP + CREATE).
CREATE OR REPLACE FUNCTION tdigest_count(p_digest tdigest)
    RETURNS bigint
    AS 'tdigest', 'tdigest_count'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION tdigest_json(p_digest tdigest)
    RETURNS json
    AS 'tdigest', 'tdigest_to_json'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION tdigest_double_array(p_digest tdigest)
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_to_array'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION tdigest_is_valid(p_digest tdigest)
    RETURNS bool
    AS 'tdigest', 'tdigest_is_valid'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- rename to preserve dependent objects, grants, comments, and defaults
ALTER FUNCTION tdigest_digest_sum(tdigest, double precision, double precision)
    RENAME TO tdigest_sum;
ALTER FUNCTION tdigest_digest_avg(tdigest, double precision, double precision)
    RENAME TO tdigest_avg;
