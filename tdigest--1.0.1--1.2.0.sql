CREATE OR REPLACE FUNCTION tdigest_add_double_count(p_pointer internal, p_element double precision, p_count bigint, p_compression int)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_count'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double_count(p_pointer internal, p_element double precision, p_count bigint, p_compression int, p_quantile double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_count'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double_array_count(p_pointer internal, p_element double precision, p_count bigint, p_compression int, p_quantile double precision[])
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_array_count'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double_values_count(p_pointer internal, p_element double precision, p_count bigint, p_compression int, p_value double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_values_count'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double_array_values_count(p_pointer internal, p_element double precision, p_count bigint, p_compression int, p_value double precision[])
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_array_values_count'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE tdigest(double precision, bigint, int) (
    SFUNC = tdigest_add_double_count,
    STYPE = internal,
    FINALFUNC = tdigest_digest,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile(double precision, bigint, int, double precision) (
    SFUNC = tdigest_add_double_count,
    STYPE = internal,
    FINALFUNC = tdigest_percentiles,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile(double precision, bigint, int, double precision[]) (
    SFUNC = tdigest_add_double_array_count,
    STYPE = internal,
    FINALFUNC = tdigest_array_percentiles,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile_of(double precision, bigint, int, double precision) (
    SFUNC = tdigest_add_double_values_count,
    STYPE = internal,
    FINALFUNC = tdigest_percentiles_of,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile_of(double precision, bigint, int, double precision[]) (
    SFUNC = tdigest_add_double_array_values_count,
    STYPE = internal,
    FINALFUNC = tdigest_array_percentiles_of,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE OR REPLACE FUNCTION tdigest_add(p_digest tdigest, p_element double precision, p_compression int = NULL, p_compact bool = true)
    RETURNS tdigest
    AS 'tdigest', 'tdigest_add_double_increment'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add(p_digest tdigest, p_elements double precision[], p_compression int = NULL, p_compact bool = true)
    RETURNS tdigest
    AS 'tdigest', 'tdigest_add_double_array_increment'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_union(p_digest1 tdigest, p_digest2 tdigest, p_compact bool = true)
    RETURNS tdigest
    AS 'tdigest', 'tdigest_union_double_increment'
    LANGUAGE C IMMUTABLE;
