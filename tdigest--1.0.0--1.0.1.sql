CREATE OR REPLACE FUNCTION tdigest_add_double_count(p_pointer internal, p_element double precision, p_count bigint, p_compression int)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_count'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double_count(p_pointer internal, p_element double precision, p_count bigint, p_compression int, p_quantile double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_count'
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
