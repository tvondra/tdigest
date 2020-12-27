CREATE FUNCTION tdigest_json(tdigest)
    RETURNS json
    AS 'tdigest', 'tdigest_to_json'
    LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (tdigest AS json)
    WITH FUNCTION tdigest_json(tdigest)
    AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION tdigest_add_double_mean(p_pointer internal, p_element double precision, p_compression int, p_low double precision, p_high double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_mean'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_digest_mean(p_pointer internal, p_element tdigest, p_low double precision, p_high double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_digest_mean'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_trimmed_mean(p_pointer internal)
    RETURNS double precision
    AS 'tdigest', 'tdigest_trimmed_mean'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE tdigest_mean(double precision, int, double precision, double precision) (
    SFUNC = tdigest_add_double_mean,
    STYPE = internal,
    FINALFUNC = tdigest_trimmed_mean,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_mean(tdigest, double precision, double precision) (
    SFUNC = tdigest_add_digest_mean,
    STYPE = internal,
    FINALFUNC = tdigest_trimmed_mean,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);
