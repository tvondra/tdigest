CREATE FUNCTION tdigest_json(tdigest)
    RETURNS json
    AS 'tdigest', 'tdigest_to_json'
    LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (tdigest AS json)
    WITH FUNCTION tdigest_json(tdigest)
    AS ASSIGNMENT;

CREATE FUNCTION tdigest_double_array(tdigest)
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_to_array'
    LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (tdigest AS double precision[])
    WITH FUNCTION tdigest_double_array(tdigest)
    AS ASSIGNMENT;

-- trimmed aggregates
CREATE OR REPLACE FUNCTION tdigest_add_double_trimmed(p_pointer internal, p_element double precision, p_compression int, p_low double precision, p_high double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_trimmed'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double_count_trimmed(p_pointer internal, p_element double precision, p_count bigint, p_compression int, p_low double precision, p_high double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_count_trimmed'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_digest_trimmed(p_pointer internal, p_element tdigest, p_low double precision, p_high double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_digest_trimmed'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_trimmed_avg(p_pointer internal)
    RETURNS double precision
    AS 'tdigest', 'tdigest_trimmed_avg'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_trimmed_sum(p_pointer internal)
    RETURNS double precision
    AS 'tdigest', 'tdigest_trimmed_sum'
    LANGUAGE C IMMUTABLE;


CREATE AGGREGATE tdigest_avg(double precision, int, double precision, double precision) (
    SFUNC = tdigest_add_double_trimmed,
    STYPE = internal,
    FINALFUNC = tdigest_trimmed_avg,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_avg(double precision, bigint, int, double precision, double precision) (
    SFUNC = tdigest_add_double_count_trimmed,
    STYPE = internal,
    FINALFUNC = tdigest_trimmed_avg,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_avg(tdigest, double precision, double precision) (
    SFUNC = tdigest_add_digest_trimmed,
    STYPE = internal,
    FINALFUNC = tdigest_trimmed_avg,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);


CREATE AGGREGATE tdigest_sum(double precision, int, double precision, double precision) (
    SFUNC = tdigest_add_double_trimmed,
    STYPE = internal,
    FINALFUNC = tdigest_trimmed_sum,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_sum(double precision, bigint, int, double precision, double precision) (
    SFUNC = tdigest_add_double_count_trimmed,
    STYPE = internal,
    FINALFUNC = tdigest_trimmed_sum,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_sum(tdigest, double precision, double precision) (
    SFUNC = tdigest_add_digest_trimmed,
    STYPE = internal,
    FINALFUNC = tdigest_trimmed_sum,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

-- non-aggregate functions to extract trimmed sum/avg from a tdigest

CREATE OR REPLACE FUNCTION tdigest_digest_sum(p_digest tdigest, p_low double precision = 0.0, p_high double precision = 1.0)
    RETURNS double precision
    AS 'tdigest', 'tdigest_digest_sum'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tdigest_digest_avg(p_digest tdigest, p_low double precision = 0.0, p_high double precision = 1.0)
    RETURNS double precision
    AS 'tdigest', 'tdigest_digest_avg'
    LANGUAGE C IMMUTABLE STRICT;
