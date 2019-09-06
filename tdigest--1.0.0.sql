/* tdigest for the double precision */
CREATE OR REPLACE FUNCTION tdigest_add_double(p_pointer internal, p_element double precision, p_compression int)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double(p_pointer internal, p_element double precision, p_compression int, p_quantile double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double_array(p_pointer internal, p_element double precision, p_compression int, p_quantile double precision[])
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double_values(p_pointer internal, p_element double precision, p_compression int, p_value double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_values'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_double_array_values(p_pointer internal, p_element double precision, p_compression int, p_value double precision[])
    RETURNS internal
    AS 'tdigest', 'tdigest_add_double_array_values'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_percentiles(p_pointer internal)
    RETURNS double precision
    AS 'tdigest', 'tdigest_percentiles'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_array_percentiles(p_pointer internal)
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_array_percentiles'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_percentiles_of(p_pointer internal)
    RETURNS double precision
    AS 'tdigest', 'tdigest_percentiles_of'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_array_percentiles_of(p_pointer internal)
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_array_percentiles_of'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_combine(a internal, b internal)
    RETURNS internal
    AS 'tdigest', 'tdigest_combine'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_serial(a internal)
    RETURNS bytea
    AS 'tdigest', 'tdigest_serial'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tdigest_deserial(a bytea, b internal)
    RETURNS internal
    AS 'tdigest', 'tdigest_deserial'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE tdigest_percentile(double precision, int, double precision) (
    SFUNC = tdigest_add_double,
    STYPE = internal,
    FINALFUNC = tdigest_percentiles,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile(double precision, int, double precision[]) (
    SFUNC = tdigest_add_double_array,
    STYPE = internal,
    FINALFUNC = tdigest_array_percentiles,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile_of(double precision, int, double precision) (
    SFUNC = tdigest_add_double_values,
    STYPE = internal,
    FINALFUNC = tdigest_percentiles_of,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile_of(double precision, int, double precision[]) (
    SFUNC = tdigest_add_double_array_values,
    STYPE = internal,
    FINALFUNC = tdigest_array_percentiles_of,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE TYPE tdigest;

CREATE OR REPLACE FUNCTION tdigest_in(cstring)
    RETURNS tdigest
    AS 'tdigest', 'tdigest_in'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tdigest_out(tdigest)
    RETURNS cstring
    AS 'tdigest', 'tdigest_out'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tdigest_send(tdigest)
    RETURNS bytea
    AS 'tdigest', 'tdigest_send'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tdigest_recv(internal)
    RETURNS tdigest
    AS 'tdigest', 'tdigest_recv'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE tdigest (
    INPUT = tdigest_in,
    OUTPUT = tdigest_out,
    RECEIVE = tdigest_recv,
    SEND = tdigest_send,
    INTERNALLENGTH = variable,
    STORAGE = external
);

CREATE OR REPLACE FUNCTION tdigest_digest(p_pointer internal)
    RETURNS tdigest
    AS 'tdigest', 'tdigest_digest'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE tdigest(double precision, int) (
    SFUNC = tdigest_add_double,
    STYPE = internal,
    FINALFUNC = tdigest_digest,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE OR REPLACE FUNCTION tdigest_add_digest(p_pointer internal, p_element tdigest)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_digest'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_digest(p_pointer internal, p_element tdigest, p_quantile double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_digest'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_digest_array(p_pointer internal, p_element tdigest, p_quantile double precision[])
    RETURNS internal
    AS 'tdigest', 'tdigest_add_digest_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_digest_values(p_pointer internal, p_element tdigest, p_value double precision)
    RETURNS internal
    AS 'tdigest', 'tdigest_add_digest_values'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION tdigest_add_digest_array_values(p_pointer internal, p_element tdigest, p_value double precision[])
    RETURNS internal
    AS 'tdigest', 'tdigest_add_digest_array_values'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE tdigest_percentile(tdigest, double precision) (
    SFUNC = tdigest_add_digest,
    STYPE = internal,
    FINALFUNC = tdigest_percentiles,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile(tdigest, double precision[]) (
    SFUNC = tdigest_add_digest_array,
    STYPE = internal,
    FINALFUNC = tdigest_array_percentiles,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile_of(tdigest, double precision) (
    SFUNC = tdigest_add_digest_values,
    STYPE = internal,
    FINALFUNC = tdigest_percentiles_of,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest_percentile_of(tdigest, double precision[]) (
    SFUNC = tdigest_add_digest_array_values,
    STYPE = internal,
    FINALFUNC = tdigest_array_percentiles_of,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE tdigest(tdigest) (
    SFUNC = tdigest_add_digest,
    STYPE = internal,
    FINALFUNC = tdigest_digest,
    SERIALFUNC = tdigest_serial,
    DESERIALFUNC = tdigest_deserial,
    COMBINEFUNC = tdigest_combine,
    PARALLEL = SAFE
);

CREATE OR REPLACE FUNCTION tdigest_count(tdigest)
    RETURNS bigint
    AS 'tdigest', 'tdigest_count'
    LANGUAGE C IMMUTABLE STRICT;
