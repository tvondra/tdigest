CREATE FUNCTION tdigest_percentile_scalar(tdigest, double precision)
    RETURNS double precision
    AS 'tdigest', 'tdigest_percentiles_scalar'
    LANGUAGE C IMMUTABLE;

CREATE FUNCTION tdigest_percentile_scalar(tdigest, double precision[])
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_array_percentiles_scalar'
    LANGUAGE C IMMUTABLE;

CREATE FUNCTION tdigest_percentile_of_scalar(tdigest, double precision)
    RETURNS double precision
    AS 'tdigest', 'tdigest_percentiles_of_scalar'
    LANGUAGE C IMMUTABLE;

CREATE FUNCTION tdigest_percentile_of_scalar(tdigest, double precision[])
    RETURNS double precision[]
    AS 'tdigest', 'tdigest_array_percentiles_of_scalar'
    LANGUAGE C IMMUTABLE;
