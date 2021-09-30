CREATE OR REPLACE FUNCTION tdigest_sum(p_digest tdigest, p_low double precision = 0.0, p_high double precision = 1.0)
    RETURNS double precision
    AS 'tdigest', 'tdigest_sum'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tdigest_avg(p_digest tdigest, p_low double precision = 0.0, p_high double precision = 1.0)
    RETURNS double precision
    AS 'tdigest', 'tdigest_avg'
    LANGUAGE C IMMUTABLE STRICT;
