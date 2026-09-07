CREATE OR REPLACE FUNCTION tdigest_is_valid(tdigest)
    RETURNS bool
    AS 'tdigest', 'tdigest_is_valid'
    LANGUAGE C IMMUTABLE STRICT;
