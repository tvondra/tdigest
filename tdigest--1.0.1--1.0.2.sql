CREATE FUNCTION tdigest_json(tdigest)
    RETURNS json
    AS 'tdigest', 'tdigest_to_json'
    LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (tdigest AS json)
    WITH FUNCTION tdigest_json(tdigest)
    AS ASSIGNMENT;
