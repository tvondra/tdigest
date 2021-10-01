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
