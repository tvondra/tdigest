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
