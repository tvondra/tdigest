\set ECHO none

-- disable the notices for the create script (shell types etc.)
SET client_min_messages = 'WARNING';
\i tdigest--1.0.0.sql
\i tdigest--1.0.0--1.0.1.sql
\i tdigest--1.0.1--1.2.0.sql
\i tdigest--1.2.0--1.3.0.sql
\i tdigest--1.3.0--1.4.0.sql
\i tdigest--1.4.0--1.4.1.sql
\i tdigest--1.4.1--1.4.2.sql
\i tdigest--1.4.2--1.4.3.sql
\i tdigest--1.4.3--1.4.4.sql
\i tdigest--1.4.4--1.4.5.sql
SET client_min_messages = 'NOTICE';
SET extra_float_digits = 0;

\set ECHO all

-- SRF function implementing a simple deterministict PRNG

CREATE OR REPLACE FUNCTION prng(nrows int, seed int = 23982, p1 bigint = 16807, p2 bigint = 0, n bigint = 2147483647) RETURNS SETOF double precision AS $$
DECLARE
    val INT := seed;
BEGIN
    FOR i IN 1..nrows LOOP
        val := (val * p1 + p2) % n;

        RETURN NEXT (val::double precision / n);
    END LOOP;

    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION random_normal(nrows int, mean double precision = 0.5, stddev double precision = 0.1, minval double precision = 0.0, maxval double precision = 1.0, seed int = 23982, p1 bigint = 16807, p2 bigint = 0, n bigint = 2147483647) RETURNS SETOF double precision AS $$
DECLARE
    v BIGINT := seed;
    x DOUBLE PRECISION;
    y DOUBLE PRECISION;
    s DOUBLE PRECISION;
    r INT := nrows;
BEGIN

    WHILE true LOOP

        -- random x
        v := (v * p1 + p2) % n;
        x := 2 * v / n::double precision - 1.0;

        -- random y
        v := (v * p1 + p2) % n;
        y := 2 * v / n::double precision - 1.0;

        s := x^2 + y^2;

        IF s != 0.0 AND s < 1.0 THEN

            s = sqrt(-2 * ln(s) / s);

            x := mean + stddev * s * x;

            IF x >= minval AND x <= maxval THEN
                RETURN NEXT x;
                r := r - 1;
            END IF;

            EXIT WHEN r = 0;

            y := mean + stddev * s * y;

            IF y >= minval AND y <= maxval THEN
                RETURN NEXT y;
                r := r - 1;
            END IF;

            EXIT WHEN r = 0;

        END IF;

    END LOOP;

END;
$$ LANGUAGE plpgsql;
