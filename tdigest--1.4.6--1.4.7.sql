-- The plain functions operating on a tdigest value are all pure computations
-- on their arguments, with no access to any shared state, so they are safe to
-- run in a parallel worker. None of them were ever marked as such, which
-- silently disabled parallelism for any query using them - a single unsafe
-- function makes the whole plan serial, including the tdigest() aggregate
-- building the digest they consume.
ALTER FUNCTION tdigest_add(tdigest, double precision, int, bool) PARALLEL SAFE;
ALTER FUNCTION tdigest_add(tdigest, double precision[], int, bool) PARALLEL SAFE;
ALTER FUNCTION tdigest_union(tdigest, tdigest, bool) PARALLEL SAFE;
ALTER FUNCTION tdigest_count(tdigest) PARALLEL SAFE;
ALTER FUNCTION tdigest_json(tdigest) PARALLEL SAFE;
ALTER FUNCTION tdigest_double_array(tdigest) PARALLEL SAFE;
ALTER FUNCTION tdigest_is_valid(tdigest) PARALLEL SAFE;

-- The type input/output functions are pure serialization routines, and every
-- one of the built-in type I/O functions in PostgreSQL is marked parallel safe.
-- These were not, which made any query merely casting a tdigest to text run
-- serially.
ALTER FUNCTION tdigest_in(cstring) PARALLEL SAFE;
ALTER FUNCTION tdigest_out(tdigest) PARALLEL SAFE;
ALTER FUNCTION tdigest_recv(internal) PARALLEL SAFE;
ALTER FUNCTION tdigest_send(tdigest) PARALLEL SAFE;
