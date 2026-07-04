CREATE TABLE tdigest_src (id int, s tdigest);
CREATE TABLE tdigest_dst (id int, s tdigest);

-- generate 100 random sketches, export/import them in text/binary mode
DO $$
DECLARE
	compression int;
BEGIN

	FOR i IN 1..100 LOOP

		-- random compression value
		compression := 100 + random() * 1000;

		INSERT INTO tdigest_src SELECT i, tdigest(random(), compression) FROM generate_series(1, compression * 10) s(x);

	END LOOP;

END;
$$ LANGUAGE plpgsql;

-- export in text and binary formats
COPY tdigest_src TO '/tmp/tdigest_copy_text.out';
COPY tdigest_src TO '/tmp/tdigest_copy_binary.out' WITH (FORMAT BINARY);

-- import in text and binary formats
COPY tdigest_dst FROM '/tmp/tdigest_copy_text.out';
COPY tdigest_dst FROM '/tmp/tdigest_copy_binary.out' WITH (FORMAT BINARY);

-- all imported values have to match the source
SELECT
  COUNT(*) AS count_all,
  COUNT(CASE WHEN (src.s::text != dst.s::text) THEN 1 ELSE NULL END) AS count_mismatching
FROM tdigest_src src JOIN tdigest_dst dst ON (src.id = dst.id);
