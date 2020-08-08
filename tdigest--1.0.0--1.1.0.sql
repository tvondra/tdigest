CREATE OR REPLACE FUNCTION equiwidth_histogram(p_digest tdigest, p_bins int)
RETURNS TABLE (bin_start double precision,
               bin_end double precision,
               bin_density double precision)
AS $$

    WITH
      range  AS (SELECT tdigest_percentile(p_digest, 0.0) AS min_value, tdigest_percentile(p_digest, 1.0) AS max_value),
      bounds AS (SELECT
                     range.min_value + (i - 1) * (range.max_value - range.min_value) / p_bins AS bin_start,
                     range.min_value + i * (range.max_value - range.min_value) / p_bins AS bin_end
                 FROM range, generate_series(1,p_bins) AS s(i))
      SELECT
          bounds.bin_start,
          bounds.bin_end,
          tdigest_percentile_of(p_digest, bounds.bin_end) - tdigest_percentile_of(p_digest, bounds.bin_start)
      FROM bounds
      GROUP BY 1, 2
      ORDER BY 1, 2;

$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION equiheight_histogram(p_digest tdigest, p_bins int)
RETURNS TABLE (bin_start double precision,
               bin_end double precision,
               bin_density double precision)
AS $$

    WITH
      freqs AS (SELECT
                     (i - 1)::double precision / p_bins AS freq_start,
                     i::double precision / p_bins AS freq_end
                 FROM generate_series(1,p_bins) AS s(i))
      SELECT
          tdigest_percentile(p_digest, freqs.freq_start),
          tdigest_percentile(p_digest, freqs.freq_end),
          freqs.freq_end - freqs.freq_start
      FROM freqs
      GROUP BY freqs.freq_start, freqs.freq_end
      ORDER BY 1, 2;

$$ LANGUAGE sql;
