CREATE AGGREGATING INDEX ix_agg_query_pattern ON persistent_query_history (
  query_text_normalized_hash,
  engine_name,
  MIN(query_text_normalized),
  COUNT(*),
  COUNT(DISTINCT DATE_TRUNC('hour', submitted_time)),
  MIN(duration_us::DOUBLE PRECISION),
  MAX(duration_us::DOUBLE PRECISION),
  AVG(duration_us::DOUBLE PRECISION),
  STDDEV(duration_us::DOUBLE PRECISION),
  PERCENTILE_CONT(0.50) WITHIN GROUP (
    ORDER BY
      duration_us::DOUBLE PRECISION
  ),
  PERCENTILE_CONT(0.95) WITHIN GROUP (
    ORDER BY
      duration_us::DOUBLE PRECISION
  ),
  AVG(scanned_bytes),
  SUM(spilled_bytes),
  COUNT(
    CASE
      WHEN spilled_bytes > 0 THEN 1
    END
  ),
  AVG(time_in_queue_us::DOUBLE PRECISION),
  MAX(time_in_queue_us::DOUBLE PRECISION)
);
