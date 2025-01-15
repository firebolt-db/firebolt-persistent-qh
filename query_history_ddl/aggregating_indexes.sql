CREATE AGGREGATING INDEX ix_agg_query_pattern ON persistent_query_history (
  query_text_normalized_hash,
  engine_name,
  MIN(query_text_normalized),
  COUNT(*),
  COUNT(DISTINCT DATE_TRUNC('hour', submitted_time)),
  MIN(duration_us),
  MAX(duration_us),
  AVG(duration_us)
);
