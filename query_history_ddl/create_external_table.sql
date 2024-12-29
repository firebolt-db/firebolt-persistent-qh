CREATE EXTERNAL TABLE IF NOT EXISTS "ex_persistent_query_history" (
  engine_name TEXT,
  engine_id TEXT,    
  account_name TEXT,        
  user_name TEXT,
  login_name TEXT,
  service_account_name TEXT,
  submitted_time TIMESTAMPTZ,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_us BIGINT,
  status TEXT,
  request_id TEXT,
  query_id TEXT,
  query_label TEXT,
  query_text TEXT,
  query_text_normalized TEXT,
  query_text_normalized_hash TEXT,
  error_message TEXT,
  scanned_rows BIGINT,
  scanned_bytes BIGINT,
  inserted_rows BIGINT,
  inserted_bytes BIGINT,
  spilled_bytes BIGINT,
  returned_rows BIGINT,
  returned_bytes BIGINT,
  time_in_queue_us BIGINT,
  retries BIGINT,
  proxy_time_us BIGINT
)        
  "CREDENTIALS" = ("AWS_ROLE_ARN" = '<arn_name>') 
  "OBJECT_PATTERN" = '*.parquet' 
  "TYPE" = ("PARQUET")
  "URL" = '<s3_URL>';
