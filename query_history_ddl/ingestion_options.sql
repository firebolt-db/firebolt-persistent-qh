/* Option 1: Incremental Load */
INSERT INTO
  persistent_query_history
SELECT
  engine_name,
  engine_id,
  account_name,
  user_name,
  login_name,
  service_account_name,
  submitted_time,
  start_time,
  end_time,
  duration_us,
  status,
  request_id,
  query_id,
  query_label,
  query_text,
  query_text_normalized,
  query_text_normalized_hash,
  error_message,
  scanned_rows,
  scanned_bytes,
  inserted_rows,
  inserted_bytes,
  spilled_bytes,
  returned_rows,
  returned_bytes,
  time_in_queue_us,
  retries,
  proxy_time_us,
  $source_file_name,
  $source_file_timestamp
FROM
  ex_persistent_query_history
WHERE
  $source_file_timestamp >= (
    SELECT
      COALESCE(MAX(source_file_timestamp), '1900-01-01 00:00:00')
    FROM
      persistent_query_history
  );

/* Option 2: Full Load */
TRUNCATE TABLE persistent_query_history;
INSERT INTO
  persistent_query_history
SELECT
  engine_name,
  engine_id,
  account_name,
  user_name,
  login_name,
  service_account_name,
  submitted_time,
  start_time,
  end_time,
  duration_us,
  status,
  request_id,
  query_id,
  query_label,
  query_text,
  query_text_normalized,
  query_text_normalized_hash,
  error_message,
  scanned_rows,
  scanned_bytes,
  inserted_rows,
  inserted_bytes,
  spilled_bytes,
  returned_rows,
  returned_bytes,
  time_in_queue_us,
  retries,
  proxy_time_us,
  $source_file_name,
  $source_file_timestamp
FROM
  ex_persistent_query_history
;

/* Option 3: Copy From */

COPY persistent_query_history(
"engine_name" "engine_name",
"engine_id" "engine_id",
"account_name" "account_name",
"user_name" "user_name",
"login_name" "login_name",
"service_account_name" "service_account_name",
"submitted_time" "submitted_time",
"start_time" "start_time",
"end_time" "end_time",
"duration_us" "duration_us",
"status" "status",
"request_id" "request_id",
"query_id" "query_id",
"query_label" "query_label",
"query_text" "query_text",
"query_text_normalized" "query_text_normalized",
"query_text_normalized_hash" "query_text_normalized_hash",
"error_message" "error_message",
"scanned_rows" "scanned_rows",
"scanned_bytes" "scanned_bytes",
"inserted_rows" "inserted_rows",
"inserted_bytes" "inserted_bytes",
"spilled_bytes" "spilled_bytes",
"returned_rows" "returned_rows",
"returned_bytes" "returned_bytes",
"time_in_queue_us" "time_in_queue_us",
"retries" "retries",
"proxy_time_us" "proxy_time_us",
"source_file_name" $source_file_name,
"source_file_timestamp" $source_file_timestamp
)
FROM 's3://firebolt-production-data-lake-us-east-1/queryhistory/persistent/account_id=01jbcpnd7wsdhm234r5dw0td6q/'
WITH TYPE=PARQUET
PATTERN = '*.parquet'
"CREDENTIALS" = (AWS_ROLE_ARN = 'arn:aws:iam::088947231907:role/bucket-access-891377356463-01j2meefpz4z1b312nvss96a2q')
WHERE $source_file_timestamp > (
    SELECT
      COALESCE(MAX(source_file_timestamp), '1900-01-01 00:00:00')
    FROM
      persistent_query_history
  )