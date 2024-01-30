create aggregating index ix_agg_query_concurrency on persistent_query_history (
status,
query_text,
date_trunc('hour', start_time),
date_trunc('day', start_time),
date_trunc('minute', start_time),
count(*), 
avg(duration_usec/1000000),
sum(total_ram_consumed/(1024*1024*1024)),
sum(case when lower(query_text) like '%insert into%' then 1 else 0 end),
 sum(case when (lower(query_text) like '%select%' and  lower(query_text) not like '%insert into%') then 1 else 0 end),
sum(case when lower(query_text) like '%create dimension%' or lower(query_text) like '%create fact%' or lower(query_text) like '%drop table%' then 1 else 0 end),
sum(case when error_message != '' then 1 else 0 end)
);

create aggregating index ix_agg_per_query_stats on persistent_query_history (
status,
start_date,
query_text_normalized,
median(duration_usec/1000000), 
avg(total_ram_consumed/(1024*1024*1024)),
max(total_ram_consumed/(1024*1024*1024)),
median(total_ram_consumed/(1024*1024*1024)),
max_by(query_id, total_ram_consumed),
count(query_id)
);
