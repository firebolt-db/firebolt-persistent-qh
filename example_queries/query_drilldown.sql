select 
    query_text_normalized,
    median(duration_usec/1000000) as duration_sec, 
    start_date, 
    avg(total_ram_consumed/(1024*1024*1024)) as avg_ram_used_gb,
    max(total_ram_consumed/(1024*1024*1024)) as max_ram_used_gb,
    median(total_ram_consumed/(1024*1024*1024)) as median_ram_used_gb,
    max_by(query_id, total_ram_consumed) as example_query_id,
    count(query_id) as executions
from customer_query_history_craftable_us_west_2
where status = 'ENDED_SUCCESSFULLY'
and match(lower(query_text_normalized), 'information_schema') = 0
and match(lower(query_text_normalized), 'create|insert|drop|delete') = 0
and start_date >= '2023-12-25'
group by all;
