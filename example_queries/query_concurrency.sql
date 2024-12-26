with base_queries as (
select 
        date_trunc('minute',qh.start_time) as minute,
        date_trunc('hour',qh.start_time) as hour,
        date_trunc('day',qh.start_time) as day,
        query_id,
        query_text,
        status
        ,duration_usec
        ,case 
            when lower(query_text) like '%insert into%' then 1
            else 0 
         end insert_count
        ,case 
            when (lower(query_text) like '%select%' 
            and  lower(query_text) not like '%insert into%') then 1 
            else 0
         end select_count
        ,case 
            when lower(query_text) like '%create dimension%'
                or lower(query_text) like '%create fact%'
                or lower(query_text) like '%drop table%' then 1 
            else 0
         end ddl_count
        ,case 
            when error_message != '' then 1 
            else 0 
        end err_count
        ,total_ram_consumed/(1024*1024*1024) as total_ram_gb
        ,scanned_bytes/(1024*1024*1024) as scanned_gb
    from persistent_query_history qh
    where lower(query_text) not like '%query_history%'
    and lower(query_text) not like '%running_queries%'
    and lower(query_text) not like '%show_indexes%'
    and lower(query_text) not like '%show_tables%'
    and status != 'STARTED_EXECUTION'
)


, by_minute as (
select minute, day, hour,
    count(*) as query_count,
    avg(duration_usec/1000000) as avg_dur_sec,
    sum(total_ram_gb) as sum_mem_usage_gb,
    sum(select_count) as select_count,
    sum(insert_count) as insert_count,
    sum(ddl_count) as ddl_count,
    sum(err_count) as err_count
from base_queries
group by all
)

, by_hour as (
select hour, day,
    round(avg(query_count)) as avg_concurrent_queries_per_min,
    round(avg(avg_dur_sec),3) as avg_query_dur_sec,
    round(avg(sum_mem_usage_gb),3) as avg_mem_usage_gb,
    round(max(sum_mem_usage_gb),3) as max_mem_usage_gb,
    round(max(sum_mem_usage_gb)/0.6) as minimum_recommended_ram_gb,
    sum(select_count) as select_count,
    sum(insert_count) as insert_count,
    sum(ddl_count) as ddl_count,
    sum(err_count) as err_count
from by_minute
group by all
)

select * from by_hour;
