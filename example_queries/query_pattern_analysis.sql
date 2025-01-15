/*
Monitoring Through Persistent Query History
=========================================

Query Pattern Analysis Implementation
----------------------------------
This implementation focuses on identifying problematic query patterns through
statistical analysis of performance, resource usage, and cost metrics.

*/

-- Step 1: Collect base query metrics
-- Purpose: Gather raw performance metrics for each query pattern
WITH BaseQueryMetrics AS (
    SELECT
        query_text_normalized_hash,
        MIN(query_text_normalized) as query_pattern,
        engine_name,
        -- Execution frequency metrics
        COUNT(*) as pattern_occurrence,
        COUNT(DISTINCT DATE_TRUNC('hour', submitted_time)) as distinct_hours,

        -- Performance metrics (all in seconds)
        MIN(duration_us::DOUBLE PRECISION)/1000000 as min_duration_seconds,
        MAX(duration_us::DOUBLE PRECISION)/1000000 as max_duration_seconds,
        AVG(duration_us::DOUBLE PRECISION)/1000000 as avg_duration_seconds,
        STDDEV(duration_us::DOUBLE PRECISION)/1000000 as stddev_duration_seconds,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_us::DOUBLE PRECISION)/1000000 as median_duration_seconds,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_us::DOUBLE PRECISION)/1000000 as p95_duration_seconds,

        -- Resource utilization metrics (in GB)
        AVG(scanned_bytes::DOUBLE PRECISION)/(1024^3) as avg_gigs_scanned,
        SUM(spilled_bytes::DOUBLE PRECISION)/(1024^3) as total_gigs_spilled,
        COUNT(CASE WHEN spilled_bytes > 0 THEN 1 END) as spill_occurrences,

        -- Queue metrics (in seconds)
        AVG(time_in_queue_us::DOUBLE PRECISION)/1000000 as avg_queue_seconds,
        MAX(time_in_queue_us::DOUBLE PRECISION)/1000000 as max_queue_seconds
    FROM persistent_query_history
    WHERE status = 'ENDED_SUCCESSFULLY'
        -- Query type filter:
        -- For analytical queries (current):
    AND query_text_normalized ILIKE 'SELECT%'
        -- For ingestion analysis, use:
        -- AND (query_text_normalized ILIKE 'INSERT%' OR query_text_normalized ILIKE 'COPY FROM%')
        -- Default outlier detection:
        AND query_text_normalized ILIKE 'SELECT%'
        -- Performance filter (identifies outliers)
        AND duration_us > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_us) FROM persistent_query_history)
        -- Time window filter (adjustable)
        AND submitted_time >= CURRENT_DATE() - INTERVAL '7 DAYS'
    GROUP BY ALL
    HAVING COUNT(*) >= 5  -- Minimum executions threshold - change based on your needs
),

-- Step 2: Collect engine utilization metrics
-- Purpose: Analyze engine-level performance and cost patterns
BaseEngineMetrics AS (
    SELECT
        eh.engine_name,
        COUNT(DISTINCT CASE WHEN eh.event_type = 'START' THEN eh.event_start_time END) as start_count,
        -- Calculate average running time (in seconds)
        AVG(CASE
            WHEN eh.event_type IN ('STOP', 'SUSPEND')
            THEN (EXTRACT(EPOCH FROM eh.event_finish_time)::DOUBLE PRECISION -
                  EXTRACT(EPOCH FROM eh.event_start_time)::DOUBLE PRECISION)
        END) as avg_running_time,
        AVG(em.consumed_fbu::DOUBLE PRECISION) as avg_hourly_fbu
    FROM information_schema.engine_history eh
    LEFT JOIN information_schema.engine_metering_history em
        ON eh.engine_name = em.engine_name
        AND eh.event_start_time >= em.start_hour
        AND eh.event_start_time < em.end_hour
    WHERE eh.event_start_time >= CURRENT_DATE() - INTERVAL '7 DAYS'
    GROUP BY ALL
),

-- Step 3: Calculate statistical thresholds
-- Purpose: Establish dynamic performance baselines
/*
Note: These statistical thresholds can be replaced with fixed values if needed:
Example for query duration:
  CASE
      WHEN avg_duration_seconds > 300 THEN 'Very Slow'  -- 5 minutes
      WHEN avg_duration_seconds > 60 THEN 'Slow'        -- 1 minute
      ELSE 'Normal'
  END
*/
PerformanceStats AS (
    SELECT
        AVG(avg_duration_seconds) as global_avg_duration,
        STDDEV(avg_duration_seconds::DOUBLE PRECISION) as global_stddev_duration,
        AVG(pattern_occurrence::DOUBLE PRECISION) as avg_pattern_occurrence
    FROM BaseQueryMetrics
),

-- Purpose: Calculate engine-specific statistical thresholds
EngineStats AS (
    SELECT
        AVG(avg_hourly_fbu) as avg_fbu,
        STDDEV(avg_hourly_fbu::DOUBLE PRECISION) as stddev_fbu,
        AVG(start_count::DOUBLE PRECISION) as avg_starts,
        STDDEV(start_count::DOUBLE PRECISION) as stddev_starts,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY avg_running_time) as p25_runtime
    FROM BaseEngineMetrics
),

-- Step 4: Analyze patterns
-- Purpose: Classify query patterns based on performance, resource usage, and cost
PatternMetrics AS (
    SELECT
        bqm.*,
        bem.start_count as engine_starts_7d,
        bem.avg_running_time/3600 as avg_running_hours,
        bem.avg_hourly_fbu as avg_engine_fbu,

        -- Performance classification
        CASE
            WHEN bqm.avg_duration_seconds > (SELECT global_avg_duration + 2 * global_stddev_duration FROM PerformanceStats)::DOUBLE PRECISION THEN 'Poor'
            WHEN bqm.avg_duration_seconds > (SELECT global_avg_duration + global_stddev_duration FROM PerformanceStats)::DOUBLE PRECISION THEN 'Slow'
            WHEN bqm.p95_duration_seconds > (2 * bqm.median_duration_seconds)::DOUBLE PRECISION THEN 'Inconsistent'
            ELSE 'Good'
        END as performance_pattern,

        -- Resource utilization classification
        CASE
            WHEN bqm.spill_occurrences > (bqm.pattern_occurrence * 0.2)::DOUBLE PRECISION THEN 'Memory Issues'
            WHEN bqm.avg_gigs_scanned > 100 AND bqm.avg_duration_seconds >
                (SELECT global_avg_duration + global_stddev_duration FROM PerformanceStats)::DOUBLE PRECISION THEN 'High Scan Impact'
            WHEN bqm.avg_queue_seconds > (bqm.avg_duration_seconds * 0.5)::DOUBLE PRECISION THEN 'Queue Issues'
            ELSE 'Good'
        END as resource_pattern,

        -- Cost efficiency classification
        CASE
            WHEN bqm.avg_duration_seconds > (SELECT global_avg_duration + global_stddev_duration FROM PerformanceStats)::DOUBLE PRECISION
                AND bem.avg_hourly_fbu > (SELECT avg_fbu + stddev_fbu FROM EngineStats)::DOUBLE PRECISION THEN 'High Cost'
            WHEN bem.start_count > (SELECT avg_starts + stddev_starts FROM EngineStats)::DOUBLE PRECISION
                AND bem.avg_running_time < (SELECT p25_runtime FROM EngineStats)::DOUBLE PRECISION THEN 'Inefficient Runs'
            ELSE 'Normal'
        END as cost_pattern
    FROM BaseQueryMetrics bqm
    LEFT JOIN BaseEngineMetrics bem ON bqm.engine_name = bem.engine_name
),

-- Step 5: Generate recommendations
-- Purpose: Provide actionable insights based on detected patterns
PatternRecommendations AS (
    SELECT
        *,
        ARRAY_AGG(DISTINCT
            CASE
                WHEN spill_occurrences > (pattern_occurrence * 0.2)::DOUBLE PRECISION
                    THEN 'Query needs more memory - try reducing data or increasing engine size'
                WHEN p95_duration_seconds > (2 * median_duration_seconds)::DOUBLE PRECISION
                    THEN 'Query speed varies too much - check for data skew or concurrent loads'
                WHEN avg_queue_seconds > (avg_duration_seconds * 0.5)::DOUBLE PRECISION
                    THEN 'Query often waits in queue - consider running at different times'
                WHEN avg_gigs_scanned > 100 AND avg_duration_seconds >
                    (SELECT global_avg_duration + global_stddev_duration FROM PerformanceStats)::DOUBLE PRECISION
                    THEN 'Query scans too much data - add filters or check table structure'
            END
        ) as recommendations
    FROM PatternMetrics
    GROUP BY ALL
)

-- Final output: Show only patterns requiring attention
-- Ordered by performance impact (most critical first)
SELECT
    query_text_normalized_hash,
    engine_name,
    query_pattern,
    pattern_occurrence,
    avg_duration_seconds,
    p95_duration_seconds,
    avg_gigs_scanned,
    spill_occurrences,
    avg_queue_seconds,
    engine_starts_7d,
    avg_engine_fbu,
    performance_pattern,
    resource_pattern,
    cost_pattern,
    recommendations
FROM PatternRecommendations
WHERE performance_pattern != 'Good'
   OR resource_pattern != 'Good'
   OR cost_pattern != 'Normal'
ORDER BY
    CASE performance_pattern
        WHEN 'Poor' THEN 1
        WHEN 'Slow' THEN 2
        WHEN 'Inconsistent' THEN 3
        ELSE 4
    END,
    avg_duration_seconds DESC
LIMIT 50;