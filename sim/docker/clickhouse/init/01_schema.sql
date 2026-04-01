-- ============================================================================
-- ClickHouse Schema for RCA Metrics
-- ============================================================================

CREATE DATABASE IF NOT EXISTS rca_metrics;

-- ============================================================================
-- Create User for DMV Collector
-- ============================================================================
CREATE USER IF NOT EXISTS rca IDENTIFIED BY 'rca_password';
GRANT ALL ON rca_metrics.* TO rca;

-- ============================================================================
-- Incidents Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.incidents (
    incident_id String,
    name String,
    scenario Nullable(String),
    started_at DateTime64(3),
    ended_at Nullable(DateTime64(3)),
    baseline_start Nullable(DateTime64(3)),
    baseline_end Nullable(DateTime64(3)),
    status LowCardinality(String) DEFAULT 'active',
    notes Nullable(String),
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY incident_id;

-- ============================================================================
-- Wait Stats (from custom DMV collector)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.wait_stats (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id Nullable(String),
    is_baseline UInt8 DEFAULT 0,
    wait_type LowCardinality(String),
    waiting_tasks_count UInt64,
    wait_time_ms UInt64,
    max_wait_time_ms UInt64,
    signal_wait_time_ms UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (collected_at, wait_type)
TTL toDateTime(collected_at) + INTERVAL 30 DAY;

-- ============================================================================
-- Active Requests (from custom DMV collector)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.active_requests (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id Nullable(String),
    is_baseline UInt8 DEFAULT 0,
    session_id UInt32,
    request_id UInt32,
    status LowCardinality(String),
    command LowCardinality(String),
    blocking_session_id Nullable(UInt32),
    wait_type LowCardinality(Nullable(String)),
    wait_time_ms Nullable(UInt64),
    wait_resource Nullable(String),
    cpu_time_ms Nullable(UInt64),
    logical_reads Nullable(UInt64),
    writes Nullable(UInt64),
    database_name Nullable(String),
    sql_text Nullable(String),
    context_info Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (collected_at, session_id)
TTL toDateTime(collected_at) + INTERVAL 30 DAY;

-- ============================================================================
-- Blocking Chains (from custom DMV collector)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.blocking_chains (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id Nullable(String),
    is_baseline UInt8 DEFAULT 0,
    blocking_level UInt8,
    session_id UInt32,
    blocking_session_id Nullable(UInt32),
    wait_type LowCardinality(Nullable(String)),
    wait_time_ms Nullable(UInt64),
    wait_resource Nullable(String),
    lock_mode Nullable(String),
    status Nullable(String),
    command Nullable(String),
    database_name Nullable(String),
    sql_text Nullable(String),
    transaction_id Nullable(UInt64),
    open_transaction_count Nullable(UInt8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (collected_at, blocking_level, session_id)
TTL toDateTime(collected_at) + INTERVAL 30 DAY;

-- ============================================================================
-- Query Stats (from custom DMV collector)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.query_stats (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id Nullable(String),
    is_baseline UInt8 DEFAULT 0,
    query_hash String,
    query_plan_hash Nullable(String),
    execution_count UInt64,
    total_worker_time_us UInt64,
    total_elapsed_time_us UInt64,
    total_logical_reads UInt64,
    total_logical_writes UInt64,
    total_physical_reads UInt64,
    total_grant_kb Nullable(UInt64),
    total_spills Nullable(UInt64),
    database_name Nullable(String),
    sql_text Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (collected_at, query_hash)
TTL toDateTime(collected_at) + INTERVAL 30 DAY;

-- ============================================================================
-- Memory Grants (from custom DMV collector)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.memory_grants (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id Nullable(String),
    is_baseline UInt8 DEFAULT 0,
    session_id UInt32,
    request_time Nullable(DateTime64(3)),
    grant_time Nullable(DateTime64(3)),
    requested_memory_mb Float64,
    granted_memory_mb Float64,
    required_memory_mb Float64,
    used_memory_mb Float64,
    max_used_memory_mb Float64,
    ideal_memory_mb Float64,
    wait_time_ms UInt32,
    grant_status LowCardinality(String),  -- WAITING, SPILL_LIKELY, SPILLED, OK
    query_cost Nullable(Float64),
    dop Nullable(UInt8),
    sql_text Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (collected_at, session_id)
TTL toDateTime(collected_at) + INTERVAL 30 DAY;

-- ============================================================================
-- File I/O Stats (from custom DMV collector)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.file_stats (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id Nullable(String),
    is_baseline UInt8 DEFAULT 0,
    database_id UInt16,
    database_name Nullable(String),
    file_id UInt16,
    file_name Nullable(String),
    file_type LowCardinality(String),
    num_of_reads UInt64,
    num_of_bytes_read UInt64,
    io_stall_read_ms UInt64,
    num_of_writes UInt64,
    num_of_bytes_written UInt64,
    io_stall_write_ms UInt64,
    io_stall_ms UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (collected_at, database_id, file_id)
TTL toDateTime(collected_at) + INTERVAL 30 DAY;

-- ============================================================================
-- Schedulers (from custom DMV collector)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.schedulers (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id Nullable(String),
    is_baseline UInt8 DEFAULT 0,
    scheduler_id UInt16,
    cpu_id UInt16,
    status LowCardinality(String),
    is_online UInt8,
    current_tasks_count UInt32,
    runnable_tasks_count UInt32,
    current_workers_count UInt32,
    active_workers_count UInt32,
    work_queue_count UInt64,
    context_switches_count UInt64,
    yield_count UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (collected_at, scheduler_id)
TTL toDateTime(collected_at) + INTERVAL 30 DAY;

-- ============================================================================
-- Performance Counters (from custom DMV collector)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.perf_counters (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id Nullable(String),
    is_baseline UInt8 DEFAULT 0,
    object_name LowCardinality(String),
    counter_name LowCardinality(String),
    instance_name Nullable(String),
    counter_value Int64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (collected_at, object_name, counter_name)
TTL toDateTime(collected_at) + INTERVAL 30 DAY;

-- ============================================================================
-- Missing Indexes (from custom DMV collector)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.missing_indexes (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id Nullable(String),
    is_baseline UInt8 DEFAULT 0,
    database_name String,
    schema_name String,
    table_name String,
    equality_columns Nullable(String),
    inequality_columns Nullable(String),
    included_columns Nullable(String),
    unique_compiles UInt64,
    user_seeks UInt64,
    user_scans UInt64,
    avg_total_user_cost Float64,
    avg_user_impact Float64,
    impact_score Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (collected_at, database_name, table_name)
TTL toDateTime(collected_at) + INTERVAL 30 DAY;

-- ============================================================================
-- Blitz Results (from Blitz integration)
-- ============================================================================
CREATE TABLE IF NOT EXISTS rca_metrics.blitz_results (
    collected_at DateTime64(3) DEFAULT now64(3),
    incident_id String,
    blitz_type LowCardinality(String),  -- BlitzFirst, BlitzCache, BlitzWho, BlitzIndex, BlitzLock
    priority UInt8,
    findings_group Nullable(String),
    finding Nullable(String),
    details Nullable(String),
    url Nullable(String),
    query_text Nullable(String),
    database_name Nullable(String),
    -- BlitzCache specific
    total_cpu Nullable(UInt64),
    total_reads Nullable(UInt64),
    total_writes Nullable(UInt64),
    execution_count Nullable(UInt64),
    avg_duration_ms Nullable(Float64),
    warnings Nullable(String),
    total_spills Nullable(UInt64),
    -- BlitzWho specific
    session_id Nullable(UInt32),
    status Nullable(String),
    wait_info Nullable(String),
    blocking_session_id Nullable(UInt32),
    cpu_ms Nullable(UInt64),
    reads Nullable(UInt64),
    -- BlitzIndex specific
    schema_name Nullable(String),
    table_name Nullable(String),
    index_name Nullable(String),
    index_definition Nullable(String),
    create_tsql Nullable(String),
    -- BlitzLock specific
    deadlock_type Nullable(String),
    victim_query Nullable(String),
    blocking_query Nullable(String),
    deadlock_graph Nullable(String),
    -- Extended data as JSON
    extended_data Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (incident_id, collected_at, blitz_type)
TTL toDateTime(collected_at) + INTERVAL 90 DAY;

-- ============================================================================
-- Materialized Views for Aggregations
-- ============================================================================

-- Wait stats aggregated per minute
CREATE MATERIALIZED VIEW IF NOT EXISTS rca_metrics.wait_stats_1m
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(minute)
ORDER BY (minute, wait_type)
AS SELECT
    toStartOfMinute(collected_at) AS minute,
    wait_type,
    sum(waiting_tasks_count) AS total_waiting_tasks,
    sum(wait_time_ms) AS total_wait_time_ms,
    max(max_wait_time_ms) AS max_wait_time_ms,
    sum(signal_wait_time_ms) AS total_signal_wait_time_ms,
    count() AS sample_count
FROM rca_metrics.wait_stats
GROUP BY minute, wait_type;

-- Query stats aggregated per minute
CREATE MATERIALIZED VIEW IF NOT EXISTS rca_metrics.query_stats_1m
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(minute)
ORDER BY (minute, query_hash)
AS SELECT
    toStartOfMinute(collected_at) AS minute,
    query_hash,
    sum(execution_count) AS total_executions,
    sum(total_worker_time_us) AS total_cpu_us,
    sum(total_elapsed_time_us) AS total_elapsed_us,
    sum(total_logical_reads) AS total_reads,
    max(sql_text) AS sql_text
FROM rca_metrics.query_stats
GROUP BY minute, query_hash;

-- Blocking events summary
CREATE MATERIALIZED VIEW IF NOT EXISTS rca_metrics.blocking_summary_1m
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(minute)
ORDER BY (minute)
AS SELECT
    toStartOfMinute(collected_at) AS minute,
    count() AS blocking_events,
    max(blocking_level) AS max_blocking_depth,
    sum(wait_time_ms) AS total_wait_time_ms,
    countDistinct(session_id) AS unique_blocked_sessions
FROM rca_metrics.blocking_chains
WHERE blocking_level > 0
GROUP BY minute;
