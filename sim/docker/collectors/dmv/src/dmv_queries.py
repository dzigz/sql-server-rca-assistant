"""SQL queries for collecting DMV metrics from SQL Server."""


class DMVQueries:
    """SQL queries for DMV collection."""

    # Benign wait types to filter out
    BENIGN_WAITS = """
        'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE',
        'SLEEP_TASK', 'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH',
        'WAITFOR', 'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE',
        'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
        'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT',
        'CLR_AUTO_EVENT', 'DISPATCHER_QUEUE_SEMAPHORE',
        'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT',
        'XE_DISPATCHER_JOIN', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        'ONDEMAND_TASK_QUEUE', 'BROKER_EVENTHANDLER',
        'SLEEP_BPOOL_FLUSH', 'DIRTY_PAGE_POLL',
        'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'SP_SERVER_DIAGNOSTICS_SLEEP',
        'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'QDS_ASYNC_QUEUE',
        'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
        'WAIT_XTP_CKPT_CLOSE', 'REDO_THREAD_PENDING_WORK',
        'PWAIT_ALL_COMPONENTS_INITIALIZED', 'PWAIT_DIRECTLOGCONSUMER_GETNEXT'
    """

    WAIT_STATS = f"""
        SELECT
            wait_type,
            waiting_tasks_count,
            wait_time_ms,
            max_wait_time_ms,
            signal_wait_time_ms
        FROM sys.dm_os_wait_stats
        WHERE wait_time_ms > 0
        AND wait_type NOT IN ({BENIGN_WAITS})
        ORDER BY wait_time_ms DESC
    """

    ACTIVE_REQUESTS = """
        SELECT
            r.session_id,
            r.request_id,
            r.status,
            r.command,
            r.blocking_session_id,
            r.wait_type,
            r.wait_time AS wait_time_ms,
            r.wait_resource,
            r.cpu_time AS cpu_time_ms,
            r.logical_reads,
            r.writes,
            DB_NAME(r.database_id) AS database_name,
            SUBSTRING(t.text, (r.statement_start_offset/2)+1,
                ((CASE r.statement_end_offset
                    WHEN -1 THEN DATALENGTH(t.text)
                    ELSE r.statement_end_offset
                END - r.statement_start_offset)/2) + 1) AS sql_text,
            CAST(s.context_info AS VARCHAR(128)) AS context_info
        FROM sys.dm_exec_requests r
        JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
        WHERE r.session_id != @@SPID
        AND r.status != 'background'
    """

    BLOCKING_CHAINS = """
        WITH BlockingTree AS (
            -- Head blockers (sessions blocking others but not blocked themselves)
            SELECT
                0 AS blocking_level,
                r.session_id,
                r.blocking_session_id,
                r.wait_type,
                r.wait_time AS wait_time_ms,
                r.wait_resource,
                r.last_wait_type AS lock_mode,
                r.status,
                r.command,
                DB_NAME(r.database_id) AS database_name,
                SUBSTRING(t.text, 1, 4000) AS sql_text,
                r.transaction_id,
                s.open_transaction_count
            FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
            OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
            WHERE r.blocking_session_id = 0
            AND r.session_id IN (
                SELECT DISTINCT blocking_session_id
                FROM sys.dm_exec_requests
                WHERE blocking_session_id != 0
            )

            UNION ALL

            -- Blocked sessions (recursive)
            SELECT
                bt.blocking_level + 1,
                r.session_id,
                r.blocking_session_id,
                r.wait_type,
                r.wait_time AS wait_time_ms,
                r.wait_resource,
                r.last_wait_type AS lock_mode,
                r.status,
                r.command,
                DB_NAME(r.database_id) AS database_name,
                SUBSTRING(t.text, 1, 4000) AS sql_text,
                r.transaction_id,
                s.open_transaction_count
            FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
            OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
            JOIN BlockingTree bt ON r.blocking_session_id = bt.session_id
            WHERE r.blocking_session_id != 0
            AND bt.blocking_level < 10  -- Prevent infinite recursion
        )
        SELECT * FROM BlockingTree
        ORDER BY blocking_level, session_id
    """

    MEMORY_GRANTS = """
        SELECT
            mg.session_id,
            mg.request_time,
            mg.grant_time,
            CAST(mg.requested_memory_kb / 1024.0 AS DECIMAL(18,2)) AS requested_memory_mb,
            CAST(ISNULL(mg.granted_memory_kb, 0) / 1024.0 AS DECIMAL(18,2)) AS granted_memory_mb,
            CAST(ISNULL(mg.required_memory_kb, 0) / 1024.0 AS DECIMAL(18,2)) AS required_memory_mb,
            CAST(ISNULL(mg.used_memory_kb, 0) / 1024.0 AS DECIMAL(18,2)) AS used_memory_mb,
            CAST(ISNULL(mg.max_used_memory_kb, 0) / 1024.0 AS DECIMAL(18,2)) AS max_used_memory_mb,
            CAST(ISNULL(mg.ideal_memory_kb, 0) / 1024.0 AS DECIMAL(18,2)) AS ideal_memory_mb,
            ISNULL(mg.wait_time_ms, 0) AS wait_time_ms,
            CASE
                WHEN mg.grant_time IS NULL THEN 'WAITING'
                WHEN mg.max_used_memory_kb > mg.granted_memory_kb THEN 'SPILLED'
                WHEN mg.granted_memory_kb < mg.required_memory_kb THEN 'SPILL_LIKELY'
                ELSE 'OK'
            END AS grant_status,
            mg.query_cost,
            mg.dop,
            SUBSTRING(t.text, 1, 4000) AS sql_text
        FROM sys.dm_exec_query_memory_grants mg
        OUTER APPLY sys.dm_exec_sql_text(mg.sql_handle) t
        WHERE mg.session_id != @@SPID
    """

    QUERY_STATS = """
        SELECT TOP 50
            CONVERT(VARCHAR(32), qs.query_hash, 1) AS query_hash,
            CONVERT(VARCHAR(32), qs.query_plan_hash, 1) AS query_plan_hash,
            qs.execution_count,
            qs.total_worker_time AS total_worker_time_us,
            qs.total_elapsed_time AS total_elapsed_time_us,
            qs.total_logical_reads,
            qs.total_logical_writes,
            qs.total_physical_reads,
            qs.total_grant_kb,
            qs.total_spills,
            DB_NAME(qt.dbid) AS database_name,
            SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
                ((CASE qs.statement_end_offset
                    WHEN -1 THEN DATALENGTH(qt.text)
                    ELSE qs.statement_end_offset
                END - qs.statement_start_offset)/2) + 1) AS sql_text
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
        WHERE qs.execution_count > 0
        ORDER BY qs.total_worker_time DESC
    """

    # Safe version for SQL Server < 2016 SP1 (no total_grant_kb, total_spills columns)
    QUERY_STATS_SAFE = """
        SELECT TOP 50
            CONVERT(VARCHAR(32), qs.query_hash, 1) AS query_hash,
            CONVERT(VARCHAR(32), qs.query_plan_hash, 1) AS query_plan_hash,
            qs.execution_count,
            qs.total_worker_time AS total_worker_time_us,
            qs.total_elapsed_time AS total_elapsed_time_us,
            qs.total_logical_reads,
            qs.total_logical_writes,
            qs.total_physical_reads,
            0 AS total_grant_kb,
            0 AS total_spills,
            DB_NAME(qt.dbid) AS database_name,
            SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
                ((CASE qs.statement_end_offset
                    WHEN -1 THEN DATALENGTH(qt.text)
                    ELSE qs.statement_end_offset
                END - qs.statement_start_offset)/2) + 1) AS sql_text
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
        WHERE qs.execution_count > 0
        ORDER BY qs.total_worker_time DESC
    """

    FILE_STATS = """
        SELECT
            vfs.database_id,
            DB_NAME(vfs.database_id) AS database_name,
            vfs.file_id,
            mf.name AS file_name,
            mf.type_desc AS file_type,
            vfs.num_of_reads,
            vfs.num_of_bytes_read,
            vfs.io_stall_read_ms,
            vfs.num_of_writes,
            vfs.num_of_bytes_written,
            vfs.io_stall_write_ms,
            vfs.io_stall AS io_stall_ms
        FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
        JOIN sys.master_files mf ON vfs.database_id = mf.database_id
            AND vfs.file_id = mf.file_id
    """

    SCHEDULERS = """
        SELECT
            scheduler_id,
            cpu_id,
            status,
            is_online,
            current_tasks_count,
            runnable_tasks_count,
            current_workers_count,
            active_workers_count,
            work_queue_count,
            context_switches_count,
            yield_count
        FROM sys.dm_os_schedulers
        WHERE status = 'VISIBLE ONLINE'
    """

    PERF_COUNTERS = """
        SELECT
            RTRIM(object_name) AS object_name,
            RTRIM(counter_name) AS counter_name,
            RTRIM(instance_name) AS instance_name,
            cntr_value AS counter_value
        FROM sys.dm_os_performance_counters
        WHERE object_name LIKE '%Buffer Manager%'
            OR object_name LIKE '%SQL Statistics%'
            OR object_name LIKE '%Locks%'
            OR object_name LIKE '%Memory Manager%'
            OR object_name LIKE '%Plan Cache%'
            OR counter_name IN (
                'Batch Requests/sec',
                'SQL Compilations/sec',
                'SQL Re-Compilations/sec',
                'Page life expectancy',
                'Lock Waits/sec',
                'Memory Grants Pending',
                'Memory Grants Outstanding',
                'Target Server Memory (KB)',
                'Total Server Memory (KB)'
            )
    """

    MISSING_INDEXES = """
        SELECT
            DB_NAME(mid.database_id) AS database_name,
            OBJECT_SCHEMA_NAME(mid.object_id, mid.database_id) AS schema_name,
            OBJECT_NAME(mid.object_id, mid.database_id) AS table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            migs.unique_compiles,
            migs.user_seeks,
            migs.user_scans,
            migs.avg_total_user_cost,
            migs.avg_user_impact,
            migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) AS impact_score
        FROM sys.dm_db_missing_index_details mid
        JOIN sys.dm_db_missing_index_groups mig ON mid.index_handle = mig.index_handle
        JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
        WHERE mid.database_id = DB_ID()
        ORDER BY impact_score DESC
    """
