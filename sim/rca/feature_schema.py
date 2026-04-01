"""
Feature Schema Builder for RCA.

Transforms raw analytics JSON into a normalized, compact Feature Schema
that serves as the primary input for the AI RCA engine.

The Feature Schema is database-agnostic and contains:
- Meta information and time windows
- Global resource metrics
- Wait profile
- Query-level metrics
- Blocking information
- Configuration/stats/index changes
- Errors and timeouts

Note: The analytics.json format v2.0 contains raw time-series data
without baseline/incident split. The Feature Schema presents current
state metrics for RCA analysis.
"""

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union


# Benign/infrastructure wait types to filter from feature schema
# These are typically irrelevant to workload issues and add noise
BENIGN_WAITS = {
    # Extended Events internals
    'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT', 'XE_DISPATCHER_JOIN',
    'XE_BUFFERMGR_ALLPROCESSED_EVENT', 'XE_FILE_TARGET_TVF', 'XE_LIVE_TARGET_TVF',
    # Service Broker background
    'BROKER_EVENTHANDLER', 'BROKER_RECEIVE_WAITFOR', 'BROKER_TASK_STOP',
    'BROKER_TO_FLUSH', 'BROKER_TRANSMITTER',
    # Background sleep/maintenance
    'SLEEP_TASK', 'SLEEP_SYSTEMTASK', 'SLEEP_BPOOL_FLUSH', 'SLEEP_DBSTARTUP',
    'SLEEP_DCOMSTARTUP', 'SLEEP_MASTERDBREADY', 'SLEEP_MASTERMDREADY',
    'SLEEP_MASTERUPGRADED', 'SLEEP_MSDBSTARTUP', 'SLEEP_TEMPDBSTARTUP',
    'LAZYWRITER_SLEEP', 'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE',
    'WAITFOR', 'WAITFOR_TASKSHUTDOWN',
    # Query Store maintenance
    'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'QDS_ASYNC_QUEUE',
    'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', 'QDS_SHUTDOWN_QUEUE',
    # Always On replication background
    'HADR_CLUSAPI_CALL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
    'HADR_LOGCAPTURE_WAIT', 'HADR_NOTIFICATION_DEQUEUE',
    'HADR_TIMER_TASK', 'HADR_WORK_QUEUE',
    # Other background/infrastructure
    'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'CLR_SEMAPHORE',
    'DIRTY_PAGE_POLL', 'DISPATCHER_QUEUE_SEMAPHORE',
    'REQUEST_FOR_DEADLOCK_SEARCH', 'SQLTRACE_BUFFER_FLUSH',
    'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'SQLTRACE_WAIT_ENTRIES',
    'SP_SERVER_DIAGNOSTICS_SLEEP', 'SNI_HTTP_ACCEPT', 'SOS_WORK_DISPATCHER',
    'SERVER_IDLE_CHECK', 'RESOURCE_QUEUE', 'ONDEMAND_TASK_QUEUE',
    'KSOURCE_WAKEUP', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'FT_IFTSHC_MUTEX',
    'EXECSYNC', 'FSAGENT', 'MEMORY_ALLOCATION_EXT',
    # Startup/shutdown
    'STARTUP_DEPENDENCY_MANAGER', 'PWAIT_ALL_COMPONENTS_INITIALIZED',
    'PWAIT_DIRECTLOGCONSUMER_GETNEXT', 'PWAIT_EXTENSIBILITY_CLEANUP_TASK',
    # In-Memory OLTP
    'XTP_PREEMPTIVE_TASK', 'WAIT_XTP_TASK_SHUTDOWN', 'WAIT_XTP_CKPT_CLOSE',
    'WAIT_XTP_HOST_WAIT', 'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', 'WAIT_XTP_RECOVERY',
    # Mirroring
    'DBMIRROR_DBM_EVENT', 'DBMIRROR_EVENTS_QUEUE', 'DBMIRROR_WORKER_QUEUE',
    'DBMIRRORING_CMD',
    # Azure/cloud
    'AZURE_IMDS_VERSIONS',
    # Parallel redo
    'PARALLEL_REDO_DRAIN_WORKER', 'PARALLEL_REDO_LOG_CACHE',
    'PARALLEL_REDO_TRAN_LIST', 'PARALLEL_REDO_WORKER_SYNC',
    'PARALLEL_REDO_WORKER_WAIT_WORK', 'REDO_THREAD_PENDING_WORK',
    # Preemptive
    'PREEMPTIVE_OS_FLUSHFILEBUFFERS', 'PREEMPTIVE_XE_GETTARGETSTATE',
    # Other
    'PVS_PREALLOCATE', 'VDI_CLIENT_OTHER', 'WAIT_FOR_RESULTS', 'CHKPT',
}


@dataclass
class TimeWindow:
    """Time window for an analysis period."""
    start: str
    end: str


@dataclass
class FeatureSchema:
    """
    Complete Feature Schema for RCA analysis.

    This is the normalized, compact representation of an incident
    that serves as input to the AI RCA engine.
    """
    meta: dict
    time_windows: dict
    global_resources: dict
    query_groups: dict
    blocking_info: dict
    config_changes: list[dict]
    errors_and_timeouts: list[dict]
    missing_indexes: list[dict] = field(default_factory=list)

    # Extended telemetry fields (Phase 1-6 expansion)
    memory_profile: dict = field(default_factory=dict)
    memory_grants: list[dict] = field(default_factory=list)  # Query memory grant status
    scheduler_health: dict = field(default_factory=dict)
    io_profile: dict = field(default_factory=dict)
    index_usage: list[dict] = field(default_factory=list)
    query_store_insights: list[dict] = field(default_factory=list)
    schema_changes: list[dict] = field(default_factory=list)
    server_config: list[dict] = field(default_factory=list)
    workload_context: dict = field(default_factory=dict)

    # Phase 7: Extended External Context
    application_events: list[dict] = field(default_factory=list)
    incident_context: list[dict] = field(default_factory=list)

    # Blitz Script Output (First Responder Kit)
    blitz_findings: list[dict] = field(default_factory=list)
    blitz_wait_stats_delta: list[dict] = field(default_factory=list)
    blitz_file_stats: list[dict] = field(default_factory=list)
    blitz_query_plan_warnings: list[dict] = field(default_factory=list)
    blitz_active_sessions: list[dict] = field(default_factory=list)
    blitz_index_analysis: list[dict] = field(default_factory=list)
    blitz_deadlocks: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        result = {
            "meta": self.meta,
            "time_windows": self.time_windows,
            "global_resources": self.global_resources,
            "query_groups": self.query_groups,
            "blocking_info": self.blocking_info,
            "config_changes": self.config_changes,
            "errors_and_timeouts": self.errors_and_timeouts,
            "missing_indexes": self.missing_indexes,
        }

        # Only include extended fields if they have data
        if self.memory_profile:
            result["memory_profile"] = self.memory_profile
        if self.memory_grants:
            result["memory_grants"] = self.memory_grants
        if self.scheduler_health:
            result["scheduler_health"] = self.scheduler_health
        if self.io_profile:
            result["io_profile"] = self.io_profile
        if self.index_usage:
            result["index_usage"] = self.index_usage
        if self.query_store_insights:
            result["query_store_insights"] = self.query_store_insights
        if self.schema_changes:
            result["schema_changes"] = self.schema_changes
        if self.server_config:
            result["server_config"] = self.server_config
        if self.workload_context:
            result["workload_context"] = self.workload_context
        if self.application_events:
            result["application_events"] = self.application_events
        if self.incident_context:
            result["incident_context"] = self.incident_context

        # Blitz Script Output
        if self.blitz_findings:
            result["blitz_findings"] = self.blitz_findings
        if self.blitz_wait_stats_delta:
            result["blitz_wait_stats_delta"] = self.blitz_wait_stats_delta
        if self.blitz_file_stats:
            result["blitz_file_stats"] = self.blitz_file_stats
        if self.blitz_query_plan_warnings:
            result["blitz_query_plan_warnings"] = self.blitz_query_plan_warnings
        if self.blitz_active_sessions:
            result["blitz_active_sessions"] = self.blitz_active_sessions
        if self.blitz_index_analysis:
            result["blitz_index_analysis"] = self.blitz_index_analysis
        if self.blitz_deadlocks:
            result["blitz_deadlocks"] = self.blitz_deadlocks

        return result
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)
    
    def save(self, path: Path) -> None:
        """Save to JSON file."""
        path = Path(path)
        with open(path, 'w') as f:
            f.write(self.to_json())


class FeatureSchemaBuilder:
    """
    Builds Feature Schema from analytics JSON data.
    
    This class transforms the raw analytics export format (v2.0) into
    the normalized Feature Schema format used by the RCA engine.
    
    The analytics.json v2.0 format contains raw time-series snapshots
    without baseline/incident split. The Feature Schema presents
    aggregated metrics for RCA analysis.
    
    Usage:
        builder = FeatureSchemaBuilder()
        schema = builder.build_from_file("analytics.json")
        # or
        schema = builder.build_from_dict(analytics_data)
    """
    
    def __init__(self, top_k_queries: int = 10, top_k_waits: int = 10):
        """
        Initialize the builder.
        
        Args:
            top_k_queries: Number of top queries to include
            top_k_waits: Number of top wait types to include
        """
        self.top_k_queries = top_k_queries
        self.top_k_waits = top_k_waits
    
    def build_from_file(self, path: Union[str, Path]) -> FeatureSchema:
        """
        Build Feature Schema from a JSON file.
        
        Args:
            path: Path to analytics JSON file
        
        Returns:
            FeatureSchema instance
        """
        path = Path(path)
        with open(path) as f:
            data = json.load(f)
        return self.build_from_dict(data)
    
    def build_from_dict(self, data: dict) -> FeatureSchema:
        """
        Build Feature Schema from analytics dictionary.

        Handles v1.0 format (baseline/incident_snapshots split),
        v2.0 format (single snapshots section), and v3.0 format
        (with extended_telemetry section).

        Args:
            data: Analytics data dictionary

        Returns:
            FeatureSchema instance
        """
        # Detect format version
        format_version = data.get("export_metadata", {}).get("format_version", "1.0")

        meta = self._build_meta(data)
        time_windows = self._build_time_windows(data)
        global_resources = self._build_global_resources(data, format_version)
        query_groups = self._build_query_groups(data, format_version)
        blocking_info = self._build_blocking_info(data, format_version)
        config_changes = self._extract_config_changes(data)
        errors_and_timeouts = self._extract_errors(data)
        missing_indexes = self._extract_missing_indexes(data, format_version)

        # Extended telemetry (v3.0 format)
        extended = data.get("extended_telemetry", {})
        memory_profile = self._build_memory_profile(extended)
        memory_grants = self._build_memory_grants(extended)
        scheduler_health = self._build_scheduler_health(extended)
        io_profile = self._build_io_profile(extended)
        index_usage = self._extract_index_usage(extended)
        query_store_insights = self._extract_query_store_insights(extended)
        schema_changes = self._extract_schema_changes(extended)
        server_config = self._extract_server_config(extended)
        workload_context = self._extract_workload_context(extended)

        # Phase 7: Extended External Context
        application_events = self._extract_application_events(extended)
        incident_context = self._extract_incident_context(extended)

        # Blitz Script Output (First Responder Kit)
        blitz = extended.get("blitz", {})
        blitz_findings = self._build_blitz_findings(blitz)
        blitz_wait_stats_delta = self._build_blitz_wait_stats_delta(blitz)
        blitz_file_stats = self._build_blitz_file_stats(blitz)
        blitz_query_plan_warnings = self._build_blitz_query_plan_warnings(blitz)
        blitz_active_sessions = self._build_blitz_active_sessions(blitz)
        blitz_index_analysis = self._build_blitz_index_analysis(blitz)
        blitz_deadlocks = self._build_blitz_deadlocks(blitz)

        return FeatureSchema(
            meta=meta,
            time_windows=time_windows,
            global_resources=global_resources,
            query_groups=query_groups,
            blocking_info=blocking_info,
            config_changes=config_changes,
            errors_and_timeouts=errors_and_timeouts,
            missing_indexes=missing_indexes,
            memory_profile=memory_profile,
            memory_grants=memory_grants,
            scheduler_health=scheduler_health,
            io_profile=io_profile,
            index_usage=index_usage,
            query_store_insights=query_store_insights,
            schema_changes=schema_changes,
            server_config=server_config,
            workload_context=workload_context,
            application_events=application_events,
            incident_context=incident_context,
            blitz_findings=blitz_findings,
            blitz_wait_stats_delta=blitz_wait_stats_delta,
            blitz_file_stats=blitz_file_stats,
            blitz_query_plan_warnings=blitz_query_plan_warnings,
            blitz_active_sessions=blitz_active_sessions,
            blitz_index_analysis=blitz_index_analysis,
            blitz_deadlocks=blitz_deadlocks,
        )
    
    def _build_meta(self, data: dict) -> dict:
        """Build meta section from incident info.
        
        Note: incident_name and scenario are intentionally NOT included here.
        The analytics.json source strips these fields to prevent leaking hints
        to the AI RCA system. The RCA must infer root cause from symptoms only.
        """
        incident = data.get("incident", {})
        summary = data.get("summary", {})
        indicators = summary.get("bottleneck_indicators", {})
        
        return {
            "incident_id": incident.get("incident_id"),
            # No incident_name or scenario - these are stripped at source
            "database_name": self._extract_database_name(data),
            # trigger_metric and trigger_value removed - they were misleading
            # (just showed top wait which could be benign infrastructure wait)
            "started_at": incident.get("started_at"),
            "ended_at": incident.get("ended_at"),
            "status": incident.get("status"),
        }
    
    def _build_time_windows(self, data: dict) -> dict:
        """Build time windows section."""
        incident = data.get("incident", {})
        
        started_at = incident.get("started_at")
        ended_at = incident.get("ended_at")
        
        return {
            "incident": {
                "start": started_at,
                "end": ended_at,
            }
        }
    
    def _build_global_resources(self, data: dict, format_version: str) -> dict:
        """Build global resources section from performance metrics."""
        summary = data.get("summary", {})
        perf = summary.get("performance_metrics", {})
        indicators = summary.get("bottleneck_indicators", {})
        
        return {
            "cpu": {
                "total_us": perf.get("total_cpu_time_us", 0),
            },
            "logical_reads": {
                "total": perf.get("total_logical_reads", 0),
            },
            "wait_time": {
                "total_ms": indicators.get("total_wait_time_ms", 0),
                # top_wait_type removed - agent should use compare_baseline_to_incident
                # to find significant wait deltas instead of relying on absolute top wait
            },
            "blocking": {
                "detected": indicators.get("blocking_detected", False),
                "blocked_session_count": indicators.get("blocked_session_count", 0),
                "max_blocking_depth": indicators.get("max_blocking_depth", 0),
            }
        }

    def _build_query_groups(self, data: dict, format_version: str) -> dict:
        """Build query groups section with top-K queries."""
        # Get queries from appropriate location based on format
        if format_version == "2.0":
            snapshots = data.get("snapshots", {})
            queries = snapshots.get("top_queries", [])
        else:
            # Legacy v1.0 format - use incident_snapshots
            incident = data.get("incident_snapshots", {})
            queries = incident.get("top_queries", [])
        
        # Compute totals for percentage calculation
        total_cpu = sum(q.get("total_cpu_time_us", 0) for q in queries)
        total_elapsed = sum(q.get("total_elapsed_time_us", 0) for q in queries)
        
        query_features = []
        for i, q in enumerate(queries[:self.top_k_queries]):
            exec_count = q.get("execution_count", 1) or 1
            
            avg_cpu = q.get("total_cpu_time_us", 0) / exec_count / 1000  # Convert to ms
            avg_elapsed = q.get("total_elapsed_time_us", 0) / exec_count / 1000
            avg_reads = q.get("total_logical_reads", 0) / exec_count
            
            pct_cpu = q.get("total_cpu_time_us", 0) / total_cpu * 100 if total_cpu > 0 else 0
            pct_dur = q.get("total_elapsed_time_us", 0) / total_elapsed * 100 if total_elapsed > 0 else 0
            
            query_features.append({
                "query_id": f"Q{i+1}",
                "query_hash": q.get("query_hash"),
                "sql_text_preview": self._truncate_sql(q.get("sql_text")),
                "database_name": q.get("database_name"),
                "metrics": {
                    "executions": exec_count,
                    "avg_duration_ms": round(avg_elapsed, 2),
                    "avg_cpu_ms": round(avg_cpu, 2),
                    "avg_logical_reads": round(avg_reads, 0),
                    "total_logical_reads": q.get("total_logical_reads", 0),
                    "pct_of_cpu": round(pct_cpu, 1),
                    "pct_of_duration": round(pct_dur, 1),
                },
            })
        
        return {
            "total_query_count": len(queries),
            "total_cpu_us": total_cpu,
            "total_elapsed_us": total_elapsed,
            "top_queries": query_features,
        }
    
    def _build_blocking_info(self, data: dict, format_version: str) -> dict:
        """Build blocking information section."""
        # Get blocking chain from appropriate location
        if format_version == "2.0":
            snapshots = data.get("snapshots", {})
            blocking_chain = snapshots.get("blocking_chain", [])
        else:
            # Legacy v1.0 format
            incident = data.get("incident_snapshots", {})
            blocking_chain = incident.get("blocking_chain", [])
        
        if not blocking_chain:
            return {
                "has_blocking": False,
                "blocking_sessions": [],
            }
        
        # Extract unique blocking relationships
        blocking_sessions = []
        seen = set()
        for b in blocking_chain:
            key = (b.get("session_id"), b.get("blocking_session_id"))
            if key not in seen:
                seen.add(key)
                blocking_sessions.append({
                    "blocked_session": b.get("session_id"),
                    "blocking_session": b.get("blocking_session_id"),
                    "wait_type": b.get("wait_type"),
                    "wait_time_ms": b.get("wait_time_ms"),
                    "sql_preview": self._truncate_sql(b.get("sql_text"), 150),
                })
        
        return {
            "has_blocking": True,
            "blocking_count": len(blocking_chain),
            "blocking_sessions": blocking_sessions[:10],  # Top 10 blocking relationships
        }
    
    def _extract_config_changes(self, data: dict) -> list[dict]:
        """Extract configuration/schema changes from the data."""
        # Currently, the analytics format doesn't capture config changes
        # This would be populated if the analytics pipeline tracked:
        # - Stats updates
        # - Index creates/drops
        # - Configuration changes
        return []
    
    def _extract_errors(self, data: dict) -> list[dict]:
        """Extract errors and timeouts from the data."""
        # Currently, the analytics format doesn't capture errors
        # This would be populated from XEvents or error logs
        return []

    def _extract_missing_indexes(self, data: dict, format_version: str) -> list[dict]:
        """Extract missing index recommendations from analytics data.

        Extracts top missing indexes from dm_db_missing_index_details data
        that was collected during the incident.

        Returns:
            List of missing index recommendations with table, columns, and impact scores
        """
        # Get missing indexes from appropriate location based on format
        if format_version == "2.0":
            snapshots = data.get("snapshots", {})
            missing_indexes = snapshots.get("missing_indexes", [])
        else:
            # Legacy v1.0 format - may not have missing indexes
            incident = data.get("incident_snapshots", {})
            missing_indexes = incident.get("missing_indexes", [])

        if not missing_indexes:
            return []

        # Process and normalize missing index data
        result = []
        for idx in missing_indexes[:10]:  # Top 10 missing indexes
            # Calculate improvement score if not already present
            user_seeks = idx.get("user_seeks", 0)
            user_scans = idx.get("user_scans", 0)
            avg_user_impact = idx.get("avg_user_impact", 0)

            # SQL Server's improvement measure formula
            improvement_score = idx.get(
                "improvement_score",
                (user_seeks + user_scans) * avg_user_impact
            )

            result.append({
                "table_name": idx.get("table_name") or idx.get("statement"),
                "equality_columns": idx.get("equality_columns"),
                "inequality_columns": idx.get("inequality_columns"),
                "included_columns": idx.get("included_columns"),
                "user_seeks": user_seeks,
                "user_scans": user_scans,
                "avg_user_impact": avg_user_impact,
                "improvement_score": improvement_score,
            })

        # Sort by improvement score descending
        result.sort(key=lambda x: x.get("improvement_score", 0), reverse=True)
        return result

    def _extract_database_name(self, data: dict) -> Optional[str]:
        """Extract database name from queries or requests."""
        # Try to find database name from v2.0 format first
        snapshots = data.get("snapshots", {})
        queries = snapshots.get("top_queries", [])
        if queries:
            db_name = queries[0].get("database_name")
            return str(db_name) if db_name is not None else None

        requests = snapshots.get("active_requests", [])
        if requests:
            db_name = requests[0].get("database_name")
            return str(db_name) if db_name is not None else None

        # Fall back to v1.0 format
        incident = data.get("incident_snapshots", {})
        queries = incident.get("top_queries", [])
        if queries:
            db_name = queries[0].get("database_name")
            return str(db_name) if db_name is not None else None

        return None
    
    def _aggregate_waits(self, waits: list[dict]) -> Dict[str, int]:
        """Aggregate wait times by wait type."""
        result: Dict[str, int] = {}
        for w in waits:
            wt = w.get("wait_type", "UNKNOWN")
            ms = w.get("wait_time_ms", 0)
            result[wt] = result.get(wt, 0) + ms
        return result
    
    def _truncate_sql(self, sql: Optional[str], max_len: int = 200) -> Optional[str]:
        """Truncate SQL text for preview."""
        if not sql:
            return None
        sql = sql.strip()
        if len(sql) <= max_len:
            return sql
        return sql[:max_len] + "..."

    # =========================================================================
    # Extended Telemetry Builders (Phase 1-6)
    # =========================================================================

    def _build_memory_profile(self, extended: dict) -> dict:
        """Build memory profile from memory clerk data.

        Analyzes memory allocation patterns to identify potential memory pressure.
        """
        memory_clerks = extended.get("memory_clerks", [])
        if not memory_clerks:
            return {}

        total_memory_mb = sum(c.get("pages_mb", 0) for c in memory_clerks)

        # Group by clerk type for analysis
        top_consumers = []
        for clerk in memory_clerks[:10]:  # Top 10 memory consumers
            pages_mb = clerk.get("pages_mb", 0)
            pct = round(pages_mb / total_memory_mb * 100, 1) if total_memory_mb > 0 else 0
            top_consumers.append({
                "clerk_type": clerk.get("clerk_type"),
                "pages_mb": pages_mb,
                "pct_of_total": pct,
            })

        # Identify potential issues
        buffer_pool_pct = 0
        query_memory_pct = 0
        for clerk in memory_clerks:
            clerk_type = clerk.get("clerk_type", "")
            pages_mb = clerk.get("pages_mb", 0)
            pct = pages_mb / total_memory_mb * 100 if total_memory_mb > 0 else 0
            if "MEMORYCLERK_SQLBUFFERPOOL" in clerk_type:
                buffer_pool_pct = pct
            elif "MEMORYCLERK_SQLQUERYEXEC" in clerk_type:
                query_memory_pct = pct

        return {
            "total_memory_mb": total_memory_mb,
            "top_consumers": top_consumers,
            "buffer_pool_pct": round(buffer_pool_pct, 1),
            "query_memory_pct": round(query_memory_pct, 1),
        }

    def _build_memory_grants(self, extended: dict) -> list[dict]:
        """Build memory grant summary from dm_exec_query_memory_grants data.

        Memory grants are critical for detecting:
        - RESOURCE_SEMAPHORE waits (queries with grant_status='WAITING')
        - Tempdb spills (queries with grant_status='SPILL_LIKELY' or 'SPILLED')
        - Memory pressure (many queries waiting or with insufficient grants)
        """
        memory_grants = extended.get("memory_grants", [])
        if not memory_grants:
            return []

        result = []
        for g in memory_grants:
            grant_status = g.get("grant_status", "UNKNOWN")
            requested_mb = g.get("requested_memory_mb", 0) or 0
            granted_mb = g.get("granted_memory_mb", 0) or 0
            required_mb = g.get("required_memory_mb", 0) or 0
            used_mb = g.get("used_memory_mb", 0) or 0
            max_used_mb = g.get("max_used_memory_mb", 0) or 0
            wait_time_ms = g.get("wait_time_ms", 0) or 0

            # Calculate memory deficit for analysis
            memory_deficit_mb = required_mb - granted_mb if granted_mb > 0 else 0

            result.append({
                "session_id": g.get("session_id"),
                "grant_status": grant_status,
                "requested_mb": round(requested_mb, 2),
                "granted_mb": round(granted_mb, 2),
                "required_mb": round(required_mb, 2),
                "used_mb": round(used_mb, 2),
                "max_used_mb": round(max_used_mb, 2),
                "memory_deficit_mb": round(memory_deficit_mb, 2),
                "wait_time_ms": wait_time_ms,
                "query_cost": g.get("query_cost"),
                "dop": g.get("dop"),
            })

        # Sort by severity: WAITING first, then SPILLED, then SPILL_LIKELY
        status_priority = {"WAITING": 0, "SPILLED": 1, "SPILL_LIKELY": 2, "OK": 3, "UNKNOWN": 4}
        result.sort(key=lambda x: (status_priority.get(x["grant_status"], 5), -x["wait_time_ms"]))

        return result[:20]  # Top 20 memory grants

    def _build_scheduler_health(self, extended: dict) -> dict:
        """Build scheduler health metrics from scheduler data.

        Analyzes CPU scheduler state to identify CPU pressure.
        """
        schedulers = extended.get("schedulers", [])
        if not schedulers:
            return {}

        total_runnable = sum(s.get("runnable_tasks_count", 0) for s in schedulers)
        total_work_queue = sum(s.get("work_queue_count", 0) for s in schedulers)
        max_runnable = max((s.get("runnable_tasks_count", 0) for s in schedulers), default=0)
        total_active_workers = sum(s.get("active_workers_count", 0) for s in schedulers)

        return {
            "scheduler_count": len(schedulers),
            "total_runnable_tasks": total_runnable,
            "max_runnable_tasks": max_runnable,
            "total_work_queue": total_work_queue,
            "total_active_workers": total_active_workers,
        }

    def _build_io_profile(self, extended: dict) -> dict:
        """Build I/O profile from computed latency data.

        Analyzes disk I/O performance to identify bottlenecks.
        """
        io_latency = extended.get("io_latency", [])
        if not io_latency:
            return {}

        # Separate data and log file metrics
        data_files = [f for f in io_latency if f.get("file_type") == "ROWS"]
        log_files = [f for f in io_latency if f.get("file_type") == "LOG"]

        def avg_latency(files: list, key: str) -> float:
            if not files:
                return 0.0
            return sum(f.get(key, 0) for f in files) / len(files)

        data_read_latency = avg_latency(data_files, "avg_read_latency_ms")
        data_write_latency = avg_latency(data_files, "avg_write_latency_ms")
        log_write_latency = avg_latency(log_files, "avg_write_latency_ms")

        # I/O health assessment
        io_health = "healthy"
        if data_read_latency > 20 or log_write_latency > 10:
            io_health = "degraded"
        if data_read_latency > 50 or log_write_latency > 20:
            io_health = "critical"

        return {
            "data_read_latency_ms": round(data_read_latency, 2),
            "data_write_latency_ms": round(data_write_latency, 2),
            "log_write_latency_ms": round(log_write_latency, 2),
            "io_health": io_health,
            "details": io_latency[:5],  # Top 5 files
        }

    def _extract_index_usage(self, extended: dict) -> list[dict]:
        """Extract index usage patterns for analysis.

        Identifies heavily used and potentially unused indexes.
        """
        index_usage = extended.get("index_usage", [])
        if not index_usage:
            return []

        # Focus on indexes with interesting patterns
        result = []
        for idx in index_usage[:20]:
            seeks = idx.get("user_seeks", 0)
            scans = idx.get("user_scans", 0)
            lookups = idx.get("user_lookups", 0)
            updates = idx.get("user_updates", 0)

            total_reads = seeks + scans + lookups

            # Identify potentially problematic patterns
            pattern = "normal"
            if scans > seeks * 10 and scans > 1000:
                pattern = "scan_heavy"  # Index causing scans, may need redesign
            elif updates > total_reads * 5 and updates > 1000:
                pattern = "update_heavy"  # Index costs more than it benefits
            elif total_reads == 0 and updates > 100:
                pattern = "unused"  # Index not used for reads

            result.append({
                "table_name": idx.get("table_name"),
                "index_name": idx.get("index_name"),
                "index_type": idx.get("index_type"),
                "seeks": seeks,
                "scans": scans,
                "lookups": lookups,
                "updates": updates,
                "pattern": pattern,
            })

        return result

    def _extract_query_store_insights(self, extended: dict) -> list[dict]:
        """Extract Query Store insights for plan regression detection.

        Identifies queries with potential performance issues from Query Store data.
        """
        query_store = extended.get("query_store", [])
        if not query_store:
            return []

        result = []
        for q in query_store[:15]:
            avg_duration_ms = q.get("avg_duration_us", 0) / 1000
            avg_cpu_ms = q.get("avg_cpu_time_us", 0) / 1000
            executions = q.get("execution_count", 0) or 0

            # Flag potentially problematic queries
            concern = None
            if avg_duration_ms > 1000:
                concern = "slow_query"
            elif avg_cpu_ms > 500:
                concern = "cpu_intensive"
            elif q.get("avg_logical_io_reads", 0) > 100000:
                concern = "high_io"

            result.append({
                "query_id": q.get("query_id"),
                "plan_id": q.get("plan_id"),
                "sql_preview": self._truncate_sql(q.get("query_text"), 150),
                "executions": executions,
                "avg_duration_ms": round(avg_duration_ms, 2),
                "avg_cpu_ms": round(avg_cpu_ms, 2),
                "avg_logical_reads": q.get("avg_logical_io_reads", 0),
                "concern": concern,
            })

        return result

    def _extract_schema_changes(self, extended: dict) -> list[dict]:
        """Extract recent schema changes from default trace.

        Identifies DDL changes and autogrowth events that may correlate with incident.
        """
        schema_changes = extended.get("schema_changes", [])
        if not schema_changes:
            return []

        # Map event class codes to human-readable descriptions
        event_descriptions = {
            46: "object_created",
            47: "object_deleted",
            92: "data_file_autogrow",
            93: "log_file_autogrow",
            94: "data_file_autoshrink",
            95: "log_file_autoshrink",
            164: "object_altered",
            20: "login_failed",
            22: "error_log",
        }

        result = []
        for event in schema_changes[:20]:
            event_class = event.get("event_class", 0)
            result.append({
                "event_type": event_descriptions.get(event_class, f"event_{event_class}"),
                "event_class": event_class,
                "object_name": event.get("object_name"),
                "database_name": event.get("database_name"),
                "login_name": event.get("login_name"),
                "start_time": event.get("start_time"),
                "text_preview": self._truncate_sql(event.get("text_data"), 100),
            })

        return result

    def _extract_server_config(self, extended: dict) -> list[dict]:
        """Extract server configuration settings.

        Provides configuration context that may be relevant to incident.
        """
        server_config = extended.get("server_config", [])
        if not server_config:
            return []

        # Focus on performance-relevant settings
        relevant_settings = {
            "max degree of parallelism",
            "cost threshold for parallelism",
            "max server memory (MB)",
            "min server memory (MB)",
            "max worker threads",
            "blocked process threshold (s)",
            "optimize for ad hoc workloads",
        }

        result = []
        for config in server_config:
            name = config.get("config_name", "")
            if name in relevant_settings:
                result.append({
                    "name": name,
                    "value": config.get("config_value"),
                    "value_in_use": config.get("config_value_in_use"),
                })

        return result

    def _extract_workload_context(self, extended: dict) -> dict:
        """Extract workload context from application metrics.

        Provides external application-side context for the incident.
        """
        workload_metrics = extended.get("workload_metrics", [])
        if not workload_metrics:
            return {}

        # Get the most recent metrics snapshot
        latest = workload_metrics[-1] if workload_metrics else {}

        # Calculate averages across all snapshots
        avg_qps = sum(m.get("queries_per_second", 0) or 0 for m in workload_metrics) / len(workload_metrics) if workload_metrics else 0
        avg_latency = sum(m.get("avg_query_duration_ms", 0) or 0 for m in workload_metrics) / len(workload_metrics) if workload_metrics else 0
        max_p99 = max((m.get("p99_query_duration_ms", 0) or 0 for m in workload_metrics), default=0)
        total_errors = sum(m.get("error_count", 0) or 0 for m in workload_metrics)
        total_timeouts = sum(m.get("timeout_count", 0) or 0 for m in workload_metrics)
        total_connection_errors = sum(m.get("connection_errors", 0) or 0 for m in workload_metrics)

        return {
            "snapshot_count": len(workload_metrics),
            "latest": {
                "active_workers": latest.get("active_workers", 0),
                "queries_executed": latest.get("queries_executed", 0),
                "queries_per_second": round(latest.get("queries_per_second", 0) or 0, 2),
                "avg_query_duration_ms": round(latest.get("avg_query_duration_ms", 0) or 0, 2),
                "p95_query_duration_ms": round(latest.get("p95_query_duration_ms", 0) or 0, 2),
                "p99_query_duration_ms": round(latest.get("p99_query_duration_ms", 0) or 0, 2),
                "error_count": latest.get("error_count", 0),
                "timeout_count": latest.get("timeout_count", 0),
                "connection_errors": latest.get("connection_errors", 0),
            },
            "aggregates": {
                "avg_qps": round(avg_qps, 2),
                "avg_query_duration_ms": round(avg_latency, 2),
                "max_p99_query_duration_ms": round(max_p99, 2),
                "total_errors": total_errors,
                "total_timeouts": total_timeouts,
                "total_connection_errors": total_connection_errors,
            },
            "query_mix_json": latest.get("query_mix_json"),
        }

    # =========================================================================
    # Phase 7: Extended External Context Extractors
    # =========================================================================

    def _extract_application_events(self, extended: dict) -> list[dict]:
        """Extract application events from extended telemetry.

        Application events provide structured logs from the workload runner
        including errors, warnings, anomalies, and milestones.
        """
        app_events = extended.get("application_events", [])
        if not app_events:
            return []

        # Group events by type for summary
        events_by_type: Dict[str, list] = {}
        for event in app_events:
            event_type = event.get("event_type", "unknown")
            if event_type not in events_by_type:
                events_by_type[event_type] = []
            events_by_type[event_type].append(event)

        # Build summary with event counts and recent events
        result = []
        for event_type, events in events_by_type.items():
            # Sort by time (most recent first)
            sorted_events = sorted(
                events,
                key=lambda e: e.get("event_time", ""),
                reverse=True
            )

            # Include summary info for each type
            for event in sorted_events[:5]:  # Top 5 most recent per type
                result.append({
                    "event_type": event.get("event_type"),
                    "event_category": event.get("event_category"),
                    "event_name": event.get("event_name"),
                    "severity": event.get("severity"),
                    "message": event.get("message"),
                    "event_time": event.get("event_time"),
                    "source_component": event.get("source_component"),
                })

        # Sort all results by time
        result.sort(key=lambda e: e.get("event_time", ""), reverse=True)
        return result[:20]  # Return top 20 events

    def _extract_incident_context(self, extended: dict) -> list[dict]:
        """Extract incident context/annotations from extended telemetry.

        Incident context provides custom annotations like deployments,
        configuration changes, traffic patterns, and service status.
        """
        incident_context = extended.get("incident_context", [])
        if not incident_context:
            return []

        result = []
        for ctx in incident_context:
            result.append({
                "context_type": ctx.get("context_type"),
                "context_key": ctx.get("context_key"),
                "context_value": ctx.get("context_value"),
                "valid_from": ctx.get("valid_from"),
                "valid_to": ctx.get("valid_to"),
                "source": ctx.get("source"),
                "confidence": ctx.get("confidence"),
            })

        return result

    # =========================================================================
    # Blitz Script Output Builders (First Responder Kit)
    # =========================================================================

    def _build_blitz_findings(self, blitz: dict) -> list[dict]:
        """Extract priority-sorted findings from BlitzFirst.

        Findings are diagnostic alerts with priority levels indicating severity.
        Priority 1-10 = Critical, 11-50 = High, 51-100 = Medium.
        """
        findings = blitz.get("findings", [])
        if not findings:
            return []

        result = []
        for f in findings:
            priority = f.get("priority", 255)
            # Only include critical/high/medium priority findings
            if priority <= 100:
                result.append({
                    "priority": priority,
                    "findings_group": f.get("findings_group"),
                    "finding": f.get("finding"),
                    "details": self._truncate_sql(f.get("details"), 300),
                })

        # Sort by priority ascending (lower = more critical)
        result.sort(key=lambda x: x.get("priority", 255))
        return result[:20]  # Top 20 findings

    def _build_blitz_wait_stats_delta(self, blitz: dict) -> list[dict]:
        """Extract delta wait statistics from BlitzFirst.

        CRITICAL: These are REAL-TIME waits sampled over 5 seconds during
        the incident. Unlike cumulative DMV waits, these show what's
        happening NOW and are the primary wait signal for RCA analysis.
        """
        wait_stats = blitz.get("wait_stats_delta", [])
        if not wait_stats:
            return []

        result = []
        for w in wait_stats:
            wait_type = w.get("wait_type", "")
            # Filter out benign waits
            if wait_type in BENIGN_WAITS:
                continue

            wait_time_ms = w.get("wait_time_ms_delta", 0) or 0
            signal_wait = w.get("signal_wait_time_ms", 0) or 0
            resource_wait = w.get("resource_wait_time_ms", 0) or 0
            total_wait = signal_wait + resource_wait

            # Calculate signal vs resource ratio (helps identify CPU vs I/O issues)
            signal_pct = round(signal_wait / total_wait * 100, 1) if total_wait > 0 else 0

            result.append({
                "wait_type": wait_type,
                "wait_time_ms_delta": wait_time_ms,
                "wait_time_pct": w.get("wait_time_pct"),
                "signal_wait_time_ms": signal_wait,
                "resource_wait_time_ms": resource_wait,
                "signal_vs_resource_pct": signal_pct,
            })

        # Sort by delta wait time descending
        result.sort(key=lambda x: x.get("wait_time_ms_delta", 0), reverse=True)
        return result[:15]  # Top 15 waits

    def _build_blitz_file_stats(self, blitz: dict) -> list[dict]:
        """Extract file I/O statistics from BlitzFirst.

        Provides per-file latency metrics to identify I/O bottlenecks.
        """
        file_stats = blitz.get("file_stats", [])
        if not file_stats:
            return []

        result = []
        for f in file_stats:
            read_latency = f.get("avg_read_latency_ms", 0) or 0
            write_latency = f.get("avg_write_latency_ms", 0) or 0
            max_latency = max(read_latency, write_latency)

            # Assess I/O health
            io_health = "healthy"
            if max_latency > 50:
                io_health = "critical"
            elif max_latency > 20:
                io_health = "degraded"

            result.append({
                "database_name": f.get("database_name"),
                "file_name": f.get("file_name"),
                "file_type": f.get("file_type"),
                "avg_read_latency_ms": round(read_latency, 2),
                "avg_write_latency_ms": round(write_latency, 2),
                "num_reads": f.get("num_reads", 0),
                "num_writes": f.get("num_writes", 0),
                "io_health": io_health,
            })

        # Sort by max latency descending (slowest files first)
        result.sort(
            key=lambda x: max(
                x.get("avg_read_latency_ms", 0),
                x.get("avg_write_latency_ms", 0)
            ),
            reverse=True
        )
        return result[:10]  # Top 10 files

    def _build_blitz_query_plan_warnings(self, blitz: dict) -> list[dict]:
        """Extract query plan warnings from BlitzCache.

        Identifies problematic query plans including:
        - Implicit conversions (data type mismatches causing scans)
        - Spills (memory grant underestimation)
        - Parameter sniffing indicators (high min/max CPU variance)
        """
        cache_analysis = blitz.get("cache_analysis", [])
        if not cache_analysis:
            return []

        result = []
        for q in cache_analysis:
            warnings = q.get("warnings") or ""
            implicit_conv = q.get("implicit_conversions") or ""
            total_spills = q.get("total_spills", 0) or 0
            min_spills = q.get("min_spills", 0) or 0
            max_spills = q.get("max_spills", 0) or 0

            # Only include queries with actual warnings or issues
            has_issues = bool(warnings) or bool(implicit_conv) or total_spills > 0

            if has_issues:
                result.append({
                    "query_preview": self._truncate_sql(q.get("query_text"), 150),
                    "warnings_summary": warnings[:200] if warnings else None,
                    "has_implicit_conversions": bool(implicit_conv),
                    "implicit_conversions_detail": implicit_conv[:200] if implicit_conv else None,
                    "has_spills": total_spills > 0,
                    "total_spills": total_spills,
                    "min_spills": min_spills,
                    "max_spills": max_spills,
                    "execution_count": q.get("execution_count", 0),
                    "total_cpu": q.get("total_cpu", 0),
                    "total_reads": q.get("total_reads", 0),
                    "query_plan_cost": q.get("query_plan_cost"),
                })

        # Sort by total CPU descending
        result.sort(key=lambda x: x.get("total_cpu", 0), reverse=True)
        return result[:15]  # Top 15 problematic queries

    def _build_blitz_active_sessions(self, blitz: dict) -> list[dict]:
        """Extract active session snapshot from BlitzWho.

        Point-in-time view of running queries, useful for identifying
        blockers and long-running transactions.
        """
        sessions = blitz.get("active_sessions", [])
        if not sessions:
            return []

        result = []
        for s in sessions:
            status = s.get("status", "")
            blocking_id = s.get("blocking_session_id")
            open_trans = s.get("open_transaction_count", 0) or 0

            # Include non-sleeping sessions or sessions involved in blocking
            if status != "sleeping" or blocking_id is not None:
                result.append({
                    "session_id": s.get("session_id"),
                    "status": status,
                    "wait_info": s.get("wait_info"),
                    "blocking_session_id": blocking_id,
                    "open_transaction_count": open_trans,
                    "cpu": s.get("cpu", 0),
                    "reads": s.get("reads", 0),
                    "writes": s.get("writes", 0),
                    "query_preview": self._truncate_sql(s.get("query_text"), 150),
                    "has_query_plan": s.get("has_query_plan", False),
                })

        # Sort by CPU descending
        result.sort(key=lambda x: x.get("cpu", 0), reverse=True)
        return result[:20]  # Top 20 active sessions

    def _build_blitz_index_analysis(self, blitz: dict) -> list[dict]:
        """Extract index analysis from BlitzIndex.

        Provides actionable index recommendations including:
        - Missing indexes with CREATE INDEX statements
        - Unused indexes (write overhead with no reads)
        - Duplicate indexes
        """
        index_analysis = blitz.get("index_analysis", [])
        if not index_analysis:
            return []

        result = []
        for idx in index_analysis:
            finding = idx.get("finding", "")

            # Determine finding type from the finding description
            finding_type = "other"
            if "missing" in finding.lower():
                finding_type = "missing_index"
            elif "unused" in finding.lower():
                finding_type = "unused_index"
            elif "duplicate" in finding.lower():
                finding_type = "duplicate_index"
            elif "disabled" in finding.lower():
                finding_type = "disabled_index"

            result.append({
                "priority": idx.get("priority", 255),
                "finding_type": finding_type,
                "finding": finding,
                "database_name": idx.get("database_name"),
                "schema_name": idx.get("schema_name"),
                "table_name": idx.get("table_name"),
                "index_name": idx.get("index_name"),
                "details": self._truncate_sql(idx.get("details"), 300),
                "index_definition": idx.get("index_definition"),
                "create_tsql": idx.get("create_tsql"),
            })

        # Sort by priority ascending
        result.sort(key=lambda x: x.get("priority", 255))
        return result[:20]  # Top 20 index findings

    def _build_blitz_deadlocks(self, blitz: dict) -> list[dict]:
        """Extract deadlock history from BlitzLock.

        Provides deadlock information from Extended Events system_health session.
        """
        deadlocks = blitz.get("deadlocks", [])
        if not deadlocks:
            return []

        result = []
        for d in deadlocks:
            result.append({
                "deadlock_type": d.get("deadlock_type"),
                "victim_query_preview": self._truncate_sql(d.get("victim_query"), 200),
                "victim_process": d.get("victim_process"),
                "blocking_query_preview": self._truncate_sql(d.get("blocking_query"), 200),
                "blocking_process": d.get("blocking_process"),
                "has_deadlock_graph": d.get("has_deadlock_graph", False),
            })

        return result[:10]  # Top 10 deadlocks
