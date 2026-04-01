"""
ClickHouse data source for RCA Engine.

Provides direct ClickHouse queries for RCA analysis without FeatureSchema abstraction.
Fetches metrics for a given incident and provides comparison between baseline and incident periods.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

import clickhouse_connect


@dataclass
class TimeWindow:
    """Time window for metric queries."""
    start: datetime
    end: datetime


@dataclass
class IncidentInfo:
    """Information about an incident."""
    incident_id: str
    name: str
    scenario: Optional[str]
    started_at: datetime
    ended_at: Optional[datetime]
    baseline_start: Optional[datetime]
    baseline_end: Optional[datetime]
    status: str


ALLOWED_QUERY_TABLE_COLUMNS: Dict[str, set[str]] = {
    "wait_stats": {
        "collected_at", "incident_id", "is_baseline", "wait_type",
        "waiting_tasks_count", "wait_time_ms", "max_wait_time_ms", "signal_wait_time_ms",
    },
    "blocking_chains": {
        "collected_at", "incident_id", "is_baseline", "blocking_level", "session_id",
        "blocking_session_id", "wait_type", "wait_time_ms", "wait_resource", "lock_mode",
        "status", "command", "database_name", "sql_text", "transaction_id", "open_transaction_count",
    },
    "memory_grants": {
        "collected_at", "incident_id", "is_baseline", "session_id", "request_time", "grant_time",
        "requested_memory_mb", "granted_memory_mb", "required_memory_mb", "used_memory_mb",
        "max_used_memory_mb", "ideal_memory_mb", "wait_time_ms", "grant_status", "query_cost", "dop",
        "sql_text",
    },
    "query_stats": {
        "collected_at", "incident_id", "is_baseline", "query_hash", "query_plan_hash", "execution_count",
        "total_worker_time_us", "total_elapsed_time_us", "total_logical_reads", "total_logical_writes",
        "total_physical_reads", "total_grant_kb", "total_spills", "database_name", "sql_text",
    },
    "schedulers": {
        "collected_at", "incident_id", "is_baseline", "scheduler_id", "cpu_id", "status", "is_online",
        "current_tasks_count", "runnable_tasks_count", "current_workers_count", "active_workers_count",
        "work_queue_count", "context_switches_count", "yield_count",
    },
    "file_stats": {
        "collected_at", "incident_id", "is_baseline", "database_id", "database_name", "file_id", "file_name",
        "file_type", "num_of_reads", "num_of_bytes_read", "io_stall_read_ms", "num_of_writes",
        "num_of_bytes_written", "io_stall_write_ms", "io_stall_ms",
    },
    "missing_indexes": {
        "collected_at", "incident_id", "is_baseline", "database_name", "schema_name", "table_name",
        "equality_columns", "inequality_columns", "included_columns", "unique_compiles", "user_seeks",
        "user_scans", "avg_total_user_cost", "avg_user_impact", "impact_score",
    },
    "blitz_results": {
        "collected_at", "incident_id", "blitz_type", "priority", "findings_group", "finding", "details", "url",
        "query_text", "database_name", "total_cpu", "total_reads", "total_writes", "execution_count",
        "avg_duration_ms", "warnings", "total_spills", "session_id", "status", "wait_info",
        "blocking_session_id", "cpu_ms", "reads", "schema_name", "table_name", "index_name",
        "index_definition", "create_tsql", "deadlock_type", "victim_query", "blocking_query",
        "deadlock_graph", "extended_data",
    },
}


class ClickHouseDataSource:
    """
    Fetches RCA metrics directly from ClickHouse.

    This replaces the FeatureSchema abstraction with direct queries,
    providing a cleaner interface for the RCA engine.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.host = host or os.getenv("CLICKHOUSE_HOST", "localhost")
        self.port = port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.database = database or os.getenv("CLICKHOUSE_DATABASE", "rca_metrics")
        self.username = username or os.getenv("CLICKHOUSE_USER", "default")
        self.password = password or os.getenv("CLICKHOUSE_PASSWORD", "")
        self._client = None

    @property
    def client(self):
        """Get the ClickHouse client, connecting if needed."""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
            )
        return self._client

    def close(self) -> None:
        """Close the ClickHouse connection."""
        if self._client:
            self._client.close()
            self._client = None

    # =========================================================================
    # Incident Management
    # =========================================================================

    def get_incident(self, incident_id: str) -> Optional[IncidentInfo]:
        """Fetch incident metadata."""
        result = self.client.query(
            """
            SELECT
                incident_id,
                name,
                scenario,
                started_at,
                ended_at,
                baseline_start,
                baseline_end,
                status
            FROM incidents
            WHERE incident_id = {id:String}
            ORDER BY
                -- Prefer records with baseline info (complete incident records)
                baseline_start IS NOT NULL DESC,
                created_at DESC
            LIMIT 1
            """,
            parameters={"id": incident_id},
        )
        rows = result.result_rows
        if rows:
            row = rows[0]
            return IncidentInfo(
                incident_id=row[0],
                name=row[1],
                scenario=row[2],
                started_at=row[3],
                ended_at=row[4],
                baseline_start=row[5],
                baseline_end=row[6],
                status=row[7],
            )
        return None

    def get_incident_window(self, incident_id: str) -> Tuple[TimeWindow, Optional[TimeWindow]]:
        """
        Get the incident and baseline time windows.

        Returns:
            Tuple of (incident_window, baseline_window)
            baseline_window may be None if not set
        """
        incident = self.get_incident(incident_id)
        if not incident:
            raise ValueError(f"Incident {incident_id} not found")

        incident_window = TimeWindow(
            start=incident.started_at,
            end=incident.ended_at or datetime.now(),
        )

        baseline_window = None
        if incident.baseline_start and incident.baseline_end:
            baseline_window = TimeWindow(
                start=incident.baseline_start,
                end=incident.baseline_end,
            )

        return incident_window, baseline_window

    # =========================================================================
    # Core Data Fetchers
    # =========================================================================

    def get_data_for_time_range(
        self,
        incident_start: datetime,
        incident_end: Optional[datetime] = None,
        baseline_start: Optional[datetime] = None,
        baseline_end: Optional[datetime] = None,
        name: str = "Ad-hoc Analysis",
        scenario: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch all data for a time range without requiring an incident record.

        This is the recommended method for ad-hoc analysis when you want to
        analyze metrics collected during a specific time window.

        Args:
            incident_start: Start of the incident period
            incident_end: End of the incident period (defaults to now)
            baseline_start: Start of baseline period (defaults to 5 min before incident)
            baseline_end: End of baseline period (defaults to incident_start)
            name: Name for the analysis
            scenario: Optional scenario type (e.g., 'blocking_chain')

        Returns:
            Dict with incident data in the same format as get_incident_data()
        """
        incident_end = incident_end or datetime.now()

        # Default baseline: 5 minutes before incident
        if baseline_start is None:
            baseline_duration = incident_end - incident_start
            baseline_start = incident_start - baseline_duration
        if baseline_end is None:
            baseline_end = incident_start

        incident_window = TimeWindow(start=incident_start, end=incident_end)
        baseline_window = TimeWindow(start=baseline_start, end=baseline_end)

        # Use empty string for incident_id in queries (will match via OR incident_id IS NULL)
        query_id = ""

        return {
            "incident": {
                "incident_id": f"adhoc_{incident_start.strftime('%Y%m%d_%H%M%S')}",
                "name": name,
                "scenario": scenario,
                "started_at": incident_start.isoformat(),
                "ended_at": incident_end.isoformat(),
                "status": "analyzing",
            },
            "time_windows": {
                "incident": {
                    "start": incident_window.start.isoformat(),
                    "end": incident_window.end.isoformat(),
                },
                "baseline": {
                    "start": baseline_window.start.isoformat(),
                    "end": baseline_window.end.isoformat(),
                },
            },
            "wait_stats": self.get_wait_stats(query_id, incident_window),
            "blocking_chains": self.get_blocking_chains(query_id, incident_window),
            "memory_grants": self.get_memory_grants(query_id, incident_window),
            "query_stats": self.get_query_stats(query_id, incident_window),
            "schedulers": self.get_schedulers(query_id, incident_window),
            "file_stats": self.get_file_stats(query_id, incident_window),
            "missing_indexes": self.get_missing_indexes(query_id, incident_window),
            "blitz_findings": [],  # No blitz for ad-hoc (requires incident_id)
        }

    def compare_time_ranges(
        self,
        incident_start: datetime,
        incident_end: Optional[datetime] = None,
        baseline_start: Optional[datetime] = None,
        baseline_end: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Compare metrics between baseline and incident time ranges.

        Works without requiring an incident record in the database.

        Args:
            incident_start: Start of the incident period
            incident_end: End of the incident period (defaults to now)
            baseline_start: Start of baseline period (defaults to 5 min before incident)
            baseline_end: End of baseline period (defaults to incident_start)

        Returns:
            Dict with comparison data (same format as compare_baseline_incident)
        """
        incident_end = incident_end or datetime.now()

        if baseline_start is None:
            baseline_duration = incident_end - incident_start
            baseline_start = incident_start - baseline_duration
        if baseline_end is None:
            baseline_end = incident_start

        incident_window = TimeWindow(start=incident_start, end=incident_end)
        baseline_window = TimeWindow(start=baseline_start, end=baseline_end)

        # Use empty string for queries (matches via OR incident_id IS NULL)
        query_id = ""

        # Compare wait stats
        baseline_waits = self._aggregate_waits(query_id, baseline_window)
        incident_waits = self._aggregate_waits(query_id, incident_window)

        # Calculate deltas
        wait_deltas = {}
        for wait_type in set(baseline_waits.keys()) | set(incident_waits.keys()):
            baseline_ms = baseline_waits.get(wait_type, 0)
            incident_ms = incident_waits.get(wait_type, 0)
            delta = incident_ms - baseline_ms
            if delta != 0:
                wait_deltas[wait_type] = {
                    "baseline_ms": baseline_ms,
                    "incident_ms": incident_ms,
                    "delta_ms": delta,
                    "change_pct": (delta / baseline_ms * 100) if baseline_ms > 0 else None,
                }

        # Sort by absolute delta
        sorted_waits = dict(
            sorted(
                wait_deltas.items(),
                key=lambda x: abs(x[1]["delta_ms"]),
                reverse=True,
            )[:15]
        )

        # Compare blocking
        baseline_blocking = self._count_blocking(query_id, baseline_window)
        incident_blocking = self._count_blocking(query_id, incident_window)

        # Compare memory grants
        baseline_grants = self._summarize_grants(query_id, baseline_window)
        incident_grants = self._summarize_grants(query_id, incident_window)

        return {
            "wait_stats_delta": sorted_waits,
            "blocking_comparison": {
                "baseline": baseline_blocking,
                "incident": incident_blocking,
                "delta": incident_blocking - baseline_blocking,
            },
            "memory_grants_comparison": {
                "baseline": baseline_grants,
                "incident": incident_grants,
            },
            "top_new_waits": [
                w for w in sorted_waits.keys()
                if sorted_waits[w]["baseline_ms"] == 0
            ][:5],
        }

    def get_incident_data(self, incident_id: str) -> Dict[str, Any]:
        """
        Fetch all data for an incident from ClickHouse.

        This is the main entry point for RCA analysis.
        """
        incident = self.get_incident(incident_id)
        if not incident:
            raise ValueError(f"Incident {incident_id} not found")

        incident_window, baseline_window = self.get_incident_window(incident_id)

        return {
            "incident": {
                "incident_id": incident.incident_id,
                "name": incident.name,
                "scenario": incident.scenario,
                "started_at": incident.started_at.isoformat(),
                "ended_at": incident.ended_at.isoformat() if incident.ended_at else None,
                "status": incident.status,
            },
            "time_windows": {
                "incident": {
                    "start": incident_window.start.isoformat(),
                    "end": incident_window.end.isoformat(),
                },
                "baseline": {
                    "start": baseline_window.start.isoformat(),
                    "end": baseline_window.end.isoformat(),
                } if baseline_window else None,
            },
            "wait_stats": self.get_wait_stats(incident_id, incident_window),
            "blocking_chains": self.get_blocking_chains(incident_id, incident_window),
            "memory_grants": self.get_memory_grants(incident_id, incident_window),
            "query_stats": self.get_query_stats(incident_id, incident_window),
            "schedulers": self.get_schedulers(incident_id, incident_window),
            "file_stats": self.get_file_stats(incident_id, incident_window),
            "missing_indexes": self.get_missing_indexes(incident_id, incident_window),
            "blitz_findings": self.get_blitz_findings(incident_id),
        }

    def get_wait_stats(
        self,
        incident_id: str,
        window: TimeWindow,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Get wait statistics for the incident window."""
        result = self.client.query(
            """
            SELECT
                wait_type,
                sum(waiting_tasks_count) as total_tasks,
                sum(wait_time_ms) as total_wait_ms,
                max(max_wait_time_ms) as max_wait_ms,
                sum(signal_wait_time_ms) as total_signal_ms
            FROM wait_stats
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
            GROUP BY wait_type
            HAVING total_wait_ms > 0
            ORDER BY total_wait_ms DESC
            LIMIT {limit:UInt32}
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "limit": limit,
            },
        )

        return [
            {
                "wait_type": row[0],
                "waiting_tasks_count": row[1],
                "wait_time_ms": row[2],
                "max_wait_time_ms": row[3],
                "signal_wait_time_ms": row[4],
            }
            for row in result.result_rows
        ]

    def get_blocking_chains(
        self,
        incident_id: str,
        window: TimeWindow,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Get blocking chain information."""
        result = self.client.query(
            """
            SELECT
                blocking_level,
                session_id,
                blocking_session_id,
                wait_type,
                wait_time_ms,
                status,
                database_name,
                sql_text
            FROM blocking_chains
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
            ORDER BY collected_at DESC, blocking_level
            LIMIT {limit:UInt32}
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "limit": limit,
            },
        )

        chains = [
            {
                "blocking_level": row[0],
                "blocked_session": row[1],
                "blocking_session": row[2],
                "wait_type": row[3],
                "wait_time_ms": row[4],
                "status": row[5],
                "database_name": row[6],
                "sql_preview": row[7][:200] if row[7] else None,
            }
            for row in result.result_rows
        ]

        # Find head blockers (level 0)
        head_blockers = [c for c in chains if c["blocking_level"] == 0]

        return {
            "has_blocking": len(chains) > 0,
            "blocking_count": len(chains),
            "head_blockers": head_blockers[:5],
            "blocking_sessions": chains[:10],
        }

    def get_memory_grants(
        self,
        incident_id: str,
        window: TimeWindow,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Get memory grant information."""
        result = self.client.query(
            """
            SELECT
                session_id,
                grant_status,
                requested_memory_mb,
                granted_memory_mb,
                required_memory_mb,
                used_memory_mb,
                max_used_memory_mb,
                wait_time_ms,
                query_cost,
                dop,
                sql_text
            FROM memory_grants
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
            ORDER BY
                CASE grant_status
                    WHEN 'WAITING' THEN 1
                    WHEN 'SPILLED' THEN 2
                    WHEN 'SPILL_LIKELY' THEN 3
                    ELSE 4
                END,
                wait_time_ms DESC
            LIMIT {limit:UInt32}
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "limit": limit,
            },
        )

        return [
            {
                "session_id": row[0],
                "grant_status": row[1],
                "requested_mb": row[2],
                "granted_mb": row[3],
                "required_mb": row[4],
                "used_mb": row[5],
                "max_used_mb": row[6],
                "wait_time_ms": row[7],
                "query_cost": row[8],
                "dop": row[9],
                "sql_preview": row[10][:200] if row[10] else None,
            }
            for row in result.result_rows
        ]

    def get_query_stats(
        self,
        incident_id: str,
        window: TimeWindow,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Get top queries by resource consumption."""
        result = self.client.query(
            """
            SELECT
                query_hash,
                sum(execution_count) as total_executions,
                sum(total_worker_time_us) as total_cpu_us,
                sum(total_elapsed_time_us) as total_elapsed_us,
                sum(total_logical_reads) as total_reads,
                sum(total_logical_writes) as total_writes,
                sum(total_spills) as total_spills,
                any(database_name) as database_name,
                any(sql_text) as sql_text
            FROM query_stats
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
            GROUP BY query_hash
            ORDER BY total_cpu_us DESC
            LIMIT {limit:UInt32}
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "limit": limit,
            },
        )

        return [
            {
                "query_hash": row[0],
                "execution_count": row[1],
                "total_cpu_us": row[2],
                "total_elapsed_us": row[3],
                "total_logical_reads": row[4],
                "total_logical_writes": row[5],
                "total_spills": row[6],
                "database_name": row[7],
                "sql_preview": row[8][:200] if row[8] else None,
            }
            for row in result.result_rows
        ]

    def get_schedulers(
        self,
        incident_id: str,
        window: TimeWindow,
    ) -> Dict[str, Any]:
        """Get scheduler health summary."""
        result = self.client.query(
            """
            SELECT
                avg(runnable_tasks_count) as avg_runnable,
                max(runnable_tasks_count) as max_runnable,
                avg(current_tasks_count) as avg_current,
                max(current_tasks_count) as max_current,
                sum(yield_count) as total_yields,
                count(DISTINCT scheduler_id) as scheduler_count
            FROM schedulers
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
            },
        )

        if result.result_rows:
            row = result.result_rows[0]
            return {
                "avg_runnable_tasks": float(row[0]) if row[0] else 0,
                "max_runnable_tasks": int(row[1]) if row[1] else 0,
                "avg_current_tasks": float(row[2]) if row[2] else 0,
                "max_current_tasks": int(row[3]) if row[3] else 0,
                "total_yields": int(row[4]) if row[4] else 0,
                "scheduler_count": int(row[5]) if row[5] else 0,
            }
        return {}

    def get_file_stats(
        self,
        incident_id: str,
        window: TimeWindow,
    ) -> List[Dict[str, Any]]:
        """Get file I/O statistics."""
        result = self.client.query(
            """
            SELECT
                database_name,
                file_type,
                sum(num_of_reads) as total_reads,
                sum(num_of_writes) as total_writes,
                sum(io_stall_read_ms) as total_read_stall_ms,
                sum(io_stall_write_ms) as total_write_stall_ms,
                sum(io_stall_ms) as total_stall_ms
            FROM file_stats
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
            GROUP BY database_name, file_type
            ORDER BY total_stall_ms DESC
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
            },
        )

        return [
            {
                "database_name": row[0],
                "file_type": row[1],
                "total_reads": row[2],
                "total_writes": row[3],
                "read_stall_ms": row[4],
                "write_stall_ms": row[5],
                "total_stall_ms": row[6],
            }
            for row in result.result_rows
        ]

    def get_missing_indexes(
        self,
        incident_id: str,
        window: TimeWindow,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get missing index recommendations."""
        result = self.client.query(
            """
            SELECT
                database_name,
                schema_name,
                table_name,
                equality_columns,
                inequality_columns,
                included_columns,
                max(impact_score) as max_impact
            FROM missing_indexes
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
            GROUP BY database_name, schema_name, table_name,
                     equality_columns, inequality_columns, included_columns
            ORDER BY max_impact DESC
            LIMIT {limit:UInt32}
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "limit": limit,
            },
        )

        return [
            {
                "database_name": row[0],
                "schema_name": row[1],
                "table_name": row[2],
                "equality_columns": row[3],
                "inequality_columns": row[4],
                "included_columns": row[5],
                "impact_score": row[6],
            }
            for row in result.result_rows
        ]

    def get_blitz_findings(
        self,
        incident_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get Blitz findings for the incident."""
        result = self.client.query(
            """
            SELECT
                blitz_type,
                priority,
                findings_group,
                finding,
                details,
                url,
                query_text,
                database_name,
                total_cpu,
                total_reads,
                execution_count,
                warnings,
                total_spills,
                session_id,
                wait_info,
                blocking_session_id
            FROM blitz_results
            WHERE incident_id = {id:String}
                AND priority <= 100
            ORDER BY priority ASC, collected_at DESC
            LIMIT {limit:UInt32}
            """,
            parameters={
                "id": incident_id,
                "limit": limit,
            },
        )

        return [
            {
                "blitz_type": row[0],
                "priority": row[1],
                "findings_group": row[2],
                "finding": row[3],
                "details": row[4][:500] if row[4] else None,
                "url": row[5],
                "query_text": row[6][:300] if row[6] else None,
                "database_name": row[7],
                "total_cpu": row[8],
                "total_reads": row[9],
                "execution_count": row[10],
                "warnings": row[11],
                "total_spills": row[12],
                "session_id": row[13],
                "wait_info": row[14],
                "blocking_session_id": row[15],
            }
            for row in result.result_rows
        ]

    # =========================================================================
    # Comparison and Analysis
    # =========================================================================

    def compare_baseline_incident(self, incident_id: str) -> Dict[str, Any]:
        """
        Compare metrics between baseline and incident periods.

        Returns a dictionary with deltas for key metrics.
        """
        incident_window, baseline_window = self.get_incident_window(incident_id)

        if not baseline_window:
            return {"error": "No baseline window defined for this incident"}

        # Compare wait stats
        baseline_waits = self._aggregate_waits(incident_id, baseline_window)
        incident_waits = self._aggregate_waits(incident_id, incident_window)

        # Calculate deltas
        wait_deltas = {}
        for wait_type in set(baseline_waits.keys()) | set(incident_waits.keys()):
            baseline_ms = baseline_waits.get(wait_type, 0)
            incident_ms = incident_waits.get(wait_type, 0)
            delta = incident_ms - baseline_ms
            if delta != 0:
                wait_deltas[wait_type] = {
                    "baseline_ms": baseline_ms,
                    "incident_ms": incident_ms,
                    "delta_ms": delta,
                    "change_pct": (delta / baseline_ms * 100) if baseline_ms > 0 else None,
                }

        # Sort by delta
        sorted_waits = dict(
            sorted(
                wait_deltas.items(),
                key=lambda x: abs(x[1]["delta_ms"]),
                reverse=True,
            )[:15]
        )

        # Compare blocking
        baseline_blocking = self._count_blocking(incident_id, baseline_window)
        incident_blocking = self._count_blocking(incident_id, incident_window)

        # Compare memory grants
        baseline_grants = self._summarize_grants(incident_id, baseline_window)
        incident_grants = self._summarize_grants(incident_id, incident_window)

        return {
            "wait_stats_delta": sorted_waits,
            "blocking_comparison": {
                "baseline": baseline_blocking,
                "incident": incident_blocking,
                "delta": incident_blocking - baseline_blocking,
            },
            "memory_grants_comparison": {
                "baseline": baseline_grants,
                "incident": incident_grants,
            },
            "top_new_waits": [
                w for w in sorted_waits.keys()
                if sorted_waits[w]["baseline_ms"] == 0
            ][:5],
        }

    def _aggregate_waits(
        self,
        incident_id: str,
        window: TimeWindow,
    ) -> Dict[str, int]:
        """Aggregate wait stats for a time window."""
        result = self.client.query(
            """
            SELECT
                wait_type,
                sum(wait_time_ms) as total_ms
            FROM wait_stats
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
            GROUP BY wait_type
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
            },
        )
        return {row[0]: int(row[1]) for row in result.result_rows}

    def _count_blocking(
        self,
        incident_id: str,
        window: TimeWindow,
    ) -> int:
        """Count blocking events in a time window."""
        result = self.client.query(
            """
            SELECT count(*)
            FROM blocking_chains
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
                AND blocking_level > 0
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
            },
        )
        return int(result.result_rows[0][0]) if result.result_rows else 0

    def _summarize_grants(
        self,
        incident_id: str,
        window: TimeWindow,
    ) -> Dict[str, int]:
        """Summarize memory grants by status."""
        result = self.client.query(
            """
            SELECT
                grant_status,
                count(*) as cnt
            FROM memory_grants
            WHERE ({id:String} = '' OR incident_id = {id:String} OR incident_id IS NULL)
                AND collected_at BETWEEN {start:String} AND {end:String}
            GROUP BY grant_status
            """,
            parameters={
                "id": incident_id,
                "start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f'),
            },
        )
        return {row[0]: int(row[1]) for row in result.result_rows}

    # =========================================================================
    # Verification Metrics
    # =========================================================================

    def get_verification_metrics(
        self,
        window: TimeWindow,
    ) -> Dict[str, Any]:
        """
        Get metrics for incident verification.

        Returns metrics in a format suitable for threshold evaluation,
        matching the MetricsSample/MetricsSummary structure from verify.py.

        Args:
            window: Time window to query

        Returns:
            Dict with verification metrics
        """
        metrics: Dict[str, Any] = {
            "window_start": window.start.isoformat(),
            "window_end": window.end.isoformat(),
            "duration_seconds": (window.end - window.start).total_seconds(),
        }

        # Blocking metrics from blocking_chains table
        blocking = self._get_blocking_metrics(window)
        metrics.update(blocking)

        # Wait stats metrics
        wait_stats = self._get_wait_stats_metrics(window)
        metrics.update(wait_stats)

        # Query stats metrics
        query_stats = self._get_query_stats_metrics(window)
        metrics.update(query_stats)

        # Memory grant metrics
        memory = self._get_memory_grant_metrics(window)
        metrics.update(memory)

        # Scheduler metrics
        scheduler = self._get_scheduler_metrics(window)
        metrics.update(scheduler)

        # File stats metrics (for tempdb and I/O latency)
        file_stats = self._get_file_stats_metrics(window)
        metrics.update(file_stats)

        return metrics

    def _get_blocking_metrics(self, window: TimeWindow) -> Dict[str, Any]:
        """Get blocking-related metrics for verification."""
        result = self.client.query(
            """
            SELECT
                count(DISTINCT session_id) as blocked_sessions,
                count(DISTINCT blocking_session_id) as blocking_sessions,
                max(wait_time_ms) as max_wait_time_ms,
                sum(wait_time_ms) as total_wait_time_ms
            FROM blocking_chains
            WHERE collected_at BETWEEN {start:String} AND {end:String}
                AND blocking_level > 0
            """,
            parameters={"start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'), "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f')},
        )

        if result.result_rows and result.result_rows[0][0]:
            row = result.result_rows[0]
            return {
                "blocked_sessions": int(row[0] or 0),
                "blocking_sessions": int(row[1] or 0),
                "max_wait_time_ms": float(row[2] or 0),
                "total_blocking_wait_ms": float(row[3] or 0),
            }
        return {
            "blocked_sessions": 0,
            "blocking_sessions": 0,
            "max_wait_time_ms": 0.0,
            "total_blocking_wait_ms": 0.0,
        }

    def _get_wait_stats_metrics(self, window: TimeWindow) -> Dict[str, Any]:
        """Get wait stats metrics for verification."""
        result = self.client.query(
            """
            SELECT
                sumIf(wait_time_ms, wait_type LIKE 'LCK_M_%') as lock_wait_ms,
                sumIf(wait_time_ms, wait_type = 'SOS_SCHEDULER_YIELD') as sos_scheduler_yield_ms,
                sumIf(signal_wait_time_ms, wait_type = 'SOS_SCHEDULER_YIELD') as signal_wait_ms,
                sumIf(wait_time_ms, wait_type IN ('PAGEIOLATCH_SH', 'PAGEIOLATCH_EX', 'PAGEIOLATCH_UP')) as pageiolatch_ms,
                sumIf(wait_time_ms, wait_type = 'WRITELOG') as writelog_ms,
                sumIf(wait_time_ms, wait_type LIKE 'RESOURCE_SEMAPHORE%') as resource_semaphore_ms,
                sum(wait_time_ms) as total_wait_ms,
                sum(signal_wait_time_ms) as total_signal_wait_ms,
                countIf(wait_type = 'WRITELOG') as writelog_count,
                countIf(wait_type IN ('PAGEIOLATCH_SH', 'PAGEIOLATCH_EX', 'PAGEIOLATCH_UP')) as pageiolatch_count,
                -- Additional latch metrics for contention detection
                sumIf(wait_time_ms, wait_type = 'LATCH_EX') as latch_ex_ms,
                sumIf(wait_time_ms, wait_type = 'LATCH_SH') as latch_sh_ms,
                sumIf(wait_time_ms, wait_type IN ('LATCH_EX', 'LATCH_SH', 'LATCH_UP', 'LATCH_DT')) as total_latch_ms
            FROM wait_stats
            WHERE collected_at BETWEEN {start:String} AND {end:String}
            """,
            parameters={"start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'), "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f')},
        )

        if result.result_rows:
            row = result.result_rows[0]
            total_wait = float(row[6] or 0)
            total_signal = float(row[7] or 0)
            writelog_ms = float(row[4] or 0)
            writelog_count = int(row[8] or 0)
            pageiolatch_ms = float(row[3] or 0)
            pageiolatch_count = int(row[9] or 0)

            # Signal wait percentage: high % indicates CPU pressure (CPU-bound, not waiting on resources)
            signal_wait_percent = (total_signal / total_wait * 100) if total_wait > 0 else 0.0

            # Average latencies
            avg_writelog_latency_ms = (writelog_ms / writelog_count) if writelog_count > 0 else 0.0
            avg_pageiolatch_latency_ms = (pageiolatch_ms / pageiolatch_count) if pageiolatch_count > 0 else 0.0

            return {
                "lock_wait_time_ms": float(row[0] or 0),
                "sos_scheduler_yield_ms": float(row[1] or 0),
                "signal_wait_time_ms": float(row[2] or 0),
                "pageiolatch_ms": pageiolatch_ms,
                "writelog_ms": writelog_ms,
                "resource_semaphore_ms": float(row[5] or 0),
                # Computed metrics for verification
                "signal_wait_percent": signal_wait_percent,
                "avg_writelog_latency_ms": avg_writelog_latency_ms,
                "avg_pageiolatch_latency_ms": avg_pageiolatch_latency_ms,
                # Latch contention metrics
                "latch_ex_ms": float(row[10] or 0),
                "latch_sh_ms": float(row[11] or 0),
                "total_latch_ms": float(row[12] or 0),
            }
        return {
            "lock_wait_time_ms": 0.0,
            "sos_scheduler_yield_ms": 0.0,
            "signal_wait_time_ms": 0.0,
            "pageiolatch_ms": 0.0,
            "writelog_ms": 0.0,
            "resource_semaphore_ms": 0.0,
            "signal_wait_percent": 0.0,
            "avg_writelog_latency_ms": 0.0,
            "avg_pageiolatch_latency_ms": 0.0,
            "latch_ex_ms": 0.0,
            "latch_sh_ms": 0.0,
            "total_latch_ms": 0.0,
        }

    def _get_query_stats_metrics(self, window: TimeWindow) -> Dict[str, Any]:
        """Get query stats metrics for verification.

        Returns both aggregate metrics and per-query metrics for stable queries
        (queries with sufficient executions to be meaningful).
        """
        # Get aggregate metrics
        result = self.client.query(
            """
            SELECT
                sum(execution_count) as total_executions,
                sum(total_worker_time_us) as total_cpu_us,
                sum(total_elapsed_time_us) as total_elapsed_us,
                sum(total_logical_reads) as total_logical_reads,
                sum(total_logical_writes) as total_logical_writes,
                sum(total_spills) as total_spills
            FROM query_stats
            WHERE collected_at BETWEEN {start:String} AND {end:String}
            """,
            parameters={"start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'), "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f')},
        )

        if result.result_rows:
            row = result.result_rows[0]
            executions = int(row[0] or 0)
            elapsed_us = float(row[2] or 0)
            logical_reads = int(row[3] or 0)
            metrics = {
                "total_executions": executions,
                "total_cpu_us": float(row[1] or 0),
                "total_elapsed_us": elapsed_us,
                "total_elapsed_ms": elapsed_us / 1000.0,
                "total_logical_reads": logical_reads,
                "total_logical_writes": int(row[4] or 0),
                "total_spills": int(row[5] or 0),
                "avg_elapsed_per_exec_ms": (elapsed_us / 1000.0 / executions) if executions > 0 else 0.0,
                "avg_logical_reads_per_exec": (logical_reads / executions) if executions > 0 else 0.0,
            }
        else:
            metrics = {
                "total_executions": 0,
                "total_cpu_us": 0.0,
                "total_elapsed_us": 0.0,
                "total_elapsed_ms": 0.0,
                "total_logical_reads": 0,
                "total_logical_writes": 0,
                "total_spills": 0,
                "avg_elapsed_per_exec_ms": 0.0,
                "avg_logical_reads_per_exec": 0.0,
            }

        # Get per-query metrics for stable queries (min 10 executions)
        per_query_result = self.client.query(
            """
            SELECT
                query_hash,
                sum(execution_count) as execs,
                sum(total_logical_reads) as reads,
                sum(total_elapsed_time_us) as elapsed_us
            FROM query_stats
            WHERE collected_at BETWEEN {start:String} AND {end:String}
            GROUP BY query_hash
            HAVING execs >= 10
            """,
            parameters={"start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'), "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f')},
        )

        per_query = {}
        if per_query_result.result_rows:
            for row in per_query_result.result_rows:
                query_hash = row[0]
                execs = int(row[1] or 0)
                reads = int(row[2] or 0)
                elapsed_us = float(row[3] or 0)
                if execs > 0:
                    per_query[query_hash] = {
                        "executions": execs,
                        "reads_per_exec": reads / execs,
                        "elapsed_per_exec_ms": elapsed_us / 1000.0 / execs,
                    }

        metrics["per_query"] = per_query
        return metrics

    def _get_memory_grant_metrics(self, window: TimeWindow) -> Dict[str, Any]:
        """Get memory grant metrics for verification."""
        result = self.client.query(
            """
            SELECT
                countIf(grant_status = 'WAITING') as pending_grants,
                countIf(grant_status = 'SPILLED') as spilled_grants,
                sum(granted_memory_mb) as total_granted_mb,
                max(wait_time_ms) as max_grant_wait_ms
            FROM memory_grants
            WHERE collected_at BETWEEN {start:String} AND {end:String}
            """,
            parameters={"start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'), "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f')},
        )

        if result.result_rows:
            row = result.result_rows[0]
            return {
                "pending_memory_grants": int(row[0] or 0),
                "spilled_memory_grants": int(row[1] or 0),
                "total_granted_memory_mb": float(row[2] or 0),
                "max_grant_wait_ms": float(row[3] or 0),
            }
        return {
            "pending_memory_grants": 0,
            "spilled_memory_grants": 0,
            "total_granted_memory_mb": 0.0,
            "max_grant_wait_ms": 0.0,
        }

    def _get_scheduler_metrics(self, window: TimeWindow) -> Dict[str, Any]:
        """Get scheduler metrics for verification."""
        result = self.client.query(
            """
            SELECT
                avg(runnable_tasks_count) as avg_runnable,
                max(runnable_tasks_count) as max_runnable,
                avg(current_tasks_count) as avg_current,
                max(current_tasks_count) as max_current
            FROM schedulers
            WHERE collected_at BETWEEN {start:String} AND {end:String}
            """,
            parameters={"start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'), "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f')},
        )

        if result.result_rows:
            row = result.result_rows[0]
            return {
                "avg_runnable_tasks": float(row[0] or 0),
                "max_runnable_tasks": int(row[1] or 0),
                "avg_current_tasks": float(row[2] or 0),
                "max_current_tasks": int(row[3] or 0),
            }
        return {
            "avg_runnable_tasks": 0.0,
            "max_runnable_tasks": 0,
            "avg_current_tasks": 0.0,
            "max_current_tasks": 0,
        }

    def _get_file_stats_metrics(self, window: TimeWindow) -> Dict[str, Any]:
        """Get file I/O statistics for verification.

        Returns tempdb I/O activity and average I/O latency metrics.
        Note: tempdb activity (high I/O ops) indicates spill activity.
        """
        # Get tempdb I/O activity and overall I/O latency
        # Note: file_stats doesn't have size_on_disk_mb, so we use I/O ops as proxy for activity
        result = self.client.query(
            """
            SELECT
                sumIf(num_of_reads + num_of_writes, database_name = 'tempdb') as tempdb_io_ops,
                sumIf(io_stall_ms, database_name = 'tempdb') as tempdb_io_stall_ms,
                sumIf(num_of_bytes_read + num_of_bytes_written, database_name = 'tempdb') as tempdb_bytes,
                sum(num_of_reads + num_of_writes) as total_io_ops,
                sum(io_stall_ms) as total_io_stall_ms
            FROM file_stats
            WHERE collected_at BETWEEN {start:String} AND {end:String}
            """,
            parameters={"start": window.start.strftime('%Y-%m-%d %H:%M:%S.%f'), "end": window.end.strftime('%Y-%m-%d %H:%M:%S.%f')},
        )

        if result.result_rows:
            row = result.result_rows[0]
            tempdb_io_ops = int(row[0] or 0)
            tempdb_io_stall_ms = float(row[1] or 0)
            tempdb_bytes = int(row[2] or 0)
            total_io_ops = int(row[3] or 0)
            total_io_stall_ms = float(row[4] or 0)

            # Average I/O latency across all files
            avg_io_latency_ms = (total_io_stall_ms / total_io_ops) if total_io_ops > 0 else 0.0

            # Average tempdb I/O latency
            avg_tempdb_io_latency_ms = (tempdb_io_stall_ms / tempdb_io_ops) if tempdb_io_ops > 0 else 0.0

            # Tempdb bytes in MB (proxy for tempdb usage/spills)
            tempdb_mb = tempdb_bytes / (1024 * 1024)

            return {
                "tempdb_internal_objects_mb": tempdb_mb,  # Bytes transferred as proxy for spill size
                "tempdb_io_ops": tempdb_io_ops,
                "tempdb_io_stall_ms": tempdb_io_stall_ms,
                "avg_tempdb_io_latency_ms": avg_tempdb_io_latency_ms,
                "total_io_ops": total_io_ops,
                "total_io_stall_ms": total_io_stall_ms,
                "avg_io_latency_ms": avg_io_latency_ms,
            }
        return {
            "tempdb_internal_objects_mb": 0.0,
            "tempdb_io_ops": 0,
            "tempdb_io_stall_ms": 0.0,
            "avg_tempdb_io_latency_ms": 0.0,
            "total_io_ops": 0,
            "total_io_stall_ms": 0.0,
            "avg_io_latency_ms": 0.0,
        }

    # =========================================================================
    # Query Tools for RCA Agent
    # =========================================================================

    def query_table(
        self,
        table: str,
        incident_id: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Generic table query for RCA agent.

        Args:
            table: Table name (without database prefix)
            incident_id: Optional incident ID filter
            filters: Additional column filters
            order_by: ORDER BY clause (e.g., "wait_time_ms DESC")
            limit: Maximum rows to return

        Returns:
            List of row dictionaries
        """
        allowed_columns = ALLOWED_QUERY_TABLE_COLUMNS.get(table)
        if allowed_columns is None:
            allowed_tables = ", ".join(sorted(ALLOWED_QUERY_TABLE_COLUMNS.keys()))
            raise ValueError(f"Unsupported table '{table}'. Allowed tables: {allowed_tables}")

        where_clauses = []
        params = {"limit": limit}

        # Bound result size to avoid high-cardinality ad-hoc scans.
        safe_limit = max(1, min(int(limit), 500))
        params["limit"] = safe_limit

        if incident_id:
            where_clauses.append("(incident_id = {id:String} OR incident_id IS NULL)")
            params["id"] = incident_id

        if filters:
            for i, (col, val) in enumerate(filters.items()):
                if col not in allowed_columns:
                    raise ValueError(f"Unsupported filter column '{col}' for table '{table}'")

                param_name = f"filter_{i}"
                if isinstance(val, bool):
                    where_clauses.append(f"{col} = {{{param_name}:UInt8}}")
                    params[param_name] = 1 if val else 0
                elif isinstance(val, int):
                    where_clauses.append(f"{col} = {{{param_name}:Int64}}")
                    params[param_name] = val
                elif isinstance(val, float):
                    where_clauses.append(f"{col} = {{{param_name}:Float64}}")
                    params[param_name] = val
                else:
                    where_clauses.append(f"{col} = {{{param_name}:String}}")
                    params[param_name] = str(val)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Map aliased column names to actual table columns.
        column_mappings = {
            "total_cpu_us": "total_worker_time_us",
            "total_elapsed_us": "total_elapsed_time_us",
            "total_stall_ms": "io_stall_ms",
        }
        actual_order_by = order_by
        if order_by:
            for alias, actual in column_mappings.items():
                actual_order_by = actual_order_by.replace(alias, actual)
            tokens = actual_order_by.strip().split()
            if len(tokens) not in (1, 2):
                raise ValueError(
                    "order_by must be '<column>' or '<column> ASC|DESC'"
                )
            order_col = tokens[0]
            order_dir = tokens[1].upper() if len(tokens) == 2 else "ASC"
            if order_col not in allowed_columns:
                raise ValueError(f"Unsupported order_by column '{order_col}' for table '{table}'")
            if order_dir not in ("ASC", "DESC"):
                raise ValueError("order_by direction must be ASC or DESC")
            actual_order_by = f"{order_col} {order_dir}"

        order_sql = f"ORDER BY {actual_order_by}" if actual_order_by else ""

        # Use fully-qualified table name with database prefix
        qualified_table = f"{self.database}.{table}"

        query = f"""
            SELECT *
            FROM {qualified_table}
            WHERE {where_sql}
            {order_sql}
            LIMIT {{limit:UInt32}}
        """

        result = self.client.query(query, parameters=params)

        return [
            dict(zip(result.column_names, row))
            for row in result.result_rows
        ]

    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute a raw ClickHouse query and return rows as dictionaries.

        Used by diagnostic tools that need direct SQL access to ClickHouse tables.
        """
        result = self.client.query(query, parameters=parameters or {})
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def insert_blitz_results(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert Blitz diagnostic rows into `blitz_results`.

        Accepts partial records and fills missing columns with NULL/defaults.
        """
        if not records:
            return 0

        columns = [
            "collected_at",
            "incident_id",
            "blitz_type",
            "priority",
            "findings_group",
            "finding",
            "details",
            "url",
            "query_text",
            "database_name",
            "total_cpu",
            "total_reads",
            "total_writes",
            "execution_count",
            "avg_duration_ms",
            "warnings",
            "total_spills",
            "session_id",
            "status",
            "wait_info",
            "blocking_session_id",
            "cpu_ms",
            "reads",
            "schema_name",
            "table_name",
            "index_name",
            "index_definition",
            "create_tsql",
            "deadlock_type",
            "victim_query",
            "blocking_query",
            "deadlock_graph",
            "extended_data",
        ]

        rows = []
        now = datetime.now(timezone.utc)
        for record in records:
            row: List[Any] = []
            for col in columns:
                if col == "collected_at":
                    value = record.get(col, now)
                    # Normalize ISO string timestamps for clickhouse-connect.
                    if isinstance(value, str):
                        value = value.replace("Z", "+00:00")
                        value = datetime.fromisoformat(value)
                elif col == "priority":
                    value = int(record.get(col, 50))
                else:
                    value = record.get(col)
                row.append(value)
            rows.append(row)

        self.client.insert(
            table=f"{self.database}.blitz_results",
            data=rows,
            column_names=columns,
        )
        return len(rows)
