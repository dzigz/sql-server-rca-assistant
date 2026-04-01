"""
DMV Collector for SQL Server metrics not covered by OTel receiver.

Collects:
- Wait stats with delta calculation
- Active requests with SQL text
- Blocking chains (recursive CTE)
- Memory grants with spill detection
- Query stats with SQL text
- File I/O stats
- Scheduler health
- Performance counters
- Missing indexes

Pushes data to ClickHouse for storage and analysis.
"""

import time
import threading
import structlog
import mssql_python as mssql
from datetime import datetime
from typing import Optional, Dict, List, Any, Callable

from .config import CollectorConfig
from .clickhouse_writer import ClickHouseWriter
from .dmv_queries import DMVQueries

logger = structlog.get_logger()


class DMVCollector:
    """Collects DMV metrics from SQL Server and writes to ClickHouse."""

    def __init__(self, config: CollectorConfig, writer: ClickHouseWriter):
        self.config = config
        self.writer = writer
        self.queries = DMVQueries()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._current_incident_id: Optional[str] = None
        self._is_baseline: bool = True  # Default to baseline mode (no active incident)

        # Previous snapshot for delta calculations
        self._prev_wait_stats: Dict[str, Dict[str, int]] = {}
        self._prev_file_stats: Dict[str, Dict[str, int]] = {}

        # Version compatibility cache (None = unknown, try full first)
        self._use_safe_query_stats: Optional[bool] = None

        # Pause control
        self._paused = False
        self._pause_lock = threading.Lock()

    def start(self) -> None:
        """Start the collection loop in a background thread."""
        if self._running:
            logger.warning("Collector already running")
            return

        self._running = True
        self._thread = threading.Thread(target=self._collection_loop, daemon=True)
        self._thread.start()
        logger.info(
            "Started DMV collector",
            interval=self.config.collection_interval,
        )

    def stop(self) -> None:
        """Stop the collection loop."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None
        logger.info("Stopped DMV collector")

    def set_incident(self, incident_id: Optional[str], is_baseline: bool = False) -> None:
        """Set current incident ID for tagging metrics."""
        self._current_incident_id = incident_id
        self._is_baseline = is_baseline
        if incident_id:
            logger.info(
                "Incident set",
                incident_id=incident_id,
                is_baseline=is_baseline,
            )
        else:
            logger.info("Incident cleared")

    def pause(self) -> None:
        """Pause the collection loop."""
        with self._pause_lock:
            self._paused = True
        logger.info("Collector paused")

    def resume(self) -> None:
        """Resume the collection loop."""
        with self._pause_lock:
            self._paused = False
        logger.info("Collector resumed")

    def is_paused(self) -> bool:
        """Check if collector is paused."""
        with self._pause_lock:
            return self._paused

    def _collection_loop(self) -> None:
        """Main collection loop."""
        while self._running:
            # Skip collection if paused
            if not self.is_paused():
                try:
                    self._collect_all()
                except Exception as e:
                    logger.error("Collection error", error=str(e))

            # Sleep for the collection interval
            for _ in range(self.config.collection_interval * 10):
                if not self._running:
                    break
                time.sleep(0.1)

    def _get_connection(self) -> mssql.Connection:
        """Get a database connection."""
        conn_str = (
            f"SERVER={self.config.sqlserver_host},{self.config.sqlserver_port};"
            f"DATABASE={self.config.sqlserver_database};"
            f"UID={self.config.sqlserver_user};"
            f"PWD={self.config.sqlserver_password};"
            f"TrustServerCertificate=yes;"
        )
        return mssql.connect(connection_str=conn_str, timeout=self.config.query_timeout)

    def _collect_all(self) -> None:
        """Collect all DMV metrics."""
        collected_at = datetime.now()

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SET LOCK_TIMEOUT {self.config.query_timeout * 1000}")

                # Collect each metric type
                self._collect_wait_stats(cursor, collected_at)
                self._collect_active_requests(cursor, collected_at)
                self._collect_blocking_chains(cursor, collected_at)
                self._collect_memory_grants(cursor, collected_at)
                self._collect_query_stats(cursor, collected_at)
                self._collect_file_stats(cursor, collected_at)
                self._collect_schedulers(cursor, collected_at)
                self._collect_perf_counters(cursor, collected_at)
                self._collect_missing_indexes(cursor, collected_at)

        except Exception as e:
            logger.error("Failed to collect metrics", error=str(e))
            raise

    def _collect_wait_stats(self, cursor, collected_at: datetime) -> None:
        """Collect wait statistics with delta calculation."""
        try:
            cursor.execute(self.queries.WAIT_STATS)
            rows = cursor.fetchall()

            current_stats = {}
            records = []

            for row in rows:
                wait_type = row[0]
                current_stats[wait_type] = {
                    "waiting_tasks_count": row[1],
                    "wait_time_ms": row[2],
                    "max_wait_time_ms": row[3],
                    "signal_wait_time_ms": row[4],
                }

                # Calculate deltas if we have previous values
                if wait_type in self._prev_wait_stats:
                    prev = self._prev_wait_stats[wait_type]
                    delta_tasks = row[1] - prev["waiting_tasks_count"]
                    delta_wait = row[2] - prev["wait_time_ms"]
                    delta_signal = row[4] - prev["signal_wait_time_ms"]

                    if delta_wait > 0:  # Only record if there's activity
                        records.append({
                            "collected_at": collected_at,
                            "wait_type": wait_type,
                            "waiting_tasks_count": max(0, delta_tasks),
                            "wait_time_ms": max(0, delta_wait),
                            "max_wait_time_ms": row[3],
                            "signal_wait_time_ms": max(0, delta_signal),
                        })
                else:
                    # First collection - record current values
                    records.append({
                        "collected_at": collected_at,
                        "wait_type": wait_type,
                        "waiting_tasks_count": row[1],
                        "wait_time_ms": row[2],
                        "max_wait_time_ms": row[3],
                        "signal_wait_time_ms": row[4],
                    })

            self._prev_wait_stats = current_stats

            if records:
                self.writer.write_wait_stats(
                    records,
                    incident_id=self._current_incident_id,
                    is_baseline=self._is_baseline,
                )

        except Exception as e:
            logger.error("Failed to collect wait stats", error=str(e))

    def _collect_active_requests(self, cursor, collected_at: datetime) -> None:
        """Collect active requests."""
        try:
            cursor.execute(self.queries.ACTIVE_REQUESTS)
            rows = cursor.fetchall()

            records = []
            for row in rows:
                records.append({
                    "collected_at": collected_at,
                    "session_id": row[0],
                    "request_id": row[1],
                    "status": row[2],
                    "command": row[3],
                    "blocking_session_id": row[4] if row[4] else None,
                    "wait_type": row[5],
                    "wait_time_ms": row[6],
                    "wait_resource": row[7],
                    "cpu_time_ms": row[8],
                    "logical_reads": row[9],
                    "writes": row[10],
                    "database_name": row[11],
                    "sql_text": row[12][:4000] if row[12] else None,
                    "context_info": row[13],
                })

            if records:
                self.writer.write_active_requests(
                    records,
                    incident_id=self._current_incident_id,
                    is_baseline=self._is_baseline,
                )

        except Exception as e:
            logger.error("Failed to collect active requests", error=str(e))

    def _collect_blocking_chains(self, cursor, collected_at: datetime) -> None:
        """Collect blocking chains using recursive CTE."""
        try:
            cursor.execute(self.queries.BLOCKING_CHAINS)
            rows = cursor.fetchall()

            records = []
            for row in rows:
                records.append({
                    "collected_at": collected_at,
                    "blocking_level": row[0],
                    "session_id": row[1],
                    "blocking_session_id": row[2] if row[2] else None,
                    "wait_type": row[3],
                    "wait_time_ms": row[4],
                    "wait_resource": row[5],
                    "lock_mode": row[6],
                    "status": row[7],
                    "command": row[8],
                    "database_name": row[9],
                    "sql_text": row[10][:4000] if row[10] else None,
                    "transaction_id": row[11],
                    "open_transaction_count": row[12],
                })

            if records:
                self.writer.write_blocking_chains(
                    records,
                    incident_id=self._current_incident_id,
                    is_baseline=self._is_baseline,
                )

        except Exception as e:
            logger.error("Failed to collect blocking chains", error=str(e))

    def _collect_memory_grants(self, cursor, collected_at: datetime) -> None:
        """Collect memory grant information."""
        try:
            cursor.execute(self.queries.MEMORY_GRANTS)
            rows = cursor.fetchall()

            records = []
            for row in rows:
                records.append({
                    "collected_at": collected_at,
                    "session_id": row[0],
                    "request_time": row[1],
                    "grant_time": row[2],
                    "requested_memory_mb": float(row[3]) if row[3] else 0,
                    "granted_memory_mb": float(row[4]) if row[4] else 0,
                    "required_memory_mb": float(row[5]) if row[5] else 0,
                    "used_memory_mb": float(row[6]) if row[6] else 0,
                    "max_used_memory_mb": float(row[7]) if row[7] else 0,
                    "ideal_memory_mb": float(row[8]) if row[8] else 0,
                    "wait_time_ms": row[9] or 0,
                    "grant_status": row[10],
                    "query_cost": float(row[11]) if row[11] else None,
                    "dop": row[12],
                    "sql_text": row[13][:4000] if row[13] else None,
                })

            if records:
                self.writer.write_memory_grants(
                    records,
                    incident_id=self._current_incident_id,
                    is_baseline=self._is_baseline,
                )

        except Exception as e:
            logger.error("Failed to collect memory grants", error=str(e))

    def _collect_query_stats(self, cursor, collected_at: datetime) -> None:
        """Collect query statistics with fallback for older SQL Server versions."""
        try:
            # Use cached decision if available
            if self._use_safe_query_stats is True:
                cursor.execute(self.queries.QUERY_STATS_SAFE)
            elif self._use_safe_query_stats is False:
                cursor.execute(self.queries.QUERY_STATS)
            else:
                # First attempt - try full query, cache the result
                try:
                    cursor.execute(self.queries.QUERY_STATS)
                    self._use_safe_query_stats = False  # Full query works
                except Exception as e:
                    if "invalid column name" in str(e).lower():
                        logger.warning(
                            "Using safe query stats (SQL Server < 2016 SP1 detected)"
                        )
                        self._use_safe_query_stats = True  # Need safe query
                        cursor.execute(self.queries.QUERY_STATS_SAFE)
                    else:
                        raise

            rows = cursor.fetchall()

            records = []
            for row in rows:
                records.append({
                    "collected_at": collected_at,
                    "query_hash": row[0],
                    "query_plan_hash": row[1],
                    "execution_count": row[2],
                    "total_worker_time_us": row[3],
                    "total_elapsed_time_us": row[4],
                    "total_logical_reads": row[5],
                    "total_logical_writes": row[6],
                    "total_physical_reads": row[7],
                    "total_grant_kb": row[8],
                    "total_spills": row[9],
                    "database_name": row[10],
                    "sql_text": row[11][:4000] if row[11] else None,
                })

            if records:
                self.writer.write_query_stats(
                    records,
                    incident_id=self._current_incident_id,
                    is_baseline=self._is_baseline,
                )

        except Exception as e:
            logger.error("Failed to collect query stats", error=str(e))

    def _collect_file_stats(self, cursor, collected_at: datetime) -> None:
        """Collect file I/O statistics."""
        try:
            cursor.execute(self.queries.FILE_STATS)
            rows = cursor.fetchall()

            records = []
            for row in rows:
                records.append({
                    "collected_at": collected_at,
                    "database_id": row[0],
                    "database_name": row[1],
                    "file_id": row[2],
                    "file_name": row[3],
                    "file_type": row[4],
                    "num_of_reads": row[5],
                    "num_of_bytes_read": row[6],
                    "io_stall_read_ms": row[7],
                    "num_of_writes": row[8],
                    "num_of_bytes_written": row[9],
                    "io_stall_write_ms": row[10],
                    "io_stall_ms": row[11],
                })

            if records:
                self.writer.write_file_stats(
                    records,
                    incident_id=self._current_incident_id,
                    is_baseline=self._is_baseline,
                )

        except Exception as e:
            logger.error("Failed to collect file stats", error=str(e))

    def _collect_schedulers(self, cursor, collected_at: datetime) -> None:
        """Collect scheduler information."""
        try:
            cursor.execute(self.queries.SCHEDULERS)
            rows = cursor.fetchall()

            records = []
            for row in rows:
                records.append({
                    "collected_at": collected_at,
                    "scheduler_id": row[0],
                    "cpu_id": row[1],
                    "status": row[2],
                    "is_online": 1 if row[3] else 0,
                    "current_tasks_count": row[4],
                    "runnable_tasks_count": row[5],
                    "current_workers_count": row[6],
                    "active_workers_count": row[7],
                    "work_queue_count": row[8],
                    "context_switches_count": row[9],
                    "yield_count": row[10],
                })

            if records:
                self.writer.write_schedulers(
                    records,
                    incident_id=self._current_incident_id,
                    is_baseline=self._is_baseline,
                )

        except Exception as e:
            logger.error("Failed to collect schedulers", error=str(e))

    def _collect_perf_counters(self, cursor, collected_at: datetime) -> None:
        """Collect performance counters."""
        try:
            cursor.execute(self.queries.PERF_COUNTERS)
            rows = cursor.fetchall()

            records = []
            for row in rows:
                records.append({
                    "collected_at": collected_at,
                    "object_name": row[0],
                    "counter_name": row[1],
                    "instance_name": row[2],
                    "counter_value": row[3],
                })

            if records:
                self.writer.write_perf_counters(
                    records,
                    incident_id=self._current_incident_id,
                    is_baseline=self._is_baseline,
                )

        except Exception as e:
            logger.error("Failed to collect perf counters", error=str(e))

    def _collect_missing_indexes(self, cursor, collected_at: datetime) -> None:
        """Collect missing index recommendations."""
        try:
            cursor.execute(self.queries.MISSING_INDEXES)
            rows = cursor.fetchall()

            records = []
            for row in rows:
                records.append({
                    "collected_at": collected_at,
                    "database_name": row[0],
                    "schema_name": row[1],
                    "table_name": row[2],
                    "equality_columns": row[3],
                    "inequality_columns": row[4],
                    "included_columns": row[5],
                    "unique_compiles": row[6],
                    "user_seeks": row[7],
                    "user_scans": row[8],
                    "avg_total_user_cost": float(row[9]) if row[9] else 0,
                    "avg_user_impact": float(row[10]) if row[10] else 0,
                    "impact_score": float(row[11]) if row[11] else 0,
                })

            if records:
                self.writer.write_missing_indexes(
                    records,
                    incident_id=self._current_incident_id,
                    is_baseline=self._is_baseline,
                )

        except Exception as e:
            logger.error("Failed to collect missing indexes", error=str(e))

    def collect_once(self) -> None:
        """Perform a single collection cycle (for testing)."""
        self._collect_all()
