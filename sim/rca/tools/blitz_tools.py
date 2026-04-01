"""
On-demand Blitz diagnostic tools for RCA.

Provides a unified tool for running First Responder Kit diagnostic scripts
directly against SQL Server, with automatic fallback to cached results.

Features:
- Single tool `run_blitz_diagnostics` for all 5 Blitz scripts
- Direct SQL connection via mssql-python (no Docker dependency)
- Automatic storage of successful results in ClickHouse
- Fallback to cached results when live execution fails
- Staleness detection and timeout diagnostics
"""

import os
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import mssql_python as mssql

from sim.rca.tools.base import RCATool, ToolResult
from sim.logging_config import get_logger

if TYPE_CHECKING:
    from sim.rca.datasources import ClickHouseDataSource

logger = get_logger(__name__)

# Staleness threshold in minutes (configurable via env var)
BLITZ_STALE_THRESHOLD_MINUTES = int(os.getenv("BLITZ_STALE_THRESHOLD_MINUTES", "30"))


class BlitzScript(str, Enum):
    """Available Blitz diagnostic scripts."""
    FIRST = "first"      # sp_BlitzFirst - real-time wait stats
    CACHE = "cache"      # sp_BlitzCache - query plan analysis
    WHO = "who"          # sp_BlitzWho - active sessions
    INDEX = "index"      # sp_BlitzIndex - index recommendations
    LOCK = "lock"        # sp_BlitzLock - deadlock analysis
    ALL = "all"          # Run all scripts


class RunBlitzDiagnosticsTool(RCATool):
    """
    Run First Responder Kit diagnostic scripts on-demand.

    Single tool that can run one or all Blitz scripts:
    - first: sp_BlitzFirst (real-time wait stats delta)
    - cache: sp_BlitzCache (top queries by CPU)
    - who: sp_BlitzWho (active sessions, blocking)
    - index: sp_BlitzIndex (missing/unused indexes)
    - lock: sp_BlitzLock (deadlock analysis)
    - all: Run all scripts (default)

    Automatically falls back to cached results from ClickHouse if live
    execution fails (timeout, FRK not installed, connection error).
    """

    def __init__(
        self,
        sqlserver_host: str,
        sqlserver_port: int = 1433,
        sqlserver_user: str = "sa",
        sqlserver_password: str = "",
        sqlserver_database: str = "master",
        data_source: Optional["ClickHouseDataSource"] = None,
        connection_timeout: int = 30,
        query_timeout: int = 120,
    ):
        """
        Initialize the Blitz diagnostics tool.

        Args:
            sqlserver_host: SQL Server hostname
            sqlserver_port: SQL Server port (default 1433)
            sqlserver_user: SQL Server username
            sqlserver_password: SQL Server password
            sqlserver_database: Target database name
            data_source: Optional ClickHouse data source for caching/fallback
            connection_timeout: Connection timeout in seconds
            query_timeout: Query execution timeout in seconds
        """
        self._host = sqlserver_host
        self._port = sqlserver_port
        self._user = sqlserver_user
        self._password = sqlserver_password
        self._database = sqlserver_database
        self._data_source = data_source
        self._connection_timeout = connection_timeout
        self._query_timeout = query_timeout

    @property
    def name(self) -> str:
        return "run_blitz_diagnostics"

    @property
    def description(self) -> str:
        return """Run First Responder Kit diagnostic scripts for incident analysis.

USE THIS TOOL PRIMARILY DURING ACTIVE INCIDENTS - it captures real-time server state.

Parameters:
- script: Which script to run (first, cache, who, index, lock, all). Default: all
- seconds: For 'first' script - sampling period in seconds. Default: 5
- top: For 'cache' script - number of top queries. Default: 20

Scripts:
- first: sp_BlitzFirst - Real-time wait stats delta (captures what's happening NOW)
- cache: sp_BlitzCache - Current query plan cache analysis
- who: sp_BlitzWho - Active sessions, wait types, blocking chains RIGHT NOW
- index: sp_BlitzIndex - Missing indexes, unused indexes, duplicates
- lock: sp_BlitzLock - Recent deadlock analysis from system_health

Data Source Indicators (check response message):
- "Live diagnostics" = Fresh data from SQL Server (best for active incidents)
- "Using cached Blitz findings (X min old)" = Stored results, recent enough
- "WARNING: STALE cached findings (X min old)" = Data >30 min old
- "DIAGNOSTIC: TIMED OUT" = Server overloaded - this is itself a finding!

When NOT to use:
- For health assessments -> use run_sp_blitz() instead
- For historical analysis -> use query_clickhouse() on wait_stats, blocking_chains"""

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "script": {
                    "type": "string",
                    "enum": ["first", "cache", "who", "index", "lock", "all"],
                    "description": "Which script to run. Default: all",
                    "default": "all"
                },
                "seconds": {
                    "type": "integer",
                    "description": "Sampling period for BlitzFirst (seconds). Default: 5",
                    "default": 5
                },
                "top": {
                    "type": "integer",
                    "description": "Number of top queries for BlitzCache. Default: 20",
                    "default": 20
                }
            },
            "required": []
        }

    def _get_connection(self) -> mssql.Connection:
        """Get a database connection."""
        conn_str = (
            f"SERVER={self._host},{self._port};"
            f"DATABASE={self._database};"
            f"UID={self._user};"
            f"PWD={self._password};"
            f"TrustServerCertificate=yes;"
        )
        return mssql.connect(connection_str=conn_str, timeout=self._connection_timeout)

    def _check_installed(self, proc_name: str) -> Optional[str]:
        """Check if stored procedure is installed. Returns error message or None."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                # Check in master.dbo where FRK is typically installed
                cursor.execute(f"SELECT OBJECT_ID('master.dbo.{proc_name}', 'P')")
                result = cursor.fetchone()
                if result is None or result[0] is None:
                    # Also check current database
                    cursor.execute(f"SELECT OBJECT_ID('dbo.{proc_name}', 'P')")
                    result = cursor.fetchone()
                    if result is None or result[0] is None:
                        return f"{proc_name} not installed. Install First Responder Kit."
            return None
        except Exception as e:
            return f"Cannot check {proc_name}: {str(e)}"

    def execute(
        self,
        script: str = "all",
        seconds: int = 5,
        top: int = 20,
        **kwargs
    ) -> ToolResult:
        """
        Execute Blitz diagnostics.

        Args:
            script: Which script to run (first, cache, who, index, lock, all)
            seconds: Sampling period for BlitzFirst
            top: Number of queries for BlitzCache

        Returns:
            ToolResult with diagnostic findings
        """
        script = script.lower()
        execution_error = None
        timed_out = False

        # Try on-demand execution first
        try:
            results = self._run_on_demand(script, seconds, top)
            if results:
                # Store successful results in ClickHouse for future fallback
                if self._data_source:
                    adhoc_id = f"adhoc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    self._store_results_in_clickhouse(results, adhoc_id)
                return ToolResult(
                    success=True,
                    data=results,
                    metadata={"source": "live", "message": "Live diagnostics"}
                )
        except TimeoutError as e:
            timed_out = True
            execution_error = f"TIMEOUT: {str(e)}"
            logger.warning("Blitz execution timed out: %s", e)
        except Exception as e:
            execution_error = str(e)
            logger.warning("Blitz execution failed: %s", e)

        # Fallback: Query latest stored Blitz findings from ClickHouse
        if self._data_source:
            stored = self._get_latest_stored_findings(script)
            if stored:
                age = stored.get("age_minutes", 0)
                is_stale = stored.get("is_stale", False)

                # Build message based on why we're using cached data
                if timed_out:
                    message = (
                        f"DIAGNOSTIC: Live Blitz execution TIMED OUT - this is a symptom of the incident! "
                        f"Server is likely overloaded. Using cached findings ({age} minutes old)."
                    )
                elif is_stale:
                    message = (
                        f"WARNING: Using STALE cached Blitz findings ({age} minutes old). "
                        f"Data may not reflect current state. On-demand failed: {execution_error}"
                    )
                else:
                    message = f"Using cached Blitz findings ({age} minutes old, on-demand unavailable)"

                return ToolResult(
                    success=True,
                    data=stored["findings"],
                    metadata={
                        "source": "cached",
                        "message": message,
                        "age_minutes": age,
                        "is_stale": is_stale,
                        "collected_at": str(stored.get("collected_at")),
                        "execution_error": execution_error,
                        "timed_out": timed_out,
                    }
                )

        # No cached data available
        if timed_out:
            return ToolResult.fail(
                "DIAGNOSTIC: Live Blitz execution TIMED OUT - server appears overloaded. "
                "No cached findings available. The timeout itself indicates severe performance issues."
            )

        return ToolResult.fail(
            f"Cannot run Blitz diagnostics: {execution_error or 'First Responder Kit not installed'} "
            "and no stored findings available in ClickHouse."
        )

    def _run_on_demand(self, script: str, seconds: int, top: int) -> Dict[str, List[Dict]]:
        """Run Blitz scripts on-demand via direct SQL connection."""
        results = {}
        errors = []
        scripts_succeeded = 0

        scripts_to_run = (
            ["first", "cache", "who", "index", "lock"]
            if script == "all"
            else [script]
        )

        for s in scripts_to_run:
            try:
                if s == "first":
                    data = self._run_blitz_first(seconds)
                    results["blitz_first"] = data  # Include even if empty
                    scripts_succeeded += 1
                elif s == "cache":
                    data = self._run_blitz_cache(top)
                    results["blitz_cache"] = data
                    scripts_succeeded += 1
                elif s == "who":
                    data = self._run_blitz_who()
                    results["blitz_who"] = data
                    scripts_succeeded += 1
                elif s == "index":
                    data = self._run_blitz_index()
                    results["blitz_index"] = data
                    scripts_succeeded += 1
                elif s == "lock":
                    data = self._run_blitz_lock()
                    results["blitz_lock"] = data
                    scripts_succeeded += 1
            except Exception as e:
                errors.append(f"{s}: {str(e)}")
                logger.debug("Blitz script %s failed: %s", s, e)

        if scripts_succeeded == 0:
            raise Exception(f"All scripts failed: {'; '.join(errors)}")

        return results

    def _run_blitz_first(self, seconds: int) -> List[Dict]:
        """Run sp_BlitzFirst for real-time wait stats and findings."""
        collected_at = datetime.now(timezone.utc)
        results = []

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET QUOTED_IDENTIFIER ON")
            cursor.execute("SET NOCOUNT ON")

            logger.debug("Running sp_BlitzFirst with seconds=%d", seconds)
            cursor.execute(f"""
                EXEC master.dbo.sp_BlitzFirst
                    @Seconds = {seconds},
                    @ExpertMode = 1,
                    @OutputType = 'Top10'
            """)

            # Loop through all result sets
            while True:
                try:
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        # Handle both Priority findings and wait_type stats
                        if "Priority" in columns or "wait_type" in columns:
                            rows = cursor.fetchall()
                            for row in rows:
                                record = self._parse_blitz_first_row(row, columns, collected_at)
                                if record:
                                    results.append(record)
                except mssql.ProgrammingError:
                    pass

                if not cursor.nextset():
                    break

        return results

    def _parse_blitz_first_row(self, row, columns: List[str], collected_at: datetime) -> Optional[Dict]:
        """Parse a BlitzFirst result row (Priority findings or wait stats)."""
        try:
            row_dict = dict(zip(columns, row))

            # Handle wait stats output (has wait_type column)
            if "wait_type" in columns:
                wait_type = row_dict.get("wait_type")
                if not wait_type:
                    return None
                return {
                    "collected_at": collected_at.isoformat(),
                    "blitz_type": "BlitzFirst",
                    "priority": 50,  # Medium priority for wait stats
                    "findings_group": "Wait Stats",
                    "finding": f"{wait_type} ({row_dict.get('wait_category', 'Unknown')})",
                    "details": f"Wait Time: {row_dict.get('Wait Time (Hours)', 0):.2f}h, "
                               f"Avg ms/Wait: {row_dict.get('Avg ms Per Wait', 0):.1f}, "
                               f"Waits: {row_dict.get('Number of Waits', 0)}",
                    "wait_type": wait_type,
                    "wait_category": str(row_dict.get("wait_category", ""))[:100],
                    "wait_time_hours": row_dict.get("Wait Time (Hours)"),
                    "avg_ms_per_wait": row_dict.get("Avg ms Per Wait"),
                    "number_of_waits": row_dict.get("Number of Waits"),
                }

            # Handle Priority findings output
            priority = row_dict.get("Priority", row_dict.get("priority", 255))
            if priority is None or priority > 200:
                return None

            return {
                "collected_at": collected_at.isoformat(),
                "blitz_type": "BlitzFirst",
                "priority": int(priority) if priority else 255,
                "findings_group": str(row_dict.get("FindingsGroup", ""))[:200],
                "finding": str(row_dict.get("Finding", ""))[:500],
                "details": str(row_dict.get("Details", ""))[:2000],
                "url": str(row_dict.get("URL", ""))[:500] if row_dict.get("URL") else None,
            }
        except Exception:
            return None

    def _run_blitz_cache(self, top: int) -> List[Dict]:
        """Run sp_BlitzCache for query plan analysis."""
        collected_at = datetime.now(timezone.utc)
        results = []

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET QUOTED_IDENTIFIER ON")
            cursor.execute("SET NOCOUNT ON")

            logger.debug("Running sp_BlitzCache with top=%d", top)
            cursor.execute(f"""
                EXEC master.dbo.sp_BlitzCache
                    @Top = {top},
                    @SortOrder = 'cpu',
                    @ExpertMode = 0
            """)

            # Loop through all result sets
            while True:
                try:
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        # Only process result sets that have query data
                        if "Query Text" in columns or "QueryText" in columns:
                            rows = cursor.fetchall()
                            for row in rows:
                                record = self._parse_blitz_cache_row(row, columns, collected_at)
                                if record:
                                    results.append(record)
                except mssql.ProgrammingError:
                    pass

                if not cursor.nextset():
                    break

        return results

    def _parse_blitz_cache_row(self, row, columns: List[str], collected_at: datetime) -> Optional[Dict]:
        """Parse a BlitzCache result row."""
        try:
            row_dict = dict(zip(columns, row))

            return {
                "collected_at": collected_at.isoformat(),
                "blitz_type": "BlitzCache",
                "priority": 50,
                "query_text": str(row_dict.get("Query Text", ""))[:4000],
                "database_name": str(row_dict.get("Database", ""))[:128],
                "total_cpu": row_dict.get("Total CPU", row_dict.get("TotalCPU")),
                "total_reads": row_dict.get("Total Reads", row_dict.get("TotalReads")),
                "total_writes": row_dict.get("Total Writes", row_dict.get("TotalWrites")),
                "execution_count": row_dict.get("Execution Count", row_dict.get("ExecutionCount")),
                "avg_duration_ms": row_dict.get("Avg Duration (ms)", row_dict.get("AvgDuration")),
                "warnings": str(row_dict.get("Warnings", ""))[:1000] if row_dict.get("Warnings") else None,
                "total_spills": row_dict.get("Total Spills", row_dict.get("TotalSpills")),
            }
        except Exception:
            return None

    def _run_blitz_who(self) -> List[Dict]:
        """Run sp_BlitzWho for active session analysis."""
        collected_at = datetime.now(timezone.utc)
        results = []

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET QUOTED_IDENTIFIER ON")
            cursor.execute("SET NOCOUNT ON")

            logger.debug("Running sp_BlitzWho")
            cursor.execute("EXEC master.dbo.sp_BlitzWho")

            # Loop through all result sets
            while True:
                try:
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        # Only process result sets that have session_id column
                        if "session_id" in columns:
                            rows = cursor.fetchall()
                            for row in rows:
                                record = self._parse_blitz_who_row(row, columns, collected_at)
                                if record:
                                    results.append(record)
                except mssql.ProgrammingError:
                    pass

                if not cursor.nextset():
                    break

        return results

    def _parse_blitz_who_row(self, row, columns: List[str], collected_at: datetime) -> Optional[Dict]:
        """Parse a BlitzWho result row."""
        try:
            row_dict = dict(zip(columns, row))

            return {
                "collected_at": collected_at.isoformat(),
                "blitz_type": "BlitzWho",
                "priority": 50,
                "session_id": row_dict.get("session_id"),
                "status": str(row_dict.get("status", ""))[:50],
                "wait_info": str(row_dict.get("wait_info", ""))[:500] if row_dict.get("wait_info") else None,
                "blocking_session_id": row_dict.get("blocking_session_id"),
                "query_text": str(row_dict.get("query_text", ""))[:4000] if row_dict.get("query_text") else None,
                "database_name": str(row_dict.get("database_name", ""))[:128],
                "cpu_ms": row_dict.get("CPU"),
                "reads": row_dict.get("reads"),
            }
        except Exception:
            return None

    def _run_blitz_index(self) -> List[Dict]:
        """Run sp_BlitzIndex for index analysis."""
        collected_at = datetime.now(timezone.utc)
        results = []

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET QUOTED_IDENTIFIER ON")
            cursor.execute("SET NOCOUNT ON")

            logger.debug("Running sp_BlitzIndex")
            cursor.execute(f"""
                EXEC master.dbo.sp_BlitzIndex
                    @DatabaseName = '{self._database}',
                    @Mode = 0
            """)

            # Loop through all result sets
            while True:
                try:
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        # Only process result sets that have Priority column
                        if "Priority" in columns or "priority" in columns:
                            rows = cursor.fetchall()
                            for row in rows:
                                record = self._parse_blitz_index_row(row, columns, collected_at)
                                if record:
                                    results.append(record)
                except mssql.ProgrammingError:
                    pass

                if not cursor.nextset():
                    break

        return results

    def _parse_blitz_index_row(self, row, columns: List[str], collected_at: datetime) -> Optional[Dict]:
        """Parse a BlitzIndex result row."""
        try:
            row_dict = dict(zip(columns, row))

            priority = row_dict.get("Priority", row_dict.get("priority", 255))
            if priority is None or priority > 200:
                return None

            return {
                "collected_at": collected_at.isoformat(),
                "blitz_type": "BlitzIndex",
                "priority": int(priority) if priority else 255,
                "finding": str(row_dict.get("Finding", row_dict.get("finding", "")))[:500],
                "database_name": str(row_dict.get("Database Name", row_dict.get("database_name", "")))[:128],
                "schema_name": str(row_dict.get("Schema Name", row_dict.get("schema_name", "")))[:128],
                "table_name": str(row_dict.get("Table Name", row_dict.get("table_name", "")))[:128],
                "index_name": str(row_dict.get("Index Name", row_dict.get("index_name", "")))[:128] if row_dict.get("Index Name") or row_dict.get("index_name") else None,
                "details": str(row_dict.get("Details", row_dict.get("details", "")))[:1000],
                "index_definition": str(row_dict.get("Index Definition", ""))[:1000] if row_dict.get("Index Definition") else None,
                "create_tsql": str(row_dict.get("Create TSQL", ""))[:2000] if row_dict.get("Create TSQL") else None,
            }
        except Exception:
            return None

    def _run_blitz_lock(self) -> List[Dict]:
        """Run sp_BlitzLock for deadlock analysis."""
        collected_at = datetime.now(timezone.utc)
        results = []

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET QUOTED_IDENTIFIER ON")
            cursor.execute("SET NOCOUNT ON")

            logger.debug("Running sp_BlitzLock")
            cursor.execute("EXEC master.dbo.sp_BlitzLock")

            # Loop through all result sets
            while True:
                try:
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        # BlitzLock uses "event_date" as a key column for deadlock rows
                        if "event_date" in columns or "deadlock_type" in columns:
                            rows = cursor.fetchall()
                            for row in rows:
                                record = self._parse_blitz_lock_row(row, columns, collected_at)
                                if record:
                                    results.append(record)
                except mssql.ProgrammingError:
                    pass

                if not cursor.nextset():
                    break

        return results

    def _parse_blitz_lock_row(self, row, columns: List[str], collected_at: datetime) -> Optional[Dict]:
        """Parse a BlitzLock result row."""
        try:
            row_dict = dict(zip(columns, row))

            return {
                "collected_at": collected_at.isoformat(),
                "blitz_type": "BlitzLock",
                "priority": 10,
                "deadlock_type": str(row_dict.get("deadlock_type", row_dict.get("DeadlockType", "")))[:100],
                "database_name": str(row_dict.get("database_name", row_dict.get("DatabaseName", "")))[:128],
                "victim_query": str(row_dict.get("victim_query", row_dict.get("VictimQuery", "")))[:4000] if row_dict.get("victim_query") or row_dict.get("VictimQuery") else None,
                "blocking_query": str(row_dict.get("blocking_query", row_dict.get("BlockingQuery", "")))[:4000] if row_dict.get("blocking_query") or row_dict.get("BlockingQuery") else None,
                "deadlock_graph": str(row_dict.get("deadlock_graph", row_dict.get("DeadlockGraph", "")))[:10000] if row_dict.get("deadlock_graph") or row_dict.get("DeadlockGraph") else None,
            }
        except Exception:
            return None

    def _get_latest_stored_findings(self, script: str) -> Optional[Dict]:
        """Query ClickHouse for latest stored Blitz results with staleness info."""
        if not self._data_source:
            return None

        blitz_type_map = {
            "first": "BlitzFirst",
            "cache": "BlitzCache",
            "who": "BlitzWho",
            "index": "BlitzIndex",
            "lock": "BlitzLock",
        }

        types_to_query = (
            list(blitz_type_map.values()) if script == "all"
            else [blitz_type_map.get(script)]
        )

        results = {}
        oldest_timestamp = None

        try:
            for blitz_type in types_to_query:
                if not blitz_type:
                    continue

                query = f"""
                    SELECT *
                    FROM blitz_results
                    WHERE blitz_type = '{blitz_type}'
                    ORDER BY collected_at DESC
                    LIMIT 100
                """
                rows = self._data_source.execute_query(query)
                if rows:
                    key = f"blitz_{blitz_type.lower().replace('blitz', '')}"
                    results[key] = rows
                    # Track oldest data timestamp
                    if rows[0].get("collected_at"):
                        ts = rows[0]["collected_at"]
                        if isinstance(ts, str):
                            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        if oldest_timestamp is None or ts < oldest_timestamp:
                            oldest_timestamp = ts
        except Exception as e:
            logger.debug("Failed to get cached Blitz findings: %s", e)
            return None

        if not results:
            return None

        # Calculate age and add metadata
        age_minutes = None
        if oldest_timestamp:
            now = datetime.now(timezone.utc)
            if oldest_timestamp.tzinfo is None:
                oldest_timestamp = oldest_timestamp.replace(tzinfo=timezone.utc)
            age_minutes = int((now - oldest_timestamp).total_seconds() / 60)

        return {
            "findings": results,
            "collected_at": oldest_timestamp,
            "age_minutes": age_minutes,
            "is_stale": age_minutes is not None and age_minutes > BLITZ_STALE_THRESHOLD_MINUTES,
        }

    def _store_results_in_clickhouse(self, results: Dict[str, List[Dict]], adhoc_id: str) -> None:
        """Store on-demand Blitz results in ClickHouse for future fallback."""
        if not self._data_source:
            return

        collected_at = datetime.now(timezone.utc)
        records = []

        for blitz_key, findings in results.items():
            # Map key back to blitz_type (e.g., "blitz_first" -> "BlitzFirst")
            blitz_type = "Blitz" + blitz_key.replace("blitz_", "").title()

            for finding in findings:
                record = {
                    "collected_at": collected_at,
                    "incident_id": adhoc_id,
                    "blitz_type": blitz_type,
                }
                # Copy finding fields, excluding collected_at (we set our own)
                for k, v in finding.items():
                    if k != "collected_at" and k != "blitz_type":
                        record[k] = v
                records.append(record)

        if records:
            try:
                self._data_source.insert_blitz_results(records)
                logger.debug("Stored %d Blitz findings in ClickHouse (adhoc_id=%s)", len(records), adhoc_id)
            except Exception as e:
                logger.debug("Failed to store Blitz findings: %s", e)


def create_blitz_diagnostic_tool(
    sqlserver_host: str,
    sqlserver_port: int = 1433,
    sqlserver_user: str = "sa",
    sqlserver_password: str = "",
    sqlserver_database: str = "master",
    data_source: Optional["ClickHouseDataSource"] = None,
) -> RCATool:
    """
    Create the unified Blitz diagnostics tool.

    Args:
        sqlserver_host: SQL Server hostname
        sqlserver_port: SQL Server port
        sqlserver_user: SQL Server username
        sqlserver_password: SQL Server password
        sqlserver_database: Target database name
        data_source: Optional ClickHouse data source for caching/fallback

    Returns:
        RunBlitzDiagnosticsTool instance
    """
    return RunBlitzDiagnosticsTool(
        sqlserver_host=sqlserver_host,
        sqlserver_port=sqlserver_port,
        sqlserver_user=sqlserver_user,
        sqlserver_password=sqlserver_password,
        sqlserver_database=sqlserver_database,
        data_source=data_source,
    )
