"""
Blitz Script Executor for incident analysis.

Executes First Responder Kit scripts during incidents and
stores results in ClickHouse. All 5 Blitz scripts run during
every incident for comprehensive diagnostics.

Scripts executed:
- sp_BlitzFirst: Real-time wait stats (5s delta), priority findings
- sp_BlitzCache: Top queries by CPU, spills, warnings
- sp_BlitzWho: Active sessions with waits, blocking
- sp_BlitzIndex: Missing/unused indexes
- sp_BlitzLock: Deadlock analysis from system_health
"""

import json
import structlog
import mssql_python as mssql
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable

from .config import CollectorConfig
from .clickhouse_writer import ClickHouseWriter

logger = structlog.get_logger()


class BlitzExecutor:
    """Executes Blitz scripts and stores results in ClickHouse."""

    def __init__(
        self,
        config: CollectorConfig,
        writer: ClickHouseWriter,
    ):
        self.config = config
        self.writer = writer

    def _get_connection(self) -> mssql.Connection:
        """Get a database connection."""
        conn_str = (
            f"SERVER={self.config.sqlserver_host},{self.config.sqlserver_port};"
            f"DATABASE={self.config.sqlserver_database};"
            f"UID={self.config.sqlserver_user};"
            f"PWD={self.config.sqlserver_password};"
            f"TrustServerCertificate=yes;"
        )
        return mssql.connect(connection_str=conn_str, timeout=60)

    def run_blitz_suite(
        self,
        incident_id: str,
        seconds: int = 5,
    ) -> Dict[str, bool]:
        """
        Run all Blitz scripts for an incident.

        Args:
            incident_id: The incident ID to tag results with
            seconds: Seconds for BlitzFirst delta (default 5)

        Returns:
            Dictionary of script names to success status
        """
        results = {}
        logger.info("Running Blitz suite", incident_id=incident_id)

        results["blitz_first"] = self._run_blitz_first(incident_id, seconds)
        results["blitz_cache"] = self._run_blitz_cache(incident_id)
        results["blitz_who"] = self._run_blitz_who(incident_id)
        results["blitz_index"] = self._run_blitz_index(incident_id)
        results["blitz_lock"] = self._run_blitz_lock(incident_id)

        success_count = sum(1 for v in results.values() if v)
        logger.info(
            "Blitz suite completed",
            incident_id=incident_id,
            success_count=success_count,
            total_count=len(results),
        )

        return results

    def _run_blitz_first(self, incident_id: str, seconds: int) -> bool:
        """Run sp_BlitzFirst for real-time wait stats and findings."""
        collected_at = datetime.now()

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SET QUOTED_IDENTIFIER ON")
                cursor.execute("SET NOCOUNT ON")

                # Run BlitzFirst - captures wait stats delta
                logger.info("Running sp_BlitzFirst", seconds=seconds)
                cursor.execute(f"""
                    EXEC dbo.sp_BlitzFirst
                        @Seconds = {seconds},
                        @ExpertMode = 1,
                        @OutputType = 'Top10'
                """)

                results = []
                result_set = 0

                while True:
                    try:
                        rows = cursor.fetchall()
                        if rows:
                            columns = [desc[0] for desc in cursor.description]
                            for row in rows:
                                record = self._parse_blitz_first_row(
                                    row, columns, incident_id, collected_at
                                )
                                if record:
                                    results.append(record)
                        result_set += 1
                    except mssql.ProgrammingError:
                        pass

                    if not cursor.nextset():
                        break

                if results:
                    self.writer.write_blitz_results(results, incident_id)
                    logger.info("BlitzFirst completed", findings=len(results))

            return True

        except Exception as e:
            logger.error("BlitzFirst error", error=str(e))
            return False

    def _parse_blitz_first_row(
        self,
        row,
        columns: List[str],
        incident_id: str,
        collected_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Parse a BlitzFirst result row."""
        try:
            row_dict = dict(zip(columns, row))

            # Skip informational rows
            priority = row_dict.get("Priority", row_dict.get("priority", 255))
            if priority is None or priority > 200:
                return None

            return {
                "collected_at": collected_at,
                "incident_id": incident_id,
                "blitz_type": "BlitzFirst",
                "priority": int(priority) if priority else 255,
                "findings_group": str(row_dict.get("FindingsGroup", ""))[:200],
                "finding": str(row_dict.get("Finding", ""))[:500],
                "details": str(row_dict.get("Details", ""))[:2000],
                "url": str(row_dict.get("URL", ""))[:500] if row_dict.get("URL") else None,
            }
        except Exception as e:
            logger.debug("Failed to parse BlitzFirst row", error=str(e))
            return None

    def _run_blitz_cache(self, incident_id: str, top: int = 20) -> bool:
        """Run sp_BlitzCache for query plan analysis."""
        collected_at = datetime.now()

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SET QUOTED_IDENTIFIER ON")
                cursor.execute("SET NOCOUNT ON")

                logger.info("Running sp_BlitzCache", top=top)
                cursor.execute(f"""
                    EXEC dbo.sp_BlitzCache
                        @Top = {top},
                        @SortOrder = 'cpu',
                        @ExpertMode = 0
                """)

                results = []
                try:
                    rows = cursor.fetchall()
                    if rows and cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        for row in rows:
                            record = self._parse_blitz_cache_row(
                                row, columns, incident_id, collected_at
                            )
                            if record:
                                results.append(record)
                except mssql.ProgrammingError:
                    pass

                if results:
                    self.writer.write_blitz_results(results, incident_id)
                    logger.info("BlitzCache completed", queries=len(results))

            return True

        except Exception as e:
            logger.error("BlitzCache error", error=str(e))
            return False

    def _parse_blitz_cache_row(
        self,
        row,
        columns: List[str],
        incident_id: str,
        collected_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Parse a BlitzCache result row."""
        try:
            row_dict = dict(zip(columns, row))

            return {
                "collected_at": collected_at,
                "incident_id": incident_id,
                "blitz_type": "BlitzCache",
                "priority": 50,  # BlitzCache doesn't have priority
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
        except Exception as e:
            logger.debug("Failed to parse BlitzCache row", error=str(e))
            return None

    def _run_blitz_who(self, incident_id: str) -> bool:
        """Run sp_BlitzWho for active session analysis."""
        collected_at = datetime.now()

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SET QUOTED_IDENTIFIER ON")
                cursor.execute("SET NOCOUNT ON")

                logger.info("Running sp_BlitzWho")
                cursor.execute("EXEC dbo.sp_BlitzWho")

                results = []
                try:
                    rows = cursor.fetchall()
                    if rows and cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        for row in rows:
                            record = self._parse_blitz_who_row(
                                row, columns, incident_id, collected_at
                            )
                            if record:
                                results.append(record)
                except mssql.ProgrammingError:
                    pass

                if results:
                    self.writer.write_blitz_results(results, incident_id)
                    logger.info("BlitzWho completed", sessions=len(results))

            return True

        except Exception as e:
            logger.error("BlitzWho error", error=str(e))
            return False

    def _parse_blitz_who_row(
        self,
        row,
        columns: List[str],
        incident_id: str,
        collected_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Parse a BlitzWho result row."""
        try:
            row_dict = dict(zip(columns, row))

            return {
                "collected_at": collected_at,
                "incident_id": incident_id,
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
        except Exception as e:
            logger.debug("Failed to parse BlitzWho row", error=str(e))
            return None

    def _run_blitz_index(self, incident_id: str) -> bool:
        """Run sp_BlitzIndex for index analysis."""
        collected_at = datetime.now()

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SET QUOTED_IDENTIFIER ON")
                cursor.execute("SET NOCOUNT ON")

                logger.info("Running sp_BlitzIndex")
                cursor.execute(f"""
                    EXEC dbo.sp_BlitzIndex
                        @DatabaseName = '{self.config.sqlserver_database}',
                        @Mode = 0
                """)

                results = []
                try:
                    rows = cursor.fetchall()
                    if rows and cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        for row in rows:
                            record = self._parse_blitz_index_row(
                                row, columns, incident_id, collected_at
                            )
                            if record:
                                results.append(record)
                except mssql.ProgrammingError:
                    pass

                if results:
                    self.writer.write_blitz_results(results, incident_id)
                    logger.info("BlitzIndex completed", findings=len(results))

            return True

        except Exception as e:
            logger.error("BlitzIndex error", error=str(e))
            return False

    def _parse_blitz_index_row(
        self,
        row,
        columns: List[str],
        incident_id: str,
        collected_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Parse a BlitzIndex result row."""
        try:
            row_dict = dict(zip(columns, row))

            priority = row_dict.get("Priority", row_dict.get("priority", 255))
            if priority is None or priority > 200:
                return None

            return {
                "collected_at": collected_at,
                "incident_id": incident_id,
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
        except Exception as e:
            logger.debug("Failed to parse BlitzIndex row", error=str(e))
            return None

    def _run_blitz_lock(self, incident_id: str) -> bool:
        """Run sp_BlitzLock for deadlock analysis."""
        collected_at = datetime.now()

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SET QUOTED_IDENTIFIER ON")
                cursor.execute("SET NOCOUNT ON")

                logger.info("Running sp_BlitzLock")
                cursor.execute("""
                    EXEC dbo.sp_BlitzLock
                        @Top = 10,
                        @StartDate = DATEADD(HOUR, -1, GETDATE()),
                        @EndDate = GETDATE()
                """)

                results = []
                try:
                    rows = cursor.fetchall()
                    if rows and cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        for row in rows:
                            record = self._parse_blitz_lock_row(
                                row, columns, incident_id, collected_at
                            )
                            if record:
                                results.append(record)
                except mssql.ProgrammingError:
                    pass

                if results:
                    self.writer.write_blitz_results(results, incident_id)
                    logger.info("BlitzLock completed", deadlocks=len(results))

            return True

        except Exception as e:
            logger.error("BlitzLock error", error=str(e))
            return False

    def _parse_blitz_lock_row(
        self,
        row,
        columns: List[str],
        incident_id: str,
        collected_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Parse a BlitzLock result row."""
        try:
            row_dict = dict(zip(columns, row))

            return {
                "collected_at": collected_at,
                "incident_id": incident_id,
                "blitz_type": "BlitzLock",
                "priority": 10,  # Deadlocks are high priority
                "deadlock_type": str(row_dict.get("deadlock_type", row_dict.get("DeadlockType", "")))[:100],
                "database_name": str(row_dict.get("database_name", row_dict.get("DatabaseName", "")))[:128],
                "victim_query": str(row_dict.get("victim_query", row_dict.get("VictimQuery", "")))[:4000] if row_dict.get("victim_query") or row_dict.get("VictimQuery") else None,
                "blocking_query": str(row_dict.get("blocking_query", row_dict.get("BlockingQuery", "")))[:4000] if row_dict.get("blocking_query") or row_dict.get("BlockingQuery") else None,
                "deadlock_graph": str(row_dict.get("deadlock_graph", row_dict.get("DeadlockGraph", "")))[:10000] if row_dict.get("deadlock_graph") or row_dict.get("DeadlockGraph") else None,
            }
        except Exception as e:
            logger.debug("Failed to parse BlitzLock row", error=str(e))
            return None
