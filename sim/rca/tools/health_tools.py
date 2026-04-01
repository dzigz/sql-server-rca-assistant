"""
Health check tools for sp_Blitz execution.

Provides tools for running sp_Blitz diagnostics and querying
SQL Server configuration for health assessments.

Supports both direct SQL connection (preferred) and Docker exec (legacy).
"""

import os
import re
import subprocess
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

import mssql_python as mssql

from sim.rca.tools.base import RCATool, ToolResult
from sim.logging_config import get_logger
from sim.config import DEFAULT_SA_PASSWORD

logger = get_logger(__name__)

# Path to sp_Blitz.sql script (for Docker-based installation)
SP_BLITZ_SQL_PATH = Path(__file__).parent.parent.parent / "sql" / "blitz" / "sp_Blitz.sql"
FRK_SCRIPT_DIR = Path(__file__).parent.parent.parent / "sql" / "blitz"
FRK_SCRIPT_FILES = [
    "sp_BlitzFirst.sql",
    "sp_BlitzCache.sql",
    "sp_BlitzWho.sql",
    "sp_BlitzIndex.sql",
    "sp_BlitzLock.sql",
    "sp_Blitz.sql",
]
FRK_REQUIRED_PROCEDURES = [
    "sp_Blitz",
    "sp_BlitzFirst",
    "sp_BlitzCache",
    "sp_BlitzWho",
    "sp_BlitzIndex",
    "sp_BlitzLock",
]
GO_BATCH_SEPARATOR = re.compile(r"^\s*GO(?:\s+(\d+))?\s*$", flags=re.IGNORECASE)


def _resolve_sqlserver_password(value: Optional[str]) -> str:
    """Resolve SQL Server password from args or environment defaults."""
    if value:
        return value
    resolved = (
        os.environ.get("SA_PASSWORD")
        or os.environ.get("SIM_SA_PASSWORD")
        or DEFAULT_SA_PASSWORD
    )
    if resolved == DEFAULT_SA_PASSWORD:
        raise ValueError(
            "SQL Server password is required. Set SA_PASSWORD or SIM_SA_PASSWORD "
            "(or set SIM_ALLOW_INSECURE_DEFAULTS=1 for local demo defaults)."
        )
    return resolved


class RunSpBlitzTool(RCATool):
    """
    Run sp_Blitz server health check.

    Executes sp_Blitz against SQL Server and returns prioritized findings
    about server configuration, security, and performance issues.

    Supports two modes:
    1. Direct SQL connection via mssql-python (preferred for cloud/remote)
    2. Docker exec (legacy, for local Docker containers)
    """

    def __init__(
        self,
        # Direct SQL connection parameters (preferred)
        sqlserver_host: Optional[str] = None,
        sqlserver_port: int = 1433,
        sqlserver_user: str = "sa",
        sqlserver_password: Optional[str] = None,
        sqlserver_database: str = "master",
        # Legacy Docker parameters
        sqlserver_container: Optional[str] = None,
        connection_timeout: int = 30,
        query_timeout: int = 120,
        offer_install_prompt: bool = False,
    ):
        """
        Initialize the sp_Blitz tool.

        Args:
            sqlserver_host: SQL Server hostname (for direct connection)
            sqlserver_port: SQL Server port (default 1433)
            sqlserver_user: SQL Server username
            sqlserver_password: SQL Server password
            sqlserver_database: Target database name
            sqlserver_container: Docker container name (legacy mode)
            connection_timeout: Connection timeout in seconds
            query_timeout: Query timeout in seconds
        """
        self._host = sqlserver_host
        self._port = sqlserver_port
        self._user = sqlserver_user
        self._password = _resolve_sqlserver_password(sqlserver_password)
        self._database = sqlserver_database
        self._container = sqlserver_container
        self._connection_timeout = connection_timeout
        self._query_timeout = query_timeout
        self._offer_install_prompt = offer_install_prompt

        # Determine mode: direct SQL or Docker
        self._use_direct_sql = bool(sqlserver_host)

    @property
    def name(self) -> str:
        return "run_sp_blitz"

    @property
    def description(self) -> str:
        return """Run sp_Blitz server health check to identify configuration issues,
security concerns, and performance problems. Returns prioritized findings:
- Priority 1-10: Critical (security, corruption)
- Priority 11-50: High (performance, config issues)
- Priority 51-100: Medium (best practices)

Use this for HEALTH ASSESSMENTS (proactive checks when no incident is detected).
For ACTIVE INCIDENT investigation, use run_blitz_diagnostics() instead."""

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "priority_threshold": {
                    "type": "integer",
                    "description": "Only return findings with priority <= this value (default: 100)",
                    "default": 100
                },
                "check_server_info": {
                    "type": "boolean",
                    "description": "Include server information in output (default: true)",
                    "default": True
                }
            },
            "required": []
        }

    def _get_connection(self, database: Optional[str] = None) -> mssql.Connection:
        """Get a direct database connection."""
        conn_str = (
            f"SERVER={self._host},{self._port};"
            f"DATABASE={database or self._database};"
            f"UID={self._user};"
            f"PWD={self._password};"
            f"TrustServerCertificate=yes;"
        )
        return mssql.connect(connection_str=conn_str, timeout=self._connection_timeout)

    def _resolve_sp_blitz_proc_direct(self) -> Optional[str]:
        """Resolve callable sp_Blitz proc name via direct SQL connection."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT "
                    "OBJECT_ID('master.dbo.sp_Blitz', 'P') AS master_proc, "
                    "OBJECT_ID('dbo.sp_Blitz', 'P') AS current_proc"
                )
                result = cursor.fetchone()
                if not result:
                    return None
                if result[0] is not None:
                    return "master.dbo.sp_Blitz"
                if result[1] is not None:
                    return "dbo.sp_Blitz"
                return None
        except Exception as e:
            logger.debug("Error checking sp_Blitz via direct SQL: %s", e)
            return None

    def _check_sp_blitz_installed_direct(self) -> bool:
        """Check if sp_Blitz is installed via direct SQL connection."""
        return self._resolve_sp_blitz_proc_direct() is not None

    def _split_sql_batches(self, sql_text: str) -> List[str]:
        """Split T-SQL script into batches separated by GO lines."""
        batches: List[str] = []
        current_lines: List[str] = []

        for line in sql_text.splitlines():
            match = GO_BATCH_SEPARATOR.match(line)
            if match:
                batch = "\n".join(current_lines).strip()
                if batch:
                    repeat = int(match.group(1) or "1")
                    for _ in range(max(repeat, 1)):
                        batches.append(batch)
                current_lines = []
                continue
            current_lines.append(line)

        tail = "\n".join(current_lines).strip()
        if tail:
            batches.append(tail)

        return batches

    def _execute_sql_script_direct(self, cursor: Any, script_path: Path) -> int:
        """Execute a SQL script file via direct connection and return batch count."""
        script_text = script_path.read_text(encoding="utf-8-sig")
        batch_count = 0

        for batch in self._split_sql_batches(script_text):
            cursor.execute(batch)
            batch_count += 1
            try:
                while cursor.nextset():
                    pass
            except Exception:
                # Some statements do not produce additional result sets.
                pass

        return batch_count

    def _get_installed_frk_procedures_direct(self, database: str = "master") -> List[str]:
        """Return installed FRK procedures in the target database."""
        quoted = ", ".join(f"'{name}'" for name in FRK_REQUIRED_PROCEDURES)
        query = (
            "SELECT name "
            "FROM sys.procedures "
            f"WHERE name IN ({quoted}) "
            "ORDER BY name"
        )
        with self._get_connection(database=database) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            return [str(row[0]) for row in rows if row and row[0]]

    def install_first_responder_kit_direct(self, target_database: str = "master") -> Dict[str, Any]:
        """Install First Responder Kit scripts via direct SQL connection."""
        if not self._use_direct_sql:
            raise ValueError("Direct SQL mode is required for FRK installation")

        missing_files = [
            str((FRK_SCRIPT_DIR / script).resolve())
            for script in FRK_SCRIPT_FILES
            if not (FRK_SCRIPT_DIR / script).exists()
        ]
        if missing_files:
            raise FileNotFoundError(
                "Missing FRK SQL script files: " + ", ".join(missing_files)
            )

        total_batches = 0
        with self._get_connection(database=target_database) as conn:
            cursor = conn.cursor()
            for script_name in FRK_SCRIPT_FILES:
                script_path = FRK_SCRIPT_DIR / script_name
                logger.info("Installing FRK script via direct SQL: %s", script_name)
                total_batches += self._execute_sql_script_direct(cursor, script_path)
            try:
                conn.commit()
            except Exception:
                pass

        installed = self._get_installed_frk_procedures_direct(database=target_database)
        missing = [name for name in FRK_REQUIRED_PROCEDURES if name not in installed]
        if missing:
            raise RuntimeError(
                "FRK installation completed with missing procedures: "
                + ", ".join(missing)
            )

        return {
            "status": "installed",
            "target_host": self._host,
            "target_database": target_database,
            "scripts_executed": FRK_SCRIPT_FILES,
            "executed_batches": total_batches,
            "procedures_installed": installed,
        }

    def set_install_offer_enabled(self, enabled: bool) -> None:
        """Enable/disable interactive FRK install offer in tool responses."""
        self._offer_install_prompt = bool(enabled)

    def _build_direct_install_offer(self) -> Dict[str, Any]:
        """Build structured install offer for missing sp_Blitz in direct mode."""
        return {
            "status": "install_required",
            "message": (
                "First Responder Kit (sp_Blitz) is not installed on the target SQL Server. "
                "You can install the scripts now from local bundled SQL files."
            ),
            "install_offer": {
                "type": "blitz_install_offer",
                "title": "Install First Responder Kit scripts?",
                "description": (
                    "Install sp_Blitz, sp_BlitzFirst, sp_BlitzCache, sp_BlitzWho, "
                    "sp_BlitzIndex, and sp_BlitzLock into the target SQL Server."
                ),
                "target_host": self._host,
                "target_database": "master",
                "procedures": FRK_REQUIRED_PROCEDURES,
            },
        }

    def _run_sp_blitz_direct(
        self,
        priority_threshold: int,
        check_server_info: bool,
        proc_name: str = "master.dbo.sp_Blitz",
    ) -> List[Dict[str, Any]]:
        """Run sp_Blitz via direct SQL connection."""
        findings = []
        check_server = 1 if check_server_info else 0
        safe_priority_threshold = max(1, min(int(priority_threshold), 255))

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET QUOTED_IDENTIFIER ON")
            cursor.execute("SET ANSI_NULLS ON")
            cursor.execute("SET NOCOUNT ON")

            logger.debug("Running sp_Blitz via direct SQL")
            try:
                cursor.execute(
                    f"EXEC {proc_name} "
                    "@CheckServerInfo = ?, "
                    "@CheckUserDatabaseObjects = 0, "
                    "@IgnorePrioritiesAbove = ?",
                    (check_server, safe_priority_threshold),
                )
            except TypeError:
                # Fallback for drivers that don't expose positional parameter binding.
                cursor.execute(
                    f"EXEC {proc_name} "
                    f"@CheckServerInfo = {check_server}, "
                    "@CheckUserDatabaseObjects = 0, "
                    f"@IgnorePrioritiesAbove = {safe_priority_threshold}"
                )

            # Process result sets
            while True:
                try:
                    rows = cursor.fetchall()
                    if rows and cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        # Check if this result set has Priority column
                        if "Priority" in columns or "priority" in [c.lower() for c in columns]:
                            for row in rows:
                                record = self._parse_sp_blitz_row(row, columns, priority_threshold)
                                if record:
                                    findings.append(record)
                except mssql.ProgrammingError:
                    pass

                if not cursor.nextset():
                    break

        return findings

    def _parse_sp_blitz_row(
        self,
        row,
        columns: List[str],
        priority_threshold: int,
    ) -> Optional[Dict[str, Any]]:
        """Parse a sp_Blitz result row."""
        try:
            row_dict = dict(zip(columns, row))

            priority = row_dict.get("Priority", row_dict.get("priority", 255))
            if priority is None or priority > priority_threshold:
                return None

            return {
                "priority": int(priority) if priority else 255,
                "findings_group": str(row_dict.get("FindingsGroup", row_dict.get("findings_group", "")))[:200],
                "finding": str(row_dict.get("Finding", row_dict.get("finding", "")))[:500],
                "url": str(row_dict.get("URL", row_dict.get("url", "")))[:500] if row_dict.get("URL") or row_dict.get("url") else "",
                "details": str(row_dict.get("Details", row_dict.get("details", "")))[:2000],
            }
        except Exception as e:
            logger.debug("Failed to parse sp_Blitz row: %s", e)
            return None

    # ===== Legacy Docker-based methods =====

    def _run_sqlcmd(self, query: str, use_file: bool = False, timeout: int = 30) -> subprocess.CompletedProcess:
        """
        Run sqlcmd in the container using bash to properly handle environment variables.
        (Legacy Docker mode)
        """
        if use_file:
            bash_cmd = f'/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d "{self._database}" -C -I -i "{query}"'
        else:
            escaped_query = query.replace("'", "'\"'\"'")
            bash_cmd = f"/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P \"$SA_PASSWORD\" -d \"{self._database}\" -C -I -h -1 -Q '{escaped_query}'"

        cmd = [
            "docker", "exec", self._container,
            "bash", "-c", bash_cmd
        ]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    def _check_sp_blitz_installed_docker(self) -> bool:
        """Check if sp_Blitz is installed via Docker exec. (Legacy)"""
        try:
            result = self._run_sqlcmd("SELECT OBJECT_ID('dbo.sp_Blitz', 'P')", timeout=10)
            output = result.stdout.strip()
            return output not in ("NULL", "", "None") and result.returncode == 0
        except Exception as e:
            logger.debug("Error checking sp_Blitz via Docker: %s", e)
            return False

    def _install_sp_blitz_docker(self) -> bool:
        """Install sp_Blitz from the SQL file via Docker. (Legacy)"""
        if not SP_BLITZ_SQL_PATH.exists():
            logger.error("sp_Blitz.sql not found at %s", SP_BLITZ_SQL_PATH)
            return False

        logger.info("Installing sp_Blitz from %s...", SP_BLITZ_SQL_PATH)

        copy_cmd = [
            "docker", "cp",
            str(SP_BLITZ_SQL_PATH),
            f"{self._container}:/tmp/sp_Blitz.sql"
        ]

        try:
            result = subprocess.run(copy_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                logger.error("Failed to copy sp_Blitz.sql to container: %s", result.stderr)
                return False

            result = self._run_sqlcmd("/tmp/sp_Blitz.sql", use_file=True, timeout=120)
            if result.returncode != 0:
                logger.error("Failed to install sp_Blitz: %s", result.stderr)
                return False

            logger.info("sp_Blitz installed successfully")
            return True

        except subprocess.TimeoutExpired:
            logger.error("sp_Blitz installation timed out")
            return False
        except Exception as e:
            logger.error("Error installing sp_Blitz: %s", e)
            return False

    def _run_sp_blitz_docker(
        self,
        priority_threshold: int,
        check_server_info: bool,
    ) -> List[Dict[str, Any]]:
        """Run sp_Blitz via Docker exec. (Legacy)"""
        check_server = 1 if check_server_info else 0
        sql_query = f"SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON; SET NOCOUNT ON; EXEC sp_Blitz @CheckServerInfo = {check_server}, @CheckUserDatabaseObjects = 0, @IgnorePrioritiesAbove = {priority_threshold};"

        bash_cmd = f'/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d "{self._database}" -C -I -s "|" -W -Q "{sql_query}"'
        cmd = [
            "docker", "exec", self._container,
            "bash", "-c", bash_cmd
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=self._query_timeout
        )

        if result.returncode != 0:
            raise Exception(f"sp_Blitz execution failed: {result.stderr or result.stdout}")

        return self._parse_sqlcmd_output(result.stdout, priority_threshold)

    def _parse_sqlcmd_output(self, output: str, priority_threshold: int = 100) -> List[Dict[str, Any]]:
        """Parse sqlcmd pipe-delimited output into list of dicts. (Legacy)"""
        findings = []
        lines = output.strip().split('\n')

        header_idx = None
        header_cols = []
        for i, line in enumerate(lines):
            lower_line = line.lower()
            if 'priority' in lower_line and ('finding' in lower_line or 'findingsgroup' in lower_line):
                header_idx = i
                header_cols = [c.strip().lower() for c in line.split('|')]
                break

        if header_idx is None:
            logger.debug("No header found in sp_Blitz output. First 500 chars: %s", output[:500])
            return findings

        priority_idx = next((i for i, c in enumerate(header_cols) if 'priority' in c), 0)
        group_idx = next((i for i, c in enumerate(header_cols) if 'findingsgroup' in c or 'findings_group' in c), 1)
        finding_idx = next((i for i, c in enumerate(header_cols) if c == 'finding'), 2)
        url_idx = next((i for i, c in enumerate(header_cols) if 'url' in c), 3)
        details_idx = next((i for i, c in enumerate(header_cols) if 'details' in c), 4)

        data_start = header_idx + 1
        while data_start < len(lines) and (lines[data_start].startswith('-') or set(lines[data_start].strip()) <= {'-', '|', ' '}):
            data_start += 1

        for line in lines[data_start:]:
            if not line.strip():
                continue
            if 'rows affected' in line.lower():
                continue
            if set(line.strip()) <= {'-', '|', ' '}:
                continue

            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 3:
                try:
                    priority_str = parts[priority_idx] if priority_idx < len(parts) else ""
                    priority = int(priority_str) if priority_str.isdigit() else 255

                    if priority > priority_threshold:
                        continue

                    findings.append({
                        "priority": priority,
                        "findings_group": parts[group_idx] if group_idx < len(parts) else "",
                        "finding": parts[finding_idx] if finding_idx < len(parts) else "",
                        "url": parts[url_idx] if url_idx < len(parts) else "",
                        "details": parts[details_idx] if details_idx < len(parts) else ""
                    })
                except (ValueError, IndexError) as e:
                    logger.debug("Failed to parse line: %s, error: %s", line[:100], e)
                    continue

        return findings

    # ===== Main execute method =====

    def execute(
        self,
        priority_threshold: int = 100,
        check_server_info: bool = True,
        **kwargs
    ) -> ToolResult:
        """
        Execute sp_Blitz and return health findings.

        Args:
            priority_threshold: Only return findings with priority <= this value
            check_server_info: Include server information

        Returns:
            ToolResult with list of findings
        """
        start_time = time.time()
        logger.info("Starting sp_Blitz execution with priority_threshold=%d", priority_threshold)

        try:
            if self._use_direct_sql:
                # Direct SQL connection mode
                proc_name = self._resolve_sp_blitz_proc_direct()
                if not proc_name:
                    if self._offer_install_prompt:
                        return ToolResult.ok(self._build_direct_install_offer())
                    return ToolResult.fail(
                        "sp_Blitz is not installed on the target server. "
                        "Please ask your DBA to install First Responder Kit."
                    )

                findings = self._run_sp_blitz_direct(
                    priority_threshold=priority_threshold,
                    check_server_info=check_server_info,
                    proc_name=proc_name,
                )
            else:
                # Legacy Docker mode
                if not self._container:
                    return ToolResult.fail(
                        "No SQL Server connection configured. Provide either sqlserver_host "
                        "(for direct connection) or sqlserver_container (for Docker)."
                    )

                if not self._check_sp_blitz_installed_docker():
                    logger.info("sp_Blitz not installed, attempting installation...")
                    if not self._install_sp_blitz_docker():
                        return ToolResult.fail(
                            "sp_Blitz is not installed and auto-installation failed. "
                            f"Please install manually from {SP_BLITZ_SQL_PATH}"
                        )

                findings = self._run_sp_blitz_docker(priority_threshold, check_server_info)

            if not findings:
                return ToolResult.ok({
                    "findings": [],
                    "message": "sp_Blitz found no issues (healthy server!)"
                })

            # Summarize by priority
            critical = [f for f in findings if f.get("priority", 255) <= 10]
            high = [f for f in findings if 10 < f.get("priority", 255) <= 50]
            medium = [f for f in findings if 50 < f.get("priority", 255) <= 100]

            total_time = time.time() - start_time
            logger.info("sp_Blitz completed successfully in %.2fs with %d findings", total_time, len(findings))

            return ToolResult.ok({
                "findings": findings,
                "summary": {
                    "total": len(findings),
                    "critical": len(critical),
                    "high": len(high),
                    "medium": len(medium)
                },
                "execution_time_seconds": round(total_time, 2)
            })

        except subprocess.TimeoutExpired:
            return ToolResult.fail(f"sp_Blitz execution timed out after {self._query_timeout} seconds")
        except Exception as e:
            logger.exception("Error running sp_Blitz")
            return ToolResult.fail(f"Error running sp_Blitz: {str(e)}")


class GetServerConfigTool(RCATool):
    """
    Get SQL Server configuration settings.

    Queries sys.configurations to return current server settings
    for memory, parallelism, and other important configurations.
    """

    def __init__(
        self,
        # Direct SQL connection parameters (preferred)
        sqlserver_host: Optional[str] = None,
        sqlserver_port: int = 1433,
        sqlserver_user: str = "sa",
        sqlserver_password: Optional[str] = None,
        sqlserver_database: str = "master",
        # Legacy Docker parameters
        sqlserver_container: Optional[str] = None,
        connection_timeout: int = 30,
    ):
        self._host = sqlserver_host
        self._port = sqlserver_port
        self._user = sqlserver_user
        self._password = _resolve_sqlserver_password(sqlserver_password)
        self._database = sqlserver_database
        self._container = sqlserver_container
        self._connection_timeout = connection_timeout
        self._use_direct_sql = bool(sqlserver_host)

    def _get_connection(self) -> mssql.Connection:
        """Get a direct database connection."""
        conn_str = (
            f"SERVER={self._host},{self._port};"
            f"DATABASE={self._database};"
            f"UID={self._user};"
            f"PWD={self._password};"
            f"TrustServerCertificate=yes;"
        )
        return mssql.connect(connection_str=conn_str, timeout=self._connection_timeout)

    @property
    def name(self) -> str:
        return "get_server_config"

    @property
    def description(self) -> str:
        return """Get SQL Server configuration settings including memory limits,
parallelism settings, and other important server options."""

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "filter": {
                    "type": "string",
                    "description": "Filter config names containing this string (optional)"
                }
            },
            "required": []
        }

    def execute(self, filter: Optional[str] = None, **kwargs) -> ToolResult:
        """Execute query to get server configuration."""
        try:
            if self._use_direct_sql:
                configs = self._query_config_direct(filter)
                return ToolResult.ok({"configurations": configs})

            if not self._container:
                return ToolResult.fail(
                    "No SQL Server connection configured. Provide either sqlserver_host "
                    "(for direct connection) or sqlserver_container (for Docker)."
                )

            where_clause = ""
            if filter:
                safe_filter = filter.replace("'", "''")
                where_clause = f"WHERE name LIKE '%{safe_filter}%'"

            sql_query = (
                "SET NOCOUNT ON; "
                "SELECT name, value, value_in_use, minimum, maximum, description "
                f"FROM sys.configurations {where_clause} ORDER BY name;"
            )

            bash_cmd = (
                '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" '
                '-d master -C -I -s "|" -W -Q '
                f'"{sql_query}"'
            )
            cmd = [
                "docker", "exec", self._container,
                "bash", "-c", bash_cmd,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                return ToolResult.fail(f"Query failed: {result.stderr or result.stdout}")

            configs = self._parse_config_output(result.stdout)
            return ToolResult.ok({"configurations": configs})

        except Exception as e:
            return ToolResult.fail(f"Error getting server config: {str(e)}")

    def _query_config_direct(self, filter_value: Optional[str]) -> list[dict]:
        """Query server configuration via direct SQL connection."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET NOCOUNT ON")

            if filter_value:
                query = (
                    "SELECT name, value, value_in_use, minimum, maximum, description "
                    "FROM sys.configurations "
                    "WHERE name LIKE ? "
                    "ORDER BY name"
                )
                like_filter = f"%{filter_value}%"
                try:
                    cursor.execute(query, (like_filter,))
                except TypeError:
                    # Fallback for drivers that don't expose positional parameter binding.
                    safe_filter = filter_value.replace("'", "''")
                    cursor.execute(
                        "SELECT name, value, value_in_use, minimum, maximum, description "
                        "FROM sys.configurations "
                        f"WHERE name LIKE '%{safe_filter}%' "
                        "ORDER BY name"
                    )
            else:
                cursor.execute(
                    "SELECT name, value, value_in_use, minimum, maximum, description "
                    "FROM sys.configurations ORDER BY name"
                )

            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            return [self._normalize_config_row(dict(zip(columns, row))) for row in rows]

    def _normalize_config_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a direct-SQL configuration row to API shape."""
        normalized = {
            "name": row.get("name"),
            "value": row.get("value"),
            "value_in_use": row.get("value_in_use"),
            "minimum": row.get("minimum"),
            "maximum": row.get("maximum"),
            "description": row.get("description"),
        }

        for key in ("value", "value_in_use", "minimum", "maximum"):
            value = normalized.get(key)
            if isinstance(value, str) and value.isdigit():
                normalized[key] = int(value)

        return normalized

    def _parse_config_output(self, output: str) -> list[dict]:
        """Parse configuration output."""
        configs = []
        lines = output.strip().split('\n')

        # Skip header lines
        data_start = 0
        for i, line in enumerate(lines):
            if line.startswith('-'):
                data_start = i + 1
                break

        for line in lines[data_start:]:
            if not line.strip() or 'rows affected' in line.lower():
                continue
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4:
                try:
                    configs.append({
                        "name": parts[0],
                        "value": int(parts[1]) if parts[1].isdigit() else parts[1],
                        "value_in_use": int(parts[2]) if parts[2].isdigit() else parts[2],
                        "minimum": parts[3] if len(parts) > 3 else None,
                        "maximum": parts[4] if len(parts) > 4 else None,
                        "description": parts[5] if len(parts) > 5 else None
                    })
                except (ValueError, IndexError):
                    continue

        return configs


def create_health_tool_registry(
    sqlserver_host: Optional[str] = None,
    sqlserver_port: int = 1433,
    sqlserver_user: str = "sa",
    sqlserver_database: str = "master",
    sqlserver_container: str = "sqlserver",
    sqlserver_password: Optional[str] = None,
    offer_install_prompt: bool = False,
):
    """
    Create a tool registry with health check tools.

    Args:
        sqlserver_host: SQL Server hostname (for direct connection mode)
        sqlserver_port: SQL Server port (for direct connection mode)
        sqlserver_user: SQL Server user (for direct connection mode)
        sqlserver_database: SQL Server database (for direct connection mode)
        sqlserver_container: Docker container name
        sqlserver_password: SA password
        offer_install_prompt: Whether to return structured install offer when FRK is missing

    Returns:
        ToolRegistry with health check tools
    """
    from sim.rca.tools.base import ToolRegistry

    registry = ToolRegistry()

    # Add health-specific tools
    registry.register(RunSpBlitzTool(
        sqlserver_host=sqlserver_host,
        sqlserver_port=sqlserver_port,
        sqlserver_user=sqlserver_user,
        sqlserver_database=sqlserver_database,
        sqlserver_container=sqlserver_container,
        sqlserver_password=sqlserver_password,
        offer_install_prompt=offer_install_prompt,
    ))
    registry.register(GetServerConfigTool(
        sqlserver_host=sqlserver_host,
        sqlserver_port=sqlserver_port,
        sqlserver_user=sqlserver_user,
        sqlserver_database=sqlserver_database,
        sqlserver_container=sqlserver_container,
        sqlserver_password=sqlserver_password,
    ))

    return registry
