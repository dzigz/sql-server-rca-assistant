#!/usr/bin/env python3
"""Standalone Blitz runner that doesn't depend on package imports."""

import os
import sys
import json
import mssql_python
import clickhouse_connect
from datetime import datetime
from dataclasses import dataclass

@dataclass
class BlitzConfig:
    sqlserver_host: str
    sqlserver_port: int
    sqlserver_database: str
    sqlserver_user: str
    sqlserver_password: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str


def get_sql_connection(config: BlitzConfig):
    conn_str = (
        f"SERVER={config.sqlserver_host},{config.sqlserver_port};"
        f"DATABASE={config.sqlserver_database};"
        f"UID={config.sqlserver_user};"
        f"PWD={config.sqlserver_password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )
    return mssql_python.connect(conn_str)


def get_clickhouse_client(config: BlitzConfig):
    return clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        database=config.clickhouse_database,
    )


def run_blitz_first(config: BlitzConfig, incident_id: str, ch_client, seconds: int = 5):
    """Run sp_BlitzFirst and store results."""
    collected_at = datetime.now()
    conn = None

    try:
        conn = get_sql_connection(config)
        cursor = conn.cursor()

        # Run BlitzFirst
        cursor.execute(f"SET NOCOUNT ON; EXEC dbo.sp_BlitzFirst @Seconds = {seconds}")

        results = []
        while True:
            try:
                # Check description FIRST - mssql_python returns None for empty result sets
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    if rows:
                        for row in rows:
                            row_dict = dict(zip(columns, row))
                            priority = row_dict.get("Priority", 255)
                            if priority is not None and priority <= 200:
                                results.append({
                                    "collected_at": collected_at,
                                    "incident_id": incident_id,
                                    "blitz_type": "BlitzFirst",
                                    "priority": int(priority),
                                    "findings_group": str(row_dict.get("FindingsGroup", ""))[:200],
                                    "finding": str(row_dict.get("Finding", ""))[:500],
                                    "details": str(row_dict.get("Details", ""))[:2000],
                                })
            except Exception:
                pass
            if not cursor.nextset():
                break

        print(f"BlitzFirst found {len(results)} findings", file=sys.stderr)
        if results:
            write_blitz_results(ch_client, results, incident_id)
        return True
    except Exception as e:
        print(f"BlitzFirst error: {e}", file=sys.stderr)
        return False
    finally:
        if conn:
            conn.close()


def run_blitz_cache(config: BlitzConfig, incident_id: str, ch_client):
    """Run sp_BlitzCache and store results."""
    collected_at = datetime.now()
    conn = None

    try:
        conn = get_sql_connection(config)
        cursor = conn.cursor()

        # Run BlitzCache
        cursor.execute("SET NOCOUNT ON; EXEC dbo.sp_BlitzCache @Top = 10, @SortOrder = 'cpu'")

        results = []
        while True:
            try:
                # Check description FIRST - mssql_python returns None for empty result sets
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    # Look for Query Text column (query results) or Finding column (findings)
                    if "Query Text" in columns:
                        rows = cursor.fetchall()
                        if rows:
                            for row in rows:
                                row_dict = dict(zip(columns, row))
                                results.append({
                                    "collected_at": collected_at,
                                    "incident_id": incident_id,
                                    "blitz_type": "BlitzCache",
                                    "priority": 50,
                                    "finding": str(row_dict.get("Query Text", ""))[:500],
                                    "details": str(row_dict.get("Warnings", ""))[:2000] if row_dict.get("Warnings") else "",
                                })
            except Exception:
                pass
            if not cursor.nextset():
                break

        print(f"BlitzCache found {len(results)} queries", file=sys.stderr)
        if results:
            write_blitz_results(ch_client, results, incident_id)
        return True
    except Exception as e:
        print(f"BlitzCache error: {e}", file=sys.stderr)
        return False
    finally:
        if conn:
            conn.close()


def run_blitz_who(config: BlitzConfig, incident_id: str, ch_client):
    """Run sp_BlitzWho and store results."""
    collected_at = datetime.now()
    conn = None

    try:
        conn = get_sql_connection(config)
        cursor = conn.cursor()

        # Run BlitzWho
        cursor.execute("SET NOCOUNT ON; EXEC dbo.sp_BlitzWho")

        results = []
        while True:
            try:
                # Check description FIRST - mssql_python returns None for empty result sets
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    # Look for session_id column
                    if "session_id" in columns:
                        rows = cursor.fetchall()
                        if rows:
                            for row in rows:
                                row_dict = dict(zip(columns, row))
                                results.append({
                                    "collected_at": collected_at,
                                    "incident_id": incident_id,
                                    "blitz_type": "BlitzWho",
                                    "priority": 50,
                                    "finding": f"Session {row_dict.get('session_id', 'N/A')}: {row_dict.get('status', '')}",
                                    "details": str(row_dict.get("wait_info", ""))[:2000] if row_dict.get("wait_info") else "",
                                })
            except Exception:
                pass
            if not cursor.nextset():
                break

        print(f"BlitzWho found {len(results)} sessions", file=sys.stderr)
        if results:
            write_blitz_results(ch_client, results, incident_id)
        return True
    except Exception as e:
        print(f"BlitzWho error: {e}", file=sys.stderr)
        return False
    finally:
        if conn:
            conn.close()


def run_blitz_index(config: BlitzConfig, incident_id: str, ch_client):
    """Run sp_BlitzIndex and store results."""
    collected_at = datetime.now()
    conn = None

    try:
        conn = get_sql_connection(config)
        cursor = conn.cursor()

        # Run BlitzIndex
        cursor.execute(f"SET NOCOUNT ON; EXEC dbo.sp_BlitzIndex @DatabaseName = '{config.sqlserver_database}', @Mode = 0")

        results = []
        while True:
            try:
                # Check description FIRST - mssql_python returns None for empty result sets
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    # Look for Priority and Finding columns
                    if "Priority" in columns and "Finding" in columns:
                        rows = cursor.fetchall()
                        if rows:
                            for row in rows:
                                row_dict = dict(zip(columns, row))
                                priority = row_dict.get("Priority", 255)
                                # Filter out header rows (priority -1) and low priority
                                if priority is not None and 0 <= priority <= 200:
                                    results.append({
                                        "collected_at": collected_at,
                                        "incident_id": incident_id,
                                        "blitz_type": "BlitzIndex",
                                        "priority": int(priority),
                                        "finding": str(row_dict.get("Finding", ""))[:500],
                                        "details": str(row_dict.get("Details", row_dict.get("Details: schema.table.index(indexid)", "")))[:2000],
                                    })
            except Exception:
                pass
            if not cursor.nextset():
                break

        print(f"BlitzIndex found {len(results)} findings", file=sys.stderr)
        if results:
            write_blitz_results(ch_client, results, incident_id)
        return True
    except Exception as e:
        print(f"BlitzIndex error: {e}", file=sys.stderr)
        return False
    finally:
        if conn:
            conn.close()


def run_blitz_lock(config: BlitzConfig, incident_id: str, ch_client):
    """Run sp_BlitzLock and store results."""
    collected_at = datetime.now()
    conn = None

    try:
        conn = get_sql_connection(config)
        # Enable autocommit to avoid deadlocks with sp_BlitzLock's internal queries
        # Without autocommit, implicit transactions cause communication buffer deadlocks
        conn.autocommit = True
        cursor = conn.cursor()

        # sp_BlitzLock analyzes recent deadlocks from system_health session
        cursor.execute("SET NOCOUNT ON; EXEC dbo.sp_BlitzLock")

        results = []
        while True:
            try:
                # Check description FIRST - mssql_python returns None for empty result sets
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    # Look for deadlock data columns
                    if "deadlock_type" in columns or "DeadlockType" in columns or "event_date" in columns:
                        rows = cursor.fetchall()
                        if rows:
                            for row in rows:
                                row_dict = dict(zip(columns, row))
                                results.append({
                                    "collected_at": collected_at,
                                    "incident_id": incident_id,
                                    "blitz_type": "BlitzLock",
                                    "priority": 10,
                                    "finding": str(row_dict.get("deadlock_type", row_dict.get("DeadlockType", "")))[:500],
                                    "details": str(row_dict.get("victim_query", row_dict.get("VictimQuery", "")))[:2000],
                                })
            except Exception:
                pass
            if not cursor.nextset():
                break

        print(f"BlitzLock found {len(results)} deadlocks", file=sys.stderr)
        if results:
            write_blitz_results(ch_client, results, incident_id)
        return True
    except Exception as e:
        print(f"BlitzLock error: {e}", file=sys.stderr)
        return False
    finally:
        if conn:
            conn.close()


def write_blitz_results(ch_client, results, incident_id):
    """Write Blitz results to ClickHouse."""
    if not results:
        return

    # Prepare data for insertion
    data = []
    for r in results:
        data.append([
            r["collected_at"],
            r["incident_id"],
            r["blitz_type"],
            r["priority"],
            r.get("findings_group", ""),
            r["finding"],
            r["details"],
            None,  # url
            None,  # query_text
            None,  # database_name
            None,  # total_cpu
            None,  # total_reads
            None,  # total_writes
            None,  # execution_count
            None,  # avg_duration_ms
            None,  # warnings
            None,  # total_spills
            None,  # session_id
            None,  # status
            None,  # wait_info
            None,  # blocking_session_id
            None,  # cpu_ms
            None,  # reads
            None,  # schema_name
            None,  # table_name
            None,  # index_name
            None,  # index_definition
            None,  # create_tsql
            None,  # deadlock_type
            None,  # victim_query
            None,  # blocking_query
            None,  # deadlock_graph
        ])

    try:
        print(f"Writing {len(data)} rows to ClickHouse blitz_results...", file=sys.stderr)
        ch_client.insert(
            "blitz_results",
            data,
            column_names=[
                "collected_at", "incident_id", "blitz_type", "priority",
                "findings_group", "finding", "details", "url", "query_text",
                "database_name", "total_cpu", "total_reads", "total_writes",
                "execution_count", "avg_duration_ms", "warnings", "total_spills",
                "session_id", "status", "wait_info", "blocking_session_id",
                "cpu_ms", "reads", "schema_name", "table_name", "index_name",
                "index_definition", "create_tsql", "deadlock_type",
                "victim_query", "blocking_query", "deadlock_graph"
            ]
        )
        print(f"Successfully wrote {len(data)} rows", file=sys.stderr)
    except Exception as e:
        print(f"ClickHouse write error: {e}", file=sys.stderr)


def run_blitz_suite(incident_id: str, config: BlitzConfig):
    """Run all Blitz scripts."""
    ch_client = get_clickhouse_client(config)

    results = {
        "blitz_first": run_blitz_first(config, incident_id, ch_client),
        "blitz_cache": run_blitz_cache(config, incident_id, ch_client),
        "blitz_who": run_blitz_who(config, incident_id, ch_client),
        "blitz_index": run_blitz_index(config, incident_id, ch_client),
        "blitz_lock": run_blitz_lock(config, incident_id, ch_client),
    }

    success_count = sum(1 for v in results.values() if v)
    print(f"SUCCESS:{success_count}:{len(results)}")

    return results


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: run_blitz.py <incident_id>")
        sys.exit(1)

    incident_id = sys.argv[1]

    config = BlitzConfig(
        sqlserver_host=os.environ.get("SQLSERVER_HOST", "sqlserver"),
        sqlserver_port=int(os.environ.get("SQLSERVER_PORT", "1433")),
        sqlserver_database=os.environ.get("SQLSERVER_DATABASE", "master"),
        sqlserver_user=os.environ.get("SQLSERVER_USER", "sa"),
        sqlserver_password=os.environ.get("SQLSERVER_PASSWORD", ""),
        clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        clickhouse_port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        clickhouse_database=os.environ.get("CLICKHOUSE_DATABASE", "rca_metrics"),
    )

    if not config.sqlserver_password:
        print("ERROR: SQLSERVER_PASSWORD environment variable is required", file=sys.stderr)
        sys.exit(2)

    run_blitz_suite(incident_id, config)
