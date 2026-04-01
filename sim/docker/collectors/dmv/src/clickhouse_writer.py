"""ClickHouse writer for DMV metrics."""

import structlog
import clickhouse_connect
from typing import List, Dict, Any, Optional
from datetime import datetime

from .config import CollectorConfig

logger = structlog.get_logger()


class ClickHouseWriter:
    """Writes DMV metrics to ClickHouse."""

    def __init__(self, config: CollectorConfig):
        self.config = config
        self._client = None

    def connect(self) -> None:
        """Establish connection to ClickHouse."""
        logger.info(
            "Connecting to ClickHouse",
            host=self.config.clickhouse_host,
            port=self.config.clickhouse_port,
            database=self.config.clickhouse_database,
        )
        self._client = clickhouse_connect.get_client(
            host=self.config.clickhouse_host,
            port=self.config.clickhouse_port,
            database=self.config.clickhouse_database,
            username=self.config.clickhouse_user,
            password=self.config.clickhouse_password,
        )
        logger.info("Connected to ClickHouse")

    def close(self) -> None:
        """Close the ClickHouse connection."""
        if self._client:
            self._client.close()
            self._client = None

    @property
    def client(self):
        """Get the ClickHouse client, connecting if needed."""
        if self._client is None:
            self.connect()
        return self._client

    def write(self, table: str, records: List[Dict[str, Any]]) -> int:
        """
        Write records to a ClickHouse table.

        Args:
            table: Table name (without database prefix)
            records: List of dictionaries with column names as keys

        Returns:
            Number of records written
        """
        if not records:
            return 0

        try:
            # Get column names from the first record
            columns = list(records[0].keys())

            # Convert records to list of tuples
            data = [[record.get(col) for col in columns] for record in records]

            # Insert into ClickHouse
            self.client.insert(
                table=table,
                data=data,
                column_names=columns,
            )

            logger.debug(
                "Wrote records to ClickHouse",
                table=table,
                count=len(records),
            )
            return len(records)

        except Exception as e:
            logger.error(
                "Failed to write to ClickHouse",
                table=table,
                error=str(e),
                count=len(records),
            )
            raise

    def write_wait_stats(
        self,
        records: List[Dict[str, Any]],
        incident_id: Optional[str] = None,
        is_baseline: bool = False,
    ) -> int:
        """Write wait stats records."""
        for record in records:
            record["incident_id"] = incident_id
            record["is_baseline"] = 1 if is_baseline else 0
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("wait_stats", records)

    def write_active_requests(
        self,
        records: List[Dict[str, Any]],
        incident_id: Optional[str] = None,
        is_baseline: bool = False,
    ) -> int:
        """Write active requests records."""
        for record in records:
            record["incident_id"] = incident_id
            record["is_baseline"] = 1 if is_baseline else 0
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("active_requests", records)

    def write_blocking_chains(
        self,
        records: List[Dict[str, Any]],
        incident_id: Optional[str] = None,
        is_baseline: bool = False,
    ) -> int:
        """Write blocking chains records."""
        for record in records:
            record["incident_id"] = incident_id
            record["is_baseline"] = 1 if is_baseline else 0
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("blocking_chains", records)

    def write_query_stats(
        self,
        records: List[Dict[str, Any]],
        incident_id: Optional[str] = None,
        is_baseline: bool = False,
    ) -> int:
        """Write query stats records."""
        for record in records:
            record["incident_id"] = incident_id
            record["is_baseline"] = 1 if is_baseline else 0
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("query_stats", records)

    def write_memory_grants(
        self,
        records: List[Dict[str, Any]],
        incident_id: Optional[str] = None,
        is_baseline: bool = False,
    ) -> int:
        """Write memory grants records."""
        for record in records:
            record["incident_id"] = incident_id
            record["is_baseline"] = 1 if is_baseline else 0
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("memory_grants", records)

    def write_file_stats(
        self,
        records: List[Dict[str, Any]],
        incident_id: Optional[str] = None,
        is_baseline: bool = False,
    ) -> int:
        """Write file stats records."""
        for record in records:
            record["incident_id"] = incident_id
            record["is_baseline"] = 1 if is_baseline else 0
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("file_stats", records)

    def write_schedulers(
        self,
        records: List[Dict[str, Any]],
        incident_id: Optional[str] = None,
        is_baseline: bool = False,
    ) -> int:
        """Write schedulers records."""
        for record in records:
            record["incident_id"] = incident_id
            record["is_baseline"] = 1 if is_baseline else 0
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("schedulers", records)

    def write_perf_counters(
        self,
        records: List[Dict[str, Any]],
        incident_id: Optional[str] = None,
        is_baseline: bool = False,
    ) -> int:
        """Write performance counters records."""
        for record in records:
            record["incident_id"] = incident_id
            record["is_baseline"] = 1 if is_baseline else 0
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("perf_counters", records)

    def write_missing_indexes(
        self,
        records: List[Dict[str, Any]],
        incident_id: Optional[str] = None,
        is_baseline: bool = False,
    ) -> int:
        """Write missing indexes records."""
        for record in records:
            record["incident_id"] = incident_id
            record["is_baseline"] = 1 if is_baseline else 0
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("missing_indexes", records)

    def write_blitz_results(
        self,
        records: List[Dict[str, Any]],
        incident_id: str,
    ) -> int:
        """Write Blitz results records."""
        for record in records:
            record["incident_id"] = incident_id
            if "collected_at" not in record:
                record["collected_at"] = datetime.now()
        return self.write("blitz_results", records)

    def create_incident(
        self,
        incident_id: str,
        name: str,
        scenario: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        """Create a new incident record."""
        record = {
            "incident_id": incident_id,
            "name": name,
            "scenario": scenario,
            "started_at": datetime.now(),
            "ended_at": None,
            "baseline_start": None,
            "baseline_end": None,
            "status": "active",
            "notes": notes,
            "created_at": datetime.now(),
        }
        self.write("incidents", [record])
        logger.info("Created incident", incident_id=incident_id, name=name)

    def end_incident(self, incident_id: str) -> None:
        """Mark an incident as ended."""
        self.client.command(
            f"""
            ALTER TABLE incidents
            UPDATE ended_at = now64(3), status = 'completed'
            WHERE incident_id = '{incident_id}'
            """
        )
        logger.info("Ended incident", incident_id=incident_id)

    def set_baseline_window(
        self,
        incident_id: str,
        baseline_start: datetime,
        baseline_end: datetime,
    ) -> None:
        """Set the baseline time window for an incident."""
        self.client.command(
            f"""
            ALTER TABLE incidents
            UPDATE baseline_start = toDateTime64('{baseline_start.isoformat()}', 3),
                   baseline_end = toDateTime64('{baseline_end.isoformat()}', 3)
            WHERE incident_id = '{incident_id}'
            """
        )
        logger.info(
            "Set baseline window",
            incident_id=incident_id,
            start=baseline_start,
            end=baseline_end,
        )
