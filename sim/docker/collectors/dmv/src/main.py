"""
Main entry point for the DMV Collector.

Continuously collects SQL Server DMV metrics and writes them to ClickHouse.
Exposes HTTP API for incident signaling on port 8080.
"""

import signal
import sys
import time
import threading
import structlog
from flask import Flask, request, jsonify

from .config import get_config
from .clickhouse_writer import ClickHouseWriter
from .collector import DMVCollector

# Global collector reference for API access
_collector: DMVCollector | None = None
_writer: ClickHouseWriter | None = None

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Flask app for HTTP API
app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})


@app.route("/incident/start", methods=["POST"])
def start_incident():
    """Signal that an incident has started.

    POST body: {"incident_id": "string", "name": "string (optional)"}
    """
    global _collector, _writer
    if not _collector:
        return jsonify({"error": "Collector not initialized"}), 500

    data = request.get_json() or {}
    incident_id = data.get("incident_id")
    name = data.get("name", "")

    if not incident_id:
        return jsonify({"error": "incident_id is required"}), 400

    # Set collector to incident mode (is_baseline=False).
    # This endpoint is only for explicit monitoring-window markers.
    _collector.set_incident(incident_id, is_baseline=False)

    logger.info("Incident started", incident_id=incident_id, name=name)
    return jsonify({"status": "ok", "incident_id": incident_id, "is_baseline": False})


@app.route("/incident/stop", methods=["POST"])
def stop_incident():
    """Signal that an incident has stopped.

    POST body: {"incident_id": "string (optional)"}
    """
    global _collector, _writer
    if not _collector:
        return jsonify({"error": "Collector not initialized"}), 500

    data = request.get_json() or {}
    incident_id = data.get("incident_id")

    # End incident record in ClickHouse
    if _writer and incident_id:
        try:
            _writer.end_incident(incident_id)
        except Exception as e:
            logger.warning("Failed to end incident record", error=str(e))

    # Set collector back to baseline mode
    _collector.set_incident(None, is_baseline=True)

    logger.info("Incident stopped, returning to baseline mode", incident_id=incident_id)
    return jsonify({"status": "ok", "is_baseline": True})


@app.route("/status", methods=["GET"])
def status():
    """Get current collector status."""
    global _collector
    if not _collector:
        return jsonify({"error": "Collector not initialized"}), 500

    return jsonify({
        "running": _collector._running,
        "paused": _collector.is_paused(),
        "incident_id": _collector._current_incident_id,
        "is_baseline": _collector._is_baseline,
    })


@app.route("/collector/pause", methods=["POST"])
def pause_collector():
    """Pause the collector to avoid resource contention during Blitz execution."""
    global _collector
    if not _collector:
        return jsonify({"error": "Collector not initialized"}), 500

    _collector.pause()
    return jsonify({"status": "ok", "paused": True})


@app.route("/collector/resume", methods=["POST"])
def resume_collector():
    """Resume the collector after Blitz execution."""
    global _collector
    if not _collector:
        return jsonify({"error": "Collector not initialized"}), 500

    _collector.resume()
    return jsonify({"status": "ok", "paused": False})


def run_api_server():
    """Run the Flask API server in a background thread."""
    import logging
    # Suppress Flask request logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.WARNING)

    app.run(host="0.0.0.0", port=8080, threaded=True)


def main():
    """Main entry point."""
    logger.info("Starting DMV Collector")

    # Load configuration
    config = get_config()
    logger.info(
        "Configuration loaded",
        sqlserver_host=config.sqlserver_host,
        clickhouse_host=config.clickhouse_host,
        collection_interval=config.collection_interval,
    )

    # Create ClickHouse writer
    writer = ClickHouseWriter(config)

    # Wait for ClickHouse to be ready
    max_retries = 30
    for i in range(max_retries):
        try:
            writer.connect()
            logger.info("Connected to ClickHouse")
            break
        except Exception as e:
            logger.warning(
                "Waiting for ClickHouse",
                attempt=i + 1,
                max_retries=max_retries,
                error=str(e),
            )
            time.sleep(2)
    else:
        logger.error("Failed to connect to ClickHouse after max retries")
        sys.exit(1)

    # Create collector and set global references for API access
    global _collector, _writer
    collector = DMVCollector(config, writer)
    _collector = collector
    _writer = writer

    # Wait for SQL Server to be ready
    for i in range(max_retries):
        try:
            collector.collect_once()
            logger.info("Successfully collected from SQL Server")
            break
        except Exception as e:
            logger.warning(
                "Waiting for SQL Server",
                attempt=i + 1,
                max_retries=max_retries,
                error=str(e),
            )
            time.sleep(2)
    else:
        logger.error("Failed to connect to SQL Server after max retries")
        sys.exit(1)

    # Set up signal handlers
    def handle_shutdown(signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        collector.stop()
        writer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    # Start the API server in a background thread
    api_thread = threading.Thread(target=run_api_server, daemon=True)
    api_thread.start()
    logger.info("API server started on port 8080")

    # Start the collector
    collector.start()

    # Keep the main thread alive
    logger.info("DMV Collector is running")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
