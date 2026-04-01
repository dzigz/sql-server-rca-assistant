# SQL Server RCA Assistant (Stage 1)

A local AI-assisted SQL Server troubleshooting app.

This stage intentionally removes simulation/training workflows and focuses on real SQL Server targets.

## What It Does

- Connects to your SQL Server instance.
- Runs direct diagnostics (`sp_Blitz`, server configuration checks).
- Provides chat-driven root cause analysis and remediation guidance.
- Optionally uses ClickHouse metrics for recent-vs-baseline comparison.

## Prerequisites

- Python 3.11+
- Node.js 20.9+
- SQL Server credentials with permissions required to run diagnostics
- Optional: Docker (for ClickHouse monitoring stack)

## Install

```bash
python3.11 -m venv sim/.venv
source sim/.venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r sim/requirements.txt
```

## Run Web App

```bash
# Required SQL Server target
export SQLSERVER_HOST='your-sqlserver-host'
export SQLSERVER_PORT='1433'
export SQLSERVER_USER='sa'
export SQLSERVER_PASSWORD='your-password'
export SQLSERVER_DATABASE='master'

# Default one-command bootstrap
# - starts optional monitoring stack
# - enables monitoring tools
# - auto-installs FRK/Blitz if missing
python -m sim webapp start
```

Opt-out flags:

```bash
python -m sim webapp start --no-monitoring-stack
python -m sim webapp start --no-monitoring
python -m sim webapp start --no-auto-install-blitz
```

## CLI Commands

```bash
python -m sim webapp start
python -m sim webapp backend
python -m sim webapp frontend
```

## Environment Variables

### Required for Direct SQL Mode

- `SQLSERVER_HOST`
- `SQLSERVER_PASSWORD` (or `SA_PASSWORD` / `SIM_SA_PASSWORD`)

### Optional SQL Defaults

- `SQLSERVER_PORT` (default `1433`)
- `SQLSERVER_USER` (default `sa`)
- `SQLSERVER_DATABASE` (default `master`)

### Optional Monitoring

- `SIM_ENABLE_MONITORING=1`
- `CLICKHOUSE_HOST` (default `localhost`)
- `CLICKHOUSE_PORT` (default `8123`)
- `CLICKHOUSE_DATABASE` (default `rca_metrics`)
- `CLICKHOUSE_USER` (default `default`)
- `CLICKHOUSE_PASSWORD`

## Repository Focus

Kept for stage 1:
- `sim/webapp`
- `sim/rca`
- `sim/sql/blitz`
- `sim/docker` (optional monitoring stack)

Removed from stage 1 product flow:
- incident simulation workflows
- synthetic baseline workload
- training/evaluation scenario orchestration
