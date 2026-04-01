# Optional Monitoring Stack (Stage 1)

This compose setup is optional. It adds a ClickHouse-backed monitoring pipeline for the SQL Server RCA Assistant.

It does **not** run a bundled SQL Server test database.
The DMV collector connects to your own SQL Server instance.

`python -m sim webapp start` runs this stack by default unless `--no-monitoring-stack` is passed.

## Services

- `clickhouse`: stores collected DMV snapshots.
- `clickhouse-init`: bootstraps schema.
- `dmv-collector`: polls SQL Server DMVs and writes to ClickHouse.
- `grafana`: optional dashboards over ClickHouse metrics.

## Quick Start

```bash
cd sim/docker

export SQLSERVER_HOST='your-sqlserver-host'
export SQLSERVER_PORT='1433'
export SQLSERVER_USER='sa'
export SQLSERVER_PASSWORD='your-password'
export SQLSERVER_DATABASE='master'
export GRAFANA_PASSWORD='choose-your-own-password'  # You create this value yourself; it becomes Grafana admin password (user: admin) at http://localhost:3001. For `python -m sim webapp start`, password is auto-set and printed in terminal.

docker compose up -d
```

## Health Checks

```bash
curl http://localhost:8080/health
curl http://localhost:3001/api/health
```

Grafana login (only if you open dashboards):
- URL: `http://localhost:3001`
- Username: `admin`
- Password: value of `GRAFANA_PASSWORD`

## Stop

```bash
docker compose down
```

## Notes

- Use this stack only when you want historical/trend analysis in the assistant.
- For direct SQL Server diagnostics (sp_Blitz, server config) you can run the web app without this stack.
- If you start via `python -m sim webapp start`, the CLI sets a default Grafana password automatically when not provided.
