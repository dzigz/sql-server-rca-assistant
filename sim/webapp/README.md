# SQL Server RCA Web App

Web UI + FastAPI backend for SQL Server troubleshooting.

## Features

- Session-based chat with streaming responses
- SQL Server diagnostics tools (`run_sp_blitz`, `get_server_config`)
- Optional monitoring tools when ClickHouse backend is enabled
- Optional code analysis tools (if `claude-agent-sdk` is installed and `repo_path` is set)

## Start

```bash
# Required SQL target
export SQLSERVER_HOST='your-sqlserver-host'
export SQLSERVER_PASSWORD='your-password'

python -m sim webapp start
```

Open [http://localhost:3000](http://localhost:3000).

Default `webapp start` behavior:
- starts monitoring stack (`clickhouse`, `dmv-collector`, `grafana`)
- enables monitoring-backed tools
- auto-installs FRK/Blitz scripts if missing

Opt-out:

```bash
python -m sim webapp start --no-monitoring-stack
python -m sim webapp start --no-monitoring
python -m sim webapp start --no-auto-install-blitz
```

## API Endpoints

### Session

- `POST /api/session/create`
- `GET /api/session/summaries`
- `GET /api/session/{id}`
- `GET /api/session/{id}/history`
- `DELETE /api/session/{id}`

### Chat

- `POST /api/chat/stream`
- `POST /api/chat/{session_id}`

### FRK Install Actions

- `POST /api/session/{id}/blitz/install`
- `POST /api/session/{id}/blitz/decline`

## Session Create Request Fields

- `sqlserver_host`, `sqlserver_port`, `sqlserver_user`, `sqlserver_password`, `sqlserver_database`
- `enable_monitoring` (bool)
- `auto_install_blitz` (bool)
- Optional monitoring fields: `clickhouse_host`, `clickhouse_port`, `clickhouse_database`, `clickhouse_user`, `clickhouse_password`
- `repo_path` (optional)
