# SQL Server RCA Assistant

Open-source local web app for SQL Server performance analysis.

Stage 1 scope:
- SQL Server-only diagnostics
- Chat-based RCA assistant
- First Responder Kit (sp_Blitz*) integration
- Optional ClickHouse monitoring backend

## Manual (Start Here)

1. Prerequisites
- Python 3.11+
- Node.js 20.9+
- Docker Desktop/Engine running (default setup)
- A reachable SQL Server instance and credentials with diagnostic permissions

2. Install dependencies

```bash
# From repo root
python -m venv sim/.venv
source sim/.venv/bin/activate
pip install -r sim/requirements.txt
```

3. Point the app to your SQL Server target

```bash
export SQLSERVER_HOST='your-sqlserver-host'
export SQLSERVER_PORT='1433'
export SQLSERVER_USER='sa'
export SQLSERVER_PASSWORD='your-password'
export SQLSERVER_DATABASE='master'
```

4. Start everything (default one-command flow)

```bash
python -m sim webapp start
```

This default command:
- starts ClickHouse + collector + Grafana monitoring stack
- enables monitoring tools for analysis
- auto-installs FRK/Blitz scripts if missing
- uses Grafana only for optional dashboards (not required for RCA chat flow)
- prints Grafana login in terminal at startup

5. Open the app and run analysis
- Open [http://localhost:3000](http://localhost:3000).
- Create a session for your SQL Server target.
- Ask for analysis of the issue you are seeing (for example: "CPU spikes started at 09:40 UTC, analyze likely root causes and next checks").
- The assistant runs SQL Server diagnostics and returns RCA + recommended actions.
- Optional dashboards: open `http://localhost:3001` and sign in with `admin` plus the password printed by startup.

6. Optional: run without monitoring stack

```bash
python -m sim webapp start --no-monitoring-stack --no-monitoring
```

7. Stop services

```bash
# Stop web app/backend process
# (Ctrl+C in the terminal where you started it)

# Stop monitoring containers
docker compose -f sim/docker/docker-compose.yaml down
```

## Command Options

- `--no-monitoring-stack`: do not run docker compose stack
- `--no-monitoring`: SQL-direct mode only (no ClickHouse tools)
- `--no-auto-install-blitz`: skip automatic FRK install

## Docs

- App guide: `sim/README.md`
- Web app details: `sim/webapp/README.md`
- Optional monitoring stack: `sim/docker/README.md`

## License

MIT
