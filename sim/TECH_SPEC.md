# SQL Server RCA Assistant - Stage 1 Technical Spec

## Scope

Stage 1 provides a SQL Server-only troubleshooting assistant with:
- FastAPI backend + Next.js frontend chat experience
- SQL Server diagnostics tools (`run_sp_blitz`, `get_server_config`)
- AI reasoning loop over tool output
- Optional ClickHouse monitoring backend for time-window analysis

Removed from stage 1:
- incident simulation engine
- synthetic workload generator
- training/evaluation orchestration workflows

## Runtime Architecture

1. Frontend (`sim/webapp/frontend`) sends chat/session requests.
2. Backend (`sim/webapp/backend`) creates a session-scoped `AgentRCAEngine`.
3. Tool registry is built per session:
   - always: SQL Server health tools
   - optional: ClickHouse/Grafana tools when monitoring is enabled
   - optional: code analysis tools when repo path is provided
4. Engine streams reasoning/tool/text events back over SSE.

## Session Model

Each session is bound to one SQL Server target:
- host
- port
- user
- database

Password is accepted at session creation time or resolved from backend env.

## Monitoring Mode

When `enable_monitoring=true`:
- Backend initializes `ClickHouseDataSource`
- ClickHouse-based tools are registered (`compare_baseline`, `query_clickhouse`, `get_query_details`)
- Grafana embedding tools are registered

When monitoring is disabled:
- Session runs in direct SQL-only mode
- Agent uses Blitz/config tools and user context

## Security Notes

- Designed for read-only diagnostics.
- Passwords should be supplied via backend env variables or secure session creation input.
- Avoid logging credentials in clear text.
