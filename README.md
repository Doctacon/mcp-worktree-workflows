# MCP Servers

A collection of local MCP (Model Context Protocol) servers for data engineering and development workflows. All servers run locally via `uv` — no cloud accounts required.

## Servers

### Data Engineering

| Server | File | Purpose |
|--------|------|---------|
| `duckdb` | `duckdb_server.py` | Query execution, schema inspection, data import/export |
| `sqlmesh` | `sqlmesh_server.py` | SQL transformation — model execution, planning, lineage |
| `dagster` | `dagster_server.py` | Orchestration — assets, jobs, schedules, sensors, runs |
| `dlt` | `dlt_server.py` | Data pipelines — sources, destinations, load operations |
| `soda` | `soda_server.py` | Data quality contracts — verify, validate, scaffold, history |

### Infrastructure

| Server | File | Purpose |
|--------|------|---------|
| `docker` | `docker_server.py` | Container lifecycle, logs, exec, images, Compose-aware |
| `worktrees` | `worktree_server.py` | Git worktree creation and management for parallel development |

## Setup

All servers are registered in `.mcp.json` and run via:

```bash
uv run --no-project --with fastmcp --with <deps> ./mcp-servers/<server>.py
```

Restart Claude Code after any changes to `.mcp.json`.

## Server Notes

### duckdb
Manages multiple named DuckDB connections. Supports querying local files (CSV, Parquet, JSON) and remote files (HTTP, S3, GCS). State persisted to `.duckdb_state/`.

### sqlmesh
Wraps the SQLMesh CLI for model management. Requires a SQLMesh project directory set via `SQLMESH_STATE_DIR`. Supports plan generation, model execution, audits, and lineage introspection.

### dagster
Wraps the Dagster CLI for asset and job management. Requires a Dagster workspace file. Supports asset materialization, job execution, schedule/sensor triggering, and run monitoring.

### dlt
Manages dlt pipelines for loading data from REST APIs, databases, and files into destinations (DuckDB, etc.). State persisted to `.dlt_state/`.

### soda
Runs Soda Core data quality contract verifications locally (no Soda Cloud). Expects a `./soda/` directory with:
```
soda/
  datasources/   # datasource YAML configs
  contracts/     # contract YAML files
  results/       # scan results (auto-created)
```

### docker
Connects to Docker via Colima on macOS (`~/.colima/default/docker.sock`). Includes Compose-aware tools that read `com.docker.compose.*` labels.

### worktrees
Creates isolated git worktrees branching from `origin/main` for running parallel Claude sessions on independent tasks.
