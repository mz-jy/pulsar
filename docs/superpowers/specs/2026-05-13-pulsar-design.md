# Pulsar — Design Spec

**Date:** 2026-05-13  
**Status:** Approved

## Overview

Four changes to the existing Pulsar codebase:

1. **Python module support** — run jobs with `python -m package.module` in addition to script paths
2. **Execution dots timeline** — Airflow-style per-job row of last 30 colored dots in the UI
3. **Rename to Pulsar** — new name across all user-facing strings and the CLI command
4. **Package setup** — installable via pip/uv, exposes `pulsar` CLI entry point

---

## Feature 1 — Python Module Support

### Goal

Allow jobs to invoke installed Python packages via `python -m module.name` instead of only file paths.

### DB Schema

Add one column to `jobs`:

```sql
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS run_as_module BOOLEAN DEFAULT FALSE;
```

`_init_schema()` runs this `ALTER TABLE` after the `CREATE TABLE IF NOT EXISTS` statement, so it is safe on both fresh and existing databases. DuckDB supports `ADD COLUMN IF NOT EXISTS`. Existing rows default to `FALSE`.

### Data Model

`Job` dataclass gains field: `run_as_module: bool`

### Executor

`_run()` in `executor.py`:

```python
if job.run_as_module:
    cmd = [sys.executable, "-m", job.script_path]
else:
    cmd = [sys.executable, job.script_path]
```

`script_path` stores the module name when `run_as_module=True` (e.g. `mypackage.tasks.report`).

### API

`AddJobRequest` in `server.py` gains `run_as_module: bool = False`.  
`db.add_job()` gains `run_as_module: bool = False` param.  
`/api/jobs` list response includes `run_as_module` per job.

### CLI

`pulsar add` gains `--module` flag (boolean). When passed, sets `run_as_module=True`.

### UI

- Field label: "Script Path / Module"
- Hint text updates to: "Path to a .py file, or dotted module name (e.g. mypackage.tasks.report)"
- Checkbox: "Run as module (`-m`)" below the field
- No other modal changes

---

## Feature 2 — Execution Dots Timeline

### Goal

Show the last 30 run outcomes per job as a row of colored circles in the Jobs table, matching Airflow's DAG list view.

### Data

`/api/jobs` response extended: each job object includes `recent_runs` — an array of up to 30 objects `{id, status, started_at}`, ordered oldest→newest. Fetched in `list_jobs()` via `db.get_runs(j.id, limit=30)`.

### UI

- New **Runs** column added to the jobs table (after "Last Run", before "On")
- Each dot: `<span>` with `width:8px; height:8px; border-radius:50%` rendered inline
- Color map (reuses existing badge palette):
  - `success` → `#4ade80` (green)
  - `failed` → `#f87171` (red)
  - `running` → `#60a5fa` (blue, pulsing animation)
  - `crashed` → `#fbbf24` (amber)
  - `cancelled` → `#d1d5db` (grey)
- Dots ordered left (oldest) → right (newest); empty slots filled with grey hollow circles when fewer than 30 runs exist
- Hover tooltip: `title` attribute showing `status — HH:MM DD/MM` (browser native tooltip, no extra JS)
- X-axis: implicit via tooltip, no axis line rendered

---

## Feature 3 — Rename to Pulsar

### Scope

User-facing strings and CLI only. Internal Python module names, file names, and DB filename are **not** renamed (avoid needless churn; the package name in `pyproject.toml` becomes `pulsar`).

### Changes

| Location                         | From                                     | To                                        |
| -------------------------------- | ---------------------------------------- | ----------------------------------------- |
| `server.py` FastAPI title        | `"Pulsar"`                               | `"Pulsar"`                                |
| `server.py` HTML `<title>`       | `Pulsar`                                 | `Pulsar`                                  |
| `server.py` HTML `<h1>`          | `⚡ Pulsar`                              | `◉ Pulsar`                                |
| `main.py` argparse `prog`        | `"Pulsar"`                               | `"pulsar"`                                |
| `main.py` argparse `description` | `"Pulsar — …"`                           | `"Pulsar — …"`                            |
| `main.py` log messages           | `"Pulsar starting"` / `"Pulsar stopped"` | `"Pulsar starting"` / `"Pulsar stopped"`  |
| Log prefix `Pulsar`              | `logging.getLogger("Pulsar")`            | `logging.getLogger("pulsar")` (all files) |

---

## Feature 4 — Package Setup

### Goal

`pip install pulsar` (or `uv add pulsar`) installs the package and registers a `pulsar` CLI command.

### File Layout

Reorganize from flat root to `src/` layout:

```
src/
  pulsar/
    __init__.py       (empty)
    main.py
    db.py
    executor.py
    scheduler.py
    server.py
pyproject.toml
```

Old files at root are removed after moving.

### Internal Imports

All `from db import`, `from executor import`, etc. → `from .db import`, `from .executor import`, etc. (relative imports within the package).

### pyproject.toml

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "pulsar"
version = "0.1.0"
description = "Pulsar — lightweight Python job scheduler"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.110",
    "uvicorn[standard]>=0.29",
    "apscheduler>=3.10",
    "duckdb>=0.10",
    "pydantic>=2.0",
]

[project.scripts]
pulsar = "pulsar.main:main"

[tool.hatch.build.targets.wheel]
packages = ["src/pulsar"]
```

### Dev workflow

```bash
uv pip install -e .   # editable install
pulsar serve          # CLI works immediately
```

---

## Error Handling

- Module not found: surfaces as a non-zero exit code in the subprocess, captured as `status=failed` in the run — no special handling needed beyond what already exists
- Schema migration: `_init_schema` uses `CREATE TABLE IF NOT EXISTS` with the new column; existing DBs get the column at first boot

## Out of Scope

- Renaming the `.duckdb` file
- Renaming internal logger hierarchy beyond the top-level prefix
- Publishing to PyPI
