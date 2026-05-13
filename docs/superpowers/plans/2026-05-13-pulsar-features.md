# Pulsar Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform Pulsar into Pulsar — an installable Python job scheduler with `python -m` module support and an Airflow-style execution timeline in the UI.

**Architecture:** Files move from project root into `src/pulsar/` with relative imports, pyproject.toml gains build metadata and entry points, executor branches on a new `run_as_module` field, and the FastAPI `list_jobs` endpoint adds `recent_runs` per job for the dots UI.

**Tech Stack:** Python 3.12, FastAPI, APScheduler 3.x, DuckDB 1.x, hatchling (build), pytest + httpx (tests)

---

## File Map

| File                          | Action        | Responsibility                       |
| ----------------------------- | ------------- | ------------------------------------ |
| `src/pulsar/__init__.py`      | Create        | Package marker (empty)               |
| `src/pulsar/db.py`            | Move + extend | DuckDB layer + `run_as_module` field |
| `src/pulsar/executor.py`      | Move + extend | Subprocess runner + `_build_cmd`     |
| `src/pulsar/scheduler.py`     | Move          | APScheduler wrapper                  |
| `src/pulsar/server.py`        | Move + extend | FastAPI app + UI HTML                |
| `src/pulsar/main.py`          | Move + extend | CLI entry point                      |
| `tests/__init__.py`           | Create        | Test package marker                  |
| `tests/test_db.py`            | Create        | DB layer tests                       |
| `tests/test_executor.py`      | Create        | Executor command-build tests         |
| `tests/test_api.py`           | Create        | API endpoint tests                   |
| `pyproject.toml`              | Rewrite       | Build metadata, deps, entry point    |
| `main.py` `db.py` etc. (root) | Delete        | Superseded by src/pulsar/            |

---

## Task 1: Restructure into src/pulsar package

**Files:**

- Create: `src/pulsar/__init__.py`
- Create: `src/pulsar/db.py`
- Create: `src/pulsar/executor.py`
- Create: `src/pulsar/scheduler.py`
- Create: `src/pulsar/server.py`
- Create: `src/pulsar/main.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Create package directory and empty init**

```bash
mkdir -p src/pulsar
touch src/pulsar/__init__.py
```

- [ ] **Step 2: Write src/pulsar/db.py** (identical to root db.py — no cross-module imports)

```bash
cp db.py src/pulsar/db.py
```

- [ ] **Step 3: Write src/pulsar/executor.py** (update import)

Replace the first import line:

```python
# OLD
from db import Database, Job, JobRun
# NEW
from .db import Database, Job, JobRun
```

Full file at `src/pulsar/executor.py`:

```python
"""Job executor — runs Python scripts as subprocesses in background threads."""

import subprocess
import sys
import shlex
import threading
import logging
from typing import Dict, Optional

from .db import Database, Job, JobRun

logger = logging.getLogger("Pulsar.executor")


class JobExecutor:
    def __init__(self, db: Database):
        self.db = db
        self._procs: Dict[str, subprocess.Popen] = {}   # run_id → Popen
        self._lock = threading.Lock()

    # ── public API ──────────────────────────────────────────────────────

    def execute(self, job_id: str, triggered_by: str = "scheduler") -> Optional[str]:
        """Kick off a job. Returns the run_id or None if the job was skipped."""
        job = self.db.get_job(job_id)
        if not job:
            logger.error("Job %s not found — skipping", job_id)
            return None
        if not job.enabled and triggered_by == "scheduler":
            logger.debug("Job %s disabled — skipping scheduled run", job.name)
            return None

        run = self.db.create_run(job_id, triggered_by)
        t = threading.Thread(target=self._run, args=(job, run), daemon=True)
        t.start()
        return run.id

    def cancel(self, run_id: str) -> bool:
        with self._lock:
            proc = self._procs.get(run_id)
        if proc is None:
            return False
        proc.terminate()
        self.db.finish_run(run_id, status="cancelled", pid=proc.pid)
        with self._lock:
            self._procs.pop(run_id, None)
        return True

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._procs)

    @property
    def active_run_ids(self) -> list:
        with self._lock:
            return list(self._procs.keys())

    # ── internal ────────────────────────────────────────────────────────

    def _run(self, job: Job, run: JobRun):
        cmd = [sys.executable, job.script_path]
        if job.args:
            cmd.extend(shlex.split(job.args))

        try:
            logger.info("▶ Starting  %s  (run %s): %s", job.name, run.id, " ".join(cmd))
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            with self._lock:
                self._procs[run.id] = proc
            self.db.set_run_pid(run.id, proc.pid)

            stdout, stderr = proc.communicate()

            status = "success" if proc.returncode == 0 else "failed"
            self.db.finish_run(
                run.id,
                status=status,
                exit_code=proc.returncode,
                stdout=stdout or "",
                stderr=stderr or "",
                pid=proc.pid,
            )
            logger.info("■ Finished  %s  (run %s) → %s  (exit %s)", job.name, run.id, status, proc.returncode)

        except Exception as exc:
            self.db.finish_run(run.id, status="failed", stderr=str(exc))
            logger.exception("✗ Error running %s (run %s): %s", job.name, run.id, exc)

        finally:
            with self._lock:
                self._procs.pop(run.id, None)
```

- [ ] **Step 4: Write src/pulsar/scheduler.py** (update imports)

```python
"""Cron scheduler — wraps APScheduler to drive the executor."""

import logging
from typing import Dict, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .db import Database, Job
from .executor import JobExecutor

logger = logging.getLogger("Pulsar.scheduler")


class JobScheduler:
    def __init__(self, executor: JobExecutor):
        self.executor = executor
        self._scheduler = BackgroundScheduler(daemon=True)
        self._map: Dict[str, str] = {}  # job.id → apscheduler job id

    # ── lifecycle ───────────────────────────────────────────────────────

    def start(self):
        self._scheduler.start()
        logger.info("Scheduler started")

    def stop(self):
        self._scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")

    # ── job management ──────────────────────────────────────────────────

    def schedule(self, job: Job):
        """Add or replace a job's cron trigger."""
        self.unschedule(job.id)
        try:
            trigger = CronTrigger.from_crontab(job.cron_expression)
        except ValueError as exc:
            logger.error("Bad cron '%s' for job %s: %s", job.cron_expression, job.name, exc)
            return
        ap_job = self._scheduler.add_job(
            self.executor.execute,
            trigger=trigger,
            args=[job.id, "scheduler"],
            id=f"Pulsar_{job.id}",
            name=job.name,
            replace_existing=True,
        )
        self._map[job.id] = ap_job.id
        logger.info("Scheduled  %s  cron='%s'", job.name, job.cron_expression)

    def unschedule(self, job_id: str):
        ap_id = self._map.pop(job_id, None)
        if ap_id:
            try:
                self._scheduler.remove_job(ap_id)
            except Exception:
                pass

    def reload(self, db: Database):
        """(Re)load all jobs from the database."""
        for job in db.get_jobs():
            if job.enabled:
                self.schedule(job)
            else:
                self.unschedule(job.id)
        logger.info("Reloaded %d jobs from DB", len(db.get_jobs()))

    # ── queries ─────────────────────────────────────────────────────────

    def get_next_runs(self) -> Dict[str, str]:
        """Return {job_id: next_run_iso} for every scheduled job."""
        result = {}
        for job_id, ap_id in self._map.items():
            try:
                ap_job = self._scheduler.get_job(ap_id)
                if ap_job and ap_job.next_run_time:
                    result[job_id] = ap_job.next_run_time.isoformat()
            except Exception:
                pass
        return result
```

- [ ] **Step 5: Write src/pulsar/server.py** (update imports only — all other changes come in later tasks)

Replace the three import lines at the top:

```python
# OLD
from db import Database
from executor import JobExecutor
from scheduler import JobScheduler
# NEW
from .db import Database
from .executor import JobExecutor
from .scheduler import JobScheduler
```

Full file at `src/pulsar/server.py` — copy `server.py` from root, then apply the import change above.

- [ ] **Step 6: Write src/pulsar/main.py** (update imports only)

Replace the four import lines:

```python
# OLD
from db import Database
from executor import JobExecutor
from scheduler import JobScheduler
from server import create_app
# NEW
from .db import Database
from .executor import JobExecutor
from .scheduler import JobScheduler
from .server import create_app
```

Full file at `src/pulsar/main.py` — copy `main.py` from root, then apply the import change above.

- [ ] **Step 7: Rewrite pyproject.toml**

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
    "fastapi>=0.111",
    "uvicorn[standard]>=0.30",
    "apscheduler>=3.10,<4",
    "duckdb>=1.0",
    "pydantic>=2.0",
]

[project.scripts]
pulsar = "pulsar.main:main"

[project.optional-dependencies]
dev = ["pytest>=8.0", "httpx>=0.27"]

[tool.hatch.build.targets.wheel]
packages = ["src/pulsar"]
```

- [ ] **Step 8: Install the package in editable mode**

```bash
uv pip install -e ".[dev]"
```

Expected output: successful install, no errors.

- [ ] **Step 9: Verify the CLI entry point works**

```bash
pulsar --help
```

Expected output:

```
usage: Pulsar [-h] [--db DB] {serve,add,list,trigger,remove,history} ...

Pulsar — lightweight Python process manager
...
```

(The name still says "Pulsar" — that gets fixed in Task 2.)

- [ ] **Step 10: Delete the old root-level source files**

```bash
rm main.py db.py executor.py scheduler.py server.py
```

- [ ] **Step 11: Re-verify CLI still works**

```bash
pulsar --help
```

Expected: same output as Step 9.

- [ ] **Step 12: Commit**

```bash
git add src/ pyproject.toml .gitignore .python-version
git rm main.py db.py executor.py scheduler.py server.py
git commit -m "feat: restructure into src/pulsar package with entry point"
```

---

## Task 2: Rename to Pulsar

**Files:**

- Modify: `src/pulsar/main.py`
- Modify: `src/pulsar/executor.py`
- Modify: `src/pulsar/scheduler.py`
- Modify: `src/pulsar/server.py`

- [ ] **Step 1: Update logger names and CLI strings in src/pulsar/main.py**

Change these lines:

```python
# OLD
log = logging.getLogger("Pulsar")
# NEW
log = logging.getLogger("pulsar")
```

```python
# OLD
log.info("Pulsar starting → http://%s:%s", args.host, args.port)
# NEW
log.info("Pulsar starting → http://%s:%s", args.host, args.port)
```

```python
# OLD
log.info("Pulsar stopped.")
# NEW
log.info("Pulsar stopped.")
```

```python
# OLD
    p = argparse.ArgumentParser(
        prog="Pulsar",
        description="Pulsar — lightweight Python process manager",
# NEW
    p = argparse.ArgumentParser(
        prog="pulsar",
        description="Pulsar — lightweight Python job scheduler",
```

Also update the module docstring at the top of the file:

```python
# OLD
"""
Pulsar — a lightweight Python process manager.

Usage:
    python main.py serve [--host 0.0.0.0] [--port 8844]
    python main.py add <name> <script> <cron> [--args "..."]
    python main.py list
    python main.py trigger <job_id> [--host localhost] [--port 8844]
    python main.py remove <job_id>
    python main.py history [--job-id <id>] [--limit 20]
"""
# NEW
"""
Pulsar — a lightweight Python job scheduler.

Usage:
    pulsar serve [--host 0.0.0.0] [--port 8844]
    pulsar add <name> <script> <cron> [--args "..."]
    pulsar list
    pulsar trigger <job_id> [--host localhost] [--port 8844]
    pulsar remove <job_id>
    pulsar history [--job-id <id>] [--limit 20]
"""
```

- [ ] **Step 2: Update logger name in src/pulsar/executor.py**

```python
# OLD
logger = logging.getLogger("Pulsar.executor")
# NEW
logger = logging.getLogger("pulsar.executor")
```

- [ ] **Step 3: Update logger name in src/pulsar/scheduler.py**

```python
# OLD
logger = logging.getLogger("Pulsar.scheduler")
# NEW
logger = logging.getLogger("pulsar.scheduler")
```

Also update the APScheduler job id prefix (cosmetic but consistent):

```python
# OLD
            id=f"Pulsar_{job.id}",
# NEW
            id=f"pulsar_{job.id}",
```

- [ ] **Step 4: Update FastAPI title and HTML in src/pulsar/server.py**

```python
# OLD
    app = FastAPI(title="Pulsar", docs_url="/docs")
# NEW
    app = FastAPI(title="Pulsar", docs_url="/docs")
```

In `_HTML`, update the `<title>` and `<h1>`:

```html
<!-- OLD -->
<title>Pulsar</title>
<!-- NEW -->
<title>Pulsar</title>
```

```html
<!-- OLD -->
<h1><span>⚡</span> Pulsar</h1>
<!-- NEW -->
<h1><span>◉</span> Pulsar</h1>
```

- [ ] **Step 5: Verify rename**

```bash
pulsar --help
```

Expected:

```
usage: pulsar [-h] [--db DB] {serve,add,list,trigger,remove,history} ...

Pulsar — lightweight Python job scheduler
```

- [ ] **Step 6: Commit**

```bash
git add src/pulsar/main.py src/pulsar/executor.py src/pulsar/scheduler.py src/pulsar/server.py
git commit -m "feat: rename to Pulsar across all user-facing strings"
```

---

## Task 3: DB migration for run_as_module

**Files:**

- Create: `tests/__init__.py`
- Create: `tests/test_db.py`
- Modify: `src/pulsar/db.py`

- [ ] **Step 1: Create test package**

```bash
mkdir -p tests
touch tests/__init__.py
```

- [ ] **Step 2: Write the failing tests at tests/test_db.py**

```python
import pytest
import duckdb
from pulsar.db import Database


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "test.duckdb"))
    yield d
    d.close()


def test_add_job_defaults_no_module(db):
    job = db.add_job("test", "script.py", "* * * * *")
    assert job.run_as_module is False


def test_add_job_as_module(db):
    job = db.add_job("modtest", "mypackage.tasks", "* * * * *", run_as_module=True)
    assert job.run_as_module is True


def test_get_job_preserves_module_flag(db):
    db.add_job("modtest", "mypackage.tasks", "* * * * *", run_as_module=True)
    job = db.get_job_by_name("modtest")
    assert job.run_as_module is True


def test_existing_db_migration(tmp_path):
    """An 8-column DB (no run_as_module) is migrated safely on open."""
    conn = duckdb.connect(str(tmp_path / "old.duckdb"))
    conn.execute("""
        CREATE TABLE jobs (
            id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL UNIQUE,
            script_path VARCHAR NOT NULL, cron_expression VARCHAR NOT NULL,
            args VARCHAR DEFAULT '', enabled BOOLEAN DEFAULT TRUE,
            created_at VARCHAR, updated_at VARCHAR
        )
    """)
    conn.execute(
        "INSERT INTO jobs VALUES ('abc','legacy','old.py','* * * * *','',TRUE,'now','now')"
    )
    conn.execute("""
        CREATE TABLE job_runs (
            id VARCHAR PRIMARY KEY, job_id VARCHAR NOT NULL,
            status VARCHAR NOT NULL, triggered_by VARCHAR DEFAULT 'scheduler',
            started_at VARCHAR, finished_at VARCHAR, exit_code INTEGER,
            stdout VARCHAR DEFAULT '', stderr VARCHAR DEFAULT '', pid INTEGER
        )
    """)
    conn.close()

    db = Database(str(tmp_path / "old.duckdb"))
    job = db.get_job("abc")
    assert job is not None
    assert job.run_as_module is False
    db.close()
```

- [ ] **Step 3: Run tests to confirm they fail**

```bash
pytest tests/test_db.py -v
```

Expected: `AttributeError: 'Job' object has no attribute 'run_as_module'` or similar.

- [ ] **Step 4: Update src/pulsar/db.py — Job dataclass, schema, and add_job**

Add `run_as_module: bool = False` as the 9th field of `Job`:

```python
@dataclass
class Job:
    id: str
    name: str
    script_path: str
    cron_expression: str
    args: str
    enabled: bool
    created_at: str
    updated_at: str
    run_as_module: bool = False
```

Update `_init_schema` to include the column in CREATE TABLE and add the ALTER TABLE migration:

```python
    def _init_schema(self):
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id          VARCHAR PRIMARY KEY,
                    name        VARCHAR NOT NULL UNIQUE,
                    script_path VARCHAR NOT NULL,
                    cron_expression VARCHAR NOT NULL,
                    args        VARCHAR DEFAULT '',
                    enabled     BOOLEAN DEFAULT TRUE,
                    created_at  VARCHAR,
                    updated_at  VARCHAR,
                    run_as_module BOOLEAN DEFAULT FALSE
                )
            """)
            self._conn.execute(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS run_as_module BOOLEAN DEFAULT FALSE"
            )
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS job_runs (
                    id           VARCHAR PRIMARY KEY,
                    job_id       VARCHAR NOT NULL,
                    status       VARCHAR NOT NULL,
                    triggered_by VARCHAR DEFAULT 'scheduler',
                    started_at   VARCHAR,
                    finished_at  VARCHAR,
                    exit_code    INTEGER,
                    stdout       VARCHAR DEFAULT '',
                    stderr       VARCHAR DEFAULT '',
                    pid          INTEGER
                )
            """)
```

Update `add_job` to accept and persist `run_as_module`:

```python
    def add_job(self, name: str, script_path: str, cron_expression: str,
                args: str = "", run_as_module: bool = False) -> Job:
        jid = _gen_id()
        now = _now()
        self._exec(
            "INSERT INTO jobs VALUES (?,?,?,?,?,TRUE,?,?,?)",
            [jid, name, script_path, cron_expression, args, now, now, run_as_module],
        )
        return Job(jid, name, script_path, cron_expression, args, True, now, now, run_as_module)
```

- [ ] **Step 5: Run tests to confirm they pass**

```bash
pytest tests/test_db.py -v
```

Expected:

```
tests/test_db.py::test_add_job_defaults_no_module PASSED
tests/test_db.py::test_add_job_as_module PASSED
tests/test_db.py::test_get_job_preserves_module_flag PASSED
tests/test_db.py::test_existing_db_migration PASSED
```

- [ ] **Step 6: Commit**

```bash
git add tests/__init__.py tests/test_db.py src/pulsar/db.py
git commit -m "feat: add run_as_module field to jobs with schema migration"
```

---

## Task 4: Executor \_build_cmd + module support

**Files:**

- Create: `tests/test_executor.py`
- Modify: `src/pulsar/executor.py`

- [ ] **Step 1: Write the failing tests at tests/test_executor.py**

```python
import sys
import pytest
from unittest.mock import MagicMock
from pulsar.db import Job
from pulsar.executor import JobExecutor


@pytest.fixture
def executor():
    return JobExecutor(MagicMock())


def make_job(**kwargs):
    defaults = dict(
        id="abc", name="test", script_path="script.py",
        cron_expression="* * * * *", args="", enabled=True,
        created_at="now", updated_at="now", run_as_module=False,
    )
    defaults.update(kwargs)
    return Job(**defaults)


def test_build_cmd_script(executor):
    job = make_job(script_path="script.py", run_as_module=False)
    assert executor._build_cmd(job) == [sys.executable, "script.py"]


def test_build_cmd_module(executor):
    job = make_job(script_path="mypackage.tasks", run_as_module=True)
    assert executor._build_cmd(job) == [sys.executable, "-m", "mypackage.tasks"]


def test_build_cmd_script_with_args(executor):
    job = make_job(script_path="script.py", args="--verbose --date today", run_as_module=False)
    assert executor._build_cmd(job) == [
        sys.executable, "script.py", "--verbose", "--date", "today"
    ]


def test_build_cmd_module_with_args(executor):
    job = make_job(script_path="mypackage.tasks", args="--env prod", run_as_module=True)
    assert executor._build_cmd(job) == [
        sys.executable, "-m", "mypackage.tasks", "--env", "prod"
    ]
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_executor.py -v
```

Expected: `AttributeError: 'JobExecutor' object has no attribute '_build_cmd'`

- [ ] **Step 3: Add \_build_cmd to src/pulsar/executor.py and update \_run**

Add this method inside `JobExecutor` (before `_run`):

```python
    def _build_cmd(self, job: Job) -> list:
        if job.run_as_module:
            cmd = [sys.executable, "-m", job.script_path]
        else:
            cmd = [sys.executable, job.script_path]
        if job.args:
            cmd.extend(shlex.split(job.args))
        return cmd
```

Update the start of `_run` to use `_build_cmd` (replace the existing `cmd = ...` lines):

```python
    def _run(self, job: Job, run: JobRun):
        cmd = self._build_cmd(job)

        try:
            logger.info("▶ Starting  %s  (run %s): %s", job.name, run.id, " ".join(cmd))
            # ... rest of _run unchanged
```

Remove the now-redundant lines from `_run`:

```python
# DELETE these two lines that were at the top of _run:
        cmd = [sys.executable, job.script_path]
        if job.args:
            cmd.extend(shlex.split(job.args))
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
pytest tests/test_executor.py -v
```

Expected:

```
tests/test_executor.py::test_build_cmd_script PASSED
tests/test_executor.py::test_build_cmd_module PASSED
tests/test_executor.py::test_build_cmd_script_with_args PASSED
tests/test_executor.py::test_build_cmd_module_with_args PASSED
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_executor.py src/pulsar/executor.py
git commit -m "feat: add _build_cmd with python -m module support"
```

---

## Task 5: API + CLI module support

**Files:**

- Create: `tests/test_api.py`
- Modify: `src/pulsar/server.py`
- Modify: `src/pulsar/main.py`

- [ ] **Step 1: Write the failing API tests at tests/test_api.py**

```python
import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from pulsar.db import Database
from pulsar.executor import JobExecutor
from pulsar.server import create_app


@pytest.fixture
def client(tmp_path):
    db = Database(str(tmp_path / "test.duckdb"))
    executor = JobExecutor(db)
    scheduler = MagicMock()
    scheduler.get_next_runs.return_value = {}
    app = create_app(db, executor, scheduler)
    with TestClient(app) as c:
        yield c, db
    db.close()


def test_add_job_with_module_flag(client):
    c, db = client
    resp = c.post("/api/jobs", json={
        "name": "mod-job",
        "script_path": "mypackage.tasks",
        "cron_expression": "* * * * *",
        "run_as_module": True,
    })
    assert resp.status_code == 200
    job = db.get_job_by_name("mod-job")
    assert job.run_as_module is True


def test_add_job_default_not_module(client):
    c, db = client
    resp = c.post("/api/jobs", json={
        "name": "script-job",
        "script_path": "script.py",
        "cron_expression": "* * * * *",
    })
    assert resp.status_code == 200
    job = db.get_job_by_name("script-job")
    assert job.run_as_module is False


def test_list_jobs_includes_run_as_module(client):
    c, db = client
    c.post("/api/jobs", json={
        "name": "j1", "script_path": "s.py",
        "cron_expression": "* * * * *", "run_as_module": True,
    })
    resp = c.get("/api/jobs")
    assert resp.status_code == 200
    jobs = resp.json()
    assert len(jobs) == 1
    assert jobs[0]["run_as_module"] is True
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_api.py::test_add_job_with_module_flag tests/test_api.py::test_list_jobs_includes_run_as_module -v
```

Expected: tests fail because `AddJobRequest` has no `run_as_module` field and the list response doesn't include it.

- [ ] **Step 3: Update AddJobRequest in src/pulsar/server.py**

```python
class AddJobRequest(BaseModel):
    name: str
    script_path: str
    cron_expression: str
    args: str = ""
    run_as_module: bool = False
```

- [ ] **Step 4: Update add_job endpoint to pass run_as_module**

```python
    @app.post("/api/jobs")
    async def add_job(req: AddJobRequest):
        try:
            job = db.add_job(req.name, req.script_path, req.cron_expression,
                             req.args, req.run_as_module)
            if job.enabled:
                scheduler.schedule(job)
            return {"id": job.id, "name": job.name}
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
```

- [ ] **Step 5: Update list_jobs to include run_as_module in response**

In the `list_jobs` endpoint, add `"run_as_module": j.run_as_module` to the dict:

```python
        out.append({
            "id": j.id, "name": j.name, "script_path": j.script_path,
            "cron_expression": j.cron_expression, "args": j.args,
            "enabled": j.enabled, "created_at": j.created_at,
            "run_as_module": j.run_as_module,
            "next_run": next_runs.get(j.id), "last_run": last,
        })
```

- [ ] **Step 6: Run API tests to confirm they pass**

```bash
pytest tests/test_api.py::test_add_job_with_module_flag tests/test_api.py::test_add_job_default_not_module tests/test_api.py::test_list_jobs_includes_run_as_module -v
```

Expected: all three PASS.

- [ ] **Step 7: Add --module flag to CLI in src/pulsar/main.py**

In `cmd_add`, update the print to show the mode:

```python
def cmd_add(args):
    """Register a new job."""
    db = Database(args.db)
    try:
        job = db.add_job(args.name, args.script, args.cron, args.args or "", args.module)
        mode = "module (-m)" if args.module else "script"
        print(f"✓ Added job '{job.name}' (id={job.id}, cron='{job.cron_expression}', type={mode})")
        _ping_reload(args)
    except Exception as exc:
        print(f"✗ Error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()
```

In the `add` subparser section, add the `--module` flag and update the script help text:

```python
    # add
    s = sub.add_parser("add", help="Register a new job")
    s.add_argument("name", help="Unique job name")
    s.add_argument("script", help="Path to a Python script or dotted module name")
    s.add_argument("cron", help="5-field cron expression (quote it)")
    s.add_argument("--args", default="", help="Extra CLI args for the script")
    s.add_argument("--module", action="store_true", default=False,
                   help="Run as module: python -m <name>")
    s.add_argument("--host", default="localhost")
    s.add_argument("--port", type=int, default=8844)
```

- [ ] **Step 8: Run full test suite**

```bash
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 9: Commit**

```bash
git add tests/test_api.py src/pulsar/server.py src/pulsar/main.py
git commit -m "feat: expose run_as_module in API, add --module CLI flag"
```

---

## Task 6: UI — module checkbox in Add Job modal

**Files:**

- Modify: `src/pulsar/server.py` (the `_HTML` string)

- [ ] **Step 1: Update the Script Path field label and hint in \_HTML**

Find this block in `_HTML`:

```html
<div class="fg">
  <label>Script Path</label
  ><input id="fS" placeholder="/home/user/scripts/report.py" />
  <div class="hint">Absolute or relative path to a Python script</div>
</div>
```

Replace with:

```html
<div class="fg">
  <label>Script / Module</label
  ><input
    id="fS"
    placeholder="/home/user/scripts/report.py or mypackage.tasks"
  />
  <div class="hint">
    Path to a .py file, or dotted module name (e.g. mypackage.tasks.report)
  </div>
</div>
<div class="fg">
  <label
    style="display:flex;align-items:center;gap:6px;font-size:12px;font-weight:600;color:#374151"
    ><input type="checkbox" id="fM" style="width:auto;margin:0" /> Run as module
    (<code>-m</code>)</label
  >
</div>
```

- [ ] **Step 2: Update doAdd() in the script block to include run_as_module**

Find:

```javascript
async function doAdd() {
  const [n, s, c, a] = [
    $("fN").value.trim(),
    $("fS").value.trim(),
    $("fC").value.trim(),
    $("fA").value.trim(),
  ];
  if (!n || !s || !c) {
    alert("Name, script and cron are required.");
    return;
  }
  const r = await fetch("/api/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name: n,
      script_path: s,
      cron_expression: c,
      args: a,
    }),
  });
  if (!r.ok) {
    const e = await r.json();
    alert(e.detail || "Error");
    return;
  }
  closeAdd();
  ["fN", "fS", "fC", "fA"].forEach((i) => ($(i).value = ""));
  refresh();
}
```

Replace with:

```javascript
async function doAdd() {
  const [n, s, c, a, m] = [
    $("fN").value.trim(),
    $("fS").value.trim(),
    $("fC").value.trim(),
    $("fA").value.trim(),
    $("fM").checked,
  ];
  if (!n || !s || !c) {
    alert("Name, script and cron are required.");
    return;
  }
  const r = await fetch("/api/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name: n,
      script_path: s,
      cron_expression: c,
      args: a,
      run_as_module: m,
    }),
  });
  if (!r.ok) {
    const e = await r.json();
    alert(e.detail || "Error");
    return;
  }
  closeAdd();
  ["fN", "fS", "fC", "fA"].forEach((i) => ($(i).value = ""));
  $("fM").checked = false;
  refresh();
}
```

- [ ] **Step 3: Commit**

```bash
git add src/pulsar/server.py
git commit -m "feat: add module checkbox to Add Job modal"
```

---

## Task 7: API — recent_runs per job

**Files:**

- Modify: `tests/test_api.py`
- Modify: `src/pulsar/server.py`

- [ ] **Step 1: Add failing tests to tests/test_api.py**

Append these four tests to the existing file:

```python
def test_list_jobs_includes_recent_runs_key(client):
    c, db = client
    c.post("/api/jobs", json={"name": "j1", "script_path": "s.py", "cron_expression": "* * * * *"})
    resp = c.get("/api/jobs")
    assert "recent_runs" in resp.json()[0]


def test_recent_runs_empty_for_new_job(client):
    c, db = client
    c.post("/api/jobs", json={"name": "j1", "script_path": "s.py", "cron_expression": "* * * * *"})
    resp = c.get("/api/jobs")
    assert resp.json()[0]["recent_runs"] == []


def test_recent_runs_ordered_oldest_first(client):
    c, db = client
    resp = c.post("/api/jobs", json={"name": "j1", "script_path": "s.py", "cron_expression": "* * * * *"})
    job_id = resp.json()["id"]
    r1 = db.create_run(job_id, "manual")
    db.finish_run(r1.id, "success")
    r2 = db.create_run(job_id, "manual")
    db.finish_run(r2.id, "failed")
    recent = c.get("/api/jobs").json()[0]["recent_runs"]
    assert len(recent) == 2
    assert recent[0]["id"] == r1.id   # oldest first (left dot)
    assert recent[1]["id"] == r2.id   # newest last (right dot)


def test_recent_runs_capped_at_30(client):
    c, db = client
    resp = c.post("/api/jobs", json={"name": "j1", "script_path": "s.py", "cron_expression": "* * * * *"})
    job_id = resp.json()["id"]
    for _ in range(35):
        r = db.create_run(job_id, "manual")
        db.finish_run(r.id, "success")
    recent = c.get("/api/jobs").json()[0]["recent_runs"]
    assert len(recent) == 30
```

- [ ] **Step 2: Run new tests to confirm they fail**

```bash
pytest tests/test_api.py::test_list_jobs_includes_recent_runs_key tests/test_api.py::test_recent_runs_empty_for_new_job -v
```

Expected: `KeyError` or `AssertionError` — `recent_runs` not present in response.

- [ ] **Step 3: Update list_jobs in src/pulsar/server.py**

Replace the existing `list_jobs` endpoint:

```python
    @app.get("/api/jobs")
    async def list_jobs():
        jobs = db.get_jobs()
        next_runs = scheduler.get_next_runs()
        out = []
        for j in jobs:
            last_runs = db.get_runs(j.id, limit=30)
            last = None
            if last_runs:
                lr = last_runs[0]
                last = {"id": lr.id, "status": lr.status,
                        "started_at": lr.started_at, "finished_at": lr.finished_at}
            recent = [
                {"id": r.id, "status": r.status, "started_at": r.started_at}
                for r in reversed(last_runs)
            ]
            out.append({
                "id": j.id, "name": j.name, "script_path": j.script_path,
                "cron_expression": j.cron_expression, "args": j.args,
                "enabled": j.enabled, "created_at": j.created_at,
                "run_as_module": j.run_as_module,
                "next_run": next_runs.get(j.id), "last_run": last,
                "recent_runs": recent,
            })
        return out
```

Note: `get_runs` returns newest-first (DESC). `reversed(last_runs)` gives oldest-first order for the dots (left=oldest, right=newest).

- [ ] **Step 4: Run all tests to confirm they pass**

```bash
pytest tests/ -v
```

Expected: all tests pass including the four new ones.

- [ ] **Step 5: Commit**

```bash
git add tests/test_api.py src/pulsar/server.py
git commit -m "feat: include recent_runs (last 30, oldest-first) in /api/jobs response"
```

---

## Task 8: UI — execution dots column

**Files:**

- Modify: `src/pulsar/server.py` (the `_HTML` string)

- [ ] **Step 1: Add renderDots helper function to the script block in \_HTML**

Find the line `let R=[];` at the top of the `<script>` block and insert `renderDots` after the existing helper functions (after the `function bg(...)` line):

```javascript
function renderDots(runs) {
  const colMap = {
    success: "#4ade80",
    failed: "#f87171",
    running: "#60a5fa",
    crashed: "#fbbf24",
    cancelled: "#d1d5db",
  };
  const filled = (runs || []).map((r) => {
    const col = colMap[r.status] || "#d1d5db";
    const tip = `${r.status} — ${r.started_at ? new Date(r.started_at).toLocaleString() : "-"}`;
    const anim = r.status === "running" ? ";animation:pulse 2s infinite" : "";
    return `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${col};margin:1px${anim}" title="${tip}"></span>`;
  });
  const empty = 30 - filled.length;
  const empties =
    empty > 0
      ? Array(empty).fill(
          '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;border:1px solid #e5e7eb;background:transparent;margin:1px"></span>',
        )
      : [];
  return [...empties, ...filled].join("");
}
```

- [ ] **Step 2: Add "Runs" column header to the jobs table**

Find:

```html
<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Script</th>
      <th>Cron</th>
      <th>Next Run</th>
      <th>Last Run</th>
      <th>On</th>
      <th>Actions</th>
    </tr>
  </thead>
</table>
```

Replace with:

```html
<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Script</th>
      <th>Cron</th>
      <th>Next Run</th>
      <th>Last Run</th>
      <th>Runs</th>
      <th>On</th>
      <th>Actions</th>
    </tr>
  </thead>
</table>
```

- [ ] **Step 3: Add dots cell to each job row in fJobs()**

Find the `tb.innerHTML=jobs.map(j=>\`<tr>` block. The rows currently end with the toggle and actions cells. Add the dots cell before the toggle cell.

Find this part of the row template:

```javascript
<td>${j.last_run?bg(j.last_run.status)+' '+ago(j.last_run.started_at):'-'}</td>
<td><input type="checkbox" class="tgl" ${j.enabled?'checked':''} onchange="doToggle('${j.id}')"></td>
```

Replace with:

```javascript
<td>${j.last_run?bg(j.last_run.status)+' '+ago(j.last_run.started_at):'-'}</td>
<td style="white-space:nowrap">${renderDots(j.recent_runs)}</td>
<td><input type="checkbox" class="tgl" ${j.enabled?'checked':''} onchange="doToggle('${j.id}')"></td>
```

- [ ] **Step 4: Run full test suite to confirm nothing broke**

```bash
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 5: Start the server and visually verify the dots appear**

```bash
pulsar serve
```

Open `http://localhost:8844` in a browser. Add a job, trigger it a few times, confirm colored dots appear in the Runs column of the jobs table.

- [ ] **Step 6: Commit**

```bash
git add src/pulsar/server.py
git commit -m "feat: add Airflow-style execution dots to jobs table (last 30 runs)"
```

---

## Final Verification

- [ ] **Run full test suite one last time**

```bash
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Smoke test the CLI**

```bash
pulsar --help
pulsar add test-job mypackage.tasks "* * * * *" --module
pulsar list
```

Expected: job appears with module type, no errors.
