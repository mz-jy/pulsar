"""DuckDB storage layer for PyRunner."""

import duckdb
import uuid
import threading
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional, List


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id() -> str:
    return uuid.uuid4().hex[:8]


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


@dataclass
class JobRun:
    id: str
    job_id: str
    status: str  # running | success | failed | crashed | cancelled
    triggered_by: str  # scheduler | manual | cli
    started_at: Optional[str]
    finished_at: Optional[str]
    exit_code: Optional[int]
    stdout: str
    stderr: str
    pid: Optional[int]


class Database:
    """Thread-safe DuckDB wrapper. All mutations go through a lock."""

    def __init__(self, db_path: str = "pyrunner.duckdb"):
        self._lock = threading.Lock()
        self._conn = duckdb.connect(db_path)
        self._init_schema()

    # ── schema ──────────────────────────────────────────────────────────

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

    # ── helpers ─────────────────────────────────────────────────────────

    def _query(self, sql: str, params: list = None) -> list:
        with self._lock:
            return self._conn.execute(sql, params or []).fetchall()

    def _exec(self, sql: str, params: list = None):
        with self._lock:
            self._conn.execute(sql, params or [])

    # ── jobs CRUD ───────────────────────────────────────────────────────

    def add_job(self, name: str, script_path: str, cron_expression: str,
                args: str = "", run_as_module: bool = False) -> Job:
        jid = _gen_id()
        now = _now()
        self._exec(
            "INSERT INTO jobs VALUES (?,?,?,?,?,TRUE,?,?,?)",
            [jid, name, script_path, cron_expression, args, now, now, run_as_module],
        )
        return Job(jid, name, script_path, cron_expression, args, True, now, now, run_as_module)

    def get_jobs(self) -> List[Job]:
        return [Job(*r) for r in self._query("SELECT * FROM jobs ORDER BY name")]

    def get_job(self, jid: str) -> Optional[Job]:
        rows = self._query("SELECT * FROM jobs WHERE id = ?", [jid])
        return Job(*rows[0]) if rows else None

    def get_job_by_name(self, name: str) -> Optional[Job]:
        rows = self._query("SELECT * FROM jobs WHERE name = ?", [name])
        return Job(*rows[0]) if rows else None

    def update_job(self, jid: str, **kwargs):
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [_now(), jid]
        self._exec(f"UPDATE jobs SET {sets}, updated_at = ? WHERE id = ?", vals)

    def remove_job(self, jid: str):
        self._exec("DELETE FROM job_runs WHERE job_id = ?", [jid])
        self._exec("DELETE FROM jobs WHERE id = ?", [jid])

    def toggle_job(self, jid: str) -> Optional[bool]:
        job = self.get_job(jid)
        if not job:
            return None
        new_state = not job.enabled
        self.update_job(jid, enabled=new_state)
        return new_state

    # ── runs CRUD ───────────────────────────────────────────────────────

    def create_run(self, job_id: str, triggered_by: str = "scheduler") -> JobRun:
        rid = _gen_id()
        now = _now()
        self._exec(
            "INSERT INTO job_runs (id, job_id, status, triggered_by, started_at, stdout, stderr) "
            "VALUES (?, ?, 'running', ?, ?, '', '')",
            [rid, job_id, triggered_by, now],
        )
        return JobRun(rid, job_id, "running", triggered_by, now, None, None, "", "", None)

    def finish_run(self, rid: str, status: str, exit_code: int = None,
                   stdout: str = "", stderr: str = "", pid: int = None):
        self._exec(
            "UPDATE job_runs SET status=?, finished_at=?, exit_code=?, stdout=?, stderr=?, pid=? WHERE id=?",
            [status, _now(), exit_code, stdout[-50_000:] if stdout else "", stderr[-50_000:] if stderr else "", pid, rid],
        )

    def set_run_pid(self, rid: str, pid: int):
        self._exec("UPDATE job_runs SET pid = ? WHERE id = ?", [pid, rid])

    def get_runs(self, job_id: str = None, limit: int = 50) -> List[JobRun]:
        if job_id:
            rows = self._query(
                "SELECT * FROM job_runs WHERE job_id = ? ORDER BY started_at DESC LIMIT ?",
                [job_id, limit],
            )
        else:
            rows = self._query(
                "SELECT * FROM job_runs ORDER BY started_at DESC LIMIT ?", [limit],
            )
        return [JobRun(*r) for r in rows]

    def mark_stale_runs(self):
        """On startup, mark any 'running' rows as 'crashed' (previous process died)."""
        self._exec(
            "UPDATE job_runs SET status = 'crashed', finished_at = ? WHERE status = 'running'",
            [_now()],
        )

    def close(self):
        self._conn.close()
