"""Job executor — runs Python scripts as subprocesses in background threads."""

import subprocess
import sys
import shlex
import threading
import logging
from typing import Dict, Optional

from .db import Database, Job, JobRun

logger = logging.getLogger("pulsar.executor")


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
