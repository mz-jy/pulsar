"""Cron scheduler — wraps APScheduler to drive the executor."""

import logging
from typing import Dict, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .db import Database, Job
from .executor import JobExecutor

logger = logging.getLogger("pulsar.scheduler")


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
            id=f"pulsar_{job.id}",
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
