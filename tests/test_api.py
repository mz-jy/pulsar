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
