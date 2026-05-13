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
