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
