import pytest


@pytest.fixture
def isolated_pulsar_home(monkeypatch, tmp_path):
    monkeypatch.setenv("PULSAR_HOME", str(tmp_path))
    yield tmp_path


def test_pulsar_home_env(isolated_pulsar_home):
    from pulsar.paths import default_db_path, log_path, pid_path, pulsar_home

    assert pulsar_home() == isolated_pulsar_home.resolve()
    assert default_db_path() == isolated_pulsar_home / "pulsar.duckdb"
    assert log_path() == isolated_pulsar_home / "pulsar.log"
    assert pid_path() == isolated_pulsar_home / "pulsar.pid"