"""Filesystem layout: data directory, DB, logs, PID (default ~/.pulsar/)."""

from __future__ import annotations

import os
from pathlib import Path


def pulsar_home() -> Path:
    return Path(os.environ.get("PULSAR_HOME", Path.home() / ".pulsar")).expanduser().resolve()


def ensure_pulsar_home() -> Path:
    home = pulsar_home()
    home.mkdir(parents=True, exist_ok=True)
    return home


def default_db_path() -> Path:
    return ensure_pulsar_home() / "pulsar.duckdb"


def log_path() -> Path:
    return ensure_pulsar_home() / "pulsar.log"


def pid_path() -> Path:
    return ensure_pulsar_home() / "pulsar.pid"
