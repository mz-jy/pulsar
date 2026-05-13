---
title: Pulsar PyPI Publishing Prep
date: 2026-05-13
status: approved
---

# PyPI Publishing Prep — pulsar-scheduler

## Goal

Prepare the Pulsar project for publication to PyPI as `pulsar-scheduler`. The Python import namespace (`import pulsar`) and CLI command (`pulsar`) remain unchanged.

## GitHub Repo

Set via `gh repo edit`:

- **Description:** `Lightweight Python job scheduler with cron, DuckDB, web UI, and CLI`
- **Topics:** `python`, `scheduler`, `cron`, `duckdb`, `fastapi`

## pyproject.toml Changes

- `name` → `"pulsar-scheduler"`
- `license = {text = "MIT"}`
- `authors = [{name = "mz-jy"}]`
- `keywords = ["scheduler", "cron", "job-runner", "duckdb", "process-manager"]`
- Classifiers:
  - `"Development Status :: 3 - Alpha"`
  - `"License :: OSI Approved :: MIT License"`
  - `"Programming Language :: Python :: 3"`
  - `"Programming Language :: Python :: 3.12"`
  - `"Topic :: Utilities"`
  - `"Operating System :: OS Independent"`
- `[project.urls]` → `Repository = "https://github.com/mz-jy/pulsar"`

## New Files

### LICENSE

MIT license, year 2026, author `mz-jy`.

### .gitignore

Covers standard Python artifacts:

```
dist/
build/
*.egg-info/
__pycache__/
.venv/
*.pyc
```

## README Updates

Quick Start section updated to reflect the installable package:

```bash
pip install pulsar-scheduler
pulsar serve
```

## Out of Scope

- GitHub Actions CI/CD (manual publish via `uv publish`)
- CHANGELOG
- Automated version bumping
