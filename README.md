# ⚡ Pulsar

A lightweight Python process manager with DuckDB persistence, cron scheduling, a web UI, and a CLI.

## Features

- **Cron scheduling** — register Python scripts with standard 5-field cron expressions
- **DuckDB storage** — all jobs and run history persisted in a single `.duckdb` file
- **Web UI** — real-time dashboard at `http://localhost:8844` (auto-refreshes every 5s)
- **REST API** — full CRUD on jobs and runs at `/api/*` (Swagger docs at `/docs`)
- **CLI** — add, list, trigger, remove jobs and view history from the terminal
- **Manual triggers** — run any job on-demand from the UI or CLI
- **Log capture** — stdout/stderr captured per run and viewable in the UI
- **Process tracking** — PIDs tracked, stale runs marked as `crashed` on restart

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start the server
python main.py serve

# 3. Open http://localhost:8844 in your browser
```

## Project Structure

```
Pulsar/
├── main.py           # Entry point — CLI + server bootstrap
├── db.py             # DuckDB schema & thread-safe queries
├── executor.py       # Subprocess execution in background threads
├── scheduler.py      # APScheduler cron wrapper
├── server.py         # FastAPI app + embedded HTML UI
├── requirements.txt
├── example_job.py    # Sample job (succeeds)
└── example_fail.py   # Sample job (fails — for testing)
```

## CLI Usage

### Start the server

```bash
python main.py serve [--host 0.0.0.0] [--port 8844] [--db Pulsar.duckdb]
```

### Register a job

```bash
# Run example_job.py every 5 minutes
python main.py add my-job ./example_job.py "*/5 * * * *"

# With extra arguments
python main.py add etl-daily ./etl.py "0 2 * * *" --args "--env prod --verbose"
```

### List jobs

```bash
python main.py list
```

### Trigger a job manually

```bash
python main.py trigger <job_id>
```

### Remove a job

```bash
python main.py remove <job_id>
```

### View run history

```bash
python main.py history
python main.py history --job-id <id> --limit 10
```

## REST API

| Method   | Endpoint                     | Description            |
| -------- | ---------------------------- | ---------------------- |
| `GET`    | `/api/status`                | Server status & uptime |
| `GET`    | `/api/jobs`                  | List all jobs          |
| `POST`   | `/api/jobs`                  | Register a new job     |
| `DELETE` | `/api/jobs/{id}`             | Remove a job           |
| `POST`   | `/api/jobs/{id}/trigger`     | Trigger a job manually |
| `POST`   | `/api/jobs/{id}/toggle`      | Enable/disable a job   |
| `GET`    | `/api/runs?job_id=&limit=50` | List runs              |
| `POST`   | `/api/runs/{id}/cancel`      | Cancel a running job   |
| `POST`   | `/api/reload`                | Reload jobs from DB    |

### Example: add a job via API

```bash
curl -X POST http://localhost:8844/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"name":"my-job","script_path":"./example_job.py","cron_expression":"*/5 * * * *"}'
```

## Cron Syntax

Standard 5-field cron expressions:

```
┌───────────── minute (0–59)
│ ┌───────────── hour (0–23)
│ │ ┌───────────── day of month (1–31)
│ │ │ ┌───────────── month (1–12)
│ │ │ │ ┌───────────── day of week (0–6, Mon=1)
│ │ │ │ │
* * * * *
```

| Expression    | Meaning                     |
| ------------- | --------------------------- |
| `*/5 * * * *` | Every 5 minutes             |
| `0 9 * * *`   | Daily at 09:00              |
| `0 */2 * * *` | Every 2 hours               |
| `0 9 * * 1`   | Every Monday at 09:00       |
| `30 8 1 * *`  | 1st of every month at 08:30 |

## Architecture

```
                ┌──────────┐
  CLI ─────────►│          │
                │  DuckDB  │◄──── state persistence
  Web UI ──────►│          │
                └────┬─────┘
                     │
              ┌──────┴──────┐
              │   FastAPI    │◄──── REST API + HTML
              │   Server     │
              └──────┬──────┘
                     │
              ┌──────┴──────┐
              │ APScheduler  │◄──── cron triggers
              └──────┬──────┘
                     │
              ┌──────┴──────┐
              │  Executor    │◄──── subprocess.Popen per job
              │  (threads)   │
              └─────────────┘
```

## Notes

- The server uses `sys.executable` to run scripts, so jobs use the same Python interpreter.
- Stdout/stderr are captured after the process finishes (not streamed).
- Logs are truncated to 50 KB per run to keep the database lean.
- On server restart, any runs still marked as `running` are set to `crashed`.
- The DuckDB file is portable — just copy `Pulsar.duckdb` to move your setup.
