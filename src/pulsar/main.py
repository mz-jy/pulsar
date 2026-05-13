"""
Pulsar — a lightweight Python job scheduler.

Usage:
    pulsar serve [--host 0.0.0.0] [--port 8844]
    pulsar add <name> <script> <cron> [--args "..."]
    pulsar list
    pulsar trigger <job_id> [--host localhost] [--port 8844]
    pulsar remove <job_id>
    pulsar history [--job-id <id>] [--limit 20]
"""

import argparse
import logging
import sys
import textwrap

import uvicorn

from .db import Database
from .executor import JobExecutor
from .scheduler import JobScheduler
from .server import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pulsar")


# ═══════════════════════════════════════════════════════════════════════
# Commands
# ═══════════════════════════════════════════════════════════════════════


def cmd_serve(args):
    """Start the web server + scheduler."""
    db = Database(args.db)
    db.mark_stale_runs()  # clean up leftovers from previous crashes

    executor = JobExecutor(db)
    sched = JobScheduler(executor)
    sched.start()
    sched.reload(db)

    app = create_app(db, executor, sched)

    log.info("Pulsar starting → http://%s:%s", args.host, args.port)
    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        sched.stop()
        db.close()
        log.info("Pulsar stopped.")


def cmd_add(args):
    """Register a new job (writes directly to DB)."""
    db = Database(args.db)
    try:
        job = db.add_job(
            args.name, args.script, args.cron, args.args or "", args.module
        )
        mode = "module (-m)" if args.module else "script"
        print(
            f"✓ Added job '{job.name}' (id={job.id}, cron='{job.cron_expression}', type={mode})"
        )
        # try to tell a running server to reload
        _ping_reload(args)
    except Exception as exc:
        print(f"✗ Error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


def cmd_list(args):
    """List all registered jobs."""
    db = Database(args.db)
    jobs = db.get_jobs()
    db.close()
    if not jobs:
        print("No jobs registered.")
        return
    hdr = f"{'ID':<10} {'Name':<22} {'Cron':<18} {'On':<5} Script"
    print(hdr)
    print("─" * len(hdr))
    for j in jobs:
        flag = "✓" if j.enabled else "✗"
        print(
            f"{j.id:<10} {j.name:<22} {j.cron_expression:<18} {flag:<5} {j.script_path}"
        )


def cmd_trigger(args):
    """Trigger a job via the running server's API."""
    import json
    import urllib.request

    url = f"http://{args.host}:{args.port}/api/jobs/{args.job_id}/trigger"
    try:
        req = urllib.request.Request(url, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            print(f"✓ Triggered — run_id={data.get('run_id')}")
    except Exception as exc:
        print(f"✗ Failed (is the server running?): {exc}", file=sys.stderr)
        sys.exit(1)


def cmd_remove(args):
    """Remove a job and its history."""
    db = Database(args.db)
    job = db.get_job(args.job_id)
    if not job:
        print(f"✗ Job '{args.job_id}' not found.", file=sys.stderr)
        db.close()
        sys.exit(1)
    db.remove_job(args.job_id)
    db.close()
    print(f"✓ Removed job '{job.name}' ({job.id})")
    _ping_reload(args)


def cmd_history(args):
    """Show run history."""
    db = Database(args.db)
    runs = db.get_runs(args.job_id, limit=args.limit)
    names = {j.id: j.name for j in db.get_jobs()}
    db.close()
    if not runs:
        print("No runs found.")
        return
    hdr = f"{'Run':<10} {'Job':<22} {'Status':<11} {'Trigger':<10} {'Started':<22} {'Duration'}"
    print(hdr)
    print("─" * len(hdr))
    for r in runs:
        d = ""
        if r.started_at and r.finished_at:
            from datetime import datetime

            sa = datetime.fromisoformat(r.started_at)
            fa = datetime.fromisoformat(r.finished_at)
            secs = (fa - sa).total_seconds()
            d = f"{secs:.1f}s"
        started = r.started_at[:19].replace("T", " ") if r.started_at else "-"
        print(
            f"{r.id:<10} {names.get(r.job_id, '?'):<22} {r.status:<11} {r.triggered_by:<10} {started:<22} {d}"
        )


def _ping_reload(args):
    """Best-effort POST /api/reload to a running server."""
    import urllib.request

    host = getattr(args, "host", "localhost")
    port = getattr(args, "port", 8844)
    try:
        req = urllib.request.Request(f"http://{host}:{port}/api/reload", method="POST")
        urllib.request.urlopen(req, timeout=2)
        print("  ↻ Server reloaded.")
    except Exception:
        pass  # server might not be running — that's fine


# ═══════════════════════════════════════════════════════════════════════
# CLI parser
# ═══════════════════════════════════════════════════════════════════════


def main():
    p = argparse.ArgumentParser(
        prog="pulsar",
        description="Pulsar — lightweight Python job scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            examples:
              pulsar serve
              pulsar add daily-report ./report.py "0 9 * * *"
              pulsar trigger abc12345
              pulsar history --limit 10
        """),
    )
    p.add_argument(
        "--db",
        default="Pulsar.duckdb",
        help="DuckDB file path (default: Pulsar.duckdb)",
    )
    sub = p.add_subparsers(dest="cmd")

    # serve
    s = sub.add_parser("serve", help="Start the server + scheduler")
    s.add_argument("--host", default="0.0.0.0")
    s.add_argument("--port", type=int, default=8844)

    # add
    s = sub.add_parser("add", help="Register a new job")
    s.add_argument("name", help="Unique job name")
    s.add_argument("script", help="Path to a Python script or dotted module name")
    s.add_argument("cron", help="5-field cron expression (quote it)")
    s.add_argument("--args", default="", help="Extra CLI args for the script")
    s.add_argument(
        "--module",
        action="store_true",
        default=False,
        help="Run as module: python -m <name>",
    )
    s.add_argument("--host", default="localhost")
    s.add_argument("--port", type=int, default=8844)

    # list
    sub.add_parser("list", help="List registered jobs")

    # trigger
    s = sub.add_parser("trigger", help="Trigger a job (server must be running)")
    s.add_argument("job_id")
    s.add_argument("--host", default="localhost")
    s.add_argument("--port", type=int, default=8844)

    # remove
    s = sub.add_parser("remove", help="Remove a job and its run history")
    s.add_argument("job_id")
    s.add_argument("--host", default="localhost")
    s.add_argument("--port", type=int, default=8844)

    # history
    s = sub.add_parser("history", help="Show run history")
    s.add_argument("--job-id", default=None, dest="job_id")
    s.add_argument("--limit", type=int, default=20)

    args = p.parse_args()
    dispatch = {
        "serve": cmd_serve,
        "add": cmd_add,
        "list": cmd_list,
        "trigger": cmd_trigger,
        "remove": cmd_remove,
        "history": cmd_history,
    }
    fn = dispatch.get(args.cmd)
    if fn:
        fn(args)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
