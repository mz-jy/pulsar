"""
Pulsar — a lightweight Python job scheduler.

Usage:
    pulsar serve [--host 0.0.0.0] [--port 8844] [--daemon]
    pulsar start   # serve in background
    pulsar stop | restart | status
    pulsar add <name> <script> <cron> [--args "..."]
    pulsar list
    pulsar trigger <job_id> [--host localhost] [--port 8844]
    pulsar remove <job_id>
    pulsar history [--job-id <id>] [--limit 20]
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import textwrap
import time

import uvicorn

from .db import Database
from .executor import JobExecutor
from .paths import (
    default_db_path,
    ensure_pulsar_home,
    log_path,
    pid_path,
    pulsar_home,
)
from .scheduler import JobScheduler
from .server import create_app
from .subproc import child_environ

log = logging.getLogger("pulsar")

_LOG_FORMAT = "%(asctime)s  %(levelname)-5s  %(name)s — %(message)s"
_LOG_DATEFMT = "%H:%M:%S"


def _configure_logging(*, also_console: bool) -> None:
    ensure_pulsar_home()
    fmt = logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT)
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
    root.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path(), encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    if also_console:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        root.addHandler(sh)


def _configure_logging_stderr_only() -> None:
    """For background worker: stderr is already redirected to pulsar.log by the parent."""
    fmt = logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT)
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
    root.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    root.addHandler(sh)


def _read_pid_file() -> int | None:
    p = pid_path()
    if not p.is_file():
        return None
    try:
        return int(p.read_text().strip())
    except (ValueError, OSError):
        return None


def _write_pid_file(pid: int) -> None:
    ensure_pulsar_home()
    pid_path().write_text(str(pid) + "\n", encoding="utf-8")


def _remove_pid_file_if_current() -> None:
    p = pid_path()
    if not p.is_file():
        return
    try:
        if int(p.read_text().strip()) == os.getpid():
            p.unlink(missing_ok=True)
    except (ValueError, OSError):
        pass


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _ensure_not_already_running() -> None:
    pid = _read_pid_file()
    if pid is None:
        return
    if not _pid_is_alive(pid):
        try:
            pid_path().unlink(missing_ok=True)
        except OSError:
            pass
        return
    print(
        f"Pulsar is already running (pid {pid}). Use `pulsar stop` or `pulsar restart`.",
        file=sys.stderr,
    )
    sys.exit(1)


def _spawn_background_server(args: argparse.Namespace) -> None:
    """Spawn a detached `pulsar serve --background-child` and exit this process."""
    _ensure_not_already_running()
    ensure_pulsar_home()
    log_fp = open(log_path(), "a", encoding="utf-8", buffering=1)  # noqa: SIM115
    popen_kw: dict = {
        "stdin": subprocess.DEVNULL,
        "stdout": log_fp,
        "stderr": subprocess.STDOUT,
        "cwd": str(pulsar_home()),
        "env": child_environ(),
    }
    if sys.platform != "win32":
        popen_kw["start_new_session"] = True
    proc = subprocess.Popen(
        _cli_spawn_argv(
            args,
            "serve",
            "--host",
            args.host,
            "--port",
            str(args.port),
            "--background-child",
        ),
        **popen_kw,
    )
    _write_pid_file(proc.pid)
    sys.exit(0)


def _cli_spawn_argv(args: argparse.Namespace, *tail: str) -> list[str]:
    return [sys.executable, "-m", "pulsar.main", "--db", str(args.db), *tail]


def _serve_impl(args: argparse.Namespace) -> None:
    db = Database(args.db)
    db.mark_stale_runs()

    executor = JobExecutor(db)
    sched = JobScheduler(executor)
    sched.start()
    sched.reload(db)

    app = create_app(db, executor, sched)

    log.info("Pulsar starting → http://%s:%s", args.host, args.port)

    def _on_term(signum, frame):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _on_term)

    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        sched.stop()
        db.close()
        _remove_pid_file_if_current()
        log.info("Pulsar stopped.")


def cmd_serve(args: argparse.Namespace) -> None:
    """Start the web server + scheduler."""
    if getattr(args, "background_child", False):
        _configure_logging_stderr_only()
        _serve_impl(args)
        return
    if getattr(args, "daemon", False):
        _spawn_background_server(args)
        return

    _configure_logging(also_console=True)
    _serve_impl(args)


def cmd_start(args: argparse.Namespace) -> None:
    _spawn_background_server(args)


def cmd_stop(_args: argparse.Namespace) -> None:
    pid = _read_pid_file()
    if pid is None:
        print("No PID file — Pulsar does not appear to be running.")
        return
    if not _pid_is_alive(pid):
        print(f"Stale PID file (pid {pid} not running); removing.")
        try:
            pid_path().unlink(missing_ok=True)
        except OSError:
            pass
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pid_path().unlink(missing_ok=True)
        return
    except PermissionError as exc:
        print(f"Cannot stop pid {pid}: {exc}", file=sys.stderr)
        sys.exit(1)

    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if not _pid_is_alive(pid):
            break
        time.sleep(0.1)
    if _pid_is_alive(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        pid_path().unlink(missing_ok=True)
    except OSError:
        pass
    print(f"Stopped Pulsar (was pid {pid}).")


def cmd_restart(args: argparse.Namespace) -> None:
    cmd_stop(args)
    time.sleep(0.3)
    argv = _cli_spawn_argv(
        args,
        "start",
        "--host",
        args.host,
        "--port",
        str(args.port),
    )
    subprocess.run(argv, check=False, env=child_environ())


def cmd_status(_args: argparse.Namespace) -> None:
    pid = _read_pid_file()
    if pid is None:
        print("status: not running (no PID file)")
        return
    if _pid_is_alive(pid):
        print(f"status: running (pid {pid})")
    else:
        print(f"status: not running (stale pid {pid} in {pid_path()})")


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
        pass


def _add_serve_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8844)


def main():
    p = argparse.ArgumentParser(
        prog="pulsar",
        description="Pulsar — lightweight Python job scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Default data directory: ~/.pulsar/ (override with $PULSAR_HOME)
              pulsar.duckdb   database
              pulsar.log      logs
              pulsar.pid      background server PID

            examples:
              pulsar start
              pulsar serve --daemon
              pulsar restart
              pulsar add daily-report ./report.py "0 9 * * *"
              pulsar trigger abc12345
              pulsar history --limit 10
        """),
    )
    p.add_argument(
        "--db",
        default=str(default_db_path()),
        help=f"DuckDB file path (default: {default_db_path()})",
    )
    sub = p.add_subparsers(dest="cmd")

    s = sub.add_parser("serve", help="Start the server + scheduler")
    _add_serve_flags(s)
    s.add_argument(
        "--daemon",
        action="store_true",
        help="Run in background; append logs to ~/.pulsar/pulsar.log, PID in pulsar.pid",
    )
    s.add_argument(
        "--background-child",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    st = sub.add_parser(
        "start",
        help="Start server in background (logs and PID in ~/.pulsar/)",
    )
    _add_serve_flags(st)

    sub.add_parser("stop", help="Stop background server using PID file")
    sr = sub.add_parser("restart", help="Stop then start background server")
    _add_serve_flags(sr)
    sub.add_parser("status", help="Show whether background server is running")

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

    sub.add_parser("list", help="List registered jobs")

    s = sub.add_parser("trigger", help="Trigger a job (server must be running)")
    s.add_argument("job_id")
    s.add_argument("--host", default="localhost")
    s.add_argument("--port", type=int, default=8844)

    s = sub.add_parser("remove", help="Remove a job and its run history")
    s.add_argument("job_id")
    s.add_argument("--host", default="localhost")
    s.add_argument("--port", type=int, default=8844)

    s = sub.add_parser("history", help="Show run history")
    s.add_argument("--job-id", default=None, dest="job_id")
    s.add_argument("--limit", type=int, default=20)

    args = p.parse_args()
    dispatch = {
        "serve": cmd_serve,
        "start": cmd_start,
        "stop": cmd_stop,
        "restart": cmd_restart,
        "status": cmd_status,
        "add": cmd_add,
        "list": cmd_list,
        "trigger": cmd_trigger,
        "remove": cmd_remove,
        "history": cmd_history,
    }
    fn = dispatch.get(args.cmd)
    if not fn:
        p.print_help()
        return

    if args.cmd not in ("serve", "start"):
        _configure_logging(also_console=True)

    fn(args)


if __name__ == "__main__":
    main()
