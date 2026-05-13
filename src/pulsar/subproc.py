"""Helpers for spawning subprocesses safely across platforms."""

from __future__ import annotations

import os
import sys


def child_environ() -> dict[str, str]:
    """Environment for Popen/subprocess.run after the interpreter may have started threads.

    On macOS, fork+exec from a multi-threaded process that touched ObjC can abort the child
    with ``objc_initializeAfterForkError`` unless this guard is set (inherited at fork time).
    """
    env = dict(os.environ)
    if sys.platform == "darwin":
        env.setdefault("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")
    return env
