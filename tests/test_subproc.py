import sys

from pulsar.subproc import child_environ


def test_child_environ_sets_objc_guard_on_darwin(monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin")
    env = child_environ()
    assert env["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] == "YES"


def test_child_environ_respects_existing_objc(monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setenv("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "NO")
    env = child_environ()
    assert env["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] == "NO"
