"""Failure-injection toolkit (library + CLI).

Library use (experiments):  lock_db_process(path, seconds), faults.write(...), corrupt_file(path)
CLI use (live demo against a running `python -m launcher.cluster`):

  python -m tools.inject --data-dir data/ft kill payment-1          # application crash
  python -m tools.inject --data-dir data/ft kill-node A --repair 15  # server / node failure
  python -m tools.inject --data-dir data/ft lock-db --seconds 10     # database outage (exclusive lock)
  python -m tools.inject --data-dir data/ft delay student 8000 --error-rate 0.2   # slow / failing link
  python -m tools.inject --data-dir data/ft crash-mid-transaction 0.3              # interrupted payment/grade
  python -m tools.inject --data-dir data/ft corrupt-db                              # storage corruption
  python -m tools.inject --data-dir data/ft clear                                   # remove all injected faults
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common import events, faults  # noqa: E402


def lock_db_process(db_path, seconds):
    """Start a separate process that holds an EXCLUSIVE lock on the database file (= DB unavailable).
    Returns (Popen, t_locked) once the lock is really held."""
    p = subprocess.Popen([sys.executable, "-m", "tools.inject", "_hold-lock", db_path, str(seconds)], cwd=ROOT,
                         stdout=subprocess.PIPE, text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    line = p.stdout.readline()
    if "LOCKED" not in line:
        raise RuntimeError("could not lock database: " + line)
    return p, time.time()


def _hold_lock(db_path, seconds):
    import sqlite3
    c = sqlite3.connect(db_path, timeout=10, isolation_level=None)
    c.execute("BEGIN EXCLUSIVE")
    print("LOCKED", flush=True)
    time.sleep(seconds)
    c.execute("ROLLBACK")
    c.close()


def corrupt_file(path, nbytes=7 * 4096, offset=4096):
    """Overwrite data pages 2..8 of a database file with garbage (bad sectors / bit rot)."""
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(os.urandom(nbytes))


def corrupt_header(path):
    with open(path, "r+b") as f:
        f.write(b"CORRUPTED-HEADER!")


def kill_pid(pid):
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.path.join(ROOT, "data", "ft"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    k = sub.add_parser("kill"); k.add_argument("instance")
    n = sub.add_parser("kill-node"); n.add_argument("node"); n.add_argument("--repair", type=float, default=15)
    l = sub.add_parser("lock-db"); l.add_argument("--seconds", type=float, default=10); l.add_argument("--file", default=None)
    d = sub.add_parser("delay"); d.add_argument("service"); d.add_argument("ms", type=int); d.add_argument("--error-rate", type=float, default=0)
    c = sub.add_parser("crash-mid-transaction"); c.add_argument("p", type=float)
    sub.add_parser("corrupt-db")
    sub.add_parser("clear")
    h = sub.add_parser("_hold-lock"); h.add_argument("path"); h.add_argument("seconds", type=float)
    a = ap.parse_args()
    dd = a.data_dir

    if a.cmd == "_hold-lock":
        _hold_lock(a.path, a.seconds)
    elif a.cmd == "kill":
        pids = json.load(open(os.path.join(dd, "pids.json")))
        kill_pid(pids[a.instance])
        events.emit("injector", "instance_killed", data_dir=dd, target=a.instance)
        print("killed", a.instance)
    elif a.cmd == "kill-node":
        with open(os.path.join(dd, "control.json"), "w") as f:
            json.dump({"down": {a.node: time.time() + a.repair}}, f)
        pids = json.load(open(os.path.join(dd, "pids.json")))
        nodes = json.load(open(os.path.join(dd, "topology.json")))["instances"]
        for i in nodes:
            if i["node"] == a.node:
                kill_pid(pids[i["id"]])
        events.emit("injector", "node_failed", data_dir=dd, node=a.node, repair_after=a.repair)
        print(f"node {a.node} down for {a.repair}s")
    elif a.cmd == "lock-db":
        path = a.file or next(p for p in (os.path.join(dd, "primary.db"), os.path.join(dd, "baseline.db"))
                              if os.path.exists(p))
        p, _ = lock_db_process(path, a.seconds)
        events.emit("injector", "db_locked", data_dir=dd, seconds=a.seconds)
        print(f"{path} locked for {a.seconds}s"); p.wait()
    elif a.cmd == "delay":
        try:
            f = json.load(open(os.path.join(dd, "faults.json")))
        except Exception:
            f = {}
        f[f"delay_ms:{a.service}"] = a.ms
        f[f"error_rate:{a.service}"] = a.error_rate
        faults.write(dd, f)
        print("injected", f)
    elif a.cmd == "crash-mid-transaction":
        try:
            f = json.load(open(os.path.join(dd, "faults.json")))
        except Exception:
            f = {}
        f["crash_after_charge"] = a.p
        f["crash_mid_grade"] = a.p
        faults.write(dd, f)
        print("injected", f)
    elif a.cmd == "corrupt-db":
        path = os.path.join(dd, "primary.db")
        if not os.path.exists(path):
            path = os.path.join(dd, "baseline.db")
        corrupt_file(path)
        events.emit("injector", "db_corrupted", data_dir=dd, file=os.path.basename(path))
        print("corrupted", path)
    elif a.cmd == "clear":
        faults.clear(dd)
        print("faults cleared")


if __name__ == "__main__":
    main()
