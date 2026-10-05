"""Multi-process closed-loop load: one Python event loop saturates at ~100 concurrent sockets on Windows, so heavy
load tests spread the virtual users over several OS processes (each with <= ~40 users).

Child:  python -m tools.loadproc --urls a,b --users 38 --duration 15 --seed 3 --out file.json
Parent: start_distributed(urls, total_users, duration) -> handle.collect() merges rows/intents into a LoadRunner.
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def start_distributed(urls, total_users, duration, per_proc=38, seed=100):
    procs, outs = [], []
    n = max(1, round(total_users / per_proc))
    for k in range(n):
        out = os.path.join(tempfile.gettempdir(), f"loadproc-{os.getpid()}-{k}.json")
        outs.append(out)
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "tools.loadproc", "--urls", ",".join(urls), "--users", str(round(total_users / n)),
             "--duration", str(duration), "--seed", str(seed + k), "--out", out],
            cwd=ROOT, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)))
    return Handle(procs, outs, duration)


class Handle:
    def __init__(self, procs, outs, duration):
        self.procs, self.outs, self.duration = procs, outs, duration

    def collect(self, runner):
        for p in self.procs:
            p.wait(timeout=self.duration + 60)
        for o in self.outs:
            try:
                with open(o) as f:
                    d = json.load(f)
                runner.rec.rows.extend(d["rows"])
                runner.rec.intents.extend(d["intents"])
                os.remove(o)
            except (OSError, ValueError):
                pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--urls", required=True)
    ap.add_argument("--users", type=int, required=True)
    ap.add_argument("--duration", type=float, required=True)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    from tools.loadgen import LoadRunner
    r = LoadRunner(a.urls.split(","), seed=a.seed)
    f = r.start_closed_loop(a.users, a.duration)
    f.result()
    r.stop()
    with open(a.out, "w") as fh:
        json.dump({"rows": r.rec.rows, "intents": r.rec.intents}, fh)


if __name__ == "__main__":
    main()
