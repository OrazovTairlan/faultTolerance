"""Live demonstration: four failure-and-recovery scenarios on the running system.

    python demo.py                 # fault-tolerant system (default)
    python demo.py --mode baseline # same scenarios on the baseline, for contrast
    python demo.py --only 1 3      # run selected scenarios

Each scenario prints, once per second, how many user requests succeeded / failed, plus the system's own detection and
recovery events, then a verdict.
"""
import argparse
import os
import sys
import time
import uuid

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from common import events, faults  # noqa: E402
from launcher.cluster import Cluster  # noqa: E402
from tools import consistency, inject  # noqa: E402
from tools.loadgen import LoadRunner  # noqa: E402

SHOW = {"instance_killed", "node_failed", "crash_detected", "replica_ejected", "replica_healthy", "target_down",
        "target_up", "instance_restarted", "instance_ready", "node_repaired", "db_primary_unavailable",
        "db_circuit_open", "db_primary_available", "op_journaled", "journal_replayed", "payment_recovered",
        "circuit_open", "circuit_closed", "db_locked"}


class Demo:
    def __init__(self, mode):
        self.mode = mode
        self.data = os.path.join(ROOT, "data", f"demo-{mode}")
        self.cluster = Cluster(mode, self.data).start()
        urls = [i.url for i in self.cluster.instances if i.service == "gateway"]
        self.runner = LoadRunner(urls, seed=3)
        self.runner.warm_up()
        self.runner.start_open_loop(20)
        self.seen = 0
        self.cursor = 0

    def watch(self, seconds, label):
        end = time.time() + seconds
        t0 = time.time()
        while time.time() < end:
            time.sleep(1.0)
            rows = [r for r in self.runner.rec.rows if r["kind"] == "load" and time.time() - 1.0 - 0.0 <= r["t"] + r["lat"]
                    and r["t"] + r["lat"] <= time.time()]
            ok = sum(r["ok"] for r in rows)
            line = f"  t+{time.time() - t0:4.1f}s  requests ok={ok:3d} failed={len(rows) - ok:3d}"
            evs = events.read_all(self.data)
            new = [e for e in evs[self.cursor:] if e["event"] in SHOW]
            self.cursor = len(evs)
            seen, out = set(), []
            for e in new:
                key = (e["component"].split("-")[0], e["event"], e.get("target", ""))
                if key not in seen:
                    seen.add(key)
                    out.append(f"{e['component']}:{e['event']}" + (f"({e['target']})" if e.get("target") else ""))
            print(line + ("   <- " + ", ".join(out[:5]) if out else ""), flush=True)

    def close(self):
        self.runner.stop()
        self.cluster.stop()


def scenario_crash(d):
    print("\n=== 1. APPLICATION CRASH: kill payment-1 (SIGKILL) ===")
    d.watch(3, "steady")
    inject.os.kill  # noqa: B018
    d.cluster.kill_instance("payment-1")
    d.watch(9, "after crash")


def scenario_db(d):
    print("\n=== 2. DATABASE FAILURE: primary database locked for 8 s ===")
    path = os.path.join(d.data, "baseline.db" if d.mode == "baseline" else "primary.db")
    d.watch(2, "steady")
    proc, _ = inject.lock_db_process(path, 8)
    d.watch(14, "outage + recovery")
    proc.wait()
    if d.mode == "ft":
        r, _ = consistency.wait_consistent(d.data, d.mode, d.runner.rec.intents, 15)
        print(f"  -> queued writes replayed; consistency check: consistent={r['consistent']}, "
              f"acked-but-missing payments={r['acked_payments_missing']}, journal pending={r['journal_pending']}")


def scenario_node(d):
    print("\n=== 3. HARDWARE / NODE FAILURE: node A (a whole server) down for 10 s ===")
    d.watch(2, "steady")
    d.cluster.kill_node("A", 10)
    d.watch(16, "node down + repaired")


def scenario_txn(d):
    print("\n=== 4. LOST / CORRUPTED TRANSACTION: process crashes in the middle of payments & grade updates ===")
    items = []
    for i in range(40):
        key = "demo-" + uuid.uuid4().hex[:8]
        st = f"S{i + 1:04d}"
        items.append(("payment", "POST", "/api/payment/payments", {"student_id": st, "amount": 100.0, "idempotency_key": key},
                      {"op": "payment", "key": key, "student_id": st, "amount": 100.0}))
    faults.write(d.data, {"crash_after_charge": 0.3})
    fut = d.runner.submit_batch(items, concurrency_rate=6, retries=3, retry_delay=1.0)
    d.watch(8, "payments with crashes")
    fut.result(timeout=120)
    faults.clear(d.data)
    r, waited = consistency.wait_consistent(d.data, d.mode, d.runner.rec.intents, 20 if d.mode == "ft" else 5)
    print(f"  -> after recovery ({waited:.1f}s): consistent={r['consistent']}  duplicate charges={r['duplicate_charges']}  "
          f"lost payments (charged, no receipt)={r['lost_payments']}  unfinished sagas={r['unfinished_sagas']}")


SCENARIOS = {1: scenario_crash, 2: scenario_db, 3: scenario_node, 4: scenario_txn}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["ft", "baseline"], default="ft")
    ap.add_argument("--only", nargs="*", type=int, default=[1, 2, 3, 4])
    a = ap.parse_args()
    print(f"Starting the {a.mode} system ...")
    d = Demo(a.mode)
    try:
        for k in a.only:
            SCENARIOS[k](d)
            if k != a.only[-1]:
                d.watch(4, "settle")
        print("\nDemo finished.")
    finally:
        d.close()


if __name__ == "__main__":
    main()
