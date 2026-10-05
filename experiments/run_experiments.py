"""Controlled failure-injection experiments: baseline vs fault-tolerant system.

Usage:
    python -m experiments.run_experiments --reps 3                 # everything
    python -m experiments.run_experiments --only E1 E5 --reps 1    # selected experiments
Outputs (results/):  raw/<exp>/<mode>-<rep>/{requests.csv,events.jsonl,meta.json}  and  experiments.json
"""
import argparse
import csv
import json
import math
import os
import random
import shutil
import statistics
import sys
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common import events, faults  # noqa: E402
from launcher.cluster import Cluster  # noqa: E402
from tools import consistency, inject  # noqa: E402
from tools.loadgen import LoadRunner, CRITICAL, PAY_STUDENTS  # noqa: E402
from tools.loadproc import start_distributed  # noqa: E402

RESULTS = os.path.join(ROOT, "results")
WORK = os.path.join(ROOT, "data", "experiments")
RATE = 20                       # background open-loop requests / second

DETECT = {"crash_detected", "target_down", "replica_ejected", "replica_unhealthy", "circuit_open",
          "db_primary_unavailable", "db_circuit_open"}
RECOVER = {"instance_ready", "node_repaired", "db_primary_available", "db_primary_restored_from_replica",
           "journal_replayed", "payment_recovered"}


def pct(vals, p):
    if not vals:
        return None
    v = sorted(vals)
    k = min(len(v) - 1, max(0, math.ceil(p / 100 * len(v)) - 1))
    return v[k]


class Ctx:
    def __init__(self, cluster, runner, mode, data_dir):
        self.cluster, self.runner, self.mode, self.data_dir = cluster, runner, mode, data_dir
        self.t0 = time.time()
        self.t_inject = None
        self.t_fault_end = None
        self.extra = {}
        self.t_end = None

    def at(self, rel):
        d = self.t0 + rel - time.time()
        if d > 0:
            time.sleep(d)

    def mark_inject(self, t=None):
        self.t_inject = t or time.time()
        return self.t_inject

    def mark_fault_end(self, t=None):
        self.t_fault_end = t or time.time()


# ============================================================================ experiments
def e1_crash(ctx):
    ctx.at(8)
    ctx.mark_inject(ctx.cluster.kill_instance("payment-1"))
    ctx.mark_fault_end(ctx.t_inject)                    # the fault itself is instantaneous
    ctx.at(30)


def e2_dbfail(ctx):
    path = os.path.join(ctx.data_dir, "baseline.db" if ctx.mode == "baseline" else "primary.db")
    ctx.at(8)
    proc, t = inject.lock_db_process(path, 10)
    ctx.mark_inject(t)
    ctx.mark_fault_end(t + 10)
    ctx.at(36)
    proc.wait(timeout=5)


def e3_network(ctx):
    ctx.at(8)
    faults.write(ctx.data_dir, {"delay_ms:student": 8000, "error_rate:student": 0.2})
    ctx.mark_inject()
    ctx.at(20)
    faults.clear(ctx.data_dir)
    ctx.mark_fault_end()
    ctx.at(34)


def e4_node(ctx):
    ctx.at(8)
    ctx.mark_inject(ctx.cluster.kill_node("A", repair_after=15))
    ctx.mark_fault_end(ctx.t_inject + 15)
    ctx.at(40)


def e5_transaction(ctx):
    rng = random.Random(7)
    items = []
    for i in range(60):
        key = f"e5-pay-{uuid.uuid4().hex[:8]}"
        st = f"S{i + 1:04d}"
        items.append(("payment", "POST", "/api/payment/payments",
                      {"student_id": st, "amount": 100.0, "idempotency_key": key},
                      {"op": "payment", "key": key, "student_id": st, "amount": 100.0}))
    for i in range(40):
        st, co = f"S{i + 1:04d}", f"C{i % 12 + 1:03d}"
        items.append(("grade", "POST", "/api/records/grades", {"student_id": st, "course_id": co, "grade": 90 + i % 10},
                      {"op": "grade", "student_id": st, "course_id": co}))
    rng.shuffle(items)
    ctx.at(1)
    faults.write(ctx.data_dir, {"crash_after_charge": 0.25, "crash_mid_grade": 0.25})
    ctx.mark_inject()
    ok = ctx.runner.submit_batch(items, concurrency_rate=8, retries=3, retry_delay=1.0)
    ok.result(timeout=120)
    faults.clear(ctx.data_dir)
    ctx.mark_fault_end()
    ctx.at(time.time() - ctx.t0 + 3)                    # short quiet period before verification


def e6_load(ctx):
    urls = [i.url for i in ctx.cluster.instances if i.service == "gateway"]
    ctx.at(6)
    ctx.mark_inject()
    handle = start_distributed(urls, total_users=152, duration=15)       # 4 processes x 38 virtual users
    ctx.mark_fault_end(ctx.t_inject + 15)
    ctx.at(29)
    handle.collect(ctx.runner)


def e7_storage(ctx):
    path = os.path.join(ctx.data_dir, "baseline.db" if ctx.mode == "baseline" else "primary.db")
    ctx.at(8)
    inject.corrupt_file(path)
    ctx.mark_inject()
    ctx.mark_fault_end(ctx.t_inject)
    ctx.at(32)


EXPERIMENTS = {
    "E1": dict(name="Application crash", failure="kill payment instance (SIGKILL)", fn=e1_crash, load=True),
    "E2": dict(name="Database failure", failure="exclusive lock on DB for 10 s", fn=e2_dbfail, load=True),
    "E3": dict(name="Network / service timeout", failure="student svc: 8 s delay + 20 % errors for 12 s",
               fn=e3_network, load=True),
    "E4": dict(name="Hardware / node failure", failure="whole node A down for 15 s", fn=e4_node, load=True),
    "E5": dict(name="Corrupted / lost transaction", failure="process crash mid-payment / mid-grade (p = 0.25)",
               fn=e5_transaction, load=False),
    "E6": dict(name="High load", failure="152 concurrent users for 15 s", fn=e6_load, load=True),
    "E7": dict(name="Storage corruption (bit rot)", failure="28 KB of random bytes written over data pages of the primary DB file",
               fn=e7_storage, load=True),
}


# ============================================================================ analysis
def analyze(rows, evs, ctx, t_end):
    ti, tf = ctx.t_inject, ctx.t_fault_end or ctx.t_inject
    win = [r for r in rows if ti <= r["t"] < t_end]
    load = [r for r in win if r["kind"] == "load"]
    probes = [r for r in win if r["kind"] == "probe"]
    fails = [r for r in win if not r["ok"]]
    out = {"t_inject": ti, "window_s": round(t_end - ti, 2)}
    out["requests_total_run"] = len([r for r in rows if r["kind"] == "load"])
    out["requests_in_window"] = len(load)
    out["failed_requests"] = sum(1 for r in load if not r["ok"])
    out["successful_requests"] = len(load) - out["failed_requests"]
    out["success_rate"] = round(out["successful_requests"] / len(load), 4) if load else None
    out["degraded_responses"] = sum(1 for r in load if r["ok"] and r["degraded"])
    out["queued_responses"] = sum(1 for r in load if r["ok"] and r["queued"])
    out["probe_availability"] = round(sum(r["ok"] for r in probes) / len(probes), 4) if probes else None
    out["combined_availability"] = round(sum(r["ok"] for r in win) / len(win), 4) if win else None
    okl = [r["lat"] for r in load if r["ok"]]
    out["latency_p50_ms"] = round(pct(okl, 50) * 1000, 1) if okl else None
    out["latency_p95_ms"] = round(pct(okl, 95) * 1000, 1) if okl else None
    out["latency_p99_ms"] = round(pct(okl, 99) * 1000, 1) if okl else None
    pre = [r["lat"] for r in rows if r["kind"] == "load" and r["ok"] and r["t"] < ti]
    out["latency_p95_pre_ms"] = round(pct(pre, 95) * 1000, 1) if pre else None
    by = {}
    for r in load:
        k = "critical" if r["op"] in CRITICAL else ("low" if r["op"] in ("transcript", "timetable") else "normal")
        b = by.setdefault(k, [0, 0])
        b[0] += 1
        b[1] += r["ok"]
    out["success_by_class"] = {k: round(v[1] / v[0], 4) for k, v in by.items()}
    # detection (system-internal events; baseline only has the passive monitor)
    det = [e["ts"] - ti for e in evs if e["event"] in DETECT and e["ts"] >= ti - 0.05 and e["component"] != "injector"]
    out["detection_s"] = round(min(det), 3) if det else None
    out["detected_by"] = next((e["component"] + ":" + e["event"] for e in sorted(evs, key=lambda e: e["ts"])
                               if e["event"] in DETECT and e["ts"] >= ti - 0.05 and e["component"] != "injector"), None)
    rec = [e["ts"] - ti for e in evs if e["event"] in ("instance_ready", "node_repaired", "db_primary_restored_from_replica")
           and e["ts"] >= ti]
    out["capacity_restored_s"] = round(max(rec), 3) if rec and ctx.cluster.supervise else None
    # user-visible outage / recovery from the black-box request log
    if fails:
        first, last = min(r["t"] for r in fails), max(r["t"] + r["lat"] for r in fails)   # until the failure *completed*
        out["first_failure_s"] = round(first - ti, 3)
        out["outage_s"] = round(last - first, 3)
        out["recovery_s"] = round(last - ti, 3)
        out["post_fault_recovery_s"] = round(max(0.0, last - tf), 3)
        tail_ok = [r for r in win if r["t"] > last and r["kind"] == "probe"]
        out["recovered"] = len(tail_ok) >= 4 and all(r["ok"] for r in tail_ok[-4:])
    else:
        out.update(first_failure_s=None, outage_s=0.0, recovery_s=0.0, post_fault_recovery_s=0.0, recovered=True)
    out["masked"] = out["failed_requests"] == 0
    return out


def timeline(rows, t0, t_end, bin_s=1.0):
    n = int((t_end - t0) / bin_s) + 1
    ok, bad = [0] * n, [0] * n
    for r in rows:
        i = int((r["t"] - t0) / bin_s)
        if 0 <= i < n:
            (ok if r["ok"] else bad)[i] += 1
    return {"ok": ok, "failed": bad}


def run_one(exp_id, mode, rep, keep=True):
    exp = EXPERIMENTS[exp_id]
    data_dir = os.path.join(WORK, f"{exp_id}-{mode}-{rep}")
    shutil.rmtree(data_dir, ignore_errors=True)
    cluster = Cluster(mode, data_dir).start()
    urls = [i.url for i in cluster.instances if i.service == "gateway"]
    runner = LoadRunner(urls, seed=rep + 1)
    try:
        runner.warm_up()
        ctx = Ctx(cluster, runner, mode, data_dir)
        runner.start_prober()
        if exp["load"]:
            runner.start_open_loop(RATE)
        ctx.t0 = time.time()
        exp["fn"](ctx)
        t_end = time.time()
        runner.stop()
        rows = runner.rec.rows
        # verification: wait for recovery processes (journal replay, saga recovery) to finish
        wait = 20 if mode == "ft" else 6
        cons, waited = consistency.wait_consistent(data_dir, mode, runner.rec.intents, timeout=wait)
        res = analyze(rows, events.read_all(data_dir), ctx, t_end)
        res.update(experiment=exp_id, name=exp["name"], failure=exp["failure"], mode=mode, rep=rep,
                   consistency=cons, consistent=cons.get("consistent"), consistency_wait_s=round(waited, 2))
        res["consistency_restored_s"] = (round(max(0.0, (t_end - (ctx.t_fault_end or ctx.t_inject)) + waited), 2)
                                         if cons.get("consistent") else None)
        if exp_id == "E5":
            pay = [i for i in runner.rec.intents if i["op"] == "payment"]
            gr = [i for i in runner.rec.intents if i["op"] == "grade"]
            res["intents_failed_final"] = sum(1 for i in pay + gr if not i.get("acked"))
            res["attempts_failed"] = sum(1 for r in rows if not r["ok"] and r["kind"] == "load")
            res["attempts_total"] = sum(1 for r in rows if r["kind"] == "load")
            res["retried_ok"] = sum(1 for r in rows if r["ok"] and r["attempt"] > 1)
        if exp_id == "E6":
            after = [r for r in sorted(rows, key=lambda r: r["t"]) if r["kind"] == "probe" and r["t"] >= ctx.t_fault_end]
            rec_t = None
            for i in range(len(after) - 3):
                if all(a["ok"] and a["lat"] < 0.5 for a in after[i:i + 4]):
                    rec_t = after[i]["t"] - ctx.t_fault_end
                    break
            res["post_load_recovery_s"] = round(rec_t, 2) if rec_t is not None else None
            res["throughput_rps"] = round(res["requests_in_window"] / (ctx.t_fault_end - ctx.t_inject), 1)
            res["goodput_rps"] = round(res["successful_requests"] / (ctx.t_fault_end - ctx.t_inject), 1)
            res["shed_responses"] = sum(1 for r in rows if r["status"] == 503 and r["kind"] == "load"
                                        and ctx.t_inject <= r["t"] < ctx.t_fault_end)
        # persist raw data (experimental dataset)
        raw = os.path.join(RESULTS, "raw", exp_id, f"{mode}-{rep}")
        os.makedirs(raw, exist_ok=True)
        with open(os.path.join(raw, "requests.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t_rel", "latency_s", "ok", "status", "op", "kind", "degraded", "queued", "attempt", "gw_attempts"])
            for r in sorted(rows, key=lambda r: r["t"]):
                w.writerow([round(r["t"] - ctx.t0, 4), round(r["lat"], 4), int(r["ok"]), r["status"], r["op"], r["kind"],
                            int(r["degraded"]), int(r["queued"]), r["attempt"], r.get("gw_attempts", 1)])
        with open(os.path.join(raw, "events.jsonl"), "w") as f:
            for e in events.read_all(data_dir):
                e = dict(e, t_rel=round(e["ts"] - ctx.t0, 4))
                f.write(json.dumps(e) + "\n")
        res["t0"] = ctx.t0
        res["timeline"] = timeline(rows, ctx.t0, t_end)
        res["t_inject_rel"] = round(ctx.t_inject - ctx.t0, 2)
        res["t_fault_end_rel"] = round((ctx.t_fault_end or ctx.t_inject) - ctx.t0, 2)
        with open(os.path.join(raw, "meta.json"), "w") as f:
            json.dump(res, f, indent=1, default=str)
        return res
    finally:
        try:
            runner.stop()
        except Exception:  # noqa: BLE001
            pass
        cluster.stop()
        if not keep:
            shutil.rmtree(data_dir, ignore_errors=True)


def summarize(r):
    f = lambda v, d=2: "n/a" if v is None else (f"{v:.{d}f}" if isinstance(v, float) else str(v))  # noqa: E731
    return (f"{r['experiment']} {r['mode']:<8} rep{r['rep']}  det={f(r['detection_s'])}s rec={f(r['recovery_s'])}s "
            f"outage={f(r['outage_s'])}s failed={r['failed_requests']}/{r['requests_in_window']} "
            f"ok={f(r['success_rate'], 3)} consistent={r['consistent']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", default=list(EXPERIMENTS))
    ap.add_argument("--modes", nargs="*", default=["baseline", "ft"])
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--out", default=os.path.join(RESULTS, "experiments.json"))
    a = ap.parse_args()
    results = []
    if os.path.exists(a.out):
        results = json.load(open(a.out))
    for exp_id in a.only:
        for rep in range(1, a.reps + 1):
            for mode in a.modes:
                results = [x for x in results if not (x["experiment"] == exp_id and x["mode"] == mode and x["rep"] == rep)]
                t = time.time()
                r = run_one(exp_id, mode, rep, keep=False)
                results.append(r)
                print(summarize(r), f"[{time.time() - t:.0f}s]", flush=True)
                with open(a.out, "w") as f:
                    json.dump(results, f, indent=1, default=str)


if __name__ == "__main__":
    main()
