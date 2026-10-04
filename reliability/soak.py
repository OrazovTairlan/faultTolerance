"""Accelerated fault campaign for MTTF / MTBF / MTTR / availability measurement.

Every process (gateway + 4 services; two replicas of each in the fault-tolerant system) fails independently with
exponentially distributed *uptime* (mean = MTTF_ACCEL) and is repaired by the system under test (operator for the
baseline, orchestrator for the fault-tolerant version).  A black-box prober (4 requests / 250 ms) measures what users see.

    python -m reliability.soak --duration 300 --mttf 60
Output: results/soak.json, results/raw/SOAK/<mode>/*
"""
import argparse
import csv
import json
import os
import random
import shutil
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common import events  # noqa: E402
from launcher.cluster import Cluster  # noqa: E402
from reliability import calc  # noqa: E402
from tools.loadgen import LoadRunner  # noqa: E402

RESULTS = os.path.join(ROOT, "results")
TICK = 0.25


def run_soak(mode, duration, mttf, seed):
    data_dir = os.path.join(ROOT, "data", "experiments", f"SOAK-{mode}")
    shutil.rmtree(data_dir, ignore_errors=True)
    cluster = Cluster(mode, data_dir).start()
    targets = [i for i in cluster.instances if i.service != "monitor"]
    rng = random.Random(seed)
    runner = LoadRunner([i.url for i in cluster.instances if i.service == "gateway"], seed=seed)
    runner.warm_up()
    runner.start_prober(TICK)
    runner.start_open_loop(10)
    t0 = time.time()
    up_since = {i.id: t0 for i in targets}
    next_fail = {i.id: t0 + rng.expovariate(1 / mttf) for i in targets}
    kills = []                    # (t, id, uptime)
    op_time = {i.id: 0.0 for i in targets}
    state_up = {i.id: True for i in targets}
    while time.time() - t0 < duration:
        now = time.time()
        for i in targets:
            alive = cluster.procs[i.id].poll() is None
            if state_up[i.id] and not alive:
                state_up[i.id] = False               # died on its own (should not happen)
            if not state_up[i.id]:
                if alive and cluster.is_ready(i):    # repaired -> new uptime period starts
                    state_up[i.id] = True
                    up_since[i.id] = now
                    next_fail[i.id] = now + rng.expovariate(1 / mttf)
                continue
            if now >= next_fail[i.id]:
                cluster.kill_instance(i.id)
                kills.append((now - t0, i.id, now - up_since[i.id]))
                op_time[i.id] += now - up_since[i.id]
                state_up[i.id] = False
        time.sleep(0.05)
    t_end = time.time()
    for i in targets:
        if state_up[i.id]:
            op_time[i.id] += t_end - up_since[i.id]
    runner.stop()
    rows = sorted(runner.rec.rows, key=lambda r: r["t"])
    evs = events.read_all(data_dir)
    cluster.stop()

    # ---- component level: failures, uptime, repair time (kill -> ready)
    ready = [e for e in evs if e["event"] == "instance_ready"]
    repairs = []
    for t, iid, _ in kills:
        nxt = [e["ts"] - t0 for e in ready if e["target"] == iid and e["ts"] - t0 > t]
        if nxt:
            repairs.append(min(nxt) - t)
    total_uptime = sum(op_time.values())
    n_fail = len(kills)
    comp = {"component_failures": n_fail, "component_operating_time_s": round(total_uptime, 1),
            "observed_failure_rate_per_component_s": n_fail / total_uptime if total_uptime else None,
            "MTTF_component_s": total_uptime / n_fail if n_fail else None,
            "MTTR_component_s": sum(repairs) / len(repairs) if repairs else None,
            "n_components": len(targets)}
    if comp["MTTF_component_s"] and comp["MTTR_component_s"]:
        comp["MTBF_component_s"] = comp["MTTF_component_s"] + comp["MTTR_component_s"]
        comp["availability_component"] = comp["MTTF_component_s"] / comp["MTBF_component_s"]

    # ---- user level: a tick is "down" when any of the 4 probes failed
    probes = [r for r in rows if r["kind"] == "probe" and r["t"] >= t0]
    ticks = {}
    for r in probes:
        k = int((r["t"] - t0) / TICK)
        ticks.setdefault(k, []).append(r["ok"])
    n_ticks = int(duration / TICK)
    down = [k for k in range(n_ticks) if k in ticks and not all(ticks[k])]
    incidents = []
    for k in down:                                   # merge ticks separated by < 1 s into one incident
        t = k * TICK
        if incidents and t - incidents[-1][1] < 1.0:
            incidents[-1][1] = t + TICK
        else:
            incidents.append([t, t + TICK])
    down_time = sum(b - a for a, b in incidents)
    per_service = {}
    for op in ("probe_student", "probe_records", "probe_timetable", "probe_payment"):
        pr = [r for r in probes if r["op"] == op]
        per_service[op.replace("probe_", "")] = round(sum(r["ok"] for r in pr) / len(pr), 5) if pr else None
    sysm = {"probe_cycles": len(ticks), "failed_cycles": len(down), "incidents": len(incidents),
            "downtime_s": round(down_time, 2), "availability_observed": 1 - down_time / duration,
            "availability_per_service": per_service,
            "MTTR_system_s": down_time / len(incidents) if incidents else 0.0,
            "MTTF_system_s": (duration - down_time) / len(incidents) if incidents else None,
            "incident_durations_s": [round(b - a, 2) for a, b in incidents]}
    if incidents:
        sysm["MTBF_system_s"] = sysm["MTTF_system_s"] + sysm["MTTR_system_s"]
    load = [r for r in rows if r["kind"] == "load"]
    reqs = {"requests": len(load), "failed": sum(1 for r in load if not r["ok"]),
            "succeeded_first_try": sum(1 for r in load if r["ok"] and r.get("gw_attempts", 1) == 1 and not r["degraded"]),
            "recovered_by_retry_or_failover": sum(1 for r in load if r["ok"] and r.get("gw_attempts", 1) > 1),
            "served_degraded": sum(1 for r in load if r["ok"] and r["degraded"]),
            "queued_accepted": sum(1 for r in load if r["ok"] and r["queued"])}
    reqs["failure_ratio"] = reqs["failed"] / reqs["requests"] if reqs["requests"] else None

    mttr_b = comp.get("MTTR_component_s") or 6.3
    theory = calc.accelerated(mttf, mttr_b, mttr_b)       # same measured repair time for the chosen design
    raw = os.path.join(RESULTS, "raw", "SOAK", mode)
    os.makedirs(raw, exist_ok=True)
    with open(os.path.join(raw, "probes.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t_rel", "latency_s", "ok", "op"])
        for r in probes:
            w.writerow([round(r["t"] - t0, 3), round(r["lat"], 4), int(r["ok"]), r["op"]])
    with open(os.path.join(raw, "kills.json"), "w") as f:
        json.dump({"kills": kills, "repairs_s": repairs, "incidents": incidents}, f)
    with open(os.path.join(raw, "events.jsonl"), "w") as f:
        for e in evs:
            f.write(json.dumps(dict(e, t_rel=round(e["ts"] - t0, 3))) + "\n")
    shutil.rmtree(data_dir, ignore_errors=True)
    return {"mode": mode, "duration_s": duration, "mttf_injected_s": mttf, "seed": seed, "component": comp,
            "system": sysm, "requests": reqs, "theory_inputs": {"mttf_s": mttf, "mttr_s": mttr_b}, "theory": theory}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--mttf", type=float, default=60)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--modes", nargs="*", default=["baseline", "ft"])
    a = ap.parse_args()
    out = {}
    path = os.path.join(RESULTS, "soak.json")
    if os.path.exists(path):
        out = json.load(open(path))
    for m in a.modes:
        r = run_soak(m, a.duration, a.mttf, a.seed)
        out[m] = r
        s, c = r["system"], r["component"]
        print(f"[{m}] comp failures={c['component_failures']} MTTR_comp={c['MTTR_component_s']:.2f}s | "
              f"incidents={s['incidents']} A_obs={s['availability_observed']:.4f} "
              f"failed={r['requests']['failed']}/{r['requests']['requests']}", flush=True)
        with open(path, "w") as f:
            json.dump(out, f, indent=1)


if __name__ == "__main__":
    main()
