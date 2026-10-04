"""Theoretical reliability analysis: MTTF / MTBF / MTTR / availability of the baseline and fault-tolerant designs.

Method
  * every component i is a repairable item with exponential failure (rate lambda_i = 1/MTTF_i) and repair time MTTR_i;
    steady-state availability A_i = MTTF_i / (MTTF_i + MTTR_i), unavailability U_i = 1 - A_i;
  * the system structure is a fault tree (OR = series, AND = redundancy); P(top event) = system unavailability is
    computed *exactly* by state enumeration (shared nodes make a simple product formula inexact);
  * failure frequency f_sys comes from the minimal cut sets, hence
        MTTF_sys = A_sys / f_sys,    MTTR_sys = U_sys / f_sys,    MTBF_sys = 1 / f_sys = MTTF_sys + MTTR_sys.

Two parameter sets are used:
  REAL        - production-scale assumptions (hours)                      -> design-level availability prediction
  ACCELERATED - the time-compressed fault campaign of reliability/soak.py -> comparison with measurements
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from reliability.faulttree import (AND, OR, Basic, cut_sets, failure_frequency, top_probability)  # noqa: E402

SERVICES = ["student", "payment", "records", "timetable"]
HOURS_PER_YEAR = 8766.0

# ---------------------------------------------------------------- assumptions (hours)
# name: (MTTF, MTTR_baseline, MTTR_fault_tolerant)
REAL = {
    "node":     (50_000.0, 4.0, 4.0),             # server / VM host; hardware swap is slow in both designs
    "process":  (720.0, 0.5, 5 / 3600),           # app crash roughly monthly; on-call human (30 min) vs auto-restart (5 s)
    "db":       (8_760.0, 2.0, 2.0),              # DB engine/storage copy: restore from backup vs rebuild replica
    "site":     (100_000.0, 2.0, 2.0),            # power / uplink of the single site (common-cause, not redundant)
}
FAILOVER_S = 2.0                                  # time to detect & switch to the other replica (seconds)
FAILOVER_COVERAGE = 0.95                          # fraction of in-flight requests rescued by retry / failover


def _params(table, design):
    """Expand a component table into per-basic-event (MTTF, MTTR)."""
    col = 1 if design == "baseline" else 2
    p = {}
    nodes = ["A"] if design == "baseline" else ["A", "B"]
    for n in nodes:
        p[f"node:{n}"] = (table["node"][0], table["node"][col])
    reps = [1] if design == "baseline" else [1, 2]
    for t in ["gateway"] + SERVICES:
        for r in reps:
            p[f"proc:{t}:{r}"] = (table["process"][0], table["process"][col])
    for r in reps:
        p[f"db:{r}"] = (table["db"][0], table["db"][col])
    p["site"] = (table["site"][0], table["site"][col])
    return p


def baseline_tree(include_nodes=True, include_db=True, include_site=True):
    kids = []
    if include_nodes:
        kids.append(Basic("node:A"))
    kids += [Basic(f"proc:{t}:1") for t in ["gateway"] + SERVICES]
    if include_db:
        kids.append(Basic("db:1"))
    if include_site:
        kids.append(Basic("site"))
    return OR(*kids, label="University IS unavailable")


def ft_tree(include_nodes=True, include_db=True, include_site=True):
    def replica(base, r):
        node = "A" if r == 1 else "B"
        parts = [Basic(f"{base}:{r}")]
        if include_nodes:
            parts.append(Basic(f"node:{node}"))
        return OR(*parts, label=f"{base} replica {r} down")

    tiers = []
    for t in ["gateway"] + SERVICES:
        tiers.append(AND(replica(f"proc:{t}", 1), replica(f"proc:{t}", 2), label=f"{t} tier down"))
    if include_db:
        tiers.append(AND(replica("db", 1), replica("db", 2), label="database tier down"))
    if include_site:
        tiers.append(Basic("site"))
    return OR(*tiers, label="University IS unavailable")


def analyse(params, tree, failover=None):
    """params: {event: (mttf, mttr)} -> metrics dict. `failover` = (t_fo, coverage, unit-consistent) adds the
    unavailability caused by the switch-over window of every failure of a redundant replica."""
    lam = {k: 1.0 / v[0] for k, v in params.items()}
    U = {k: v[1] / (v[0] + v[1]) for k, v in params.items()}
    cuts = cut_sets(tree)
    u_sys = top_probability(tree, U)
    f_sys = failure_frequency(cuts, lam, U)
    extra_u = 0.0
    if failover:
        t_fo, cov = failover
        extra_u = sum(l for k, l in lam.items() if not k.startswith("site")) * t_fo * (1 - cov)
    u_total = min(1.0, u_sys + extra_u)
    a = 1 - u_total
    mttf = (1 - u_sys) / f_sys if f_sys else float("inf")
    mttr = u_sys / f_sys if f_sys else 0.0
    return {"availability": a, "unavailability": u_total, "u_structural": u_sys, "u_failover": extra_u,
            "failure_frequency": f_sys, "MTTF": mttf, "MTTR": mttr, "MTBF": mttf + mttr,
            "min_cut_sets": len(cuts),
            "single_points_of_failure": sorted(next(iter(c)) for c in cuts if len(c) == 1),
            "cut_set_orders": {str(k): sum(1 for c in cuts if len(c) == k) for k in sorted({len(c) for c in cuts})}}


def component_table(table):
    rows = []
    for name, (mttf, mb, mf) in table.items():
        rows.append({"component": name, "MTTF": mttf, "MTTR_baseline": mb, "MTTR_ft": mf,
                     "MTBF_baseline": mttf + mb, "MTBF_ft": mttf + mf,
                     "A_baseline": mttf / (mttf + mb), "A_ft": mttf / (mttf + mf),
                     "failure_rate_per_h": 1 / mttf})
    return rows


def real_scale():
    b = analyse(_params(REAL, "baseline"), baseline_tree())
    fo = (FAILOVER_S / 3600.0, FAILOVER_COVERAGE)
    f = analyse(_params(REAL, "ft"), ft_tree(), failover=fo)
    for r in (b, f):
        r["downtime_min_per_year"] = r["unavailability"] * HOURS_PER_YEAR * 60
        r["nines"] = -__import__("math").log10(r["unavailability"]) if r["unavailability"] > 0 else float("inf")
    # what-if analysis: contribution of each mechanism / assumption
    scenarios = {}
    no_site = analyse(_params(REAL, "ft"), ft_tree(include_site=False), failover=fo)
    scenarios["ft_without_site_common_cause"] = no_site
    slow_restart = dict(REAL, process=(720.0, 0.5, 0.5))
    scenarios["ft_with_manual_process_restart"] = analyse(_params(slow_restart, "ft"), ft_tree(), failover=fo)
    auto_only = {k: (v[0], REAL["process"][2]) if k.startswith("proc") else v
                 for k, v in _params(REAL, "baseline").items()}
    scenarios["baseline_with_auto_restart_only"] = analyse(auto_only, baseline_tree())
    for r in scenarios.values():
        r["downtime_min_per_year"] = r["unavailability"] * HOURS_PER_YEAR * 60
    return {"assumptions": component_table(REAL), "failover_s": FAILOVER_S, "failover_coverage": FAILOVER_COVERAGE,
            "baseline": b, "ft": f, "scenarios": {k: v for k, v in scenarios.items() if v}}


def accelerated(mttf_s, mttr_baseline_s, mttr_ft_s, failover_s=1.0, coverage=0.9):
    """Model of the soak campaign: only process kills are injected (no node/db/site failures)."""
    base = {k: (mttf_s, mttr_baseline_s) for k in
            [f"proc:{t}:1" for t in ["gateway"] + SERVICES]}
    ft = {k: (mttf_s, mttr_ft_s) for t in ["gateway"] + SERVICES for k in (f"proc:{t}:1", f"proc:{t}:2")}
    b = analyse(base, baseline_tree(False, False, False))
    f = analyse(ft, ft_tree(False, False, False), failover=(failover_s, coverage))
    return {"baseline": b, "ft": f, "mttf_s": mttf_s, "mttr_baseline_s": mttr_baseline_s, "mttr_ft_s": mttr_ft_s}


def main():
    out = {"real": real_scale(), "accelerated_default": accelerated(60.0, 6.3, 1.2)}
    path = os.path.join(ROOT, "results", "reliability_theory.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=1)
    r = out["real"]
    for k in ("baseline", "ft"):
        x = r[k]
        print(f"{k:9s} A={x['availability']:.7f}  downtime={x['downtime_min_per_year']:.1f} min/y  "
              f"MTTF={x['MTTF']:.1f} h  MTTR={x['MTTR']:.3f} h  MTBF={x['MTBF']:.1f} h  SPOF={x['single_points_of_failure']}")
    print(json.dumps({k: round(v['availability'], 7) for k, v in r['scenarios'].items()}, indent=1))


if __name__ == "__main__":
    main()
