"""Charts from the experiment data. Run: python -m experiments.make_charts"""
import json
import os
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(ROOT, "results")
FIG = os.path.join(RES, "figures")
COL = {"baseline": "#c0392b", "ft": "#1f7a4d"}
LABEL = {"baseline": "Baseline", "ft": "Fault-tolerant"}


def load():
    return json.load(open(os.path.join(RES, "experiments.json")))


def mean(xs):
    xs = [x for x in xs if x is not None]
    return statistics.mean(xs) if xs else None


def timelines(data):
    os.makedirs(FIG, exist_ok=True)
    exps = sorted({d["experiment"] for d in data})
    fig, axes = plt.subplots(len(exps), 2, figsize=(12, 2.3 * len(exps)), sharey=False)
    for r, e in enumerate(exps):
        for c, m in enumerate(("baseline", "ft")):
            ax = axes[r][c]
            runs = [d for d in data if d["experiment"] == e and d["mode"] == m]
            if not runs:
                continue
            d = sorted(runs, key=lambda x: x["rep"])[0]
            ok, bad = d["timeline"]["ok"], d["timeline"]["failed"]
            xs = list(range(len(ok)))
            ax.bar(xs, ok, color="#8fd0a8", width=1.0, label="successful")
            ax.bar(xs, bad, bottom=ok, color="#e57373", width=1.0, label="failed")
            ax.axvspan(d["t_inject_rel"], max(d["t_fault_end_rel"], d["t_inject_rel"] + 0.4), color="#f4d03f", alpha=0.35,
                       label="fault active")
            ax.set_title(f"{e} {d['name']} - {LABEL[m]}", fontsize=8.5)
            ax.tick_params(labelsize=7)
            if r == len(exps) - 1:
                ax.set_xlabel("time (s)", fontsize=8)
            if c == 0:
                ax.set_ylabel("requests / s", fontsize=8)
    axes[0][0].legend(fontsize=7, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "timelines.png"), dpi=140)
    plt.close(fig)


def summary(data):
    exps = sorted({d["experiment"] for d in data})
    names = {e: next(d["name"] for d in data if d["experiment"] == e) for e in exps}
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
    metrics = [("failed_requests", "Failed requests (mean of runs)"), ("outage_s", "User-visible outage (s)"),
               ("success_rate", "Success rate during experiment window")]
    w = 0.38
    for ax, (key, title) in zip(axes, metrics):
        for k, m in enumerate(("baseline", "ft")):
            vals = [mean([d[key] for d in data if d["experiment"] == e and d["mode"] == m]) or 0 for e in exps]
            bars = ax.bar([i + (k - 0.5) * w for i in range(len(exps))], vals, w, color=COL[m], label=LABEL[m])
            for b, v in zip(bars, vals):
                ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.2f}" if key != "failed_requests" else f"{v:.0f}",
                        ha="center", va="bottom", fontsize=6.5)
        ax.set_xticks(range(len(exps)))
        ax.set_xticklabels([e + chr(10) + {"E1": "crash", "E2": "DB", "E3": "network", "E4": "node", "E5": "txn", "E6": "load", "E7": "storage"}[e] for e in exps], fontsize=8)
        ax.set_title(title, fontsize=10)
        if key == "success_rate":
            ax.set_ylim(0, 1.08)
    axes[0].legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "summary.png"), dpi=150)
    plt.close(fig)


def soak():
    path = os.path.join(RES, "soak.json")
    if not os.path.exists(path):
        return
    s = json.load(open(path))
    fig, axes = plt.subplots(2, 1, figsize=(11, 5), sharex=True)
    for ax, m in zip(axes, ("baseline", "ft")):
        if m not in s:
            continue
        raw = json.load(open(os.path.join(RES, "raw", "SOAK", m, "kills.json")))
        for a, b in raw["incidents"]:
            ax.axvspan(a, b, color="#e57373", alpha=0.8)
        for t, iid, _ in raw["kills"]:
            ax.axvline(t, color="#555", lw=0.7, ls=":")
        ax.set_title(f"{LABEL[m]}: dotted = component killed, red = user-visible outage "
                     f"(A = {s[m]['system']['availability_observed']:.4f}, {s[m]['system']['incidents']} incidents, "
                     f"{s[m]['component']['component_failures']} component failures)", fontsize=9)
        ax.set_yticks([])
    axes[1].set_xlabel("time (s)")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "soak_timeline.png"), dpi=140)
    plt.close(fig)


if __name__ == "__main__":
    d = load()
    timelines(d)
    summary(d)
    soak()
    print("charts written to", FIG)
