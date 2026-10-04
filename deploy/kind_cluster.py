"""One-command local Kubernetes deployment (kind: 1 control plane + 2 workers = nodes A and B).

    python -m deploy.kind_cluster up        # build image, create cluster, deploy services + Prometheus + Grafana
    python -m deploy.kind_cluster status    # pods per node, endpoints
    python -m deploy.kind_cluster down      # delete the cluster and its shared data

After `up`:  gateway http://localhost:30080   Prometheus http://localhost:30090   Grafana http://localhost:30300
Needs Docker, kubectl (ships with Docker Desktop) and kind (`go install sigs.k8s.io/kind@v0.27.0`, or a binary
from https://kind.sigs.k8s.io).
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLUSTER = "univ"
CTX = f"kind-{CLUSTER}"
NS = "univ"
IMAGE = "univ-is:latest"
SHARED = "/tmp/univ-shared"          # directory on the Docker host mounted into every node (see kind-cluster.yaml)
URLS = {"gateway": "http://localhost:30080/api/student/students/S0001",
        "prometheus": "http://localhost:30090/-/ready", "grafana": "http://localhost:30300/api/health"}


def tool(name):
    exe = shutil.which(name) or shutil.which(os.path.join(os.path.expanduser("~"), "go", "bin", name))
    if not exe:
        sys.exit(f"'{name}' not found on PATH")
    return exe


def run(*cmd, check=True, capture=False):
    print("$", " ".join(cmd), flush=True)
    r = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=capture)
    if check and r.returncode:
        sys.exit(r.returncode if not capture else f"failed: {r.stderr.strip()}")
    return r


def kubectl(*args, **kw):
    return run(tool("kubectl"), "--context", CTX, *args, **kw)


def wait_http(name, url, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status == 200:
                    print(f"  {name:10} OK  {url}")
                    return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(2)
    print(f"  {name:10} NOT READY  {url}")
    return False


def up():
    kind = tool("kind")
    run("docker", "build", "-t", IMAGE, ".")
    existed = CLUSTER in run(kind, "get", "clusters", capture=True).stdout.split()
    if not existed:
        wipe()                                    # a new cluster starts with a fresh database
        run(kind, "create", "cluster", "--name", CLUSTER, "--config", "deploy/kind/kind-cluster.yaml")
    run(kind, "load", "docker-image", IMAGE, "--name", CLUSTER)
    kubectl("apply", "-k", "deploy/kind")
    kubectl("-n", NS, "wait", "--for=condition=complete", "job/init-data", "--timeout=180s")
    if existed:                                   # pods keep the old image until restarted (rolling, one by one)
        for svc in ("gateway", "student", "payment", "records", "timetable", "monitor"):
            kubectl("-n", NS, "rollout", "restart", f"statefulset/{svc}", check=False, capture=True)
    for svc in ("gateway", "student", "payment", "records", "timetable", "monitor"):
        kubectl("-n", NS, "rollout", "status", f"statefulset/{svc}", "--timeout=300s")
    for d in ("prometheus", "grafana"):
        kubectl("-n", NS, "rollout", "status", f"deployment/{d}", "--timeout=300s")
    status()
    print("\nendpoints:")
    ok = all([wait_http(n, u) for n, u in URLS.items()])
    print("\nGrafana dashboard: http://localhost:30300/d/univ-is   Prometheus alerts: http://localhost:30090/alerts")
    sys.exit(0 if ok else 1)


def status():
    kubectl("get", "nodes")
    kubectl("-n", NS, "get", "pods", "-o", "wide")


def wipe():
    """The shared directory lives on the Docker host and outlives the cluster - clear it."""
    run("docker", "run", "--rm", "-v", f"{SHARED}:/s", IMAGE, "sh", "-c", "rm -rf /s/* /s/.[!.]*", check=False)


def down():
    run(tool("kind"), "delete", "cluster", "--name", CLUSTER)
    wipe()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["up", "status", "down"])
    {"up": up, "status": status, "down": down}[ap.parse_args().action]()


if __name__ == "__main__":
    main()
