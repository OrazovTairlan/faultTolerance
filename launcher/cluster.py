"""Process-level cluster: launches every instance as a real OS process (uvicorn) and models the
infrastructure layer that Kubernetes provides in production.

  * instances live on "nodes" (A, B; the monitor sits on a separate management node M);
  * `supervise="auto"`      -> orchestrator behaviour (fault-tolerant system): a crashed instance is detected within
                               ~0.2 s and restarted immediately (like a Deployment/ReplicaSet + kubelet);
  * `supervise="operator"`  -> baseline behaviour: nobody watches; a human restarts the instance after
                               `restart_delay` seconds (assumed operator reaction time);
  * kill_node() simulates the loss of a whole server: all its processes die and nothing is scheduled there
    until the node is repaired.

Run it by hand:  python -m launcher.cluster --mode ft
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common import events, faults, schema, topology  # noqa: E402
from hardware import backup  # noqa: E402

OPERATOR_DELAY = 5.0          # assumed (accelerated) human reaction time for the baseline, seconds


class Cluster:
    def __init__(self, mode, data_dir, restart_delay=None, supervise=None):
        assert mode in ("baseline", "ft")
        self.mode = mode
        self.data_dir = os.path.abspath(data_dir)
        self.supervise = supervise or ("auto" if mode == "ft" else "operator")
        self.restart_delay = restart_delay if restart_delay is not None else (0.0 if mode == "ft" else OPERATOR_DELAY)
        self.instances = topology.build(mode)
        for i in self.instances:
            if i.service == "monitor":
                i.node = "M"
        self.procs = {}
        self.pending = {}          # instance id -> time at which it will be restarted
        self.down_nodes = {}       # node -> time at which it is repaired
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._threads = []
        self.restart_count = 0

    # ------------------------------------------------------------------ lifecycle
    def start(self, wait=True, timeout=40):
        os.makedirs(os.path.join(self.data_dir, "logs"), exist_ok=True)
        schema.create_data_dir(self.data_dir, self.mode)
        topology.save(self.data_dir, self.mode, self.instances)
        for f in ("events.jsonl", "faults.json"):
            p = os.path.join(self.data_dir, f)
            if os.path.exists(p):
                os.remove(p)
        faults.clear(self.data_dir)
        for inst in self.instances:
            self._spawn(inst)
        if wait:
            self.wait_ready(timeout)
        t = threading.Thread(target=self._supervisor_loop, daemon=True, name="supervisor")
        t.start()
        self._threads.append(t)
        if self.mode == "ft":
            b = threading.Thread(target=self._backup_loop, daemon=True, name="backup")
            b.start()
            self._threads.append(b)
        return self

    def stop(self):
        self._stop.set()
        for p in list(self.procs.values()):
            if p.poll() is None:
                p.kill()
        for p in list(self.procs.values()):
            try:
                p.wait(timeout=5)
            except Exception:
                pass

    def __enter__(self):
        return self.start()

    def __exit__(self, *a):
        self.stop()

    # ------------------------------------------------------------------ processes
    def _module(self, inst):
        if inst.service == "monitor":
            return "common.monitor"
        return f"{'ft' if self.mode == 'ft' else 'baseline'}.{inst.service}"

    def _spawn(self, inst):
        env = dict(os.environ, DATA_DIR=self.data_dir, INSTANCE_ID=inst.id, NODE=inst.node, PYTHONPATH=ROOT,
                   PYTHONUNBUFFERED="1")
        log = open(os.path.join(self.data_dir, "logs", f"{inst.id}.log"), "ab")
        cmd = [sys.executable, "-m", "uvicorn", f"{self._module(inst)}:app", "--host", "127.0.0.1",
               "--port", str(inst.port), "--log-level", "warning", "--no-access-log"]
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        self.procs[inst.id] = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                               creationflags=flags)
        self._save_pids()
        return time.time()

    def inst(self, iid):
        return next(i for i in self.instances if i.id == iid)

    def is_ready(self, inst):
        try:
            return httpx.get(inst.url + "/health/live", timeout=0.3).status_code == 200
        except Exception:
            return False

    def wait_ready(self, timeout=40, only=None):
        t0 = time.time()
        targets = [i for i in self.instances if only is None or i.id in only]
        while time.time() - t0 < timeout:
            if all(self.is_ready(i) for i in targets):
                return time.time() - t0
            time.sleep(0.1)
        raise TimeoutError("cluster did not become ready: " + ", ".join(i.id for i in targets if not self.is_ready(i)))

    def _watch_ready(self, inst, t_spawn):
        def w():
            while not self._stop.is_set() and time.time() - t_spawn < 30:
                if self.is_ready(inst):
                    events.emit("cluster", "instance_ready", data_dir=self.data_dir, target=inst.id,
                                startup_s=round(time.time() - t_spawn, 3))
                    return
                time.sleep(0.05)
        threading.Thread(target=w, daemon=True).start()

    def restart(self, inst, reason):
        with self._lock:
            p = self.procs.get(inst.id)
            if p is not None and p.poll() is None:
                return
            t = self._spawn(inst)
            self.restart_count += 1
        events.emit("cluster", "instance_restarted", data_dir=self.data_dir, target=inst.id, reason=reason)
        self._watch_ready(inst, t)

    # ------------------------------------------------------------------ failure injection API
    def kill_instance(self, iid):
        p = self.procs[iid]
        t = time.time()
        p.kill()
        events.emit("injector", "instance_killed", data_dir=self.data_dir, target=iid)
        return t

    def kill_node(self, node, repair_after):
        t = time.time()
        with self._lock:
            self.down_nodes[node] = t + repair_after
        self._save_pids()
        for inst in self.instances:
            if inst.node == node and self.procs[inst.id].poll() is None:
                self.procs[inst.id].kill()
        events.emit("injector", "node_failed", data_dir=self.data_dir, node=node, repair_after=repair_after)
        return t

    # ------------------------------------------------------------------ supervisor / operator
    def _save_pids(self):
        with open(os.path.join(self.data_dir, "pids.json"), "w", encoding="utf-8") as f:
            json.dump({i: p.pid for i, p in self.procs.items()}, f)

    def _read_control(self):
        """Commands written by the CLI injector (python -m tools.inject kill-node ...)."""
        path = os.path.join(self.data_dir, "control.json")
        try:
            with open(path, encoding="utf-8") as f:
                ctl = json.load(f)
            os.remove(path)
        except (OSError, ValueError):
            return
        for node, until in ctl.get("down", {}).items():
            self.down_nodes[node] = until

    def _supervisor_loop(self):
        while not self._stop.is_set():
            now = time.time()
            self._read_control()
            for node, until in list(self.down_nodes.items()):
                if now >= until:
                    del self.down_nodes[node]
                    events.emit("cluster", "node_repaired", data_dir=self.data_dir, node=node)
                    for inst in self.instances:
                        if inst.node == node:
                            self.pending[inst.id] = 0.0           # node is back: start its instances now
            for inst in self.instances:
                p = self.procs.get(inst.id)
                dead = p is not None and p.poll() is not None
                if not dead or inst.id in self.pending or inst.node in self.down_nodes:
                    continue
                self.pending[inst.id] = now + self.restart_delay
                if self.supervise == "auto":
                    events.emit("supervisor", "crash_detected", data_dir=self.data_dir, target=inst.id)
            for iid, when in list(self.pending.items()):
                inst = self.inst(iid)
                if now >= when and inst.node not in self.down_nodes:
                    del self.pending[iid]
                    self.restart(inst, "orchestrator" if self.supervise == "auto" else "operator")
            self._stop.wait(0.2)

    def _backup_loop(self):
        """Backup infrastructure: periodic consistent snapshots of the replica (keeps the last 3)."""
        while not self._stop.wait(10.0):
            try:
                path = backup.snapshot(os.path.join(self.data_dir, "replica.db"),
                                       os.path.join(self.data_dir, "backups"), keep=3)
                events.emit("backup", "snapshot_created", data_dir=self.data_dir, path=os.path.basename(path))
            except Exception:  # noqa: BLE001
                pass


def main():
    ap = argparse.ArgumentParser(description="Run the university information system locally")
    ap.add_argument("--mode", choices=["baseline", "ft"], default="ft")
    ap.add_argument("--data-dir", default=None)
    a = ap.parse_args()
    data = a.data_dir or os.path.join(ROOT, "data", a.mode)
    with Cluster(a.mode, data) as c:
        print(f"[{a.mode}] system up. Gateways: " + ", ".join(i.url for i in c.instances if i.service == "gateway"))
        print(f"monitor: http://127.0.0.1:8200/status   data dir: {data}\nCtrl+C to stop")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("stopping...")


if __name__ == "__main__":
    main()
