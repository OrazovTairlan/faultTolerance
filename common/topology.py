"""Deployment topology: which instance of which service runs on which node/port."""
import json
import os
from dataclasses import dataclass, asdict

SERVICES = ["student", "payment", "records", "timetable"]
HOST = "127.0.0.1"


@dataclass
class Instance:
    service: str      # gateway | student | payment | records | timetable | monitor
    id: str           # e.g. payment-2
    node: str         # A | B  (a "node" = one physical server / Kubernetes node)
    port: int
    host: str = HOST   # 127.0.0.1 for the local cluster, DNS name in Docker / Kubernetes

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"


def build(mode):
    """baseline: one instance of everything on one node.
    ft: two replicas of every service on two nodes, two gateways, replicated DB."""
    inst = []
    if mode == "baseline":
        inst.append(Instance("gateway", "gateway-1", "A", 8000))
        for i, s in enumerate(SERVICES):
            inst.append(Instance(s, f"{s}-1", "A", 8110 + 10 * i))
    else:
        inst.append(Instance("gateway", "gateway-1", "A", 8000))
        inst.append(Instance("gateway", "gateway-2", "B", 8001))
        for i, s in enumerate(SERVICES):
            inst.append(Instance(s, f"{s}-1", "A", 8110 + 10 * i))
            inst.append(Instance(s, f"{s}-2", "B", 8111 + 10 * i))
    inst.append(Instance("monitor", "monitor-1", "A", 8200))
    return inst


def save(data_dir, mode, instances):
    with open(os.path.join(data_dir, "topology.json"), "w", encoding="utf-8") as f:
        json.dump({"mode": mode, "instances": [asdict(i) for i in instances]}, f, indent=1)


def load(data_dir=None):
    path = os.environ.get("TOPOLOGY_FILE") or os.path.join(data_dir or os.environ["DATA_DIR"], "topology.json")
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    return d["mode"], [Instance(**i) for i in d["instances"]]


def urls_of(instances, service):
    return [i.url for i in instances if i.service == service]
