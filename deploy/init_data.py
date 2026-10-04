"""Container bootstrap: create the seeded database (once) and install the topology file into the shared volume.

    python -m deploy.init_data ft        # inside the `init` container / Kubernetes init Job
"""
import os
import shutil
import sys

from common import schema

mode = sys.argv[1] if len(sys.argv) > 1 else "ft"
data = os.environ.get("DATA_DIR", "/data")
os.makedirs(data, exist_ok=True)
marker = os.path.join(data, "baseline.db" if mode == "baseline" else "primary.db")
if not os.path.exists(marker):
    schema.create_data_dir(data, mode)
    print(f"database created in {data}")
else:
    print("database already present - keeping existing data")
src = os.environ.get("TOPOLOGY_SRC", "/app/deploy/topology.json")
if os.path.exists(src):
    shutil.copyfile(src, os.path.join(data, "topology.json"))
# written last: Kubernetes pods wait for it in an initContainer, so no service ever sees a half-created database
with open(os.path.join(data, ".initialized"), "w", encoding="utf-8") as f:
    f.write(mode + "\n")
