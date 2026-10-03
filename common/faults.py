"""Runtime fault-injection flags shared by every process through DATA_DIR/faults.json.

The failure-injection scripts write this file; services read it (cached for 100 ms).
Keys are flat strings, e.g. "delay_ms:student", "error_rate:student", "crash_after_charge".
"""
import json
import os
import random
import time

_cache = {"t": 0.0, "v": {}}


def _path(data_dir=None):
    return os.path.join(data_dir or os.environ["DATA_DIR"], "faults.json")


def all_faults():
    now = time.monotonic()
    if now - _cache["t"] > 0.1:
        try:
            with open(_path(), encoding="utf-8") as f:
                _cache["v"] = json.load(f)
        except Exception:  # missing file / concurrent replace -> keep last known value
            pass
        _cache["t"] = now
    return _cache["v"]


def get(name, service=None, default=0):
    f = all_faults()
    if service is not None and f"{name}:{service}" in f:
        return f[f"{name}:{service}"]
    return f.get(name, default)


def chance(name, service=None):
    p = get(name, service, 0)
    return bool(p) and random.random() < p


def write(data_dir, faults):
    """Atomically replace the fault set (used by injectors)."""
    tmp = _path(data_dir) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(faults, f)
    for _ in range(20):
        try:
            os.replace(tmp, _path(data_dir))
            return
        except PermissionError:
            time.sleep(0.01)
    raise RuntimeError("could not update faults.json")


def clear(data_dir):
    write(data_dir, {})


def crash_if(name, service=None):
    """Hard-kill the process (no cleanup, like a power loss / SIGKILL)."""
    if chance(name, service):
        os._exit(137)
