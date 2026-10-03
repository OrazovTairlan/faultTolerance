"""Structured event log (JSON lines) shared by all processes: DATA_DIR/events.jsonl.

Every detection / recovery action emits an event so experiments can compute
detection and recovery times from timestamps instead of guessing.
"""
import json
import os
import time


def emit(component, event, data_dir=None, **fields):
    rec = {"ts": time.time(), "component": component, "event": event, **fields}
    path = os.path.join(data_dir or os.environ.get("DATA_DIR", "."), "events.jsonl")
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except OSError:
        pass  # logging must never take a service down
    return rec


def read_all(data_dir):
    path = os.path.join(data_dir, "events.jsonl")
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    except OSError:
        pass
    return out
