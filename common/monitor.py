"""Monitoring & logging service.

Polls the health endpoint of every component (0.5 s), keeps their up/down state, writes
`target_down` / `target_up` events to the shared event log and exposes Prometheus metrics
(`/metrics`) plus a JSON view (`/status`, `/events`).  Baseline polls /health/live (process
alive); the fault-tolerant system polls /health/ready (process + database reachable).
"""
import asyncio
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from common import events, topology

MODE, INSTANCES = topology.load()
HEALTH_PATH = "/health/ready" if MODE == "ft" else "/health/live"
INTERVAL = 0.5
THRESHOLD = 2   # consecutive failed polls before a target is declared down

state = {i.id: {"service": i.service, "node": i.node, "up": True, "fails": 0, "since": time.time(),
                "last_latency_ms": None} for i in INSTANCES if i.service != "monitor"}


async def poll_one(client, inst):
    st = state[inst.id]
    t0 = time.perf_counter()
    try:
        r = await client.get(inst.url + HEALTH_PATH)
        ok = r.status_code == 200
    except Exception:
        ok = False
    st["last_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    if ok:
        st["fails"] = 0
        if not st["up"]:
            st["up"], st["since"] = True, time.time()
            events.emit("monitor", "target_up", target=inst.id)
    else:
        st["fails"] += 1
        if st["up"] and st["fails"] >= THRESHOLD:
            st["up"], st["since"] = False, time.time()
            events.emit("monitor", "target_down", target=inst.id)


async def loop():
    timeout = httpx.Timeout(1.0, connect=0.3)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            await asyncio.gather(*[poll_one(client, i) for i in INSTANCES if i.service != "monitor"])
            await asyncio.sleep(INTERVAL)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(loop())
    yield
    task.cancel()


app = FastAPI(title="monitor", lifespan=lifespan)


@app.get("/health/live")
async def live():
    return {"status": "alive"}


@app.get("/status")
async def status():
    return {"mode": MODE, "targets": state}


@app.get("/events")
async def recent_events(n: int = 50):
    return events.read_all(os.environ["DATA_DIR"])[-n:]


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    # `target_up`, not `up`: Prometheus reserves `up` for its own scrape result
    lines = ["# TYPE target_up gauge"]
    for tid, s in state.items():
        lines.append(f'target_up{{service="{s["service"]}",instance="{tid}",node="{s["node"]}"}} {1 if s["up"] else 0}')
    lines.append("# TYPE target_down_since_seconds gauge")
    for tid, s in state.items():
        lines.append(f'target_down_since_seconds{{service="{s["service"]}",instance="{tid}"}} '
                     f'{0 if s["up"] else round(time.time() - s["since"], 1)}')
    lines.append("# TYPE health_check_latency_ms gauge")
    for tid, s in state.items():
        if s["last_latency_ms"] is not None:
            lines.append(f'health_check_latency_ms{{service="{s["service"]}",instance="{tid}"}} {s["last_latency_ms"]}')
    return "\n".join(lines) + "\n"
