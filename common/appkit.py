"""Shared FastAPI plumbing: fault-injection middleware, Prometheus-style metrics, liveness endpoint."""
import asyncio
import os
import threading
import time
from collections import defaultdict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from common import faults


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.count = defaultdict(int)       # (code) -> n
        self.dur_sum = 0.0
        self.dur_n = 0
        self.extra = defaultdict(int)       # custom counters (retries, queued, ...)
        self.collectors = []                # callables -> [(name, type, {extra labels}, value)], read at scrape time

    def collect(self, fn):
        """Register a scrape-time collector (usable as a decorator)."""
        self.collectors.append(fn)
        return fn

    def observe(self, code, dur):
        with self.lock:
            self.count[code] += 1
            self.dur_sum += dur
            self.dur_n += 1

    def inc(self, name, n=1):
        with self.lock:
            self.extra[name] += n

    def render(self, service, instance):
        lab = f'service="{service}",instance="{instance}"'
        lines = ["# TYPE http_requests_total counter"]
        with self.lock:
            for code, n in sorted(self.count.items()):
                lines.append(f'http_requests_total{{{lab},code="{code}"}} {n}')
            lines.append("# TYPE http_request_duration_seconds summary")
            lines.append(f"http_request_duration_seconds_sum{{{lab}}} {self.dur_sum:.6f}")
            lines.append(f"http_request_duration_seconds_count{{{lab}}} {self.dur_n}")
            for k, v in sorted(self.extra.items()):
                lines.append(f"# TYPE ft_{k} counter")
                lines.append(f"ft_{k}{{{lab}}} {v}")
        families = {}                       # name -> (type, [lines]); the exposition format wants each family contiguous
        for fn in self.collectors:
            try:
                samples = fn()
            except Exception:  # noqa: BLE001  a broken collector must never break the scrape
                continue
            for name, kind, labels, value in samples:
                extra = "".join(f',{k}="{v}"' for k, v in labels.items())
                families.setdefault(name, (kind, []))[1].append(f"{name}{{{lab}{extra}}} {float(value):g}")
        for name, (kind, rows) in families.items():
            lines.append(f"# TYPE {name} {kind}")
            lines += rows
        return "\n".join(lines) + "\n"


def install(app: FastAPI, service: str) -> Metrics:
    """Adds metrics, /metrics, /health/live and the failure-injection middleware."""
    instance = os.environ.get("INSTANCE_ID", service)
    metrics = Metrics()
    app.state.metrics = metrics

    @app.middleware("http")
    async def inject_and_measure(request: Request, call_next):
        t0 = time.perf_counter()
        path = request.url.path
        if not path.startswith(("/health", "/metrics", "/admin")):
            delay = faults.get("delay_ms", service)
            if delay:
                await asyncio.sleep(delay / 1000.0)          # injected slow / hung network link
            if faults.chance("error_rate", service):
                metrics.observe(503, time.perf_counter() - t0)
                return JSONResponse({"error": "injected network failure"}, status_code=503)
        response = await call_next(request)
        metrics.observe(response.status_code, time.perf_counter() - t0)
        return response

    @app.get("/health/live")
    async def live():
        return {"status": "alive", "instance": instance}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def prom():
        return metrics.render(service, instance)

    return metrics
