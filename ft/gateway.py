"""FAULT-TOLERANT API gateway / load balancer.

  * load balancing        - round-robin over healthy replicas of each service
  * health checks         - active probe of /health/ready every 0.5 s (2 failures -> ejected, 1 success -> back)
  * timeouts              - 2 s per attempt, 4.2 s total budget (< client's 5 s)
  * retry + backoff       - up to 4 attempts, exponential backoff with jitter, always on a *different* replica
  * circuit breaker       - per replica (5 consecutive failures -> open 2 s -> half-open trial)
  * idempotency           - an Idempotency-Key is attached to every POST so retries cannot duplicate work
  * graceful degradation  - stale cached answer for GETs when no replica can answer; priority-based load
                            shedding (503 + Retry-After) instead of collapsing under overload
"""
import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response

from common import appkit, events, topology
from common.resilience import CircuitBreaker, backoff_delay

COMPONENT = os.environ["INSTANCE_ID"]
_mode, _inst = topology.load()

PER_ATTEMPT_TIMEOUT = 2.0
TOTAL_BUDGET = 4.2
MAX_ATTEMPTS = 4
HEALTH_INTERVAL = 0.5
CACHE_MAX_AGE = 600.0

# in-flight limits per priority class (graceful degradation under overload)
LIMIT_CRITICAL = int(os.environ.get("LIMIT_CRITICAL", 200))   # payments, grades
LIMIT_NORMAL = int(os.environ.get("LIMIT_NORMAL", 120))       # student / balance reads, enrollments
LIMIT_LOW = int(os.environ.get("LIMIT_LOW", 60))              # transcript + timetable generation


class Replica:
    def __init__(self, inst):
        self.id, self.url, self.node = inst.id, inst.url, inst.node
        self.healthy = True
        self.fails = 0
        self.breaker = CircuitBreaker(inst.id, failure_threshold=5, reset_timeout=2.0, on_change=self._changed)

    def _changed(self, name, state):
        events.emit(COMPONENT, f"circuit_{state}", target=name)


REPLICAS = {s: [Replica(i) for i in _inst if i.service == s] for s in topology.SERVICES}
_rr = {s: 0 for s in REPLICAS}
cache = {}                      # (service, path?query) -> (ts, status, body, content_type)
inflight = 0
stats = {"retries": 0, "stale": 0, "shed": 0, "failed": 0}
client = httpx.AsyncClient(timeout=httpx.Timeout(PER_ATTEMPT_TIMEOUT, connect=0.3),
                           limits=httpx.Limits(max_connections=400, max_keepalive_connections=100))
hc = httpx.AsyncClient(timeout=httpx.Timeout(1.0, connect=0.3))


def eject(rep, reason):
    if rep.healthy:
        rep.healthy = False
        events.emit(COMPONENT, "replica_ejected", target=rep.id, reason=reason)


async def check(rep):
    try:
        ok = (await hc.get(rep.url + "/health/ready")).status_code == 200
    except Exception:  # noqa: BLE001
        ok = False
    if ok:
        rep.fails = 0
        if not rep.healthy:
            rep.healthy = True
            rep.breaker.reset()
            events.emit(COMPONENT, "replica_healthy", target=rep.id)
    else:
        rep.fails += 1
        if rep.fails >= 2:
            eject(rep, "health_check")


async def health_loop():
    while True:
        await asyncio.gather(*[check(r) for reps in REPLICAS.values() for r in reps])
        await asyncio.sleep(HEALTH_INTERVAL)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(health_loop())
    yield
    task.cancel()


app = FastAPI(title="ft-gateway", lifespan=lifespan)
metrics = appkit.install(app, "gateway")


@metrics.collect
def _gateway_metrics():
    """Fault-tolerance activity for Prometheus/Grafana: what the gateway masked and how it sees each replica."""
    out = [(f"gateway_{k}_total", "counter", {}, v) for k, v in stats.items()]
    out.append(("gateway_inflight_requests", "gauge", {}, inflight))
    out += [("gateway_replica_healthy", "gauge", {"target": r.id, "target_service": s}, int(r.healthy))
            for s, rs in REPLICAS.items() for r in rs]
    out += [("gateway_circuit_open", "gauge", {"target": r.id, "target_service": s}, int(r.breaker.state != "closed"))
            for s, rs in REPLICAS.items() for r in rs]
    return out


@app.get("/health/ready")
async def ready():
    return {"status": "ready", "healthy": {s: sum(r.healthy for r in rs) for s, rs in REPLICAS.items()}}


@app.get("/admin/state")
async def state():
    return {"inflight": inflight, "stats": stats, "replicas": {s: [
        {"id": r.id, "healthy": r.healthy, "breaker": r.breaker.state} for r in rs] for s, rs in REPLICAS.items()}}


def pick(service, tried):
    reps = REPLICAS[service]
    n = len(reps)
    _rr[service] += 1
    order = [reps[(_rr[service] + i) % n] for i in range(n)]
    for want_healthy in (True, False):                   # prefer healthy replicas, fall back to "unknown" ones
        for r in order:
            if r in tried or r.healthy != want_healthy:
                continue
            if r.breaker.allow():
                return r
    return None


def classify(service, method, path):
    if method != "GET" and service in ("payment", "records"):
        return "critical", LIMIT_CRITICAL
    if service in ("records", "timetable"):
        return "low", LIMIT_LOW
    return "normal", LIMIT_NORMAL


def _resp(status, body, ctype="application/json", **headers):
    return Response(body, status, media_type=ctype, headers=headers)


@app.api_route("/api/{service}/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy(service: str, path: str, request: Request):
    global inflight
    if service not in REPLICAS:
        return _resp(404, '{"error":"unknown service"}')
    klass, limit = classify(service, request.method, path)
    if inflight >= limit:                                          # load shedding
        stats["shed"] += 1
        return _resp(503, '{"error":"overloaded","shed":true}', **{"Retry-After": "1", "X-Shed": klass})
    inflight += 1
    try:
        return await forward(service, path, request)
    finally:
        inflight -= 1


async def forward(service, path, request):
    method = request.method
    body = await request.body()
    query = request.url.query
    headers = {k: v for k, v in request.headers.items() if k.lower() in ("content-type", "idempotency-key")}
    if method != "GET" and "idempotency-key" not in headers:
        headers["idempotency-key"] = uuid.uuid4().hex             # makes gateway retries of POSTs safe
    ckey = (service, path + ("?" + query if query else ""))
    deadline = time.monotonic() + TOTAL_BUDGET
    tried, last_err = [], "no_replica_available"

    for attempt in range(MAX_ATTEMPTS):
        remaining = deadline - time.monotonic()
        if remaining <= 0.05:
            last_err = "budget_exhausted"
            break
        rep = pick(service, tried)
        if rep is None:
            if tried:                                              # every replica tried once -> allow a second round
                tried = []
                rep = pick(service, tried)
            if rep is None:
                break
        tried.append(rep)
        try:
            r = await client.request(method, f"{rep.url}/{path}", params=query or None, content=body, headers=headers,
                                     timeout=httpx.Timeout(min(PER_ATTEMPT_TIMEOUT, remaining), connect=0.3))
            if r.status_code >= 500:
                raise httpx.HTTPStatusError(str(r.status_code), request=None, response=r)
            rep.breaker.success()
            deg = r.headers.get("x-degraded")
            if method == "GET" and r.status_code == 200 and not deg:
                cache[ckey] = (time.time(), r.status_code, r.content, r.headers.get("content-type"))
            extra = {"X-Served-By": rep.id, "X-Attempts": str(attempt + 1)}
            if deg:
                extra["X-Degraded"] = deg
            return _resp(r.status_code, r.content, r.headers.get("content-type", "application/json"), **extra)
        except (httpx.ConnectError, httpx.ConnectTimeout):
            rep.breaker.failure()
            eject(rep, "connect_error")                            # passive detection: instant ejection
            last_err = "connect_error"
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            rep.breaker.failure()
            last_err = type(e).__name__
        stats["retries"] += 1
        if attempt < MAX_ATTEMPTS - 1:
            await asyncio.sleep(min(backoff_delay(attempt, base=0.05, cap=0.5), max(0, deadline - time.monotonic())))

    stats["failed"] += 1
    hit = cache.get(ckey) if method == "GET" else None
    if hit and time.time() - hit[0] < CACHE_MAX_AGE:               # graceful degradation: stale-but-useful answer
        stats["stale"] += 1
        return _resp(hit[1], hit[2], hit[3] or "application/json", **{"X-Degraded": "stale-cache"})
    return _resp(503, '{"error":"service unavailable","service":"%s","reason":"%s"}' % (service, last_err),
                 **{"Retry-After": "1"})
