"""BASELINE API gateway: a plain reverse proxy to the single instance of each service.

No health checks, no retries, no circuit breaker, no cache, no load shedding; 10 s upstream timeout.
"""
import httpx
from fastapi import FastAPI, Request, Response

from common import appkit, topology

app = FastAPI(title="baseline-gateway")
appkit.install(app, "gateway")

_mode, _inst = topology.load()
UPSTREAM = {s: topology.urls_of(_inst, s)[0] for s in topology.SERVICES}
client = httpx.AsyncClient(timeout=10.0, limits=httpx.Limits(max_connections=500))


@app.get("/health/ready")
async def ready():
    return {"status": "ok"}


@app.api_route("/api/{service}/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy(service: str, path: str, request: Request):
    if service not in UPSTREAM:
        return Response('{"error":"unknown service"}', 404, media_type="application/json")
    try:
        r = await client.request(request.method, f"{UPSTREAM[service]}/{path}", params=request.url.query or None,
                                 content=await request.body(),
                                 headers={k: v for k, v in request.headers.items()
                                          if k.lower() in ("content-type", "idempotency-key")})
    except Exception:  # noqa: BLE001
        return Response('{"error":"bad gateway"}', 502, media_type="application/json")
    return Response(r.content, r.status_code, media_type=r.headers.get("content-type"))
