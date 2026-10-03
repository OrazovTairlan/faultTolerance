"""Glue shared by the fault-tolerant services: DB handle, readiness endpoint, response helpers."""
import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from common import appkit
from ft.db import DBUnavailable, ReplicatedDB


def setup(app: FastAPI, service: str):
    instance = os.environ["INSTANCE_ID"]
    metrics = appkit.install(app, service)
    db = ReplicatedDB(os.environ["DATA_DIR"], instance)

    @metrics.collect
    def _db_metrics():
        out = [("db_primary_available", "gauge", {}, int(db._ok)),
               ("db_circuit_open", "gauge", {}, int(db.breaker.state != "closed")),
               ("db_journal_pending", "gauge", {}, db.journal.size())]
        return out + [(f"db_{k}_total", "counter", {}, v) for k, v in db.counters.items()]

    @app.get("/health/ready")
    def ready():
        """Readiness: can the service reach at least one copy of the database?"""
        try:
            return {"status": "ready", "db": db.check(), "journal": db.journal.size()}
        except DBUnavailable as e:
            return JSONResponse({"status": "not_ready", "error": str(e)[:80]}, status_code=503)

    return db


def reply(status, body, degraded=None):
    headers = {"X-Degraded": degraded} if degraded else None
    if degraded and isinstance(body, dict):
        body = {**body, "degraded": True}
    return JSONResponse(body, status_code=status, headers=headers)


def unavailable(err):
    return JSONResponse({"error": "database unavailable", "detail": str(err)[:80]}, status_code=503)
