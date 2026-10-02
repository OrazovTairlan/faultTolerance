"""BASELINE Timetable service: builds a schedule from the student's enrollments (remote call, no protection)."""
import httpx
from fastapi import FastAPI, HTTPException

from baseline.db import connect
from common import appkit, topology

app = FastAPI(title="baseline-timetable")
appkit.install(app, "timetable")

_mode, _inst = topology.load()
STUDENT_URL = topology.urls_of(_inst, "student")[0]
http = httpx.Client(timeout=10.0)


@app.get("/timetable/{sid}")
def timetable(sid: str):
    r = http.get(f"{STUDENT_URL}/enrollments/{sid}")
    if r.status_code == 404:
        raise HTTPException(404, "student not found")
    r.raise_for_status()
    courses = r.json()["courses"]
    c = connect()
    try:
        marks = ",".join("?" * len(courses)) or "NULL"
        rows = c.execute(f"SELECT id, title, slot, room FROM courses WHERE id IN ({marks}) ORDER BY slot",
                         courses).fetchall()
    finally:
        c.close()
    return {"student_id": sid, "entries": [{"course": a, "title": b, "slot": s, "room": r_} for a, b, s, r_ in rows]}
