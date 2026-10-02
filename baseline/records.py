"""BASELINE Academic-records / transcript service.

Calls the student service synchronously with a generous (10 s) timeout, no retry, no cache.
Grade submission commits the grade and its audit entry separately (non-atomic).
"""
import time

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from baseline.db import connect
from common import appkit, faults, topology

app = FastAPI(title="baseline-records")
appkit.install(app, "records")

_mode, _inst = topology.load()
STUDENT_URL = topology.urls_of(_inst, "student")[0]
http = httpx.Client(timeout=10.0)


class Grade(BaseModel):
    student_id: str
    course_id: str
    grade: float
    idempotency_key: str | None = None


@app.get("/transcript/{sid}")
def transcript(sid: str):
    r = http.get(f"{STUDENT_URL}/students/{sid}")           # any failure here fails the whole request
    if r.status_code == 404:
        raise HTTPException(404, "student not found")
    r.raise_for_status()
    student = r.json()
    c = connect()
    try:
        rows = c.execute("SELECT course_id, grade FROM grades WHERE student_id=? ORDER BY course_id", (sid,)).fetchall()
    finally:
        c.close()
    gpa = round(sum(g for _, g in rows) / len(rows), 2) if rows else None
    return {"student": student, "grades": [{"course": a, "grade": b} for a, b in rows], "average": gpa}


@app.post("/grades")
def submit_grade(g: Grade):
    c = connect()
    try:
        c.execute("INSERT OR REPLACE INTO grades VALUES(?,?,?,?)", (g.student_id, g.course_id, g.grade, time.time()))
        c.commit()
        faults.crash_if("crash_mid_grade")                    # injected crash between the two commits
        c.execute("INSERT INTO audit_log(kind, ref, ts) VALUES('grade', ?, ?)",
                  (f"{g.student_id}:{g.course_id}", time.time()))
        c.commit()
    finally:
        c.close()
    return {"status": "recorded"}
