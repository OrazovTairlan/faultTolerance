"""FAULT-TOLERANT Academic-records / transcript service.

  * transcript: student data comes from the student service through a resilient client
    (0.6 s timeout, retry with backoff on another replica, circuit breaker); if it stays unavailable the
    transcript is served from the last-known copy (graceful degradation) -- grades still come from our DB.
  * grade submission: grade + audit entry are one atomic transaction (a crash in the middle rolls back).
"""
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common import faults
from ft.clients import ServiceClient, ServiceUnavailable
from ft.common_ft import reply, setup, unavailable
from ft.db import DBUnavailable

app = FastAPI(title="ft-records")
db = setup(app, "records")
import os  # noqa: E402

students = ServiceClient("student", os.environ["INSTANCE_ID"], timeout=0.6, attempts=2)
student_cache = {}          # last-known-good student profile (fallback when the student service is down)


class Grade(BaseModel):
    student_id: str
    course_id: str
    grade: float
    idempotency_key: str | None = None


def op_grade(d, p):
    with d.txn() as c:
        c.execute("INSERT OR REPLACE INTO grades VALUES(?,?,?,?)", (p["student_id"], p["course_id"], p["grade"], p["ts"]))
        faults.crash_if("crash_mid_grade")            # injected crash inside the transaction -> automatic rollback
        c.execute("INSERT INTO audit_log(kind, ref, ts) VALUES('grade', ?, ?)",
                  (f"{p['student_id']}:{p['course_id']}", p["ts"]))
    return {"status": "recorded"}


db.register("grade", op_grade)
db.start_background()


@app.get("/transcript/{sid}")
def transcript(sid: str):
    degraded = []
    try:
        r = students.get(f"/students/{sid}")
        if r.status_code == 404:
            raise HTTPException(404, "student not found")
        student_cache[sid] = r.json()
        student = student_cache[sid]
    except ServiceUnavailable:
        student = student_cache.get(sid)                         # may be None on a cold cache
        degraded.append("student-service-down")
    try:
        rows, from_replica = db.query("SELECT course_id, grade FROM grades WHERE student_id=? ORDER BY course_id", (sid,))
    except DBUnavailable as e:
        return unavailable(e)
    if from_replica:
        degraded.append("replica-read")
    avg = round(sum(g for _, g in rows) / len(rows), 2) if rows else None
    body = {"student": student, "grades": [{"course": a, "grade": b} for a, b in rows], "average": avg}
    return reply(200, body, ",".join(degraded) or None)


@app.post("/grades")
def submit_grade(g: Grade):
    status, body = db.run_op("grade", {"student_id": g.student_id, "course_id": g.course_id,
                                       "grade": g.grade, "ts": time.time()})
    return reply(status, body)
