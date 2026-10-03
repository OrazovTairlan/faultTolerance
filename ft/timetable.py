"""FAULT-TOLERANT Timetable service: resilient call to the student service + cached fallback."""
import os

from fastapi import FastAPI, HTTPException

from ft.clients import ServiceClient, ServiceUnavailable
from ft.common_ft import reply, setup, unavailable
from ft.db import DBUnavailable

app = FastAPI(title="ft-timetable")
db = setup(app, "timetable")
students = ServiceClient("student", os.environ["INSTANCE_ID"], timeout=0.6, attempts=2)
enrollment_cache = {}


@app.get("/timetable/{sid}")
def timetable(sid: str):
    degraded = []
    try:
        r = students.get(f"/enrollments/{sid}")
        if r.status_code == 404:
            raise HTTPException(404, "student not found")
        enrollment_cache[sid] = r.json()["courses"]
        courses = enrollment_cache[sid]
    except ServiceUnavailable:
        courses = enrollment_cache.get(sid)
        degraded.append("student-service-down")
        if courses is None:                                       # nothing cached: still answer, explicitly empty
            return reply(200, {"student_id": sid, "entries": []}, "no-enrollment-data")
    try:
        marks = ",".join("?" * len(courses)) or "NULL"
        rows, from_replica = db.query(f"SELECT id, title, slot, room FROM courses WHERE id IN ({marks}) ORDER BY slot",
                                      tuple(courses))
    except DBUnavailable as e:
        return unavailable(e)
    if from_replica:
        degraded.append("replica-read")
    entries = [{"course": a, "title": b, "slot": s, "room": r_} for a, b, s, r_ in rows]
    return reply(200, {"student_id": sid, "entries": entries}, ",".join(degraded) or None)
