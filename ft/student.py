"""FAULT-TOLERANT Student service: registration and enrollment.

Idempotent writes (natural keys / Idempotency-Key), replicated DB with outbox, readiness probe.
"""
import hashlib
import time

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from ft.common_ft import reply, setup, unavailable
from ft.db import DBUnavailable

app = FastAPI(title="ft-student")
db = setup(app, "student")


class NewStudent(BaseModel):
    id: str | None = None
    name: str
    email: str | None = None


class Enrollment(BaseModel):
    student_id: str
    course_id: str


def op_create_student(d, p):
    with d.txn() as c:
        if c.execute("SELECT 1 FROM students WHERE id=?", (p["id"],)).fetchone():
            return {"id": p["id"], "name": p["name"], "status": "exists"}      # duplicate request -> same answer
        c.execute("INSERT INTO students VALUES(?,?,?,?)", (p["id"], p["name"], p["email"], p["ts"]))
        c.execute("INSERT INTO accounts VALUES(?,?)", (p["id"], 1000.0))
    return {"id": p["id"], "name": p["name"], "status": "created"}


def op_enroll(d, p):
    with d.txn() as c:
        cap = c.execute("SELECT capacity FROM courses WHERE id=?", (p["course_id"],)).fetchone()
        if not cap:
            raise HTTPException(404, "course not found")
        if c.execute("SELECT 1 FROM enrollments WHERE student_id=? AND course_id=?",
                     (p["student_id"], p["course_id"])).fetchone():
            return {**p, "status": "already_enrolled"}
        n = c.execute("SELECT COUNT(*) FROM enrollments WHERE course_id=?", (p["course_id"],)).fetchone()[0]
        if n >= cap[0]:
            raise HTTPException(409, "course full")
        c.execute("INSERT INTO enrollments VALUES(?,?,?)", (p["student_id"], p["course_id"], p["ts"]))
    return {**p, "status": "enrolled"}


db.register("create_student", op_create_student)
db.register("enroll", op_enroll)
db.start_background()


@app.post("/students")
def create_student(s: NewStudent, idempotency_key: str | None = Header(default=None)):
    sid = s.id or "S" + hashlib.sha1((idempotency_key or str(time.time())).encode()).hexdigest()[:8]
    status, body = db.run_op("create_student", {"id": sid, "name": s.name, "email": s.email, "ts": time.time()})
    return reply(201 if status == 200 else status, body)


@app.get("/students/{sid}")
def get_student(sid: str):
    try:
        rows, degraded = db.query("SELECT id, name, email FROM students WHERE id=?", (sid,))
    except DBUnavailable as e:
        return unavailable(e)
    if not rows:
        raise HTTPException(404, "student not found")
    r = rows[0]
    return reply(200, {"id": r[0], "name": r[1], "email": r[2]}, "replica-read" if degraded else None)


@app.post("/enrollments")
def enroll(e: Enrollment):
    status, body = db.run_op("enroll", {"student_id": e.student_id, "course_id": e.course_id, "ts": time.time()})
    return reply(status, body)


@app.get("/enrollments/{sid}")
def list_enrollments(sid: str):
    try:
        rows, degraded = db.query("SELECT course_id FROM enrollments WHERE student_id=?", (sid,))
    except DBUnavailable as e:
        return unavailable(e)
    return reply(200, {"student_id": sid, "courses": [r[0] for r in rows]}, "replica-read" if degraded else None)
