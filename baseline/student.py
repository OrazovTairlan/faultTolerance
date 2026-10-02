"""BASELINE Student service: registration + course enrollment. No fault-tolerance mechanisms."""
import time
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from baseline.db import connect
from common import appkit

app = FastAPI(title="baseline-student")
appkit.install(app, "student")


class NewStudent(BaseModel):
    id: str | None = None
    name: str
    email: str | None = None


class Enrollment(BaseModel):
    student_id: str
    course_id: str


@app.post("/students")
def create_student(s: NewStudent):
    sid = s.id or "S" + uuid.uuid4().hex[:8]
    c = connect()
    try:
        c.execute("INSERT INTO students VALUES(?,?,?,?)", (sid, s.name, s.email, time.time()))
        c.execute("INSERT INTO accounts VALUES(?,?)", (sid, 1000.0))
        c.commit()
    except Exception as e:
        raise HTTPException(409 if "UNIQUE" in str(e) else 500, str(e))
    finally:
        c.close()
    return {"id": sid, "name": s.name}


@app.get("/students/{sid}")
def get_student(sid: str):
    c = connect()
    try:
        r = c.execute("SELECT id, name, email FROM students WHERE id=?", (sid,)).fetchone()
    finally:
        c.close()
    if not r:
        raise HTTPException(404, "student not found")
    return {"id": r[0], "name": r[1], "email": r[2]}


@app.post("/enrollments")
def enroll(e: Enrollment):
    c = connect()
    try:
        cap = c.execute("SELECT capacity FROM courses WHERE id=?", (e.course_id,)).fetchone()
        if not cap:
            raise HTTPException(404, "course not found")
        n = c.execute("SELECT COUNT(*) FROM enrollments WHERE course_id=?", (e.course_id,)).fetchone()[0]
        if n >= cap[0]:
            raise HTTPException(409, "course full")
        c.execute("INSERT OR IGNORE INTO enrollments VALUES(?,?,?)", (e.student_id, e.course_id, time.time()))
        c.commit()
    finally:
        c.close()
    return {"student_id": e.student_id, "course_id": e.course_id, "status": "enrolled"}


@app.get("/enrollments/{sid}")
def list_enrollments(sid: str):
    c = connect()
    try:
        rows = c.execute("SELECT course_id FROM enrollments WHERE student_id=?", (sid,)).fetchall()
    finally:
        c.close()
    return {"student_id": sid, "courses": [r[0] for r in rows]}
