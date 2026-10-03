"""Database schema and deterministic seed data (shared by baseline and fault-tolerant systems)."""
import os
import shutil
import sqlite3
import time

N_STUDENTS = 200
INITIAL_BALANCE = 1000.0
N_COURSES = 12

SCHEMA = """
CREATE TABLE IF NOT EXISTS students(id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT, created REAL);
CREATE TABLE IF NOT EXISTS courses(id TEXT PRIMARY KEY, title TEXT, capacity INTEGER, slot TEXT, room TEXT);
CREATE TABLE IF NOT EXISTS enrollments(student_id TEXT, course_id TEXT, ts REAL, PRIMARY KEY(student_id, course_id));
CREATE TABLE IF NOT EXISTS accounts(student_id TEXT PRIMARY KEY, balance_due REAL NOT NULL);
-- payments: saga state used by the fault-tolerant payment service (PENDING -> CHARGED -> COMPLETED | FAILED)
CREATE TABLE IF NOT EXISTS payments(id TEXT PRIMARY KEY, idem_key TEXT UNIQUE, student_id TEXT, amount REAL,
                                    status TEXT, created REAL, updated REAL);
CREATE TABLE IF NOT EXISTS ledger(id INTEGER PRIMARY KEY AUTOINCREMENT, client_ref TEXT, student_id TEXT,
                                  amount REAL, ts REAL);
-- simulated external payment processor (the bank): a charge here is real money leaving the student
CREATE TABLE IF NOT EXISTS processor_charges(ref TEXT PRIMARY KEY, client_ref TEXT, student_id TEXT, amount REAL,
                                             status TEXT DEFAULT 'CHARGED', ts REAL);
CREATE TABLE IF NOT EXISTS grades(student_id TEXT, course_id TEXT, grade REAL, ts REAL,
                                  PRIMARY KEY(student_id, course_id));
CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, ref TEXT, ts REAL);
"""

SLOTS = ["Mon 09:00", "Mon 11:00", "Tue 09:00", "Tue 11:00", "Wed 09:00", "Wed 11:00",
         "Thu 09:00", "Thu 11:00", "Fri 09:00", "Fri 11:00", "Mon 14:00", "Tue 14:00"]


def sid(i):
    return f"S{i:04d}"


def cid(i):
    return f"C{i:03d}"


def init_db(path):
    if os.path.exists(path):
        os.remove(path)
    c = sqlite3.connect(path)
    c.executescript(SCHEMA)
    now = time.time()
    for i in range(1, N_STUDENTS + 1):
        c.execute("INSERT INTO students VALUES(?,?,?,?)", (sid(i), f"Student {i}", f"s{i}@uni.edu", now))
        c.execute("INSERT INTO accounts VALUES(?,?)", (sid(i), INITIAL_BALANCE))
    for i in range(1, N_COURSES + 1):
        c.execute("INSERT INTO courses VALUES(?,?,?,?,?)",
                  (cid(i), f"Course {i}", 100000, SLOTS[(i - 1) % len(SLOTS)], f"R{100 + i}"))
    for i in range(1, N_STUDENTS + 1):
        for k in range(3):  # three enrollments and three seed grades per student
            course = cid((i + k) % N_COURSES + 1)
            c.execute("INSERT INTO enrollments VALUES(?,?,?)", (sid(i), course, now))
            c.execute("INSERT INTO grades VALUES(?,?,?,?)", (sid(i), course, 60 + (i * 7 + k * 11) % 40, now))
            c.execute("INSERT INTO audit_log(kind, ref, ts) VALUES('grade', ?, ?)", (f"{sid(i)}:{course}", now))
    c.commit()
    c.close()


def create_data_dir(data_dir, mode):
    """Create a fresh database for the given mode. ft = primary + byte-identical replica (RAID-1 style)."""
    os.makedirs(data_dir, exist_ok=True)
    if mode == "baseline":
        init_db(os.path.join(data_dir, "baseline.db"))
    else:
        init_db(os.path.join(data_dir, "primary.db"))
        shutil.copyfile(os.path.join(data_dir, "primary.db"), os.path.join(data_dir, "replica.db"))
        os.makedirs(os.path.join(data_dir, "journal"), exist_ok=True)
        os.makedirs(os.path.join(data_dir, "backups"), exist_ok=True)
