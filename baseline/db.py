"""Baseline data access: one SQLite file, one connection per request, no retries, default 5 s lock wait."""
import os
import sqlite3


def connect():
    return sqlite3.connect(os.path.join(os.environ["DATA_DIR"], "baseline.db"))
