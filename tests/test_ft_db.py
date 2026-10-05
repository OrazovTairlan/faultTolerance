"""Unit tests of the replicated database layer: replication, failover reads, outbox + replay, repair."""
import os
import sqlite3

import pytest

from common import schema
from ft.db import DBUnavailable, ReplicatedDB
from tools import inject


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    schema.create_data_dir(str(tmp_path), "ft")
    return ReplicatedDB(str(tmp_path), "test")


def balance(path, sid="S0001"):
    c = sqlite3.connect(path)
    try:
        return c.execute("SELECT balance_due FROM accounts WHERE student_id=?", (sid,)).fetchone()[0]
    finally:
        c.close()                                  # an open handle would block the file swap on Windows


def test_transaction_is_replicated_to_mirror(db):
    with db.txn() as c:
        c.execute("UPDATE accounts SET balance_due=balance_due-? WHERE student_id='S0001'", (100,))
    assert balance(db.primary) == balance(db.replica) == 900.0


def test_rollback_leaves_both_copies_untouched(db):
    with pytest.raises(RuntimeError):
        with db.txn() as c:
            c.execute("UPDATE accounts SET balance_due=0 WHERE student_id='S0001'")
            raise RuntimeError("crash in the middle")
    assert balance(db.primary) == balance(db.replica) == 1000.0


def test_reads_fall_back_to_replica_when_primary_locked(db):
    proc, _ = inject.lock_db_process(db.primary, 3)
    try:
        rows, degraded = db.query("SELECT balance_due FROM accounts WHERE student_id='S0001'")
        assert rows == [(1000.0,)] and degraded
    finally:
        proc.terminate(); proc.wait()


def test_writes_are_journaled_then_replayed_exactly_once(db):
    def op(d, p):
        with d.txn() as c:
            if c.execute("SELECT 1 FROM payments WHERE idem_key=?", (p["k"],)).fetchone():
                return "dup"
            c.execute("INSERT INTO payments VALUES(?,?,?,?,?,?,?)", (p["k"], p["k"], "S0001", 1, "COMPLETED", 0, 0))
        return "ok"

    db.register("p", op)
    proc, _ = inject.lock_db_process(db.primary, 4)
    try:
        status, body = db.run_op("p", {"k": "key1"})
        assert status == 202 and body["status"] == "queued" and db.journal.size() == 1
        db.run_op("p", {"k": "key1"})                          # duplicate client retry, also queued
        assert db.journal.size() == 2
    finally:
        proc.terminate(); proc.wait()
    db.breaker.reset()
    assert db.replay_journal() == 2
    assert db.journal.size() == 0
    n = sqlite3.connect(db.primary).execute("SELECT COUNT(*) FROM payments").fetchone()[0]
    assert n == 1                                              # idempotent: executed once


def test_corrupt_primary_is_repaired_from_replica(db):
    inject.corrupt_header(db.primary)
    with pytest.raises(DBUnavailable):
        with db.txn():
            pass
    db.repair_cycle()
    assert db._integrity(db.primary) is True
    assert balance(db.primary) == 1000.0


def test_lagging_replica_is_resynced(db):
    with db.txn() as c:
        c.execute("UPDATE accounts SET balance_due=1 WHERE student_id='S0002'")
    conn = sqlite3.connect(db.replica)                         # replica silently loses the update (simulated bit rot)
    conn.execute("UPDATE accounts SET balance_due=1000 WHERE student_id='S0002'")
    conn.execute("UPDATE repl_seq SET n=0")
    conn.commit(); conn.close()
    db.repair_cycle()
    import time
    time.sleep(1.1)
    db.repair_cycle()
    assert balance(db.replica, "S0002") == 1.0


def test_replica_ahead_after_writer_killed_between_commits_is_detected(db):
    """Writer dies after the replica commit but before the primary commit: replica holds a transaction the primary lacks."""
    c = sqlite3.connect(db.primary, isolation_level=None)
    c.execute("BEGIN IMMEDIATE")
    n, tok = c.execute("SELECT n, tok FROM repl_seq").fetchone()
    c.execute("UPDATE repl_seq SET n=?, tok='orphan'", (n + 1,))
    r = sqlite3.connect(db.replica, isolation_level=None)               # replica commit of the orphan txn
    r.execute("UPDATE accounts SET balance_due=1 WHERE student_id='S0003'")
    r.execute("UPDATE repl_seq SET n=?, tok='orphan'", (n + 1,))
    r.close()
    c.execute("ROLLBACK")                                              # primary never committed (process killed)
    c.close()
    with db.txn() as cc:                                                # next real transaction, same sequence number
        cc.execute("UPDATE accounts SET balance_due=2 WHERE student_id='S0004'")
    assert balance(db.primary, "S0003") == 1000.0 and balance(db.replica, "S0003") == 1.0   # diverged
    import time
    db.repair_cycle(); time.sleep(1.1); db.repair_cycle()
    assert balance(db.replica, "S0003") == 1000.0 and balance(db.replica, "S0004") == 2.0   # repaired from the primary
