"""FAULT-TOLERANT Payment service -- a checkpointed, idempotent saga.

  step 1  validate + persist PENDING                      (checkpoint 1, duplicate-request detection by key)
  step 2  charge the external processor (ref = key)       (idempotent: INSERT OR IGNORE)  -> CHARGED  (checkpoint 2)
  step 3  apply to balance + write ledger -> COMPLETED    (single atomic transaction; compensation on failure)

A crash after any checkpoint is repaired either by the client's retry (same key) or by the recovery loop,
which rolls unfinished sagas forward.  Rollback of a partial transaction is performed by SQLite's journal.
"""
import time
import uuid

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from common import events, faults
from ft.common_ft import reply, setup, unavailable
from ft.db import DBUnavailable

app = FastAPI(title="ft-payment")
db = setup(app, "payment")
INSTANCE = __import__("os").environ["INSTANCE_ID"]


class Payment(BaseModel):
    student_id: str
    amount: float
    idempotency_key: str | None = None


def _result(status, key, student_id=None, amount=None):
    return {"status": status.lower(), "payment_key": key, "student_id": student_id, "amount": amount}


def _advance(d, key):
    """Drive a payment to a terminal state (idempotent; safe to call repeatedly from any replica)."""
    with d.txn() as c:                                                        # step 2: charge processor
        row = c.execute("SELECT status, student_id, amount FROM payments WHERE idem_key=?", (key,)).fetchone()
        if row[0] == "PENDING":
            c.execute("INSERT OR IGNORE INTO processor_charges(ref, client_ref, student_id, amount, ts) "
                      "VALUES(?,?,?,?,?)", (key, key, row[1], row[2], time.time()))
            c.execute("UPDATE payments SET status='CHARGED', updated=? WHERE idem_key=?", (time.time(), key))
    faults.crash_if("crash_after_charge")                                     # injected process crash

    with d.txn() as c:                                                        # step 3: apply (atomic)
        status, sid, amount = c.execute("SELECT status, student_id, amount FROM payments WHERE idem_key=?",
                                        (key,)).fetchone()
        if status == "CHARGED":
            due = c.execute("SELECT balance_due FROM accounts WHERE student_id=?", (sid,)).fetchone()[0]
            if amount > due + 1e-9:                                           # cannot apply -> compensate (refund)
                c.execute("UPDATE processor_charges SET status='REFUNDED' WHERE ref=?", (key,))
                status = "FAILED"
            else:
                c.execute("UPDATE accounts SET balance_due=balance_due-? WHERE student_id=?", (amount, sid))
                c.execute("INSERT INTO ledger(client_ref, student_id, amount, ts) VALUES(?,?,?,?)",
                          (key, sid, amount, time.time()))
                status = "COMPLETED"
            c.execute("UPDATE payments SET status=?, updated=? WHERE idem_key=?", (status, time.time(), key))
    return _result(status, key, sid, amount)


def op_payment(d, p):
    key = p["key"]
    with d.txn() as c:                                                        # step 1: validate + PENDING
        row = c.execute("SELECT status, student_id, amount FROM payments WHERE idem_key=?", (key,)).fetchone()
        if row is None:
            acct = c.execute("SELECT balance_due FROM accounts WHERE student_id=?", (p["student_id"],)).fetchone()
            if not acct:
                raise HTTPException(404, "account not found")
            if p["amount"] <= 0 or p["amount"] > acct[0]:
                raise HTTPException(400, "invalid amount")
            now = time.time()
            c.execute("INSERT INTO payments VALUES(?,?,?,?,?,?,?)",
                      (uuid.uuid4().hex, key, p["student_id"], p["amount"], "PENDING", now, now))
        elif row[0] in ("COMPLETED", "FAILED"):
            return {**_result(row[0], key, row[1], row[2]), "duplicate": True}   # duplicate-request detection
    return _advance(d, key)


def recover_inflight(d):
    """Recovery loop: roll forward sagas that were interrupted by a crash (PENDING/CHARGED and idle > 2 s)."""
    try:
        rows, degraded = d.query("SELECT idem_key FROM payments WHERE status IN ('PENDING','CHARGED') AND updated < ?",
                                 (time.time() - 2.0,))
        if degraded:
            return
        for (key,) in rows:
            _advance(d, key)
            events.emit(INSTANCE, "payment_recovered", data_dir=d.dir, key=key)
    except DBUnavailable:
        pass


db.register("payment", op_payment)
db.start_background(extra=recover_inflight)


@app.post("/payments")
def pay(p: Payment, idempotency_key: str | None = Header(default=None)):
    key = p.idempotency_key or idempotency_key or uuid.uuid4().hex
    status, body = db.run_op("payment", {"key": key, "student_id": p.student_id, "amount": p.amount})
    return reply(status, body)


@app.get("/payments/{key}")
def payment_status(key: str):
    try:
        rows, degraded = db.query("SELECT status, student_id, amount FROM payments WHERE idem_key=?", (key,))
    except DBUnavailable as e:
        return unavailable(e)
    if not rows:
        raise HTTPException(404, "unknown payment")
    return reply(200, _result(rows[0][0], key, rows[0][1], rows[0][2]), "replica-read" if degraded else None)


@app.get("/balance/{sid}")
def balance(sid: str):
    try:
        rows, degraded = db.query("SELECT balance_due FROM accounts WHERE student_id=?", (sid,))
    except DBUnavailable as e:
        return unavailable(e)
    if not rows:
        raise HTTPException(404, "account not found")
    return reply(200, {"student_id": sid, "balance_due": rows[0][0]}, "replica-read" if degraded else None)
