"""BASELINE Payment service.

Deliberately naive: every step is committed on its own, there is no idempotency check, no
recovery after a crash and a fresh processor reference is generated on every attempt.  A crash
between the processor charge and the balance update therefore loses the payment, and a client
retry charges the student twice -- exactly the failure modes the project studies.
"""
import time
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from baseline.db import connect
from common import appkit, faults

app = FastAPI(title="baseline-payment")
appkit.install(app, "payment")


class Payment(BaseModel):
    student_id: str
    amount: float
    idempotency_key: str | None = None   # accepted but ignored by the baseline


@app.post("/payments")
def pay(p: Payment):
    c = connect()
    try:
        row = c.execute("SELECT balance_due FROM accounts WHERE student_id=?", (p.student_id,)).fetchone()
        if not row:
            raise HTTPException(404, "account not found")
        if p.amount <= 0 or p.amount > row[0]:
            raise HTTPException(400, "invalid amount")
        # step 1: charge at the external processor (new random reference every attempt)
        c.execute("INSERT INTO processor_charges(ref, client_ref, student_id, amount, ts) VALUES(?,?,?,?,?)",
                  (uuid.uuid4().hex, p.idempotency_key, p.student_id, p.amount, time.time()))
        c.commit()
        faults.crash_if("crash_after_charge")            # injected: process dies after the money moved
        # step 2: apply to the tuition balance
        c.execute("UPDATE accounts SET balance_due=balance_due-? WHERE student_id=?", (p.amount, p.student_id))
        c.commit()
        # step 3: write the ledger (receipt)
        c.execute("INSERT INTO ledger(client_ref, student_id, amount, ts) VALUES(?,?,?,?)",
                  (p.idempotency_key, p.student_id, p.amount, time.time()))
        c.commit()
    finally:
        c.close()
    return {"status": "completed", "student_id": p.student_id, "amount": p.amount}


@app.get("/balance/{sid}")
def balance(sid: str):
    c = connect()
    try:
        row = c.execute("SELECT balance_due FROM accounts WHERE student_id=?", (sid,)).fetchone()
    finally:
        c.close()
    if not row:
        raise HTTPException(404, "account not found")
    return {"student_id": sid, "balance_due": row[0]}
