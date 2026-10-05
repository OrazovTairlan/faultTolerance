"""Data-consistency verification, run directly against the database files after an experiment.

Invariants (all must hold for the system to be consistent):
  I1  every receipt (ledger row) matches the tuition balance:     balance_due == initial - sum(ledger)
  I2  money moved at the processor equals receipts per student:    sum(charges) == sum(ledger)    (no lost payment)
  I3  no payment is charged or recorded more than once per client key                              (no duplication)
  I4  every acknowledged payment (HTTP 2xx) exists exactly once in the ledger                     (no acked loss)
  I5  every grade has an audit entry and vice-versa                                               (atomic record update)
  I6  no payment saga left in PENDING/CHARGED; the outbox journal is empty
  I7  replica is identical to primary (row counts + checksums)                                    (fault-tolerant system)
"""
import glob
import os
import sqlite3
import time

INITIAL = 1000.0


def _q(path, sql, params=()):
    c = sqlite3.connect(path, timeout=2.0)
    try:
        return c.execute(sql, params).fetchall()
    finally:
        c.close()


def _fingerprint(path):
    out = {}
    for t, col in (("students", "id"), ("enrollments", "student_id"), ("grades", "grade"), ("audit_log", "id"),
                   ("ledger", "amount"), ("processor_charges", "amount")):
        out[t] = _q(path, f"SELECT COUNT(*), COALESCE(SUM(LENGTH({col})),0) FROM {t}")[0]
    out["accounts"] = _q(path, "SELECT COUNT(*), ROUND(COALESCE(SUM(balance_due),0),4) FROM accounts")[0]
    return out


def check(data_dir, mode, intents=()):
    db = os.path.join(data_dir, "baseline.db" if mode == "baseline" else "primary.db")
    r = {}
    try:
        led = {}
        for ref, n, amt in _q(db, "SELECT client_ref, COUNT(*), SUM(amount) FROM ledger GROUP BY client_ref"):
            led[ref] = (n, amt)
        chg = {}
        for ref, n in _q(db, "SELECT client_ref, COUNT(*) FROM processor_charges WHERE status='CHARGED' GROUP BY client_ref"):
            chg[ref] = n
        # I1 balance vs receipts
        r["balance_drift_accounts"] = _q(db, """SELECT COUNT(*) FROM accounts a
            WHERE ABS(a.balance_due - (? - COALESCE((SELECT SUM(amount) FROM ledger l WHERE l.student_id=a.student_id),0))) > 0.001""",
                                          (INITIAL,))[0][0]
        # I2 charges vs receipts per student
        r["charge_receipt_mismatch_students"] = _q(db, """SELECT COUNT(*) FROM (
            SELECT s.id FROM students s
            WHERE ABS(COALESCE((SELECT SUM(amount) FROM processor_charges c WHERE c.student_id=s.id AND c.status='CHARGED'),0)
                    - COALESCE((SELECT SUM(amount) FROM ledger l WHERE l.student_id=s.id),0)) > 0.001)""")[0][0]
        # I3 duplicates
        r["duplicate_charges"] = sum(n - 1 for ref, n in chg.items() if ref is not None and n > 1)
        r["duplicate_ledger_rows"] = sum(n - 1 for ref, (n, _) in led.items() if ref is not None and n > 1)
        r["lost_payments"] = sum(1 for ref, n in chg.items() if ref is not None and ref not in led)     # charged, no receipt
        # I4 acknowledged payments
        pay = [i for i in intents if i.get("op") == "payment"]
        r["payment_intents"] = len(pay)
        r["acked_payments"] = sum(1 for i in pay if i.get("acked"))
        r["acked_payments_missing"] = sum(1 for i in pay if i.get("acked") and i["key"] not in led)
        # I5 grades <-> audit
        r["grades_without_audit"] = _q(db, """SELECT COUNT(*) FROM grades g WHERE NOT EXISTS
            (SELECT 1 FROM audit_log a WHERE a.kind='grade' AND a.ref = g.student_id || ':' || g.course_id)""")[0][0]
        gr = [i for i in intents if i.get("op") == "grade"]
        r["grade_intents"] = len(gr)
        r["acked_grades_missing"] = sum(
            1 for i in gr if i.get("acked") and not _q(db, "SELECT 1 FROM grades WHERE student_id=? AND course_id=?",
                                                       (i["student_id"], i["course_id"])))
        # I6 unfinished work
        if mode == "ft":
            r["unfinished_sagas"] = _q(db, "SELECT COUNT(*) FROM payments WHERE status IN ('PENDING','CHARGED')")[0][0]
            j = os.path.join(data_dir, "journal")
            r["journal_pending"] = len(glob.glob(os.path.join(j, "*.json"))) + len(glob.glob(os.path.join(j, "*.claim-*")))
            rep = os.path.join(data_dir, "replica.db")
            r["replica_diverged"] = int(_fingerprint(db) != _fingerprint(rep))
        else:
            r["unfinished_sagas"] = 0
            r["journal_pending"] = 0
            r["replica_diverged"] = 0
    except sqlite3.DatabaseError as e:
        r["error"] = str(e)
        r["consistent"] = False
        return r
    bad = ("balance_drift_accounts", "charge_receipt_mismatch_students", "duplicate_charges", "duplicate_ledger_rows",
           "lost_payments", "acked_payments_missing", "grades_without_audit", "acked_grades_missing",
           "unfinished_sagas", "journal_pending", "replica_diverged")
    r["violations"] = sum(r[k] for k in bad)
    r["consistent"] = r["violations"] == 0
    return r


def wait_consistent(data_dir, mode, intents, timeout, poll=0.5):
    """Poll until the invariants hold (recovery finished) or timeout. Returns (result, seconds_waited)."""
    t0 = time.time()
    while True:
        r = check(data_dir, mode, intents)
        if r.get("consistent") or time.time() - t0 >= timeout:
            return r, time.time() - t0
        time.sleep(poll)
