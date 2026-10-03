"""Fault-tolerant data layer: replicated SQLite (primary + synchronous mirror) with a write-ahead outbox.

Mechanisms implemented here
  * database replication  - every transaction is applied to the replica *inside* the primary's write lock,
                            ordered by a replication sequence number (log shipping);
  * circuit breaker       - after 3 consecutive primary failures calls fail fast for 2 s, then one trial call;
  * graceful degradation  - reads fall back to the replica (flagged stale); writes that cannot reach the
                            primary are persisted in a durable journal (store-and-forward) and acknowledged
                            with HTTP 202;
  * checkpointing/recovery - a background thread replays the journal (idempotent handlers), re-synchronises a
                            lagging replica and repairs a corrupt copy from the healthy one (RAID-1 rebuild).
"""
import glob
import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager

from common import events
from common.resilience import CircuitBreaker

WRITE_TIMEOUT = 1.0      # seconds a write may wait for the primary lock
READ_TIMEOUT = 0.25
REPLICA_TIMEOUT = 1.0
PROBE_TIMEOUT = 0.05     # readiness probe must answer fast even when the primary is locked


def _try(fn, *a):
    """File operations on Windows can fail transiently (antivirus, concurrent rename); never let that kill a worker."""
    for _ in range(5):
        try:
            return fn(*a)
        except OSError:
            time.sleep(0.02)


class DBUnavailable(Exception):
    """Primary database cannot be used right now (locked, corrupt, breaker open)."""


class _Rec:
    """Connection proxy that records data-changing statements so they can be shipped to the replica."""

    def __init__(self, conn):
        self.conn = conn
        self.log = []

    def execute(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        if not sql.lstrip()[:6].upper() == "SELECT":
            self.log.append((sql, tuple(params)))
        return cur


class Journal:
    """Durable outbox: one fsync'ed JSON file per pending operation, named <op>--<time>-<id>.json.

    The directory is shared by all services; each instance only replays the operations it has a handler for."""

    def __init__(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)

    def append(self, op, params):
        name = f"{op}--{time.time():.6f}-{uuid.uuid4().hex[:8]}"
        tmp = os.path.join(self.path, name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"op": op, "params": params}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, os.path.join(self.path, name + ".json"))   # atomic publish
        return name

    def pending(self, ops=None):
        files = glob.glob(os.path.join(self.path, "*.json"))
        if ops is not None:
            files = [f for f in files if os.path.basename(f).split("--")[0] in ops]
        return sorted(files)

    def size(self):
        return len(glob.glob(os.path.join(self.path, "*.json"))) + len(glob.glob(os.path.join(self.path, "*.claim-*")))

    def release_stale_claims(self, max_age=10.0):
        for c in glob.glob(os.path.join(self.path, "*.claim-*")):
            try:
                if time.time() - os.path.getmtime(c) > max_age:       # replayer died mid-way: hand it back
                    os.replace(c, c.split(".claim-")[0] + ".json")
            except OSError:
                pass


class ReplicatedDB:
    def __init__(self, data_dir, component):
        self.dir = data_dir
        self.component = component
        self.primary = os.path.join(data_dir, "primary.db")
        self.replica = os.path.join(data_dir, "replica.db")
        self.journal = Journal(os.path.join(data_dir, "journal"))
        self.handlers = {}
        self.breaker = CircuitBreaker("db-primary", failure_threshold=3, reset_timeout=2.0, on_change=self._on_breaker)
        self._ok = True
        self.counters = {"writes_queued": 0, "journal_replayed": 0, "replica_reads": 0}   # exported as metrics
        self._mismatch_since = None
        self._stop = threading.Event()
        self._ensure_seq(self.primary)
        self._ensure_seq(self.replica)

    # ---------------------------------------------------------------- helpers
    def _on_breaker(self, name, state):
        events.emit(self.component, "db_circuit_" + state.replace("half_open", "half_open"), data_dir=self.dir)

    def _conn(self, path, timeout):
        return sqlite3.connect(path, timeout=timeout, isolation_level=None)

    def _ensure_seq(self, path):
        try:
            c = self._conn(path, 2.0)
            c.execute("CREATE TABLE IF NOT EXISTS repl_seq(id INTEGER PRIMARY KEY, n INTEGER, tok TEXT)")
            c.execute("INSERT OR IGNORE INTO repl_seq VALUES(1, 0, 'genesis')")
            c.close()
        except sqlite3.DatabaseError:
            pass

    def _primary_failed(self, err):
        self.breaker.failure()
        if self._ok:
            self._ok = False
            events.emit(self.component, "db_primary_unavailable", data_dir=self.dir, error=str(err)[:80])

    def _primary_recovered(self):
        self.breaker.success()
        if not self._ok:
            self._ok = True
            events.emit(self.component, "db_primary_available", data_dir=self.dir)

    # ---------------------------------------------------------------- writes
    @contextmanager
    def txn(self):
        """Atomic transaction on the primary; applied to the replica before the primary commits."""
        if not self.breaker.allow():
            raise DBUnavailable("primary circuit open")
        c = None
        try:
            c = self._conn(self.primary, WRITE_TIMEOUT)
            c.execute("BEGIN IMMEDIATE")
            prev_n, prev_tok = c.execute("SELECT n, tok FROM repl_seq WHERE id=1").fetchone()
            seq, tok = prev_n + 1, uuid.uuid4().hex[:12]
            c.execute("UPDATE repl_seq SET n=?, tok=? WHERE id=1", (seq, tok))
        except sqlite3.DatabaseError as e:
            if c is not None:
                c.close()                               # never leak a handle: it would block repair on Windows
            self._primary_failed(e)
            raise DBUnavailable(str(e)) from e
        rec = _Rec(c)
        try:
            yield rec
            self._replicate(seq, prev_tok, tok, rec.log)       # replica first, still holding the primary write lock
            c.execute("COMMIT")
        except sqlite3.IntegrityError:
            self._rollback(c)
            raise
        except sqlite3.DatabaseError as e:
            self._rollback(c)
            self._primary_failed(e)
            raise DBUnavailable(str(e)) from e
        except BaseException:
            self._rollback(c)
            raise
        finally:
            c.close()
        self._primary_recovered()

    @staticmethod
    def _rollback(c):
        try:
            c.execute("ROLLBACK")
        except Exception:
            pass

    def _replicate(self, seq, prev_tok, tok, log):
        """Apply a shipped transaction to the replica iff it directly follows the replica's current state.

        The (sequence, token) pair identifies the exact history: a replica that is "ahead" because a writer was killed
        between the replica commit and the primary commit has the right sequence number but a foreign token, and is
        detected as diverged instead of silently skipping the next transaction."""
        try:
            r = self._conn(self.replica, REPLICA_TIMEOUT)
            try:
                r.execute("BEGIN IMMEDIATE")
                have, htok = r.execute("SELECT n, tok FROM repl_seq WHERE id=1").fetchone()
                if have == seq and htok == tok:       # already contained (e.g. replica was just re-synced)
                    r.execute("ROLLBACK")
                    return
                if have != seq - 1 or htok != prev_tok:   # gap or foreign history -> repair loop will resync
                    r.execute("ROLLBACK")
                    events.emit(self.component, "replica_gap", data_dir=self.dir, have=have, want=seq - 1)
                    return
                for sql, params in log:
                    r.execute(sql, params)
                r.execute("UPDATE repl_seq SET n=?, tok=? WHERE id=1", (seq, tok))
                r.execute("COMMIT")
            except BaseException:
                self._rollback(r)
                raise
            finally:
                r.close()
        except sqlite3.DatabaseError as e:            # replica trouble must never block the primary
            events.emit(self.component, "replica_write_failed", data_dir=self.dir, error=str(e)[:80])

    # ---------------------------------------------------------------- reads
    def query(self, sql, params=(), primary_timeout=READ_TIMEOUT, replica_timeout=REPLICA_TIMEOUT):
        """Returns (rows, degraded). Reads the primary; falls back to the (possibly stale) replica."""
        if self.breaker.allow():
            try:
                c = self._conn(self.primary, primary_timeout)
                try:
                    rows = c.execute(sql, params).fetchall()
                finally:
                    c.close()
                self._primary_recovered()
                return rows, False
            except sqlite3.DatabaseError as e:
                self._primary_failed(e)
        try:
            c = self._conn(self.replica, replica_timeout)
            try:
                rows = c.execute(sql, params).fetchall()
                self.counters["replica_reads"] += 1
                return rows, True
            finally:
                c.close()
        except sqlite3.DatabaseError as e:
            raise DBUnavailable(f"primary and replica unavailable: {e}") from e

    def check(self):
        """Readiness probe: 'primary', 'replica' (degraded) or raises DBUnavailable."""
        _, degraded = self.query("SELECT 1", primary_timeout=PROBE_TIMEOUT, replica_timeout=0.2)
        return "replica" if degraded else "primary"

    # ---------------------------------------------------------------- operations with outbox
    def register(self, op, fn):
        self.handlers[op] = fn

    def run_op(self, op, params):
        """Execute handler now; if the primary is unreachable persist the op in the journal (HTTP 202)."""
        try:
            return 200, self.handlers[op](self, params)
        except DBUnavailable:
            self.journal.append(op, params)
            self.counters["writes_queued"] += 1
            events.emit(self.component, "op_journaled", data_dir=self.dir, op=op)
            return 202, {"status": "queued", "op": op}

    def replay_journal(self):
        self.journal.release_stale_claims()
        done = 0
        for f in self.journal.pending(self.handlers.keys()):
            claim = f[:-5] + f".claim-{os.getpid()}"
            try:
                os.replace(f, claim)                  # atomic claim: only one replica replays an entry
            except OSError:
                continue
            _try(os.utime, claim, None)               # claim age is measured from now, not from the entry's creation
            try:
                with open(claim, encoding="utf-8") as fh:
                    item = json.load(fh)
                self.handlers[item["op"]](self, item["params"])
                _try(os.remove, claim)
                done += 1
            except DBUnavailable:
                _try(os.replace, claim, f)            # still down: put it back and stop
                break
            except OSError:
                _try(os.replace, claim, f)            # transient file-system race: retry on the next cycle
                continue
            except Exception as e:                    # business error (e.g. insufficient funds) -> don't retry forever
                _try(os.replace, claim, f[:-5] + ".failed")
                events.emit(self.component, "journal_entry_failed", data_dir=self.dir, error=repr(e)[:120])
        if done:
            self.counters["journal_replayed"] += done
            events.emit(self.component, "journal_replayed", data_dir=self.dir, count=done)
        return done

    # ---------------------------------------------------------------- integrity / repair
    def _integrity(self, path):
        """True = healthy, False = corrupt, None = cannot tell (locked)."""
        try:
            c = self._conn(path, 0.3)
            try:
                return c.execute("PRAGMA quick_check").fetchone()[0] == "ok"
            finally:
                c.close()
        except sqlite3.OperationalError:
            return None
        except sqlite3.DatabaseError:
            return False

    def _seq(self, path):
        try:
            c = self._conn(path, 0.3)
            try:
                return tuple(c.execute("SELECT n, tok FROM repl_seq WHERE id=1").fetchone())
            finally:
                c.close()
        except sqlite3.DatabaseError:
            return None

    @staticmethod
    def _copy_db(src, dst):
        """Consistent snapshot of src written to a temp file and atomically swapped in as dst."""
        tmp = dst + f".tmp-{os.getpid()}"
        s = sqlite3.connect(src, timeout=0.5)
        d = sqlite3.connect(tmp)
        try:
            s.backup(d)
        finally:
            d.close()
            s.close()
        os.replace(tmp, dst)

    def repair_cycle(self):
        """Detect replica lag or corruption and repair (mirror re-sync / rebuild)."""
        p_ok, r_ok = self._integrity(self.primary), self._integrity(self.replica)
        try:
            if p_ok is False and r_ok:
                self._copy_db(self.replica, self.primary)
                events.emit(self.component, "db_primary_restored_from_replica", data_dir=self.dir)
            elif r_ok is False and p_ok:
                self._copy_db(self.primary, self.replica)
                events.emit(self.component, "db_replica_rebuilt", data_dir=self.dir)
            elif p_ok and r_ok:
                sp, sr = self._seq(self.primary), self._seq(self.replica)
                if sp is not None and sr is not None and sp != sr:
                    now = time.monotonic()
                    if self._mismatch_since is None:
                        self._mismatch_since = now          # require a persistent mismatch (not an in-flight txn)
                    elif now - self._mismatch_since > 1.0:
                        self._copy_db(self.primary, self.replica)
                        events.emit(self.component, "db_replica_resynced", data_dir=self.dir, primary=sp, replica=sr)
                        self._mismatch_since = None
                else:
                    self._mismatch_since = None
        except (sqlite3.DatabaseError, OSError):
            pass   # locked / in use right now: try again next cycle

    # ---------------------------------------------------------------- background worker
    def start_background(self, extra=None):
        def loop():
            n = 0
            while not self._stop.is_set():
                try:
                    if self.journal.size():
                        self.replay_journal()
                    if n % 4 == 0:
                        self.repair_cycle()
                    if extra:
                        extra(self)
                except Exception as e:  # noqa: BLE001  the worker must never die
                    events.emit(self.component, "background_error", data_dir=self.dir, error=str(e)[:80])
                n += 1
                self._stop.wait(0.5)

        t = threading.Thread(target=loop, daemon=True, name="ft-db-worker")
        t.start()
        return t
