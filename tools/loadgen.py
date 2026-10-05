"""Workload generator + black-box prober.

* open-loop mode  : fixed arrival rate (requests/s), independent of response time
* closed-loop mode: N concurrent virtual users, each sends the next request when the previous one finished
* prober          : every 250 ms sends one read to each of the four services (service-level availability)

Every request is recorded (timestamp, latency, status, operation, degraded/queued flags).  The client has a
5 s patience (timeout) and, like a load-balanced VIP / DNS name, fails over to the next gateway address when
a gateway refuses the connection.
"""
import asyncio
import random
import threading
import time
import uuid

import httpx

CLIENT_TIMEOUT = 5.0
HOT_STUDENTS = [f"S{i:04d}" for i in range(101, 141)]      # read-mostly "hot set" (cache-friendly, realistic)
PAY_STUDENTS = [f"S{i:04d}" for i in range(141, 201)]      # accounts used by background payment traffic
COURSES = [f"C{i:03d}" for i in range(1, 13)]

MIX = [("student_read", 0.20), ("transcript", 0.20), ("timetable", 0.15), ("balance", 0.15),
       ("payment", 0.15), ("grade", 0.05), ("enroll", 0.10)]
CRITICAL = {"payment", "grade"}


class Recorder:
    def __init__(self):
        self.rows = []
        self.intents = []      # state-changing operations the client believes it submitted
        self._lock = threading.Lock()

    def add(self, row):
        with self._lock:
            self.rows.append(row)

    def intent(self, d):
        with self._lock:
            self.intents.append(d)


class GatewayClient:
    def __init__(self, urls):
        self.urls = urls
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(CLIENT_TIMEOUT, connect=0.5),
                                      limits=httpx.Limits(max_connections=1000, max_keepalive_connections=200))
        self._i = random.randrange(len(urls))

    async def request(self, method, path, **kw):
        n = len(self.urls)
        self._i += 1
        start = self._i                      # local copy: concurrent requests advance self._i while we await
        last = None
        for k in range(n):
            url = self.urls[(start + k) % n]
            try:
                return await self.http.request(method, url + path, **kw)
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:    # gateway refused: try the next address
                last = e
        raise last

    async def close(self):
        await self.http.aclose()


def pick_op(rng, mix=MIX):
    x, acc = rng.random(), 0.0
    for name, w in mix:
        acc += w
        if x <= acc:
            return name
    return mix[-1][0]


def build(op, rng):
    """-> (method, path, json_body, intent|None)"""
    s = rng.choice(HOT_STUDENTS)
    if op == "student_read":
        return "GET", f"/api/student/students/{s}", None, None
    if op == "transcript":
        return "GET", f"/api/records/transcript/{s}", None, None
    if op == "timetable":
        return "GET", f"/api/timetable/timetable/{s}", None, None
    if op == "balance":
        return "GET", f"/api/payment/balance/{rng.choice(PAY_STUDENTS)}", None, None
    if op == "payment":
        key = "mix-" + uuid.uuid4().hex
        st = rng.choice(PAY_STUDENTS)
        return ("POST", "/api/payment/payments", {"student_id": st, "amount": 1.0, "idempotency_key": key},
                {"op": "payment", "key": key, "student_id": st, "amount": 1.0})
    if op == "grade":
        st, co = rng.choice(PAY_STUDENTS), rng.choice(COURSES)
        return ("POST", "/api/records/grades", {"student_id": st, "course_id": co, "grade": rng.randint(50, 100)},
                {"op": "grade", "student_id": st, "course_id": co})
    if op == "enroll":
        st, co = rng.choice(PAY_STUDENTS), rng.choice(COURSES)
        return ("POST", "/api/student/enrollments", {"student_id": st, "course_id": co}, None)
    raise ValueError(op)


async def one_request(client, rec, op, method, path, body, intent=None, kind="load", retries=0, retry_delay=0.5):
    """Send one logical request (with optional naive client-side retries that reuse the same body/key)."""
    attempts = 0
    while True:
        attempts += 1
        t0 = time.time()
        p0 = time.perf_counter()
        status, degraded, queued, gw_attempts = 0, False, False, 1
        try:
            r = await asyncio.wait_for(client.request(method, path, json=body), timeout=CLIENT_TIMEOUT)   # hard 5 s deadline
            status = r.status_code
            degraded = "x-degraded" in r.headers
            queued = status == 202
            gw_attempts = int(r.headers.get("x-attempts", 1))      # >1: the gateway retried on another replica
        except Exception:  # noqa: BLE001  timeout / connection error
            status = 0
        ok = 200 <= status < 300
        rec.add({"t": t0, "lat": time.perf_counter() - p0, "ok": ok, "status": status, "op": op, "kind": kind,
                 "degraded": degraded, "queued": queued, "attempt": attempts, "gw_attempts": gw_attempts})
        if intent is not None and ok:
            intent["acked"] = True
        if ok or attempts > retries:
            return ok
        await asyncio.sleep(retry_delay)


class LoadRunner:
    """Runs workloads on a private event loop in a background thread."""

    def __init__(self, urls, seed=1):
        self.urls = urls
        self.rec = Recorder()
        self.rng = random.Random(seed)
        self._stop = threading.Event()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self.client = None
        self.submit(self._init()).result()
        self.futures = []

    def _run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _init(self):
        self.client = GatewayClient(self.urls)

    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    # ---------------------------------------------------------------- workloads
    def start_open_loop(self, rate, mix=MIX, retries=0):
        async def run():
            nxt = time.time()
            tasks = []
            while not self._stop.is_set():
                op = pick_op(self.rng, mix)
                method, path, body, intent = build(op, self.rng)
                if intent:
                    self.rec.intent(intent)
                tasks.append(asyncio.create_task(
                    one_request(self.client, self.rec, op, method, path, body, intent, retries=retries)))
                nxt += 1.0 / rate
                await asyncio.sleep(max(0.0, nxt - time.time()))
                tasks = [t for t in tasks if not t.done()]
            await asyncio.gather(*tasks, return_exceptions=True)
        self.futures.append(self.submit(run()))

    def start_closed_loop(self, users, duration, mix=MIX):
        async def user(seed):
            rng = random.Random(seed)
            end = time.time() + duration
            while time.time() < end and not self._stop.is_set():
                op = pick_op(rng, mix)
                method, path, body, intent = build(op, rng)
                if intent:
                    self.rec.intent(intent)
                await one_request(self.client, self.rec, op, method, path, body, intent)

        async def run():
            await asyncio.gather(*[user(1000 + i) for i in range(users)])
        f = self.submit(run())
        self.futures.append(f)
        return f

    def start_prober(self, interval=0.25):
        paths = {"probe_student": "/api/student/students/S0001", "probe_records": "/api/records/transcript/S0001",
                 "probe_timetable": "/api/timetable/timetable/S0001", "probe_payment": "/api/payment/balance/S0001"}

        async def run():
            tasks = []
            nxt = time.time()
            while not self._stop.is_set():
                for op, path in paths.items():
                    tasks.append(asyncio.create_task(
                        one_request(self.client, self.rec, op, "GET", path, None, kind="probe")))
                nxt += interval
                await asyncio.sleep(max(0.0, nxt - time.time()))
                tasks = [t for t in tasks if not t.done()]
            await asyncio.gather(*tasks, return_exceptions=True)
        self.futures.append(self.submit(run()))

    def submit_batch(self, items, concurrency_rate, retries=2, retry_delay=0.5):
        """items: list of (op, method, path, body, intent). Launched at `concurrency_rate` per second."""
        async def run():
            tasks = []
            for op, method, path, body, intent in items:
                self.rec.intent(intent)
                tasks.append(asyncio.create_task(
                    one_request(self.client, self.rec, op, method, path, body, intent, retries=retries,
                                retry_delay=retry_delay)))
                await asyncio.sleep(1.0 / concurrency_rate)
            await asyncio.gather(*tasks)
        f = self.submit(run())
        self.futures.append(f)
        return f

    def warm_up(self, n=40):
        async def run():
            for s in HOT_STUDENTS[:n]:
                for path in (f"/api/student/students/{s}", f"/api/records/transcript/{s}",
                             f"/api/timetable/timetable/{s}"):
                    for _ in range(4):          # several rounds so that every gateway / replica caches every key
                        try:
                            await self.client.request("GET", path)
                        except Exception:  # noqa: BLE001
                            pass
        self.submit(run()).result(timeout=60)

    def stop(self):
        self._stop.set()
        for f in self.futures:
            try:
                f.result(timeout=30)
            except Exception:  # noqa: BLE001
                pass
        self.submit(self.client.close()).result(timeout=5)
        self._loop.call_soon_threadsafe(self._loop.stop)
