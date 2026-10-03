"""Reusable resilience primitives: circuit breaker and exponential backoff with jitter."""
import random
import threading
import time

CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"


class CircuitBreaker:
    """Classic three-state breaker.

    closed    -> calls flow; `failure_threshold` consecutive failures open it
    open      -> calls are rejected immediately for `reset_timeout` seconds
    half_open -> one trial call is allowed; success closes, failure re-opens
    """

    def __init__(self, name, failure_threshold=5, reset_timeout=2.0, on_change=None):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.on_change = on_change
        self._state = CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._trial_in_flight = False
        self._lock = threading.Lock()

    @property
    def state(self):
        return self._state

    def _set(self, state):
        if state != self._state:
            self._state = state
            if self.on_change:
                try:
                    self.on_change(self.name, state)
                except Exception:
                    pass

    def allow(self):
        with self._lock:
            if self._state == CLOSED:
                return True
            if self._state == OPEN:
                if time.monotonic() - self._opened_at >= self.reset_timeout:
                    self._set(HALF_OPEN)
                    self._trial_in_flight = True
                    return True
                return False
            # HALF_OPEN: exactly one trial call at a time
            if not self._trial_in_flight:
                self._trial_in_flight = True
                return True
            return False

    def success(self):
        with self._lock:
            self._failures = 0
            self._trial_in_flight = False
            self._set(CLOSED)

    def failure(self):
        with self._lock:
            self._trial_in_flight = False
            if self._state == HALF_OPEN:
                self._opened_at = time.monotonic()
                self._set(OPEN)
                return
            self._failures += 1
            if self._failures >= self.failure_threshold and self._state == CLOSED:
                self._opened_at = time.monotonic()
                self._set(OPEN)

    def reset(self):
        with self._lock:
            self._failures = 0
            self._trial_in_flight = False
            self._set(CLOSED)


def backoff_delay(attempt, base=0.1, cap=2.0, jitter=0.5):
    """Exponential backoff: base * 2^attempt, capped, with +/- jitter fraction (attempt starts at 0)."""
    d = min(cap, base * (2 ** attempt))
    return d * (1 - jitter + random.random() * jitter * 2)


def retry(fn, attempts=3, base=0.1, cap=2.0, retry_on=(Exception,), sleep=time.sleep):
    """Run fn() with retries and exponential backoff; re-raises the last error."""
    last = None
    for i in range(attempts):
        try:
            return fn()
        except retry_on as e:  # noqa: PERF203
            last = e
            if i < attempts - 1:
                sleep(backoff_delay(i, base, cap))
    raise last
