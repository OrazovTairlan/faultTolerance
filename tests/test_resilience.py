import time

import pytest

from common.resilience import CLOSED, HALF_OPEN, OPEN, CircuitBreaker, backoff_delay, retry


def test_breaker_opens_after_threshold_and_recovers():
    changes = []
    b = CircuitBreaker("x", failure_threshold=3, reset_timeout=0.05, on_change=lambda n, s: changes.append(s))
    for _ in range(2):
        b.failure()
    assert b.state == CLOSED and b.allow()
    b.failure()
    assert b.state == OPEN and not b.allow()           # fail fast while open
    time.sleep(0.06)
    assert b.allow() and b.state == HALF_OPEN          # one trial call
    assert not b.allow()                               # no second concurrent trial
    b.success()
    assert b.state == CLOSED
    assert changes == [OPEN, HALF_OPEN, CLOSED]


def test_breaker_half_open_failure_reopens():
    b = CircuitBreaker("x", failure_threshold=1, reset_timeout=0.02)
    b.failure()
    time.sleep(0.03)
    assert b.allow()
    b.failure()
    assert b.state == OPEN


def test_success_resets_failure_count():
    b = CircuitBreaker("x", failure_threshold=3)
    b.failure(); b.failure(); b.success(); b.failure(); b.failure()
    assert b.state == CLOSED


def test_backoff_grows_exponentially_and_is_capped():
    d = [backoff_delay(i, base=0.1, cap=1.0, jitter=0.0) for i in range(6)]
    assert d == [0.1, 0.2, 0.4, 0.8, 1.0, 1.0]
    j = [backoff_delay(2, base=0.1, jitter=0.5) for _ in range(200)]
    assert all(0.2 <= x <= 0.6 for x in j)


def test_retry_succeeds_after_transient_failures():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("boom")
        return "ok"

    assert retry(flaky, attempts=4, base=0.001, sleep=lambda s: None) == "ok"
    assert calls["n"] == 3


def test_retry_gives_up():
    with pytest.raises(ConnectionError):
        retry(lambda: (_ for _ in ()).throw(ConnectionError("x")), attempts=2, base=0.001, sleep=lambda s: None)
