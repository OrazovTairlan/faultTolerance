"""Resilient service-to-service client: short timeout, retry with exponential backoff on another replica,
per-replica circuit breaker."""
import itertools
import os
import time

import httpx

from common import events, topology
from common.resilience import CircuitBreaker, backoff_delay


class ServiceUnavailable(Exception):
    pass


class _Upstream5xx(Exception):
    pass


class ServiceClient:
    def __init__(self, service, component, timeout=0.6, attempts=2):
        _, instances = topology.load()
        self.service = service
        self.component = component
        self.urls = topology.urls_of(instances, service)
        self.attempts = attempts
        self.http = httpx.Client(timeout=httpx.Timeout(timeout, connect=0.3), limits=httpx.Limits(max_connections=100))
        self.breakers = {u: CircuitBreaker(f"{component}->{u}", failure_threshold=3, reset_timeout=2.0,
                                           on_change=self._changed) for u in self.urls}
        self._rr = itertools.count()

    def _changed(self, name, state):
        events.emit(self.component, f"circuit_{state}", target=name)

    def get(self, path):
        last = None
        for attempt in range(self.attempts):
            start = next(self._rr)
            order = [self.urls[(start + i) % len(self.urls)] for i in range(len(self.urls))]
            chosen = next((u for u in order if self.breakers[u].allow()), None)
            if chosen is None:
                raise ServiceUnavailable(f"{self.service}: all circuits open")
            try:
                r = self.http.get(chosen + path)
                if r.status_code >= 500:
                    raise _Upstream5xx(r.status_code)
                self.breakers[chosen].success()
                return r
            except (httpx.HTTPError, _Upstream5xx) as e:
                self.breakers[chosen].failure()
                last = e
                if attempt < self.attempts - 1:
                    time.sleep(backoff_delay(attempt, base=0.05, cap=0.5))
        raise ServiceUnavailable(f"{self.service}: {type(last).__name__}")
