from common.appkit import Metrics


def _families(text):
    """Metric names in exposition order (one entry per contiguous block)."""
    names = []
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        name = line.split("{")[0]
        if not names or names[-1] != name:
            names.append(name)
    return names


def test_collectors_are_exported_with_instance_labels():
    m = Metrics()
    m.observe(200, 0.01)
    m.observe(503, 0.02)
    m.collect(lambda: [("db_journal_pending", "gauge", {}, 3),
                       ("gateway_replica_healthy", "gauge", {"target": "payment-1"}, 1)])
    text = m.render("payment", "payment-1")
    assert 'http_requests_total{service="payment",instance="payment-1",code="503"} 1' in text
    assert 'db_journal_pending{service="payment",instance="payment-1"} 3' in text
    assert 'gateway_replica_healthy{service="payment",instance="payment-1",target="payment-1"} 1' in text


def test_families_stay_contiguous_and_broken_collectors_are_skipped():
    m = Metrics()
    m.collect(lambda: [("a", "gauge", {"t": "1"}, 1), ("b", "gauge", {}, 2)])
    m.collect(lambda: [("a", "gauge", {"t": "2"}, 3)])
    m.collect(lambda: 1 / 0)
    names = _families(m.render("s", "i"))
    assert len(names) == len(set(names))          # every family appears in exactly one block
    assert names.count("a") == 1
