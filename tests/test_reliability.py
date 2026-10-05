import math

from reliability import calc
from reliability.faulttree import AND, OR, Basic, cut_sets, top_probability


def test_single_component_availability_matches_formula():
    r = calc.analyse({"c": (100.0, 1.0)}, OR(Basic("c")))
    assert math.isclose(r["availability"], 100 / 101)
    assert math.isclose(r["MTTF"], 100.0, rel_tol=1e-9)
    assert math.isclose(r["MTTR"], 1.0, rel_tol=1e-9)
    assert math.isclose(r["MTBF"], 101.0, rel_tol=1e-9)


def test_series_and_parallel_structures():
    u = {"a": 0.1, "b": 0.2}
    assert math.isclose(top_probability(OR(Basic("a"), Basic("b")), u), 1 - 0.9 * 0.8)
    assert math.isclose(top_probability(AND(Basic("a"), Basic("b")), u), 0.02)


def test_minimal_cut_sets_with_shared_event():
    n = Basic("node")
    tree = OR(AND(OR(Basic("p1"), n), OR(Basic("p2"), Basic("node2"))))
    cs = {tuple(sorted(c)) for c in cut_sets(tree)}
    assert ("node", "node2") in cs and ("node", "p2") in cs and ("node2", "p1") in cs and ("p1", "p2") in cs
    assert all(len(c) == 2 for c in cs)


def test_baseline_has_spofs_ft_has_only_common_cause():
    real = calc.real_scale()
    assert len(real["baseline"]["single_points_of_failure"]) >= 7
    assert real["ft"]["single_points_of_failure"] == ["site"]
    assert real["ft"]["availability"] > real["baseline"]["availability"]
    assert real["ft"]["MTBF"] > real["baseline"]["MTBF"]


def test_exact_matches_closed_form_for_two_nodes():
    """A_sys = An^2 * prod(1-(1-Ap)^2) + 2 An (1-An) prod(Ap)   (tiers independent given the nodes)."""
    p = calc._params(calc.REAL, "ft")
    tree = calc.ft_tree(include_db=False, include_site=False)
    an = calc.REAL["node"][0] / (calc.REAL["node"][0] + calc.REAL["node"][2])
    ap = calc.REAL["process"][0] / (calc.REAL["process"][0] + calc.REAL["process"][2])
    tiers = 5
    closed = an ** 2 * (1 - (1 - ap) ** 2) ** tiers + 2 * an * (1 - an) * ap ** tiers
    exact = 1 - top_probability(tree, {k: v[1] / (v[0] + v[1]) for k, v in p.items()})
    assert math.isclose(exact, closed, rel_tol=1e-12)
