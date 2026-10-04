"""Small fault-tree engine: gates, minimal cut sets (MOCUS), exact top-event probability by state enumeration,
and cut-set based failure-frequency approximation."""
import itertools


class Basic:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name


class Gate:
    def __init__(self, kind, children, label=""):
        assert kind in ("AND", "OR")
        self.kind, self.children, self.label = kind, list(children), label


def OR(*c, label=""):
    return Gate("OR", c, label)


def AND(*c, label=""):
    return Gate("AND", c, label)


def basic_events(node, acc=None):
    acc = set() if acc is None else acc
    if isinstance(node, Basic):
        acc.add(node.name)
    else:
        for c in node.children:
            basic_events(c, acc)
    return acc


def cut_sets(node):
    """MOCUS: returns the list of minimal cut sets (frozensets of basic-event names)."""
    def expand(n):
        if isinstance(n, Basic):
            return [frozenset([n.name])]
        subs = [expand(c) for c in n.children]
        if n.kind == "OR":
            return [cs for s in subs for cs in s]
        out = [frozenset()]
        for s in subs:
            out = [a | b for a in out for b in s]
        return out
    sets = set(expand(node))
    return sorted([s for s in sets if not any(o < s for o in sets)], key=lambda s: (len(s), sorted(s)))


def evaluate(node, failed):
    if isinstance(node, Basic):
        return node.name in failed
    vals = [evaluate(c, failed) for c in node.children]
    return any(vals) if node.kind == "OR" else all(vals)


def top_probability(node, unavail):
    """Exact probability that the top event is TRUE; `unavail[name]` = P(basic event is failed)."""
    names = sorted(basic_events(node))
    total = 0.0
    for state in itertools.product((0, 1), repeat=len(names)):
        p, failed = 1.0, set()
        for n, s in zip(names, state):
            if s:
                p *= unavail[n]
                failed.add(n)
            else:
                p *= 1 - unavail[n]
        if p and evaluate(node, failed):
            total += p
    return total


def failure_frequency(cuts, lam, unavail):
    """Rare-event approximation of system failure frequency (per time unit of lam):
    a cut set starts to be down when its last working member fails while all others are already down."""
    f = 0.0
    for cs in cuts:
        for i in cs:
            term = lam[i] * (1.0 - unavail[i])        # component i must be up to be able to fail
            for j in cs:
                if j != i:
                    term *= unavail[j]
            f += term
    return f
