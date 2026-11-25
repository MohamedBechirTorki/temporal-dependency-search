"""Unit tests for iterative graph analytics algorithms."""
from __future__ import annotations

import pytest

from app.graph.graph_algorithms import (
    betweenness_centrality,
    compute_degrees,
    find_sample_cycles,
    pagerank,
    strongly_connected_components,
    weakly_connected_components,
)


def test_empty_and_isolated_degrees():
    d0 = compute_degrees([], [])
    assert d0.in_degree == {} and d0.self_loops == []

    d1 = compute_degrees(["A", "B", "C"], [("A", "B"), ("A", "B"), ("C", "C")])
    assert d1.out_degree["A"] == 2
    assert d1.in_degree["B"] == 2
    assert d1.in_degree["C"] == 1 and d1.out_degree["C"] == 1
    assert d1.unique_pairs == {("A", "B")}
    assert d1.self_loops == ["C"]


def test_wcc_and_iterative_tarjan_scc_deep_chain():
    # Deep 5000-node chain: must not hit Python recursion limit
    nodes = [f"n{i:04d}" for i in range(5000)]
    edges = [(nodes[i], nodes[i + 1]) for i in range(4999)]
    wccs = weakly_connected_components(nodes, edges)
    assert len(wccs) == 1 and len(wccs[0]) == 5000

    sccs = strongly_connected_components(nodes, edges)
    assert len(sccs) == 5000
    assert all(len(c) == 1 for c in sccs)


def test_scc_and_cycle_extraction():
    nodes = ["A", "B", "C", "D", "E"]
    edges = [("A", "B"), ("B", "C"), ("C", "A"), ("B", "D"), ("D", "E"), ("E", "E")]
    sccs = strongly_connected_components(nodes, edges)
    non_trivial = [c for c in sccs if len(c) > 1]
    assert non_trivial == [["A", "B", "C"]]

    deg = compute_degrees(nodes, edges)
    cycles = find_sample_cycles(edges, non_trivial, deg.self_loops, max_cycles=10)
    assert ["E", "E"] in cycles
    assert any(c[0] == c[-1] and set(c) == {"A", "B", "C"} for c in cycles)


def test_pagerank_and_brandes_betweenness():
    # Line A -> B -> C -> D: B and C have exact directed betweenness 2 / ((4-1)*(4-2)) = 1/3
    nodes = ["A", "B", "C", "D"]
    edges = {("A", "B"), ("B", "C"), ("C", "D")}
    cb, algo, count = betweenness_centrality(nodes, edges)
    assert algo == "brandes_exact" and count == 4
    assert cb["A"] == pytest.approx(0.0)
    assert cb["D"] == pytest.approx(0.0)
    assert cb["B"] == pytest.approx(1.0 / 3.0)
    assert cb["C"] == pytest.approx(1.0 / 3.0)

    # Sampled mode runs cleanly
    cb_s, algo_s, count_s = betweenness_centrality(nodes, edges, sample_k=2, seed=7)
    assert algo_s == "brandes_sampled" and count_s == 2
    assert set(cb_s) == set(nodes)

    # Star hub: L1..L4 -> HUB -> rank(HUB) is highest
    star_nodes = ["HUB", "L1", "L2", "L3", "L4"]
    star_edges = {("L1", "HUB"), ("L2", "HUB"), ("L3", "HUB"), ("L4", "HUB")}
    pr = pagerank(star_nodes, star_edges)
    assert sum(pr.values()) == pytest.approx(1.0, rel=1e-4)
    assert max(pr, key=pr.__getitem__) == "HUB"
