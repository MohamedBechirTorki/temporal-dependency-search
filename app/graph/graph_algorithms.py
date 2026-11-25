"""Iterative graph-wide analytics algorithms (degrees, WCC, Tarjan SCC, cycles, PageRank, Brandes)."""
from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class DegreeStats:
    in_degree: dict[str, int]
    out_degree: dict[str, int]
    total_degree: dict[str, int]
    unique_pairs: set[tuple[str, str]]
    self_loops: list[str]


def compute_degrees(nodes: Iterable[str], edges: Iterable[tuple[str, str]]) -> DegreeStats:
    node_list = list(nodes)
    in_deg = {n: 0 for n in node_list}
    out_deg = {n: 0 for n in node_list}
    unique_pairs: set[tuple[str, str]] = set()
    self_loops_set: set[str] = set()

    for u, v in edges:
        if u not in out_deg:
            out_deg[u] = 0
            in_deg.setdefault(u, 0)
        if v not in in_deg:
            in_deg[v] = 0
            out_deg.setdefault(v, 0)
        out_deg[u] += 1
        in_deg[v] += 1
        if u == v:
            self_loops_set.add(u)
        else:
            unique_pairs.add((u, v))

    total_deg = {n: in_deg[n] + out_deg[n] for n in in_deg}
    return DegreeStats(
        in_degree=in_deg,
        out_degree=out_deg,
        total_degree=total_deg,
        unique_pairs=unique_pairs,
        self_loops=sorted(self_loops_set),
    )


def weakly_connected_components(nodes: Iterable[str], edges: Iterable[tuple[str, str]]) -> list[list[str]]:
    node_list = list(nodes)
    adj: dict[str, list[str]] = {n: [] for n in node_list}
    for u, v in edges:
        adj.setdefault(u, []).append(v)
        if u != v:
            adj.setdefault(v, []).append(u)

    visited: set[str] = set()
    comps: list[list[str]] = []
    for start in node_list:
        if start in visited:
            continue
        comp: list[str] = []
        q = deque([start])
        visited.add(start)
        while q:
            u = q.popleft()
            comp.append(u)
            for w in adj.get(u, ()):
                if w not in visited:
                    visited.add(w)
                    q.append(w)
        comps.append(sorted(comp))
    comps.sort(key=lambda c: (-len(c), c[0]))
    return comps


def strongly_connected_components(nodes: Iterable[str], edges: Iterable[tuple[str, str]]) -> list[list[str]]:
    """Iterative Tarjan's SCC algorithm (safe for arbitrarily deep graphs)."""
    node_list = sorted(set(nodes))
    adj: dict[str, list[str]] = {n: [] for n in node_list}
    for u, v in edges:
        if u != v:
            adj.setdefault(u, []).append(v)
            adj.setdefault(v, [])

    index_counter = 0
    indices: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_stack: set[str] = set()
    tarjan_stack: list[str] = []
    sccs: list[list[str]] = []

    for start in node_list:
        if start in indices:
            continue
        indices[start] = lowlink[start] = index_counter
        index_counter += 1
        tarjan_stack.append(start)
        on_stack.add(start)
        call_stack: list[tuple[str, int]] = [(start, 0)]

        while call_stack:
            v, idx = call_stack[-1]
            nbrs = adj[v]
            if idx < len(nbrs):
                call_stack[-1] = (v, idx + 1)
                w = nbrs[idx]
                if w not in indices:
                    indices[w] = lowlink[w] = index_counter
                    index_counter += 1
                    tarjan_stack.append(w)
                    on_stack.add(w)
                    call_stack.append((w, 0))
                elif w in on_stack and indices[w] < lowlink[v]:
                    lowlink[v] = indices[w]
            else:
                call_stack.pop()
                if call_stack:
                    parent = call_stack[-1][0]
                    if lowlink[v] < lowlink[parent]:
                        lowlink[parent] = lowlink[v]
                if lowlink[v] == indices[v]:
                    comp: list[str] = []
                    while True:
                        w = tarjan_stack.pop()
                        on_stack.remove(w)
                        comp.append(w)
                        if w == v:
                            break
                    sccs.append(sorted(comp))

    sccs.sort(key=lambda c: (-len(c), c[0]))
    return sccs


def _canonical_cycle(cycle_no_repeat: list[str]) -> tuple[str, ...]:
    """Rotate cycle so its lexicographically smallest node is first."""
    min_idx = min(range(len(cycle_no_repeat)), key=cycle_no_repeat.__getitem__)
    return tuple(cycle_no_repeat[min_idx:] + cycle_no_repeat[:min_idx])


def find_sample_cycles(
    edges: Iterable[tuple[str, str]],
    non_trivial_sccs: list[list[str]],
    self_loops: list[str],
    *,
    max_cycles: int = 100,
    max_cycle_length: int = 15,
) -> list[list[str]]:
    """Extract representative simple directed cycles from self-loops and non-trivial SCCs."""
    cycles: list[list[str]] = []
    seen_canonical: set[tuple[str, ...]] = set()

    for u in self_loops:
        if len(cycles) >= max_cycles:
            return cycles
        cycles.append([u, u])

    if not non_trivial_sccs or len(cycles) >= max_cycles:
        return cycles

    unique_edges = {(u, v) for u, v in edges if u != v}
    for scc in non_trivial_sccs:
        if len(cycles) >= max_cycles:
            break
        scc_set = set(scc)
        adj: dict[str, list[str]] = {n: [] for n in scc}
        for u, v in unique_edges:
            if u in scc_set and v in scc_set:
                adj[u].append(v)
        for n in adj:
            adj[n].sort()

        found_in_scc = 0
        # For each node u in the SCC, find shortest simple cycle back to u via BFS
        for u in scc:
            if len(cycles) >= max_cycles:
                break
            q: deque[tuple[str, int]] = deque()
            parent: dict[str, str] = {}
            visited = {u}
            for nxt in adj[u]:
                if nxt not in visited:
                    visited.add(nxt)
                    parent[nxt] = u
                    q.append((nxt, 1))

            hit_path: list[str] | None = None
            while q:
                cur, dist = q.popleft()
                if dist >= max_cycle_length and found_in_scc > 0:
                    continue
                for nxt in adj[cur]:
                    if nxt == u:
                        # Reconstruct u -> ... -> cur -> u
                        rev = [cur]
                        p = cur
                        while p != u:
                            p = parent[p]
                            rev.append(p)
                        hit_path = list(reversed(rev))
                        break
                    if nxt not in visited:
                        visited.add(nxt)
                        parent[nxt] = cur
                        q.append((nxt, dist + 1))
                if hit_path is not None:
                    break

            if hit_path is not None:
                canon = _canonical_cycle(hit_path)
                if canon not in seen_canonical:
                    seen_canonical.add(canon)
                    cycles.append([*hit_path, hit_path[0]])
                    found_in_scc += 1

    return cycles


def pagerank(
    nodes: Iterable[str],
    unique_pairs: Iterable[tuple[str, str]],
    *,
    damping: float = 0.85,
    max_iter: int = 50,
    tol: float = 1e-6,
) -> dict[str, float]:
    """Compute PageRank over directed edges (u -> v).

    Under A -> B ('A depends on B'), rank flows from dependents to their
    dependencies, highlighting the most critical / relied-upon hubs.
    """
    node_list = list(nodes)
    n = len(node_list)
    if n == 0:
        return {}
    if n == 1:
        return {node_list[0]: 1.0}

    out_nbrs: dict[str, list[str]] = {u: [] for u in node_list}
    in_nbrs: dict[str, list[str]] = {u: [] for u in node_list}
    for u, v in unique_pairs:
        if u != v and u in out_nbrs and v in in_nbrs:
            out_nbrs[u].append(v)
            in_nbrs[v].append(u)

    dangling = [u for u in node_list if not out_nbrs[u]]
    ranks = {u: 1.0 / n for u in node_list}
    base = (1.0 - damping) / n

    for _ in range(max_iter):
        dangling_mass = damping * sum(ranks[u] for u in dangling) / n
        new_ranks: dict[str, float] = {}
        diff = 0.0
        for v in node_list:
            incoming_sum = sum(ranks[u] / len(out_nbrs[u]) for u in in_nbrs[v])
            val = base + dangling_mass + damping * incoming_sum
            new_ranks[v] = val
            diff += abs(val - ranks[v])
        ranks = new_ranks
        if diff < tol:
            break
    return ranks


def betweenness_centrality(
    nodes: Iterable[str],
    unique_pairs: Iterable[tuple[str, str]],
    *,
    sample_k: int | None = None,
    seed: int = 42,
) -> tuple[dict[str, float], str, int]:
    """Brandes' unweighted directed betweenness centrality (normalized to [0, 1])."""
    node_list = sorted(nodes)
    n = len(node_list)
    cb = {v: 0.0 for v in node_list}
    if n <= 2:
        return cb, "brandes_exact", n

    adj: dict[str, list[str]] = {u: [] for u in node_list}
    for u, v in unique_pairs:
        if u != v and u in adj and v in adj:
            adj[u].append(v)

    if sample_k is not None and 0 < sample_k < n:
        rng = random.Random(seed)
        sources = rng.sample(node_list, sample_k)
        algo = "brandes_sampled"
        scale = n / float(sample_k)
    else:
        sources = node_list
        algo = "brandes_exact"
        scale = 1.0

    for s in sources:
        stack: list[str] = []
        preds: dict[str, list[str]] = {w: [] for w in node_list}
        sigma: dict[str, float] = {w: 0.0 for w in node_list}
        dist: dict[str, int] = {w: -1 for w in node_list}
        sigma[s] = 1.0
        dist[s] = 0
        q: deque[str] = deque([s])

        while q:
            v = q.popleft()
            stack.append(v)
            dv = dist[v]
            for w in adj[v]:
                if dist[w] < 0:
                    q.append(w)
                    dist[w] = dv + 1
                if dist[w] == dv + 1:
                    sigma[w] += sigma[v]
                    preds[w].append(v)

        delta: dict[str, float] = {w: 0.0 for w in node_list}
        while stack:
            w = stack.pop()
            coeff = (1.0 + delta[w]) / sigma[w] if sigma[w] > 0 else 0.0
            for v in preds[w]:
                delta[v] += sigma[v] * coeff
            if w != s:
                cb[w] += delta[w]

    norm = float((n - 1) * (n - 2))
    for v in cb:
        cb[v] = (cb[v] * scale) / norm
    return cb, algo, len(sources)
