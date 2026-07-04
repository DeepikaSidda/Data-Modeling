"""Property-based tests for the Ordering_Key allocator and comparator.

These tests validate the design's Correctness Properties 7 and 8 for the
pure-logic ordering layer (``waiting_room/ordering.py``). Both properties run
with at least 100 Hypothesis iterations (``@settings(max_examples=100)``).

Property 7 exercises the Hybrid Logical Clock's monotonicity guarantee under a
hostile wall clock (regressions + same-millisecond clusters + large skew).
Property 8 exercises the comparator's strict-total-order contract and the
idempotence of position recomputation over a fixed set of entries.
"""

from __future__ import annotations

import functools

from hypothesis import given, settings
from hypothesis import strategies as st

from waiting_room.ordering import (
    MAX_PHYSICAL_MS,
    TIEBREAK_HEX_WIDTH,
    OrderingKeyAllocator,
    compare_ordering_keys,
    ordering_key_sort_key,
    parse_ordering_key,
    render_seq,
)


# --------------------------------------------------------------------------- #
# Strategies
# --------------------------------------------------------------------------- #
# A stream of raw wall-clock readings (whole milliseconds) that deliberately
# mixes tightly-clustered small values (same-millisecond bursts and easy
# regressions) with arbitrary large values (cross-node skew far beyond any
# configured bound, and big backward jumps). Feeding these into the allocator's
# injected clock reproduces regressions, clusters, and skew all at once.
_clock_reading = st.one_of(
    st.integers(min_value=0, max_value=5_000),
    st.integers(min_value=0, max_value=MAX_PHYSICAL_MS),
)
clock_streams = st.lists(_clock_reading, min_size=1, max_size=150)


def _make_clock(readings):
    """Return a zero-arg callable yielding each reading in order.

    After the list is exhausted it repeats the final reading, so the allocator
    can be called at least ``len(readings)`` times safely.
    """
    it = iter(readings)
    last = readings[-1]

    def clock_ms() -> int:
        nonlocal last
        try:
            last = next(it)
        except StopIteration:
            pass
        return last

    return clock_ms


# An Ordering_Key drawn from a small seq/tiebreak space so that same-seq
# collisions (distinguished only by the random tie-breaker) occur frequently,
# which is exactly the case Property 8 must cover.
@st.composite
def ordering_keys(draw) -> str:
    physical_ms = draw(st.integers(min_value=0, max_value=50))
    logical = draw(st.integers(min_value=0, max_value=5))
    tiebreak_val = draw(st.integers(min_value=0, max_value=40))
    tiebreak = format(tiebreak_val, f"0{TIEBREAK_HEX_WIDTH}x")
    return f"{render_seq(physical_ms, logical)}#{tiebreak}"


# Small sets of *distinct* Ordering_Keys. Distinctness matters because
# Property 8 is about distinct entries never comparing equal. The size is kept
# small so the O(n^3) transitivity check stays cheap.
def _distinct_keys(keys):
    seen = []
    for k in keys:
        if k not in seen:
            seen.append(k)
    return seen


key_sets = st.lists(ordering_keys(), min_size=1, max_size=12).map(_distinct_keys)


# --------------------------------------------------------------------------- #
# Property 7
# --------------------------------------------------------------------------- #
# Feature: virtual-waiting-room, Property 7: For any stream of admissions processed by a node — including injected clock regressions and cross-node skew beyond the configured bound — the emitted seq component of the Ordering_Key is monotonically non-decreasing in admission order for that node.
# Validates: Requirements 3.1, 3.6
@settings(max_examples=100)
@given(readings=clock_streams)
def test_hlc_seq_monotonic_under_clock_skew(readings):
    """The emitted ``seq`` never decreases across an admission stream, no
    matter how the injected wall clock regresses, clusters, or skews."""
    # A constant, well-formed tie-breaker keeps the test focused on ``seq``;
    # Property 7 concerns only the sequence component's monotonicity.
    constant_tiebreak = "0" * TIEBREAK_HEX_WIDTH
    allocator = OrderingKeyAllocator(
        clock_ms=_make_clock(readings),
        tiebreak_source=lambda: constant_tiebreak,
    )

    prev_seq = None
    prev_pair = None
    for _ in readings:
        key = allocator.next_ordering_key()
        parts = parse_ordering_key(key)

        # Fixed-width rendering means lexicographic seq order == chronological
        # (physical_ms, logical) order; assert both views agree and never drop.
        if prev_seq is not None:
            assert parts.seq >= prev_seq, (
                f"seq regressed: {prev_seq!r} -> {parts.seq!r}"
            )
            assert (parts.physical_ms, parts.logical) >= prev_pair, (
                f"HLC pair regressed: {prev_pair} -> "
                f"{(parts.physical_ms, parts.logical)}"
            )

        prev_seq = parts.seq
        prev_pair = (parts.physical_ms, parts.logical)


# --------------------------------------------------------------------------- #
# Property 8
# --------------------------------------------------------------------------- #
# Feature: virtual-waiting-room, Property 8: For any set of Queue_Entries, the Ordering_Key comparator is a strict total order (irreflexive, antisymmetric, transitive, and total — no two distinct entries compare equal because of the unique random tie-breaker), and recomputing Queue_Position over the same set any number of times yields identical results.
# Validates: Requirements 3.2, 3.3, 3.7, 9.5
def _queue_positions(keys):
    """Queue_Position for each key = 1 + count of keys strictly ahead of it."""
    return {
        k: 1 + sum(1 for other in keys if compare_ordering_keys(other, k) < 0)
        for k in keys
    }


@settings(max_examples=100)
@given(keys=key_sets)
def test_comparator_is_strict_total_order_and_position_is_idempotent(keys):
    """The comparator is irreflexive, antisymmetric, transitive, and total over
    distinct entries, and Queue_Position recomputation is deterministic."""
    # Irreflexive: a key never orders before or after itself.
    for a in keys:
        assert compare_ordering_keys(a, a) == 0

    # Antisymmetric + total: distinct entries always compare unequal and the
    # comparison flips sign when arguments are swapped.
    for a in keys:
        for b in keys:
            cmp_ab = compare_ordering_keys(a, b)
            cmp_ba = compare_ordering_keys(b, a)
            assert cmp_ab == -cmp_ba
            if a != b:
                assert cmp_ab != 0, f"distinct entries compared equal: {a!r}, {b!r}"

    # Transitive: a<b and b<c implies a<c (and the same for the > direction).
    for a in keys:
        for b in keys:
            for c in keys:
                if (
                    compare_ordering_keys(a, b) < 0
                    and compare_ordering_keys(b, c) < 0
                ):
                    assert compare_ordering_keys(a, c) < 0

    # Determinism: sorting the same set repeatedly (via both the comparator and
    # the sort-key helper) yields identical orderings every time.
    by_cmp_1 = sorted(keys, key=functools.cmp_to_key(compare_ordering_keys))
    by_cmp_2 = sorted(keys, key=functools.cmp_to_key(compare_ordering_keys))
    by_sortkey = sorted(keys, key=ordering_key_sort_key)
    assert by_cmp_1 == by_cmp_2
    assert by_cmp_1 == by_sortkey

    # Idempotent Queue_Position: recomputing over the same set is stable, and a
    # front-most entry (if any) holds position 1 with all positions distinct.
    positions_first = _queue_positions(keys)
    for _ in range(3):
        assert _queue_positions(keys) == positions_first
    assert sorted(positions_first.values()) == list(range(1, len(keys) + 1))
