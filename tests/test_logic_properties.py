"""Property-based tests for the pure-logic layer of the Virtual Waiting Room.

This module implements three named design Correctness Properties with
Hypothesis, each running >= 100 iterations:

* Property 5  - write-shard distribution        (waiting_room/sharding.py)
* Property 9  - position equals one plus fans ahead (waiting_room/position.py)
* Property 10 - capacity-bounded batch sizing    (waiting_room/batching.py)

The tests exercise the pure decision functions across their whole input space.
They perform no I/O and use no DynamoDB / boto3.
"""

from __future__ import annotations

from collections import Counter

from hypothesis import given, settings
from hypothesis import strategies as st

from waiting_room.batching import compute_batch_size
from waiting_room.config import EligibilityStatus
from waiting_room.ordering import (
    MAX_LOGICAL,
    TIEBREAK_HEX_WIDTH,
    render_seq,
)
from waiting_room.position import QueueEntryView, queue_position
from waiting_room.sharding import compute_write_shard


# --------------------------------------------------------------------------- #
# Strategies
# --------------------------------------------------------------------------- #
# Ordering keys are built the same way the allocator renders them: a
# fixed-width numeric ``seq`` (render_seq) joined by ``#`` to a fixed-width hex
# tiebreak. Fixed widths keep whole-key lexicographic order == the comparator's
# order, and drawing seq/tiebreak from small pools deliberately creates seq
# collisions so the tiebreak leg of the total order is exercised too.
_PHYSICAL_MS = st.integers(min_value=0, max_value=500)
_LOGICAL = st.integers(min_value=0, max_value=min(MAX_LOGICAL, 20))
_TIEBREAK = st.integers(min_value=0, max_value=0xFF).map(
    lambda n: format(n, "x").zfill(TIEBREAK_HEX_WIDTH)
)


@st.composite
def _ordering_keys(draw) -> str:
    """Draw a well-formed ``<seq>#<tiebreak>`` Ordering_Key string."""
    seq = render_seq(draw(_PHYSICAL_MS), draw(_LOGICAL))
    return f"{seq}#{draw(_TIEBREAK)}"


_STATUSES = st.sampled_from(list(EligibilityStatus))


@st.composite
def _entries(draw) -> QueueEntryView:
    """Draw a QueueEntryView with a varied Ordering_Key and status."""
    return QueueEntryView(ordering_key=draw(_ordering_keys()), status=draw(_STATUSES))


# --------------------------------------------------------------------------- #
# Property 5 - write-shard distribution
# --------------------------------------------------------------------------- #
# Feature: virtual-waiting-room, Property 5: For any large set of distinct Fan_Ids and any Shard_Count, every assigned Write_Shard lies in [0, Shard_Count) and the assignment is deterministic per Fan_Id, with the population distributed across shards within a bounded deviation of uniform (no shard receives a disproportionate share).
# Validates: Requirements 2.2, 9.4
@settings(max_examples=100)
@given(
    fan_ids=st.sets(st.text(min_size=1, max_size=24), min_size=50, max_size=250),
    shard_count=st.integers(min_value=1, max_value=16),
)
def test_property_5_write_shard_distribution(fan_ids, shard_count):
    shards = {}
    for fan_id in fan_ids:
        shard = compute_write_shard(fan_id, shard_count)
        # Range: every assigned Write_Shard lies in [0, Shard_Count).
        assert 0 <= shard < shard_count
        # Determinism: recomputing yields the identical shard for the Fan_Id.
        assert compute_write_shard(fan_id, shard_count) == shard
        shards[fan_id] = shard

    # Bounded deviation of uniform: with enough distinct ids spread over the
    # shards, no shard may receive a disproportionate share. The bound is
    # deliberately generous (robust to normal statistical variation) yet still
    # fails loudly for a broken assignment that piles ids onto one shard.
    if shard_count >= 2 and len(fan_ids) >= 10 * shard_count:
        counts = Counter(shards.values())
        mean = len(fan_ids) / shard_count
        max_count = max(counts.values())
        assert max_count <= mean * 3 + 15


# --------------------------------------------------------------------------- #
# Property 9 - position equals one plus fans ahead
# --------------------------------------------------------------------------- #
# Feature: virtual-waiting-room, Property 9: For any set of Queue_Entries and any entry f, Queue_Position(f) = 1 + |{ g : g.Eligibility_Status = WAITING AND g.Ordering_Key < f.Ordering_Key }|, so the front-most entry holds position 1 and the number of fans ahead equals Queue_Position − 1.
# Validates: Requirements 8.3
@settings(max_examples=100)
@given(entries=st.lists(_entries(), min_size=1, max_size=40), index=st.integers(min_value=0))
def test_property_9_position_equals_one_plus_fans_ahead(entries, index):
    f = entries[index % len(entries)]

    # Independent brute-force oracle. Ordering_Keys are fixed-width, so plain
    # string comparison equals the ordering comparator's strict total order.
    fans_ahead_expected = sum(
        1
        for g in entries
        if g.status is EligibilityStatus.WAITING and g.ordering_key < f.ordering_key
    )
    expected_position = 1 + fans_ahead_expected

    actual = queue_position(f.ordering_key, entries)
    assert actual == expected_position
    # Number of fans ahead equals Queue_Position - 1.
    assert actual - 1 == fans_ahead_expected

    # The front-most WAITING entry holds position 1.
    waiting_keys = [g.ordering_key for g in entries if g.status is EligibilityStatus.WAITING]
    if waiting_keys:
        front_key = min(waiting_keys)
        assert queue_position(front_key, entries) == 1


# --------------------------------------------------------------------------- #
# Property 10 - capacity-bounded batch sizing
# --------------------------------------------------------------------------- #
# Feature: virtual-waiting-room, Property 10: For any queue state with w WAITING entries, available remaining capacity r = Downstream_Capacity − (Eligible_Count + Active_Count), and configured Max_Batch_Size m, a promotion cycle promotes exactly min(w, m, max(r, 0)) entries — in particular zero when r ≤ 0.
# Validates: Requirements 5.3, 5.4, 6.1, 6.2, 6.3, 6.5
_COUNT = st.integers(min_value=0, max_value=10_000)


@settings(max_examples=100)
@given(
    waiting_count=_COUNT,
    max_batch_size=_COUNT,
    downstream_capacity=_COUNT,
    eligible_count=_COUNT,
    active_count=_COUNT,
)
def test_property_10_capacity_bounded_batch_sizing(
    waiting_count, max_batch_size, downstream_capacity, eligible_count, active_count
):
    remaining = downstream_capacity - (eligible_count + active_count)
    expected = min(waiting_count, max_batch_size, max(remaining, 0))

    actual = compute_batch_size(
        waiting_count=waiting_count,
        max_batch_size=max_batch_size,
        downstream_capacity=downstream_capacity,
        eligible_count=eligible_count,
        active_count=active_count,
    )
    assert actual == expected

    # In particular, zero whenever remaining capacity r <= 0.
    if remaining <= 0:
        assert actual == 0

    # The result never exceeds any of the three simultaneous limits.
    assert 0 <= actual <= waiting_count
    assert actual <= max_batch_size
    assert actual <= max(remaining, 0)


# --------------------------------------------------------------------------- #
# Boundary emphasis for Property 10: r = 0 and r = capacity.
# --------------------------------------------------------------------------- #
@settings(max_examples=100)
@given(
    downstream_capacity=st.integers(min_value=1, max_value=10_000),
    waiting_count=_COUNT,
    max_batch_size=st.integers(min_value=1, max_value=10_000),
)
def test_property_10_boundaries_r_zero_and_r_full(
    downstream_capacity, waiting_count, max_batch_size
):
    # r == 0: ELIGIBLE + ACTIVE exactly meets capacity -> promote zero.
    eligible = downstream_capacity // 2
    active = downstream_capacity - eligible
    at_capacity = compute_batch_size(
        waiting_count=waiting_count,
        max_batch_size=max_batch_size,
        downstream_capacity=downstream_capacity,
        eligible_count=eligible,
        active_count=active,
    )
    assert at_capacity == 0

    # r == capacity: nothing eligible/active -> min(w, m, capacity).
    fully_free = compute_batch_size(
        waiting_count=waiting_count,
        max_batch_size=max_batch_size,
        downstream_capacity=downstream_capacity,
        eligible_count=0,
        active_count=0,
    )
    assert fully_free == min(waiting_count, max_batch_size, downstream_capacity)
