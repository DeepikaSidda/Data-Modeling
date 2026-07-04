"""Property-based tests for the Batch_Promoter data-access layer.

Tasks 13.2 and 13.3. These Hypothesis property tests exercise
:class:`waiting_room.promoter.BatchPromoter` end-to-end against a ``moto``-backed
DynamoDB table, validating the design's Correctness Properties 12 and 13.

Each example spins up a fresh in-memory DynamoDB (``moto``), creates the
``WaitingRoom`` table via the provisioning module, seeds an OPEN event, and
admits a number of fans (scattered across several write shards by
``hash(Fan_Id)``) so the table holds a population of ``WAITING`` entries whose
``Ordering_Key`` global order is exactly the admission order (a single
monotonic :class:`OrderingKeyAllocator` mints the keys). It then drives
``promote_cycle`` and inspects the persisted items.

Because ``moto`` is slower than the pure-logic tests, every property runs with
``max_examples=100`` and the Hypothesis per-example ``deadline`` disabled.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, NamedTuple

import boto3
from hypothesis import given, settings
from hypothesis import strategies as st
from moto import mock_aws

from waiting_room.config import EligibilityStatus, EventConfig, WaitingRoomConfig
from waiting_room.lifecycle_manager import LifecycleManager, TransitionOutcome
from waiting_room.ordering import OrderingKeyAllocator, ordering_key_sort_key
from waiting_room.promoter import BatchPromoter, entry_sk, waiting_shard
from waiting_room.provisioning import (
    CAPACITY_SK,
    TABLE_NAME,
    create_waiting_room_table,
    event_pk,
    seed_event,
)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
SECRET = "promoter-property-test-secret"
EVENT_ID = "evt-13"
#: A small shard count keeps the k-way merge cheap while still scattering the
#: admitted fans across several shard partitions (so promotion selection must
#: genuinely merge across shards, not just read one).
SHARD_COUNT = 6

WAITING = EligibilityStatus.WAITING.value


class Env(NamedTuple):
    """A fully wired promotion environment for a single Hypothesis example."""

    client: Any
    promoter: BatchPromoter
    config: WaitingRoomConfig
    event_id: str


@contextmanager
def promoter_env(config: WaitingRoomConfig) -> Iterator[Env]:
    """Yield a fresh moto DynamoDB + provisioned table + seeded OPEN event.

    The ``CONFIG`` and ``CAPACITY`` items are seeded from ``config.event`` so the
    promoter reads its ``Shard_Count``, ``Max_Batch_Size`` and
    ``Downstream_Capacity`` straight from the table exactly as in production.
    """
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_waiting_room_table(client)
        seed_event(client, EVENT_ID, config.event)
        promoter = BatchPromoter(client=client)
        yield Env(
            client=client, promoter=promoter, config=config, event_id=EVENT_ID
        )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _admit_waiting_entries(client: Any, event_id: str, n_fans: int) -> list[str]:
    """Write ``n_fans`` WAITING entries directly, returning Ordering_Keys in order.

    A single monotonic :class:`OrderingKeyAllocator` mints the keys, so the
    returned list is the global ``Ordering_Key`` order. Each fan is placed on the
    shard ``hash`` would pick via ``i % SHARD_COUNT`` so entries genuinely spread
    across multiple shard partitions of the sparse ``WaitingIndex``.
    """
    allocator = OrderingKeyAllocator()
    ordering_keys: list[str] = []
    for i in range(n_fans):
        ordering_key = allocator.next_ordering_key()
        ordering_keys.append(ordering_key)
        shard = i % SHARD_COUNT
        ws_value = waiting_shard(event_id, shard)
        client.put_item(
            TableName=TABLE_NAME,
            Item={
                "PK": {"S": ws_value},
                "SK": {"S": entry_sk(ordering_key)},
                "Fan_Id": {"S": f"fan-{i}"},
                "Event_Id": {"S": event_id},
                "Write_Shard": {"S": str(shard)},
                "Entry_Timestamp": {"N": str(1_000 + i)},
                "Eligibility_Status": {"S": WAITING},
                "Ordering_Key": {"S": ordering_key},
                # Sparse WaitingIndex PK, present only while WAITING.
                "Waiting_Shard": {"S": ws_value},
            },
        )
    return ordering_keys


def _scan_entries(client: Any) -> list[dict[str, Any]]:
    """Return every queue-entry item (SK begins with ``ENTRY#``) in the table."""
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {"TableName": TABLE_NAME}
    while True:
        response = client.scan(**kwargs)
        items.extend(response.get("Items", []))
        start_key = response.get("LastEvaluatedKey")
        if not start_key:
            break
        kwargs["ExclusiveStartKey"] = start_key
    return [i for i in items if i.get("SK", {}).get("S", "").startswith("ENTRY#")]


def _ok(entry: dict[str, Any]) -> str:
    """Return the Ordering_Key of an entry item."""
    return entry["Ordering_Key"]["S"]


def _status(entry: dict[str, Any]) -> str:
    """Return the Eligibility_Status of an entry item."""
    return entry["Eligibility_Status"]["S"]


def _sorted_by_order(entries: list[dict[str, Any]]) -> list[str]:
    """Return the entries' Ordering_Keys sorted by the total order."""
    return [
        _ok(e)
        for e in sorted(entries, key=lambda e: ordering_key_sort_key(_ok(e)))
    ]


def _read_capacity(client: Any, event_id: str) -> dict[str, int]:
    """Read the CAPACITY counter item's numeric fields."""
    resp = client.get_item(
        TableName=TABLE_NAME,
        Key={"PK": {"S": event_pk(event_id)}, "SK": {"S": CAPACITY_SK}},
        ConsistentRead=True,
    )
    item = resp["Item"]
    return {
        "downstream_capacity": int(item["Downstream_Capacity"]["N"]),
        "eligible_count": int(item["Eligible_Count"]["N"]),
        "active_count": int(item["Active_Count"]["N"]),
    }


def _assert_prefix_invariant(entries: list[dict[str, Any]]) -> None:
    """Assert the promoted (non-WAITING) set is a prefix of the global order.

    That is: the promoted entries are exactly the earliest ``k`` entries by
    ``Ordering_Key`` (k = number promoted), and every promoted entry's key is
    ``<=`` every still-WAITING entry's key.
    """
    global_order = _sorted_by_order(entries)
    promoted = [e for e in entries if _status(e) != WAITING]
    waiting = [e for e in entries if _status(e) == WAITING]

    # Promoted entries are exactly the first k of the total Ordering_Key order.
    k = len(promoted)
    expected_prefix = set(global_order[:k])
    assert {_ok(e) for e in promoted} == expected_prefix

    # Every promoted key <= every still-WAITING key.
    if promoted and waiting:
        max_promoted = max(ordering_key_sort_key(_ok(e)) for e in promoted)
        min_waiting = min(ordering_key_sort_key(_ok(e)) for e in waiting)
        assert max_promoted <= min_waiting


# --------------------------------------------------------------------------- #
# Property 12: Order-preserving promotion (task 13.2)
# --------------------------------------------------------------------------- #
@settings(max_examples=100, deadline=None)
@given(
    n_fans=st.integers(min_value=2, max_value=14),
    max_batch=st.integers(min_value=1, max_value=4),
    capacity=st.integers(min_value=1, max_value=14),
)
def test_property_12_order_preserving_promotion(
    n_fans: int, max_batch: int, capacity: int
) -> None:
    """Feature: virtual-waiting-room, Property 12: For any queue state and any interleaving of promotion cycles, the set of promoted (non-WAITING) entries is always a prefix of the total Ordering_Key order — every promoted entry has an Ordering_Key less than or equal to every still-WAITING entry — and the pairwise relative order of any two entries by Ordering_Key is preserved across all promotion and status operations.

    Validates: Requirements 5.1, 5.6, 9.3
    """
    config = WaitingRoomConfig(
        event=EventConfig(
            shard_count=SHARD_COUNT,
            max_batch_size=max_batch,
            downstream_capacity=capacity,
        )
    )
    with promoter_env(config) as env:
        admission_order = _admit_waiting_entries(env.client, env.event_id, n_fans)

        # Before any promotion the invariant holds trivially (nothing promoted).
        _assert_prefix_invariant(_scan_entries(env.client))

        # Run promotion cycles until the queue stops making progress (capacity
        # is exhausted or no WAITING entries remain). Each cycle promotes at most
        # `max_batch` and never more than remaining capacity, so several cycles
        # are needed. The invariant is re-checked after *every* cycle.
        total_promoted = 0
        for _ in range(n_fans + 5):
            result = env.promoter.promote_cycle(env.event_id)
            total_promoted += result.promoted_count

            entries = _scan_entries(env.client)
            # (a) Promoted set is always a prefix of the total order, and every
            #     promoted key <= every still-WAITING key.
            _assert_prefix_invariant(entries)
            # (b) Pairwise relative order is preserved: the Ordering_Keys never
            #     change, so the global order still equals the admission order.
            assert _sorted_by_order(entries) == admission_order

            if result.promoted_count == 0:
                break

        # Sanity: capacity bounds how many were ever promoted, and progress was
        # made whenever capacity allowed it.
        expected_total = min(n_fans, capacity)
        assert total_promoted == expected_total

        # Final state: exactly `expected_total` promoted, the earliest by order.
        final_entries = _scan_entries(env.client)
        promoted = [e for e in final_entries if _status(e) != WAITING]
        assert len(promoted) == expected_total
        assert {_ok(e) for e in promoted} == set(admission_order[:expected_total])


# --------------------------------------------------------------------------- #
# Property 13: Idempotent conditional promotion (task 13.3)
# --------------------------------------------------------------------------- #
@settings(max_examples=100, deadline=None)
@given(
    n_fans=st.integers(min_value=1, max_value=14),
    max_batch=st.integers(min_value=1, max_value=6),
    extra_capacity=st.integers(min_value=0, max_value=4),
    extra_cycles=st.integers(min_value=1, max_value=4),
)
def test_property_13_idempotent_conditional_promotion(
    n_fans: int, max_batch: int, extra_capacity: int, extra_cycles: int
) -> None:
    """Feature: virtual-waiting-room, Property 13: For any entry, applying a WAITING → ELIGIBLE promotion once or repeatedly (including concurrently) results in exactly one successful transition; the entry ends ELIGIBLE with a single Batch_Id and a recorded Promotion_Time, and subsequent promotion attempts are no-ops.

    Validates: Requirements 5.2, 5.5, 5.7, 10.3
    """
    # Capacity is at least the whole queue so every fan can be promoted.
    config = WaitingRoomConfig(
        event=EventConfig(
            shard_count=SHARD_COUNT,
            max_batch_size=max_batch,
            downstream_capacity=n_fans + extra_capacity,
        )
    )
    with promoter_env(config) as env:
        _admit_waiting_entries(env.client, env.event_id, n_fans)

        # Promote until every fan has been moved WAITING -> ELIGIBLE.
        for _ in range(n_fans + 5):
            if env.promoter.promote_cycle(env.event_id).promoted_count == 0:
                break

        entries = _scan_entries(env.client)
        assert len(entries) == n_fans

        # Every entry ends ELIGIBLE with a single Batch_Id and a Promotion_Time.
        for e in entries:
            assert _status(e) == EligibilityStatus.ELIGIBLE.value
            assert "S" in e["Batch_Id"] and e["Batch_Id"]["S"]
            assert "N" in e["Promotion_Time"] and e["Promotion_Time"]["N"]
            # Promoted entries are evicted from the sparse WaitingIndex.
            assert "Waiting_Shard" not in e

        # Snapshot each entry's assigned Batch_Id / Promotion_Time.
        def _snapshot() -> dict[str, tuple[str, str]]:
            return {
                _ok(e): (e["Batch_Id"]["S"], e["Promotion_Time"]["N"])
                for e in _scan_entries(env.client)
            }

        before = _snapshot()
        cap_before = _read_capacity(env.client, env.event_id)
        # Eligible_Count reflects exactly the promoted population.
        assert cap_before["eligible_count"] == n_fans

        # Subsequent promotion attempts are no-ops: nothing re-promotes, no
        # entry flips back to WAITING, Batch_Id/Promotion_Time are unchanged,
        # and the capacity counter stays consistent.
        for _ in range(extra_cycles):
            result = env.promoter.promote_cycle(env.event_id)
            assert result.promoted_count == 0

        after = _snapshot()
        assert after == before  # exactly one successful transition per entry
        cap_after = _read_capacity(env.client, env.event_id)
        assert cap_after["eligible_count"] == cap_before["eligible_count"]

        # Direct idempotency at the conditional-write level: re-applying the
        # WAITING -> ELIGIBLE transition to an already-ELIGIBLE entry conflicts
        # (does not commit) and leaves the entry unchanged (Req 5.5, 5.7, 10.3).
        manager = LifecycleManager(client=env.client)
        sample = _scan_entries(env.client)[0]
        pk = sample["PK"]["S"]
        sk = sample["SK"]["S"]
        outcome = manager.apply_transition(
            pk=pk,
            sk=sk,
            from_status=EligibilityStatus.WAITING,
            to_status=EligibilityStatus.ELIGIBLE,
        )
        assert outcome.outcome is TransitionOutcome.CONFLICT
        assert outcome.committed is False
        reread = env.client.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": pk}, "SK": {"S": sk}},
            ConsistentRead=True,
        )["Item"]
        assert _status(reread) == EligibilityStatus.ELIGIBLE.value
        assert reread["Batch_Id"]["S"] == sample["Batch_Id"]["S"]
        assert reread["Promotion_Time"]["N"] == sample["Promotion_Time"]["N"]
