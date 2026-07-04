"""Example/unit tests for the lifecycle manager (task 15.2).

Deterministic pytest example tests (not Hypothesis property tests) exercising
:class:`waiting_room.lifecycle_manager.LifecycleManager` end-to-end against a
``moto``-backed DynamoDB table. They cover the design's *Eligibility Lifecycle
Integrity* behaviors:

* a disallowed transition (``WAITING -> ACTIVE``) raises
  :class:`IllegalTransitionError` and leaves the item unchanged - no write is
  ever issued (Req 10.2),
* a stale expected-from (item is ``WAITING`` but the caller passes
  ``from_status=ELIGIBLE``) fails the conditional check, yielding
  :attr:`TransitionOutcome.CONFLICT`, invoking the rollback callback, and
  re-reading the authoritative status,
* a corrupt persisted status is flagged for reconciliation and evicted from the
  sparse ``WaitingIndex`` (``Waiting_Shard`` removed) so it can never be
  promoted (Req 10.6),
* a valid conditional transition commits (happy path).

_Requirements: 10.2, 10.6._
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from waiting_room.config import EligibilityStatus
from waiting_room.lifecycle import IllegalTransitionError
from waiting_room.lifecycle_manager import (
    RECONCILE_ATTR,
    STATUS_ATTR,
    WAITING_SHARD_ATTR,
    LifecycleManager,
    ReconciliationResult,
    TransitionOutcome,
    TransitionResult,
)
from waiting_room.provisioning import (
    TABLE_NAME,
    create_waiting_room_table,
    seed_event,
)

EVENT_ID = "evt-15-2"
PK = f"EVT#{EVENT_ID}#SH#003"
SK = "ENTRY#0000000001#0001#fan-abc"
WAITING_SHARD_VALUE = f"EVT#{EVENT_ID}#SH#003"
ORDERING_KEY = "0000000001#0001#fan-abc"


# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #
@pytest.fixture()
def dynamodb_client():
    """A moto-backed DynamoDB client with the WaitingRoom table + event seeded."""
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_waiting_room_table(client)
        seed_event(client, EVENT_ID)
        yield client


def _put_entry(
    client,
    *,
    status: str,
    include_waiting_shard: bool = True,
) -> None:
    """Write a Queue_Entry item, optionally carrying the sparse Waiting_Shard."""
    item = {
        "PK": {"S": PK},
        "SK": {"S": SK},
        "Event_Id": {"S": EVENT_ID},
        "Fan_Id": {"S": "fan-abc"},
        "Ordering_Key": {"S": ORDERING_KEY},
        "Eligibility_Status": {"S": status},
    }
    if include_waiting_shard:
        item[WAITING_SHARD_ATTR] = {"S": WAITING_SHARD_VALUE}
    client.put_item(TableName=TABLE_NAME, Item=item)


def _read_item(client) -> dict:
    return client.get_item(
        TableName=TABLE_NAME,
        Key={"PK": {"S": PK}, "SK": {"S": SK}},
        ConsistentRead=True,
    ).get("Item", {})


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_disallowed_transition_raises_and_leaves_item_unchanged(dynamodb_client):
    """WAITING -> ACTIVE is illegal: it raises and never writes (Req 10.2)."""
    _put_entry(dynamodb_client, status=EligibilityStatus.WAITING.value)
    before = _read_item(dynamodb_client)

    manager = LifecycleManager(client=dynamodb_client)

    with pytest.raises(IllegalTransitionError):
        manager.apply_transition(
            PK,
            SK,
            EligibilityStatus.WAITING,
            EligibilityStatus.ACTIVE,
        )

    after = _read_item(dynamodb_client)
    # The item must be byte-for-byte unchanged (no write occurred).
    assert after == before
    assert after[STATUS_ATTR]["S"] == EligibilityStatus.WAITING.value


def test_stale_expected_from_yields_conflict_and_rolls_back(dynamodb_client):
    """A mismatched expected-from fails the conditional write -> CONFLICT (Req 10.5)."""
    # Item is actually WAITING, but the caller expects ELIGIBLE.
    _put_entry(dynamodb_client, status=EligibilityStatus.WAITING.value)

    manager = LifecycleManager(client=dynamodb_client)

    rollback_calls = {"count": 0}

    def rollback() -> None:
        rollback_calls["count"] += 1

    # ELIGIBLE -> ACTIVE is a *permitted* edge (passes lifecycle validation),
    # so the failure comes purely from the conditional check on the stale
    # expected-from status.
    result = manager.apply_transition(
        PK,
        SK,
        EligibilityStatus.ELIGIBLE,
        EligibilityStatus.ACTIVE,
        rollback=rollback,
    )

    assert isinstance(result, TransitionResult)
    assert result.outcome is TransitionOutcome.CONFLICT
    assert result.committed is False
    assert result.rolled_back is True
    assert rollback_calls["count"] == 1
    # Authoritative status was re-read: it is still WAITING.
    assert result.current_status is EligibilityStatus.WAITING

    # The stored status is untouched by the failed transition.
    after = _read_item(dynamodb_client)
    assert after[STATUS_ATTR]["S"] == EligibilityStatus.WAITING.value


def test_reconcile_if_corrupt_flags_and_evicts_from_waiting_index(dynamodb_client):
    """A corrupt persisted status is flagged and removed from promotion (Req 10.6)."""
    _put_entry(dynamodb_client, status="BOGUS_STATUS", include_waiting_shard=True)

    # Precondition: the entry is currently selectable for promotion.
    before = _read_item(dynamodb_client)
    assert WAITING_SHARD_ATTR in before

    manager = LifecycleManager(client=dynamodb_client)
    result = manager.reconcile_if_corrupt(PK, SK)

    assert isinstance(result, ReconciliationResult)
    assert result.flagged is True
    assert result.status is None
    assert result.raw_status == "BOGUS_STATUS"

    after = _read_item(dynamodb_client)
    # Flagged for reconciliation.
    assert after[RECONCILE_ATTR]["BOOL"] is True
    # Evicted from the sparse WaitingIndex -> excluded from promotion.
    assert WAITING_SHARD_ATTR not in after


def test_valid_conditional_transition_commits(dynamodb_client):
    """A matching expected-from applies the transition durably (COMMITTED)."""
    _put_entry(dynamodb_client, status=EligibilityStatus.WAITING.value)

    manager = LifecycleManager(client=dynamodb_client)
    result = manager.apply_transition(
        PK,
        SK,
        EligibilityStatus.WAITING,
        EligibilityStatus.ELIGIBLE,
    )

    assert result.outcome is TransitionOutcome.COMMITTED
    assert result.committed is True
    assert result.current_status is EligibilityStatus.ELIGIBLE

    after = _read_item(dynamodb_client)
    assert after[STATUS_ATTR]["S"] == EligibilityStatus.ELIGIBLE.value
