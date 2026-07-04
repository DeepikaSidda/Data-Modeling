"""Example/unit tests for the DynamoDB data-access layer (tasks 14.2 & 15.2).

Deterministic pytest example tests (not Hypothesis property tests) exercising
the two data-access components end-to-end against a ``moto``-backed DynamoDB
table:

* :class:`waiting_room.status_reader.Status_Reader` (task 14.2) - token
  verification, browse gating, and cacheable status results
  (_Requirements: 8.5, 8.7_).
* :class:`waiting_room.lifecycle_manager.LifecycleManager` (task 15.2) -
  conditional eligibility transitions and corruption reconciliation
  (_Requirements: 10.2, 10.6_).

Everything DynamoDB-facing is driven through the real modules against a mocked
table (no stubs of the code under test), so these tests validate actual
behavior of the frozen data model and the injected boto3 client.
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from waiting_room.config import EligibilityStatus, WaitingRoomConfig
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
from waiting_room.sharding import format_shard
from waiting_room.status_reader import (
    PositionAggregates,
    StatusAuthError,
    StatusResult,
    Status_Reader,
)
from waiting_room.token import sign

# --------------------------------------------------------------------------- #
# Shared constants
# --------------------------------------------------------------------------- #
SECRET = "dal-unit-test-secret"
EVENT_ID = "evt-dal"
FAN_ID = "fan-dal-001"
ORDERING_KEY = "0000000042#0007#fan-dal-001"
WRITE_SHARD = 5


# --------------------------------------------------------------------------- #
# Fixture: a moto-backed table with the event seeded
# --------------------------------------------------------------------------- #
@pytest.fixture()
def dynamodb_client():
    """A moto-backed DynamoDB client with the WaitingRoom table + event seeded."""
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_waiting_room_table(client)
        seed_event(client, EVENT_ID)
        yield client


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _entry_key(config: WaitingRoomConfig, write_shard: int, ordering_key: str) -> tuple[str, str]:
    """Reconstruct the base-table key the Status_Reader / manager will derive."""
    shard_str = format_shard(write_shard, config.event.shard_count)
    pk = f"EVT#{EVENT_ID}#SH#{shard_str}"
    sk = f"ENTRY#{ordering_key}"
    return pk, sk


def _put_entry(
    client,
    config: WaitingRoomConfig,
    *,
    status: str,
    promotion_time_ms: int | None = None,
    ordering_key: str = ORDERING_KEY,
    write_shard: int = WRITE_SHARD,
    include_waiting_shard: bool = False,
) -> tuple[str, str]:
    """Write a Queue_Entry item at the exact derived key. Returns (pk, sk)."""
    pk, sk = _entry_key(config, write_shard, ordering_key)
    item = {
        "PK": {"S": pk},
        "SK": {"S": sk},
        "Event_Id": {"S": EVENT_ID},
        "Fan_Id": {"S": FAN_ID},
        "Ordering_Key": {"S": ordering_key},
        "Eligibility_Status": {"S": status},
    }
    if promotion_time_ms is not None:
        item["Promotion_Time"] = {"N": str(promotion_time_ms)}
    if include_waiting_shard:
        item[WAITING_SHARD_ATTR] = {"S": pk}
    client.put_item(TableName=TABLE_NAME, Item=item)
    return pk, sk


def _token(write_shard: int = WRITE_SHARD, ordering_key: str = ORDERING_KEY) -> str:
    return sign(
        {
            "Fan_Id": FAN_ID,
            "Event_Id": EVENT_ID,
            "Ordering_Key": ordering_key,
            "Write_Shard": write_shard,
        },
        SECRET,
    )


def _aggregates(**overrides) -> PositionAggregates:
    """A simple aggregates provider payload with sensible defaults."""
    base = dict(
        admission_sequence_rank=1,
        promoted_total=0,
        promotion_rate=1.0,
        downstream_available=True,
    )
    base.update(overrides)
    return PositionAggregates(**base)


def _reader(client, aggregates: PositionAggregates, *, clock=None, config=None) -> Status_Reader:
    cfg = config or WaitingRoomConfig()
    return Status_Reader(
        client=client,
        secret=SECRET,
        aggregates_provider=lambda claims: aggregates,
        config=cfg,
        clock=clock if clock is not None else (lambda: 0.0),
    )


def _read_item(client, pk: str, sk: str) -> dict:
    return client.get_item(
        TableName=TABLE_NAME,
        Key={"PK": {"S": pk}, "SK": {"S": sk}},
        ConsistentRead=True,
    ).get("Item", {})


# =========================================================================== #
# Task 14.2 - Status_Reader error and edge cases (Req 8.5, 8.7)
# =========================================================================== #
def test_valid_token_read_status_returns_expected_fields(dynamodb_client):
    """A valid token yields a StatusResult with the expected, aggregate-derived fields."""
    config = WaitingRoomConfig()  # staleness_bound_secs == 5
    _put_entry(dynamodb_client, config, status=EligibilityStatus.WAITING.value)

    # approximate_position = max(rank - promoted_total, 1) = max(90 - 30, 1) = 60.
    reader = _reader(
        dynamodb_client,
        _aggregates(admission_sequence_rank=90, promoted_total=30, promotion_rate=3.0),
    )

    result = reader.read_status(_token())

    assert isinstance(result, StatusResult)
    assert result.eligibility_status is EligibilityStatus.WAITING
    assert result.position == 60
    # estimated_wait = position / rho = 60 / 3.0 = 20.0.
    assert result.estimated_wait == pytest.approx(20.0)
    # A WAITING fan cannot browse yet; reason is NOT_ELIGIBLE.
    assert result.may_browse is False
    assert result.reason == "NOT_ELIGIBLE"
    # Cacheable: max-age bounded by the configured staleness bound.
    assert result.cache_control == "max-age=5"


def test_tampered_token_raises_status_auth_error(dynamodb_client):
    """A single-character mutation of a valid token is rejected (Req 8.5)."""
    config = WaitingRoomConfig()
    _put_entry(dynamodb_client, config, status=EligibilityStatus.WAITING.value)

    valid = _token()
    last = valid[-1]
    tampered = valid[:-1] + ("A" if last != "A" else "B")
    assert tampered != valid

    reader = _reader(dynamodb_client, _aggregates())

    with pytest.raises(StatusAuthError):
        reader.read_status(tampered)


def test_garbage_token_raises_status_auth_error(dynamodb_client):
    """A structurally invalid token is rejected before any read (Req 8.5)."""
    config = WaitingRoomConfig()
    _put_entry(dynamodb_client, config, status=EligibilityStatus.WAITING.value)

    reader = _reader(dynamodb_client, _aggregates())

    with pytest.raises(StatusAuthError):
        reader.read_status("not-a-real-token")


def test_eligible_but_expired_reports_expired(dynamodb_client):
    """An ELIGIBLE entry past its eligibility window -> may_browse False, EXPIRED (Req 8.7)."""
    config = WaitingRoomConfig()  # eligibility_window_secs == 120
    # Promotion_Time is epoch milliseconds; 1000 ms == 1.0 s in the clock's base.
    _put_entry(
        dynamodb_client,
        config,
        status=EligibilityStatus.ELIGIBLE.value,
        promotion_time_ms=1000,
    )

    # Clock is far past promotion + window, so the window has elapsed.
    reader = _reader(
        dynamodb_client,
        _aggregates(admission_sequence_rank=1, promoted_total=0, promotion_rate=5.0),
        clock=lambda: 1.0 + config.event.eligibility_window_secs + 10.0,
    )

    result = reader.read_status(_token())

    assert isinstance(result, StatusResult)
    assert result.eligibility_status is EligibilityStatus.ELIGIBLE
    assert result.may_browse is False
    assert result.reason == "EXPIRED"


def test_downstream_unavailable_reports_downstream_unavailable(dynamodb_client):
    """An in-window ELIGIBLE entry with downstream offline -> DOWNSTREAM_UNAVAILABLE (Req 8.7)."""
    config = WaitingRoomConfig()
    now = 500.0
    # Promotion at exactly `now` (in ms) keeps elapsed == 0 < window.
    _put_entry(
        dynamodb_client,
        config,
        status=EligibilityStatus.ELIGIBLE.value,
        promotion_time_ms=int(now * 1000),
    )

    reader = _reader(
        dynamodb_client,
        _aggregates(
            admission_sequence_rank=8,
            promoted_total=3,
            promotion_rate=2.0,
            downstream_available=False,
        ),
        clock=lambda: now,
    )

    result = reader.read_status(_token())

    assert result.eligibility_status is EligibilityStatus.ELIGIBLE
    assert result.may_browse is False
    assert result.reason == "DOWNSTREAM_UNAVAILABLE"


# =========================================================================== #
# Task 15.2 - Lifecycle manager conditional transitions & reconciliation
# (Req 10.2, 10.6)
# =========================================================================== #
def test_disallowed_transition_raises_and_leaves_item_unchanged(dynamodb_client):
    """WAITING -> COMPLETED is illegal: it raises and never writes (Req 10.2)."""
    config = WaitingRoomConfig()
    pk, sk = _put_entry(
        dynamodb_client,
        config,
        status=EligibilityStatus.WAITING.value,
        include_waiting_shard=True,
    )
    before = _read_item(dynamodb_client, pk, sk)

    manager = LifecycleManager(client=dynamodb_client)

    with pytest.raises(IllegalTransitionError):
        manager.apply_transition(
            pk,
            sk,
            EligibilityStatus.WAITING,
            EligibilityStatus.COMPLETED,
        )

    after = _read_item(dynamodb_client, pk, sk)
    # The item is byte-for-byte unchanged: no UpdateItem was issued.
    assert after == before
    assert after[STATUS_ATTR]["S"] == EligibilityStatus.WAITING.value


def test_valid_transition_commits_and_removes_waiting_shard(dynamodb_client):
    """WAITING -> ELIGIBLE commits durably and evicts Waiting_Shard from the index."""
    config = WaitingRoomConfig()
    pk, sk = _put_entry(
        dynamodb_client,
        config,
        status=EligibilityStatus.WAITING.value,
        include_waiting_shard=True,
    )
    assert WAITING_SHARD_ATTR in _read_item(dynamodb_client, pk, sk)

    manager = LifecycleManager(client=dynamodb_client)
    result = manager.apply_transition(
        pk,
        sk,
        EligibilityStatus.WAITING,
        EligibilityStatus.ELIGIBLE,
        extra_remove=[WAITING_SHARD_ATTR],
    )

    assert isinstance(result, TransitionResult)
    assert result.outcome is TransitionOutcome.COMMITTED
    assert result.committed is True
    assert result.current_status is EligibilityStatus.ELIGIBLE

    after = _read_item(dynamodb_client, pk, sk)
    assert after[STATUS_ATTR]["S"] == EligibilityStatus.ELIGIBLE.value
    # Evicted from the sparse WaitingIndex -> no longer selectable for promotion.
    assert WAITING_SHARD_ATTR not in after


def test_stale_from_status_yields_conflict_and_invokes_rollback(dynamodb_client):
    """Expecting WAITING when the item is already ELIGIBLE -> CONFLICT + rollback (Req 10.5)."""
    config = WaitingRoomConfig()
    # The item is actually ELIGIBLE, but the caller expects it to still be WAITING.
    pk, sk = _put_entry(dynamodb_client, config, status=EligibilityStatus.ELIGIBLE.value)

    manager = LifecycleManager(client=dynamodb_client)

    rollback_calls = {"count": 0}

    def rollback() -> None:
        rollback_calls["count"] += 1

    # WAITING -> ELIGIBLE is a permitted edge (passes lifecycle validation), so
    # the failure comes purely from the conditional check on the stale
    # expected-from status.
    result = manager.apply_transition(
        pk,
        sk,
        EligibilityStatus.WAITING,
        EligibilityStatus.ELIGIBLE,
        rollback=rollback,
    )

    assert isinstance(result, TransitionResult)
    assert result.outcome is TransitionOutcome.CONFLICT
    assert result.committed is False
    assert result.rolled_back is True
    assert rollback_calls["count"] == 1
    # Authoritative status was re-read: it is still ELIGIBLE (unchanged).
    assert result.current_status is EligibilityStatus.ELIGIBLE

    after = _read_item(dynamodb_client, pk, sk)
    assert after[STATUS_ATTR]["S"] == EligibilityStatus.ELIGIBLE.value


def test_reconcile_if_corrupt_flags_and_evicts_from_waiting_index(dynamodb_client):
    """A corrupt persisted status is flagged and removed from promotion (Req 10.6)."""
    config = WaitingRoomConfig()
    pk, sk = _put_entry(
        dynamodb_client,
        config,
        status="CORRUPT_VALUE",
        include_waiting_shard=True,
    )
    # Precondition: the entry currently carries Waiting_Shard (selectable).
    assert WAITING_SHARD_ATTR in _read_item(dynamodb_client, pk, sk)

    manager = LifecycleManager(client=dynamodb_client)
    result = manager.reconcile_if_corrupt(pk, sk)

    assert isinstance(result, ReconciliationResult)
    assert result.flagged is True
    assert result.status is None
    assert result.raw_status == "CORRUPT_VALUE"

    after = _read_item(dynamodb_client, pk, sk)
    # needs_reconciliation set.
    assert after[RECONCILE_ATTR]["BOOL"] is True
    # Evicted from the sparse WaitingIndex -> excluded from promotion.
    assert WAITING_SHARD_ATTR not in after
