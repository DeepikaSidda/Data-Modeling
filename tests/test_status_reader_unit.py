"""Example/unit tests for the Status_Reader data-access layer (task 14.2).

These are deterministic pytest example tests (not Hypothesis property tests)
exercising :class:`waiting_room.status_reader.Status_Reader` end-to-end against
a ``moto``-backed DynamoDB table. They cover the error and edge cases called
out by the design's *Low-Latency Status Reads* section:

* invalid / tampered Entry_Token -> :class:`StatusAuthError` (Req 8.5),
* an ELIGIBLE-but-expired entry -> ``may_browse=False`` with reason ``EXPIRED``
  (Req 8.7),
* a downstream-unavailable ELIGIBLE entry -> ``may_browse=False`` with reason
  ``DOWNSTREAM_UNAVAILABLE`` (Req 8.7),
* a happy-path WAITING read returning a :class:`StatusResult` carrying a
  ``Cache-Control`` directive and a position taken from injected aggregates.

_Requirements: 8.5, 8.7._
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from waiting_room.config import EligibilityStatus, WaitingRoomConfig
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

SECRET = "unit-test-secret"
EVENT_ID = "evt-14-2"
FAN_ID = "fan-abc"
ORDERING_KEY = "0000000001#0001#fan-abc"
WRITE_SHARD = 3


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
    config: WaitingRoomConfig,
    *,
    status: str,
    promotion_time_ms: int | None = None,
    ordering_key: str = ORDERING_KEY,
    write_shard: int = WRITE_SHARD,
) -> tuple[str, str]:
    """Write a Queue_Entry item at the exact key the reader will derive."""
    shard_str = format_shard(write_shard, config.event.shard_count)
    pk = f"EVT#{EVENT_ID}#SH#{shard_str}"
    sk = f"ENTRY#{ordering_key}"
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


def _reader(client, aggregates: PositionAggregates, *, clock=None, config=None) -> Status_Reader:
    cfg = config or WaitingRoomConfig()
    return Status_Reader(
        client=client,
        secret=SECRET,
        aggregates_provider=lambda claims: aggregates,
        config=cfg,
        clock=clock if clock is not None else (lambda: 0.0),
    )


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_tampered_token_raises_status_auth_error(dynamodb_client):
    """A valid token with a single mutated character is rejected (Req 8.5)."""
    config = WaitingRoomConfig()
    _put_entry(dynamodb_client, config, status=EligibilityStatus.WAITING.value)

    valid = _token()
    # Mutate one character of the signature segment so the MAC no longer matches.
    last = valid[-1]
    replacement = "A" if last != "A" else "B"
    tampered = valid[:-1] + replacement
    assert tampered != valid

    reader = _reader(
        dynamodb_client,
        PositionAggregates(
            admission_sequence_rank=1, promoted_total=0, promotion_rate=1.0
        ),
    )

    with pytest.raises(StatusAuthError):
        reader.read_status(tampered)


def test_eligible_but_expired_reports_expired(dynamodb_client):
    """An ELIGIBLE entry past its window is not browsable, reason EXPIRED (Req 8.7)."""
    config = WaitingRoomConfig()  # eligibility_window_secs == 120
    # Promotion_Time is stored as epoch milliseconds; 1000 ms == 1.0 s.
    _put_entry(
        dynamodb_client,
        config,
        status=EligibilityStatus.ELIGIBLE.value,
        promotion_time_ms=1000,
    )

    # Clock is well past promotion + window (1.0s + 120s), so the window elapsed.
    reader = _reader(
        dynamodb_client,
        PositionAggregates(
            admission_sequence_rank=1,
            promoted_total=0,
            promotion_rate=5.0,
            downstream_available=True,
        ),
        clock=lambda: 1.0 + config.event.eligibility_window_secs + 10.0,
    )

    result = reader.read_status(_token())

    assert isinstance(result, StatusResult)
    assert result.eligibility_status is EligibilityStatus.ELIGIBLE
    assert result.may_browse is False
    assert result.reason == "EXPIRED"


def test_downstream_unavailable_reports_downstream_unavailable(dynamodb_client):
    """ELIGIBLE within window but downstream offline -> DOWNSTREAM_UNAVAILABLE (Req 8.7)."""
    config = WaitingRoomConfig()
    now = 1000.0
    # Promotion at exactly `now` (in ms) keeps elapsed == 0 < window.
    _put_entry(
        dynamodb_client,
        config,
        status=EligibilityStatus.ELIGIBLE.value,
        promotion_time_ms=int(now * 1000),
    )

    reader = _reader(
        dynamodb_client,
        PositionAggregates(
            admission_sequence_rank=10,
            promoted_total=4,
            promotion_rate=2.0,
            downstream_available=False,
        ),
        clock=lambda: now,
    )

    result = reader.read_status(_token())

    assert result.eligibility_status is EligibilityStatus.ELIGIBLE
    assert result.may_browse is False
    assert result.reason == "DOWNSTREAM_UNAVAILABLE"


def test_waiting_happy_path_returns_cacheable_result_with_injected_position(
    dynamodb_client,
):
    """A WAITING read returns a cacheable StatusResult with the aggregate position."""
    config = WaitingRoomConfig()  # staleness_bound_secs == 5
    _put_entry(dynamodb_client, config, status=EligibilityStatus.WAITING.value)

    # approximate_position = max(rank - promoted_total, 1) = max(100 - 40, 1) = 60.
    reader = _reader(
        dynamodb_client,
        PositionAggregates(
            admission_sequence_rank=100,
            promoted_total=40,
            promotion_rate=2.0,
            downstream_available=True,
        ),
        clock=lambda: 0.0,
    )

    result = reader.read_status(_token())

    assert isinstance(result, StatusResult)
    assert result.eligibility_status is EligibilityStatus.WAITING
    assert result.position == 60
    # estimated_wait = position / rho = 60 / 2.0 = 30.0
    assert result.estimated_wait == pytest.approx(30.0)
    # A WAITING fan cannot browse yet.
    assert result.may_browse is False
    assert result.reason == "NOT_ELIGIBLE"
    # Cacheable: max-age bounded by staleness_bound_secs (== 5).
    assert result.cache_control == "max-age=5"
