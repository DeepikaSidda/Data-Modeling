"""Integration tests for the Virtual Waiting Room against a moto-backed DynamoDB.

These tests exercise the DynamoDB data-access layer end-to-end
(:mod:`waiting_room.provisioning`, :mod:`waiting_room.admission`,
:mod:`waiting_room.promoter`, :mod:`waiting_room.status_reader`) against an
in-process ``moto`` ``mock_aws`` DynamoDB. They validate the four hard design
guarantees that only surface once real DynamoDB conditional writes, sparse GSI
eviction, and encoded-status GSI partitions are in play:

* **18.1** exactly-once admission atomicity (Req 1.1, 1.3, 2.5, 9.1)
* **18.2** sparse ``WaitingIndex`` eviction on promotion (Req 5.1, 5.6)
* **18.3** ``EligibilityIndex`` status-by-status queries (Req 2.4, 5.8, 6.3)
* **18.4** the status path never issues a ``Scan`` (Req 8.2)

A small ``shard_count`` (8) is used so the promoter's per-shard k-way merge and
the exact-position fallback stay fast under moto while still spreading entries
across multiple shard partitions.
"""

from __future__ import annotations

from typing import Any

import boto3
import pytest
from moto import mock_aws

from waiting_room.admission import (
    ADMISSION_SK,
    ADMIT_COUNT_SK,
    ENTRY_SK_PREFIX,
    Admission_Writer,
)
from waiting_room.config import EligibilityStatus, EventConfig, WaitingRoomConfig
from waiting_room.promoter import BatchPromoter, elig_pk, waiting_shard
from waiting_room.provisioning import (
    ELIGIBILITY_INDEX_NAME,
    TABLE_NAME,
    WAITING_INDEX_NAME,
    create_waiting_room_table,
    event_pk,
    make_dynamodb_client,
    seed_event,
)
from waiting_room.sharding import assign_shard
from waiting_room.status_reader import PositionAggregates, Status_Reader

# --------------------------------------------------------------------------- #
# Constants / helpers
# --------------------------------------------------------------------------- #
EVENT_ID = "e-integration"
SECRET = "integration-test-secret"
SHARD_COUNT = 8


def _test_config(
    *,
    downstream_capacity: int = 100,
    max_batch_size: int = 4,
    eligibility_window_secs: int = 120,
) -> WaitingRoomConfig:
    """A small, fast :class:`WaitingRoomConfig` for integration runs."""
    return WaitingRoomConfig(
        event=EventConfig(
            shard_count=SHARD_COUNT,
            downstream_capacity=downstream_capacity,
            max_batch_size=max_batch_size,
            eligibility_window_secs=eligibility_window_secs,
            max_queue_size=1_000_000,
        )
    )


@pytest.fixture()
def ddb():
    """Yield a moto-backed DynamoDB client with the WaitingRoom table created."""
    with mock_aws():
        client = make_dynamodb_client(region_name="us-east-1")
        create_waiting_room_table(client)
        yield client


def _entry_count_for_fan(client: Any, event_id: str, fan_id: str) -> int:
    """Count queue-entry items for a fan by querying its deterministic shard.

    The shard is a pure function of ``fan_id`` (:func:`assign_shard`), so we can
    address the exact base-table partition and count ``ENTRY#`` items without a
    table scan.
    """
    _, shard_str = assign_shard(fan_id, SHARD_COUNT)
    pk = f"EVT#{event_id}#SH#{shard_str}"
    resp = client.query(
        TableName=TABLE_NAME,
        KeyConditionExpression="#pk = :pk AND begins_with(#sk, :entry)",
        ExpressionAttributeNames={"#pk": "PK", "#sk": "SK"},
        ExpressionAttributeValues={
            ":pk": {"S": pk},
            ":entry": {"S": ENTRY_SK_PREFIX},
        },
    )
    return [
        item for item in resp.get("Items", []) if item["Fan_Id"]["S"] == fan_id
    ].__len__()


def _guard_item(client: Any, event_id: str, fan_id: str) -> dict | None:
    """Return the fan dedupe guard item (or ``None``)."""
    resp = client.get_item(
        TableName=TABLE_NAME,
        Key={
            "PK": {"S": f"EVT#{event_id}#FAN#{fan_id}"},
            "SK": {"S": ADMISSION_SK},
        },
        ConsistentRead=True,
    )
    return resp.get("Item")


def _waiting_index_fans(client: Any, event_id: str, shard_count: int) -> set[str]:
    """Collect every ``Fan_Id`` currently visible in the sparse ``WaitingIndex``.

    Queries each shard partition of the GSI. An entry appears here only while
    its ``Waiting_Shard`` attribute is present (i.e. while ``WAITING``); promoted
    entries have it removed and so are evicted.
    """
    fans: set[str] = set()
    for shard in range(shard_count):
        ws = waiting_shard(event_id, f"{shard:0{len(str(shard_count - 1))}d}")
        resp = client.query(
            TableName=TABLE_NAME,
            IndexName=WAITING_INDEX_NAME,
            KeyConditionExpression="#ws = :ws",
            ExpressionAttributeNames={"#ws": "Waiting_Shard"},
            ExpressionAttributeValues={":ws": {"S": ws}},
        )
        for item in resp.get("Items", []):
            fans.add(item["Fan_Id"]["S"])
    return fans


def _eligibility_index_fans(
    client: Any, event_id: str, status: EligibilityStatus
) -> set[str]:
    """Collect every ``Fan_Id`` in the ``EligibilityIndex`` for a given status."""
    fans: set[str] = set()
    start_key: dict | None = None
    while True:
        kwargs: dict[str, Any] = {
            "TableName": TABLE_NAME,
            "IndexName": ELIGIBILITY_INDEX_NAME,
            "KeyConditionExpression": "#ep = :ep",
            "ExpressionAttributeNames": {"#ep": "Elig_PK"},
            "ExpressionAttributeValues": {":ep": {"S": elig_pk(event_id, status)}},
        }
        if start_key is not None:
            kwargs["ExclusiveStartKey"] = start_key
        resp = client.query(**kwargs)
        for item in resp.get("Items", []):
            fans.add(item["Fan_Id"]["S"])
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            return fans


def _admit_fans(writer: Admission_Writer, event_id: str, n: int) -> list:
    """Admit ``n`` distinct fans sequentially, returning their AdmissionResults."""
    return [writer.admit(event_id, f"fan-{i:03d}") for i in range(n)]


# --------------------------------------------------------------------------- #
# 18.1 - exactly-once admission atomicity
# --------------------------------------------------------------------------- #
def test_18_1_exactly_once_admission_atomicity(ddb) -> None:
    """Admitting the same fan repeatedly yields exactly one entry + one guard.

    Every call returns the same ``Ordering_Key`` (idempotent duplicate path),
    only one queue entry and one dedupe guard exist, and once the guard is in
    place the atomic ``TransactWriteItems`` never creates a second entry.

    Validates: Requirements 1.1, 1.3, 2.5, 9.1
    """
    config = _test_config()
    seed_event(ddb, EVENT_ID, config.event)
    writer = Admission_Writer(client=ddb, secret=SECRET, config=config)

    fan_id = "repeat-fan"
    results = [writer.admit(EVENT_ID, fan_id) for _ in range(25)]

    # Every call returns the identical server-assigned Ordering_Key (Req 1.3).
    ordering_keys = {r.ordering_key for r in results}
    assert len(ordering_keys) == 1, f"expected one Ordering_Key, got {ordering_keys}"

    # The first admission created the entry; every subsequent one is an
    # idempotent duplicate (Req 1.3) - never a second write.
    assert results[0].duplicate is False
    assert all(r.duplicate for r in results[1:])

    # Exactly one queue entry and one guard exist for the fan (Req 1.1, 9.1).
    assert _entry_count_for_fan(ddb, EVENT_ID, fan_id) == 1
    guard = _guard_item(ddb, EVENT_ID, fan_id)
    assert guard is not None
    assert guard["Ordering_Key"]["S"] == results[0].ordering_key

    # The whole transaction aborts when the guard already exists: the admit
    # counter reflects a single committed admission, not 25 (the increment runs
    # only on the initial, non-duplicate commit) - proof no second entry write
    # slipped through (Req 2.5).
    _, shard_str = assign_shard(fan_id, SHARD_COUNT)
    counter = ddb.get_item(
        TableName=TABLE_NAME,
        Key={
            "PK": {"S": f"EVT#{EVENT_ID}#SH#{shard_str}"},
            "SK": {"S": ADMIT_COUNT_SK},
        },
        ConsistentRead=True,
    ).get("Item")
    assert counter is not None
    assert int(counter["Admitted_Count"]["N"]) == 1


def test_18_1_existing_guard_blocks_second_entry(ddb) -> None:
    """A pre-existing guard makes the transactional entry write a no-op.

    Directly seeding the dedupe guard (as if a prior admission committed it)
    forces the guard's ``attribute_not_exists`` condition to fail, so the atomic
    ``TransactWriteItems`` aborts and no queue entry is created; admission
    returns the existing entry idempotently.

    Validates: Requirements 1.1, 1.3, 2.5
    """
    config = _test_config()
    seed_event(ddb, EVENT_ID, config.event)
    writer = Admission_Writer(client=ddb, secret=SECRET, config=config)

    fan_id = "pre-guarded-fan"
    # Seed the guard as though a previous admission already committed it.
    existing_ok = "0000000000000000001#deadbeef"
    _, shard_str = assign_shard(fan_id, SHARD_COUNT)
    ddb.put_item(
        TableName=TABLE_NAME,
        Item={
            "PK": {"S": f"EVT#{EVENT_ID}#FAN#{fan_id}"},
            "SK": {"S": ADMISSION_SK},
            "Ordering_Key": {"S": existing_ok},
            "Write_Shard": {"S": shard_str},
            "Fan_Id": {"S": fan_id},
            "Event_Id": {"S": EVENT_ID},
        },
    )

    result = writer.admit(EVENT_ID, fan_id)

    # Idempotent: returns the pre-existing entry, creates no second entry.
    assert result.duplicate is True
    assert result.ordering_key == existing_ok
    assert _entry_count_for_fan(ddb, EVENT_ID, fan_id) == 0


# --------------------------------------------------------------------------- #
# 18.2 - sparse WaitingIndex eviction on promotion
# --------------------------------------------------------------------------- #
def test_18_2_waiting_index_eviction_on_promotion(ddb) -> None:
    """Promoted entries drop out of the sparse WaitingIndex; WAITING ones remain.

    All admitted fans appear in the ``WaitingIndex``. After a promotion cycle,
    the promoted (now ``ELIGIBLE``) entries have ``Waiting_Shard`` removed and so
    are evicted from the index, while the still-``WAITING`` entries continue to
    appear.

    Validates: Requirements 5.1, 5.6
    """
    config = _test_config(downstream_capacity=100, max_batch_size=4)
    seed_event(ddb, EVENT_ID, config.event)
    writer = Admission_Writer(client=ddb, secret=SECRET, config=config)

    results = _admit_fans(writer, EVENT_ID, 10)

    # Before promotion, every fan is visible in the sparse WaitingIndex.
    all_fans = {r.fan_id for r in results}
    assert _waiting_index_fans(ddb, EVENT_ID, SHARD_COUNT) == all_fans

    # Promotion selects the globally-earliest entries by Ordering_Key. Since
    # admissions are sequential, that is the first `max_batch_size` admitted.
    by_order = sorted(results, key=lambda r: r.ordering_key)
    promoted_expected = {r.fan_id for r in by_order[:4]}
    still_waiting_expected = {r.fan_id for r in by_order[4:]}

    promoter = BatchPromoter(client=ddb)
    outcome = promoter.promote_cycle(EVENT_ID)
    assert outcome.promoted_count == 4

    waiting_after = _waiting_index_fans(ddb, EVENT_ID, SHARD_COUNT)

    # Promoted entries are evicted (Waiting_Shard removed) - Req 5.6.
    assert waiting_after.isdisjoint(promoted_expected)
    # Remaining WAITING entries still appear - Req 5.1.
    assert waiting_after == still_waiting_expected


# --------------------------------------------------------------------------- #
# 18.3 - EligibilityIndex status queries
# --------------------------------------------------------------------------- #
def test_18_3_eligibility_index_status_queries(ddb) -> None:
    """Querying EligibilityIndex by status returns only entries of that status.

    After promotion the ``ELIGIBLE`` query returns exactly the promoted fans and
    none of the still-``WAITING`` ones. Expiring one entry moves it to the
    ``EXPIRED`` partition, so the ``ELIGIBLE`` query no longer returns it.

    Validates: Requirements 2.4, 5.8, 6.3
    """
    config = _test_config(downstream_capacity=100, max_batch_size=4)
    seed_event(ddb, EVENT_ID, config.event)
    writer = Admission_Writer(client=ddb, secret=SECRET, config=config)

    results = _admit_fans(writer, EVENT_ID, 10)
    by_order = sorted(results, key=lambda r: r.ordering_key)
    promoted_expected = {r.fan_id for r in by_order[:4]}
    waiting_expected = {r.fan_id for r in by_order[4:]}

    # Promote at a fixed logical time so we can later advance the clock to
    # trigger expiry deterministically.
    clock = {"now": 1_000.0}
    promoter = BatchPromoter(client=ddb, clock=lambda: clock["now"])
    promoter.promote_cycle(EVENT_ID)

    # The ELIGIBLE query returns exactly the promoted entries - not the WAITING
    # ones (status is encoded in the GSI partition key, Req 2.4, 6.3).
    eligible = _eligibility_index_fans(ddb, EVENT_ID, EligibilityStatus.ELIGIBLE)
    assert eligible == promoted_expected
    assert eligible.isdisjoint(waiting_expected)

    # Expire exactly one ELIGIBLE entry: advance the clock past a zero-length
    # window and sweep with limit=1 (Req 5.8).
    clock["now"] = 2_000.0
    expire_outcome = promoter.expire_sweep(
        EVENT_ID, eligibility_window_secs=0, limit=1
    )
    assert expire_outcome.expired_count == 1

    eligible_after = _eligibility_index_fans(
        ddb, EVENT_ID, EligibilityStatus.ELIGIBLE
    )
    # The ELIGIBLE query no longer returns the expired entry.
    assert len(eligible_after) == len(promoted_expected) - 1
    assert eligible_after.issubset(promoted_expected)
    expired_fan = promoted_expected - eligible_after
    assert len(expired_fan) == 1

    # The expired entry now shows up under the EXPIRED partition instead.
    expired = _eligibility_index_fans(ddb, EVENT_ID, EligibilityStatus.EXPIRED)
    assert expired == expired_fan


# --------------------------------------------------------------------------- #
# 18.4 - status path never issues a Scan
# --------------------------------------------------------------------------- #
def test_18_4_status_path_never_scans(ddb, monkeypatch) -> None:
    """Status_Reader answers a query using only GetItem/Query - never Scan.

    Any ``scan`` call on the client is monkeypatched to fail the test. The
    Status_Reader then serves both the cached hot path and the exact-count
    fallback without triggering it, proving the design's "no Scan" guarantee.

    Validates: Requirements 8.2
    """
    config = _test_config()
    seed_event(ddb, EVENT_ID, config.event)
    writer = Admission_Writer(client=ddb, secret=SECRET, config=config)
    admit = writer.admit(EVENT_ID, "status-fan")

    # Trip-wire: any Scan issued on the status path fails the test loudly.
    def _no_scan(*args, **kwargs):  # pragma: no cover - only runs on failure
        raise AssertionError("Status_Reader must not issue a Scan")

    monkeypatch.setattr(ddb, "scan", _no_scan)

    def aggregates(claims) -> PositionAggregates:
        return PositionAggregates(
            admission_sequence_rank=1,
            promoted_total=0,
            promotion_rate=2.0,
            downstream_available=True,
        )

    reader = Status_Reader(
        client=ddb,
        secret=SECRET,
        aggregates_provider=aggregates,
        config=config,
    )

    # Hot path: single GetItem + cached aggregates, no Scan.
    hot = reader.read_status(admit.entry_token)
    assert hot.eligibility_status is EligibilityStatus.WAITING
    assert hot.position == 1
    assert hot.cache_control.startswith("max-age=")

    # Exact fallback: bounded Query per shard against the sparse WaitingIndex,
    # still no Scan.
    exact = reader.read_status(admit.entry_token, exact=True)
    assert exact.position == 1
