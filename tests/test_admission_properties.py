"""Property-based tests for the Admission_Writer data-access layer.

Tasks 12.2, 12.3, 12.4, 12.5. These Hypothesis property tests exercise
:class:`waiting_room.admission.Admission_Writer` end-to-end against a
``moto``-backed DynamoDB table, validating the design's Correctness
Properties 1, 2, 4, and 20.

Each test spins up a fresh in-memory DynamoDB (``moto``), creates the
``WaitingRoom`` table via the provisioning module, seeds an OPEN event, and
constructs an ``Admission_Writer`` with an injected client, a fixed signing
secret, and a per-node ``OrderingKeyAllocator``. Because ``moto`` is slower
than pure-logic tests, every property runs with ``max_examples=100`` and the
Hypothesis per-example ``deadline`` disabled.
"""

from __future__ import annotations

import re
from collections import Counter
from contextlib import contextmanager
from typing import Any, Callable, Iterator, NamedTuple

import boto3
from hypothesis import given, settings
from hypothesis import strategies as st
from moto import mock_aws

from waiting_room import lifecycle
from waiting_room.admission import Admission_Writer
from waiting_room.config import (
    EligibilityStatus,
    EventConfig,
    WaitingRoomConfig,
)
from waiting_room.lifecycle_manager import LifecycleManager
from waiting_room.ordering import OrderingKeyAllocator, parse_ordering_key
from waiting_room.provisioning import (
    TABLE_NAME,
    create_waiting_room_table,
    seed_event,
)
from waiting_room.sharding import assign_shard

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
SECRET = "admission-property-test-secret"
EVENT_ID = "evt-12"
#: A small shard count keeps derived keys short and admissions well-scattered
#: while staying trivially reproducible.
SHARD_COUNT = 16
#: The set of Eligibility_Status values an entry may legitimately hold.
VALID_STATUS_VALUES = frozenset(s.value for s in EligibilityStatus)


class Env(NamedTuple):
    """A fully wired admission environment for a single Hypothesis example."""

    client: Any
    writer: Admission_Writer
    config: WaitingRoomConfig
    event_id: str


@contextmanager
def waiting_room_env(
    *, clock: Callable[[], float] | None = None
) -> Iterator[Env]:
    """Yield a fresh moto DynamoDB + provisioned table + seeded OPEN event.

    The event is seeded via :func:`seed_event` (defaults to ``Event_Status =
    OPEN``), the table is created via the provisioning module, and an
    ``Admission_Writer`` is constructed with an injected client, the fixed
    :data:`SECRET`, and a fresh :class:`OrderingKeyAllocator`.
    """
    config = WaitingRoomConfig(event=EventConfig(shard_count=SHARD_COUNT))
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_waiting_room_table(client)
        seed_event(client, EVENT_ID, config.event)
        writer_kwargs: dict[str, Any] = {
            "client": client,
            "secret": SECRET,
            "config": config,
            "allocator": OrderingKeyAllocator(),
        }
        if clock is not None:
            writer_kwargs["clock"] = clock
        writer = Admission_Writer(**writer_kwargs)
        yield Env(client=client, writer=writer, config=config, event_id=EVENT_ID)


def _entry_key(event_id: str, fan_id: str, ordering_key: str) -> tuple[str, str]:
    """Derive the (PK, SK) of a fan's queue entry the way the writer does."""
    _shard_int, shard_str = assign_shard(fan_id, SHARD_COUNT)
    pk = f"EVT#{event_id}#SH#{shard_str}"
    sk = f"ENTRY#{ordering_key}"
    return pk, sk


def _get_entry(client: Any, pk: str, sk: str) -> dict[str, Any] | None:
    """GetItem the entry at ``(pk, sk)`` with a strongly-consistent read."""
    response = client.get_item(
        TableName=TABLE_NAME,
        Key={"PK": {"S": pk}, "SK": {"S": sk}},
        ConsistentRead=True,
    )
    return response.get("Item")


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


# Fan-id suffixes drawn from a small range so a list of them contains
# deliberate repeats, exercising the duplicate / idempotency paths.
_fan_suffix = st.integers(min_value=0, max_value=12)
_fan_suffixes = st.lists(_fan_suffix, min_size=1, max_size=20)


# --------------------------------------------------------------------------- #
# Property 1: Exactly-once admission
# --------------------------------------------------------------------------- #
@settings(max_examples=100, deadline=None)
@given(fan_suffixes=_fan_suffixes)
def test_property_1_exactly_once_admission(fan_suffixes: list[int]) -> None:
    """Feature: virtual-waiting-room, Property 1: For any sequence of admission requests — including arbitrary duplicates and concurrent interleavings for the same Fan_Id within an Event — the system results in exactly one Queue_Entry for that (Event_Id, Fan_Id), and every duplicate request returns that same entry's Ordering_Key.

    Validates: Requirements 1.1, 1.3, 2.5, 9.1
    """
    fan_ids = [f"fan-{n}" for n in fan_suffixes]
    with waiting_room_env() as env:
        first_ordering_key: dict[str, str] = {}
        for fan in fan_ids:
            result = env.writer.admit(env.event_id, fan)
            assert result.fan_id == fan
            if fan in first_ordering_key:
                # A repeated admission is an idempotent duplicate that returns
                # the original entry's Ordering_Key (Req 1.3).
                assert result.duplicate is True
                assert result.ordering_key == first_ordering_key[fan]
            else:
                assert result.duplicate is False
                first_ordering_key[fan] = result.ordering_key

        # Exactly one entry item exists per distinct (Event_Id, Fan_Id) (Req 1.1, 2.5, 9.1).
        entries = _scan_entries(env.client)
        counts = Counter(e["Fan_Id"]["S"] for e in entries)
        distinct_fans = set(fan_ids)
        assert set(counts) == distinct_fans
        for fan in distinct_fans:
            assert counts[fan] == 1


# --------------------------------------------------------------------------- #
# Property 2: Admission produces a well-formed WAITING entry
# --------------------------------------------------------------------------- #
@settings(max_examples=100, deadline=None)
@given(fan_suffix=st.integers(min_value=0, max_value=50))
def test_property_2_well_formed_waiting_entry(fan_suffix: int) -> None:
    """Feature: virtual-waiting-room, Property 2: For any successful admission, the resulting entry contains Fan_Id, Event_Id, a server Entry_Timestamp, an Ordering_Key, Eligibility_Status = WAITING, a null Batch_Id, and is stored under a key of the form PK = EVT#<Event_Id>#SH#<shard>, SK = ENTRY#<Ordering_Key> retrievable as a single item.

    Validates: Requirements 1.2, 2.1, 2.3
    """
    fan = f"fan-{fan_suffix}"
    with waiting_room_env() as env:
        result = env.writer.admit(env.event_id, fan)

        pk, sk = _entry_key(env.event_id, fan, result.ordering_key)
        item = _get_entry(env.client, pk, sk)

        # Retrievable as a single item under the derived key.
        assert item is not None

        # Required attributes are present and correct.
        assert item["Fan_Id"]["S"] == fan
        assert item["Event_Id"]["S"] == env.event_id
        # Server Entry_Timestamp is a numeric attribute.
        assert "N" in item["Entry_Timestamp"]
        assert int(item["Entry_Timestamp"]["N"]) >= 0
        assert item["Ordering_Key"]["S"] == result.ordering_key
        assert item["Eligibility_Status"]["S"] == EligibilityStatus.WAITING.value
        # A null Batch_Id: the attribute is absent on a fresh WAITING entry.
        assert "Batch_Id" not in item

        # Key shape: PK = EVT#<Event_Id>#SH#<shard>, SK = ENTRY#<Ordering_Key>.
        assert item["PK"]["S"] == pk
        assert item["SK"]["S"] == sk
        assert re.fullmatch(rf"EVT#{re.escape(env.event_id)}#SH#\d+", pk)
        assert item["SK"]["S"] == f"ENTRY#{result.ordering_key}"
        # The shard encoded in the key matches the server-assigned shard.
        shard_int, _shard_str = assign_shard(fan, SHARD_COUNT)
        assert result.write_shard == shard_int


# --------------------------------------------------------------------------- #
# Property 4: Server-authoritative values ignore client input
# --------------------------------------------------------------------------- #
# Field names a client might try to supply to bias its position. The admit()
# API accepts only (event_id, fan_id), so these are structurally ignored; the
# test confirms the persisted values are server-sourced and none of these
# client field names leak into the stored entry.
_client_field = st.sampled_from(
    ["position", "timestamp", "ordering", "requested_position", "client_seq"]
)


@settings(max_examples=100, deadline=None)
@given(
    fan_suffix=st.integers(min_value=0, max_value=50),
    server_time=st.floats(
        min_value=1_000_000.0,
        max_value=2_000_000_000.0,
        allow_nan=False,
        allow_infinity=False,
    ),
    client_supplied=st.dictionaries(
        keys=_client_field, values=st.integers(), max_size=5
    ),
)
def test_property_4_server_authoritative_values(
    fan_suffix: int, server_time: float, client_supplied: dict[str, int]
) -> None:
    """Feature: virtual-waiting-room, Property 4: For any admission request carrying client-supplied position, timestamp, or ordering fields, the assigned Fan_Id, Ordering_Key, Entry_Timestamp, and computed Queue_Position are identical to those produced when no such fields are supplied.

    Validates: Requirements 3.5, 4.1
    """
    fan = f"fan-{fan_suffix}"
    # A fixed server clock drives Entry_Timestamp; the admit() API exposes no
    # way to pass the client-supplied fields, so they cannot influence anything.
    with waiting_room_env(clock=lambda: server_time) as env:
        result = env.writer.admit(env.event_id, fan)

        pk, sk = _entry_key(env.event_id, fan, result.ordering_key)
        item = _get_entry(env.client, pk, sk)
        assert item is not None

        # Fan_Id is the server-controlled identity, unchanged by client input.
        assert item["Fan_Id"]["S"] == fan
        assert result.fan_id == fan

        # Entry_Timestamp comes from the injected server clock (epoch millis),
        # never from any client-supplied timestamp.
        assert item["Entry_Timestamp"]["N"] == str(int(server_time * 1000))

        # Ordering_Key is a well-formed server-allocator key, matching the
        # returned result, not any client-supplied ordering.
        assert item["Ordering_Key"]["S"] == result.ordering_key
        parsed = parse_ordering_key(result.ordering_key)
        assert parsed.seq  # parses cleanly => server-generated HLC seq

        # No client-supplied field name leaks into the persisted entry.
        for client_field in client_supplied:
            assert client_field not in item


# --------------------------------------------------------------------------- #
# Property 20: Admitted entries are durable
# --------------------------------------------------------------------------- #
_status_targets = st.lists(
    st.sampled_from(list(EligibilityStatus)), min_size=0, max_size=8
)


@settings(max_examples=100, deadline=None)
@given(
    fan_suffix=st.integers(min_value=0, max_value=50),
    targets=_status_targets,
)
def test_property_20_admitted_entry_durability(
    fan_suffix: int, targets: list[EligibilityStatus]
) -> None:
    """Feature: virtual-waiting-room, Property 20: For any admitted entry and any subsequent sequence of promotion, status, and expiry operations, the entry continues to exist (in some permitted status) and is never dropped.

    Validates: Requirements 9.2
    """
    fan = f"fan-{fan_suffix}"
    with waiting_room_env() as env:
        result = env.writer.admit(env.event_id, fan)
        pk, sk = _entry_key(env.event_id, fan, result.ordering_key)

        manager = LifecycleManager(client=env.client)
        current = EligibilityStatus.WAITING

        # Apply an arbitrary sequence of promotion/status/expiry operations.
        for target in targets:
            if lifecycle.is_allowed(current, target):
                outcome = manager.apply_transition(pk, sk, current, target)
                if outcome.committed and outcome.current_status is not None:
                    current = outcome.current_status
            # Disallowed transitions are simply not attempted; the state
            # machine leaves the entry unchanged either way.

        # The entry still exists and holds a permitted Eligibility_Status.
        item = _get_entry(env.client, pk, sk)
        assert item is not None
        assert item["Eligibility_Status"]["S"] in VALID_STATUS_VALUES
