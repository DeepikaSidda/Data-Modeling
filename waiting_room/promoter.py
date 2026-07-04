"""Batch_Promoter - DynamoDB data-access layer for batch promotion and expiry.

This module wires the pure-logic layer (:mod:`waiting_room.batching`,
:mod:`waiting_room.ordering`) and the conditional-transition layer
(:mod:`waiting_room.lifecycle_manager`) to the ``WaitingRoom`` table so that
fans are promoted ``WAITING -> ELIGIBLE`` in position order without ever
over-saturating the downstream purchasing service, and are expired
``ELIGIBLE -> EXPIRED`` once their eligibility window elapses.

Design mapping (see design.md - "Downstream Over-Promotion Prevention" and the
Access Patterns table, patterns 5, 7, 8, 9, 12):

``promote_cycle(event_id)``
    1. **Atomically reserve capacity** against the single ``CAPACITY`` item
       (``PK = EVT#<Event_Id>``, ``SK = CAPACITY``). The design expresses the
       reservation as a conditional ``UpdateItem`` that grants
       ``n = min(Max_Batch_Size, remaining)`` where
       ``remaining = Downstream_Capacity - (Eligible_Count + Active_Count)``.

       .. note::
          DynamoDB ``ConditionExpression`` does **not** support arithmetic
          (e.g. ``Eligible_Count + Active_Count + :n <= Downstream_Capacity``
          is rejected by both DynamoDB and moto). The *identical* guarantee is
          therefore enforced with an optimistic **compare-and-set on
          ``Version``**: we read the counters, compute ``n`` bounded by the
          observed ``remaining`` via :func:`waiting_room.batching.compute_batch_size`,
          then commit ``ADD Eligible_Count :n, Promoted_Total :n, Version :1``
          **only if** ``Version`` is unchanged. A racing promoter that mutated
          the item bumps ``Version``, our conditional write fails, and we
          re-read and retry - exactly the flow modeled by
          :meth:`waiting_room.capacity.CapacityCounter.try_commit_reserve`. This
          makes it impossible for concurrent cycles to collectively exceed
          ``Downstream_Capacity`` (Req 6.1, 6.4). Partial grants are honored and
          a zero grant promotes nobody (Req 5.4, 6.2).

    2. **Read the next ``granted`` WAITING entries in ``Ordering_Key`` order**
       across shards from the sparse ``WaitingIndex`` GSI. Each shard partition
       (``Waiting_Shard = EVT#<Event_Id>#SH#<shard>``) is queried ascending with
       ``Limit = granted``; the per-shard heads are then **k-way merged** on
       ``Ordering_Key`` (via :func:`waiting_room.ordering.ordering_key_sort_key`)
       and the global first ``granted`` are selected (Req 5.1, 5.6).

    3. **Apply a conditional ``WAITING -> ELIGIBLE`` transition** to each
       selected entry through :class:`waiting_room.lifecycle_manager.LifecycleManager`,
       setting ``Batch_Id``, ``Promotion_Time`` and ``Elig_PK`` and removing
       ``Waiting_Shard`` (which evicts the entry from the sparse ``WaitingIndex``).
       The update is predicated on ``Eligibility_Status = WAITING`` so no entry is
       ever promoted twice (Req 5.2, 5.5, 5.7).

    4. **Release slots that are reserved but not consumed** so no capacity
       leaks (Req 10.5): every selected entry whose conditional transition
       CONFLICTs (a racing cycle already promoted it) releases one reserved slot
       via the manager's ``rollback`` callback, and any surplus of the grant
       over the number of entries actually available is released up front.

``expire_sweep(event_id)``
    Queries the ``EligibilityIndex`` for ``ELIGIBLE`` entries whose
    ``Promotion_Time`` is older than ``now - Eligibility_Window_Secs``,
    conditionally transitions each ``ELIGIBLE -> EXPIRED``, and releases the
    freed slot on the ``CAPACITY`` item (Req 5.8).

The promoter accepts an injected boto3 client, table name, and clock so it can
be exercised against ``moto`` / DynamoDB Local exactly as against AWS.

Requirements: 5.1, 5.2, 5.5, 5.6, 5.7, 5.8, 6.1, 6.4, 10.5.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from botocore.exceptions import ClientError

from waiting_room.batching import compute_batch_size
from waiting_room.config import EligibilityStatus
from waiting_room.lifecycle_manager import LifecycleManager
from waiting_room.ordering import ordering_key_sort_key
from waiting_room.sharding import format_shard
from waiting_room.provisioning import (
    CAPACITY_SK,
    CONFIG_SK,
    ELIGIBILITY_INDEX_NAME,
    TABLE_NAME,
    WAITING_INDEX_NAME,
    event_pk,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mypy_boto3_dynamodb.client import DynamoDBClient

__all__ = [
    "DEFAULT_RESERVE_RETRIES",
    "PromotionResult",
    "ExpireResult",
    "waiting_shard",
    "entry_pk",
    "entry_sk",
    "elig_pk",
    "generate_batch_id",
    "BatchPromoter",
]


#: Number of optimistic compare-and-set attempts a single reservation makes
#: before giving up (grant 0) for this cycle. A later scheduled cycle retries.
DEFAULT_RESERVE_RETRIES: int = 8

# Attribute names on the CAPACITY counter item.
_ELIGIBLE_ATTR = "Eligible_Count"
_ACTIVE_ATTR = "Active_Count"
_CAPACITY_ATTR = "Downstream_Capacity"
_PROMOTED_ATTR = "Promoted_Total"
_VERSION_ATTR = "Version"

# Attribute names on the queue-entry item.
_ORDERING_KEY_ATTR = "Ordering_Key"
_WAITING_SHARD_ATTR = "Waiting_Shard"
_WRITE_SHARD_ATTR = "Write_Shard"
_ELIG_PK_ATTR = "Elig_PK"
_BATCH_ID_ATTR = "Batch_Id"
_PROMOTION_TIME_ATTR = "Promotion_Time"

# Attribute names on the CONFIG item.
_SHARD_COUNT_ATTR = "Shard_Count"
_MAX_BATCH_ATTR = "Max_Batch_Size"
_ELIG_WINDOW_ATTR = "Eligibility_Window_Secs"


# --------------------------------------------------------------------------- #
# Key helpers (must match the design's queue-entry item schema)
# --------------------------------------------------------------------------- #
def waiting_shard(event_id: str, shard: int | str) -> str:
    """Return the ``Waiting_Shard`` / base ``PK`` value for a shard partition.

    Equals ``EVT#<Event_Id>#SH#<shard>``. On a queue entry the base-table ``PK``
    and the sparse-``WaitingIndex`` partition key ``Waiting_Shard`` hold the
    same value (while the entry is ``WAITING``), so this is used both to query a
    shard's head-of-line and to address the base item for the transition.
    """
    return f"{event_pk(event_id)}#SH#{shard}"


#: The base-table partition key for a queue entry is identical to its
#: ``Waiting_Shard`` value; exposed under an intention-revealing alias.
entry_pk = waiting_shard


def entry_sk(ordering_key: str) -> str:
    """Return the queue-entry sort key ``ENTRY#<Ordering_Key>``."""
    return f"ENTRY#{ordering_key}"


def elig_pk(event_id: str, status: EligibilityStatus | str) -> str:
    """Return the ``EligibilityIndex`` partition key ``EVT#<Event_Id>#<STATUS>``."""
    value = status.value if isinstance(status, EligibilityStatus) else status
    return f"{event_pk(event_id)}#{value}"


def generate_batch_id(now_secs: float) -> str:
    """Return a time-ordered, unique ``Batch_Id``.

    Format: ``BATCH#<epoch_ms zero-padded to 15 digits>-<random hex>``. The
    millisecond timestamp prefix makes batch ids lexicographically time-sorted
    (ULID-style), and the CSPRNG suffix guarantees uniqueness across concurrent
    cycles that share a millisecond.
    """
    millis = int(now_secs * 1000)
    return f"BATCH#{millis:015d}-{secrets.token_hex(6)}"


# --------------------------------------------------------------------------- #
# Result types
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class PromotionResult:
    """Structured result of :meth:`BatchPromoter.promote_cycle`."""

    #: The Batch_Id assigned to this cycle (``None`` when nothing was reserved).
    batch_id: str | None
    #: Slots reserved against the CAPACITY item this cycle.
    granted: int
    #: Entries durably transitioned ``WAITING -> ELIGIBLE`` this cycle.
    promoted_count: int
    #: Selected entries whose conditional transition CONFLICTed (already
    #: promoted by a racing cycle); each released its reserved slot.
    conflicts: int
    #: Reserved-but-unused slots released because fewer WAITING entries existed
    #: than were granted.
    released_unused: int

    @property
    def promoted(self) -> int:
        """Alias for :attr:`promoted_count`."""
        return self.promoted_count


@dataclass(frozen=True, slots=True)
class ExpireResult:
    """Structured result of :meth:`BatchPromoter.expire_sweep`."""

    #: Entries durably transitioned ``ELIGIBLE -> EXPIRED`` this sweep.
    expired_count: int
    #: Candidate ELIGIBLE entries examined (past their eligibility window).
    scanned: int


# --------------------------------------------------------------------------- #
# Batch_Promoter
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class BatchPromoter:
    """Promotes and expires fans against the ``WaitingRoom`` table.

    Parameters
    ----------
    client:
        An injected low-level boto3 DynamoDB client (real, DynamoDB Local, or
        ``moto``).
    table_name:
        The ``WaitingRoom`` table name. Defaults to
        :data:`waiting_room.provisioning.TABLE_NAME`.
    clock:
        Zero-arg callable returning the authoritative server time as epoch
        seconds. Injected for testability; defaults to :func:`time.time`.
    reserve_retries:
        Optimistic compare-and-set attempts per reservation before granting 0.
    """

    client: "DynamoDBClient"
    table_name: str = TABLE_NAME
    clock: Callable[[], float] = time.time
    reserve_retries: int = DEFAULT_RESERVE_RETRIES
    _lifecycle: LifecycleManager = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._lifecycle = LifecycleManager(
            client=self.client, table_name=self.table_name
        )

    # ------------------------------------------------------------------ #
    # Promotion
    # ------------------------------------------------------------------ #
    def promote_cycle(
        self,
        event_id: str,
        *,
        max_batch_size: int | None = None,
        shard_count: int | None = None,
    ) -> PromotionResult:
        """Run one promotion cycle for ``event_id``.

        Reserves capacity, selects the globally-earliest ``WAITING`` entries in
        ``Ordering_Key`` order across shards, and conditionally promotes them to
        ``ELIGIBLE``. ``max_batch_size`` and ``shard_count`` default to the
        event's ``CONFIG`` values when not supplied.

        Returns a :class:`PromotionResult`. When no capacity is available the
        result has ``granted == 0`` and ``promoted_count == 0`` (Req 5.4, 6.2).

        Requirements: 5.1, 5.2, 5.5, 5.6, 5.7, 6.1, 6.4, 10.5.
        """
        if max_batch_size is None or shard_count is None:
            cfg = self._load_config(event_id)
            if max_batch_size is None:
                max_batch_size = cfg["max_batch_size"]
            if shard_count is None:
                shard_count = cfg["shard_count"]

        # (1) Atomically reserve capacity (optimistic compare-and-set).
        granted = self._reserve(event_id, max_batch_size)
        if granted <= 0:
            return PromotionResult(
                batch_id=None,
                granted=0,
                promoted_count=0,
                conflicts=0,
                released_unused=0,
            )

        # (2) Read the next `granted` WAITING entries in Ordering_Key order,
        # k-way merged across all shard partitions of the sparse WaitingIndex.
        selected = self._select_next_waiting(event_id, shard_count, granted)

        # Release any grant surplus over what is actually available to promote
        # so reserved-but-unused capacity never leaks (Req 10.5).
        released_unused = granted - len(selected)
        if released_unused > 0:
            self._release_reservation(event_id, released_unused)

        # (3) + (4) Conditionally promote each selected entry; a CONFLICT means
        # a racing cycle already promoted it, so release that reserved slot.
        now = self.clock()
        batch_id = generate_batch_id(now)
        # Promotion_Time is stored as epoch MILLISECONDS to match the frozen
        # data model and the Status_Reader (which divides by 1000). Writing
        # seconds here would make every promoted entry look ~decades old to the
        # reader and be reported as EXPIRED immediately.
        promotion_time = str(int(now * 1000))
        promoted = 0
        conflicts = 0
        for ws_value, ordering_key in selected:
            result = self._lifecycle.apply_transition(
                pk=ws_value,
                sk=entry_sk(ordering_key),
                from_status=EligibilityStatus.WAITING,
                to_status=EligibilityStatus.ELIGIBLE,
                extra_set={
                    _BATCH_ID_ATTR: {"S": batch_id},
                    _PROMOTION_TIME_ATTR: {"N": promotion_time},
                    _ELIG_PK_ATTR: {"S": elig_pk(event_id, EligibilityStatus.ELIGIBLE)},
                },
                extra_remove=[_WAITING_SHARD_ATTR],
                rollback=lambda: self._release_reservation(event_id, 1),
            )
            if result.committed:
                promoted += 1
            else:
                conflicts += 1
                # On CONFLICT the rollback callback already released the slot.
                # For any other non-committed outcome, release defensively so
                # capacity cannot leak.
                if not result.rolled_back:
                    self._release_reservation(event_id, 1)

        return PromotionResult(
            batch_id=batch_id,
            granted=granted,
            promoted_count=promoted,
            conflicts=conflicts,
            released_unused=max(released_unused, 0),
        )

    # ------------------------------------------------------------------ #
    # Expiry
    # ------------------------------------------------------------------ #
    def expire_sweep(
        self,
        event_id: str,
        *,
        eligibility_window_secs: int | None = None,
        limit: int | None = None,
    ) -> ExpireResult:
        """Expire ``ELIGIBLE`` entries past their eligibility window.

        Queries the ``EligibilityIndex`` for ``ELIGIBLE`` entries whose
        ``Promotion_Time`` is older than ``now - Eligibility_Window_Secs``,
        conditionally transitions each ``ELIGIBLE -> EXPIRED``, and releases the
        freed slot on the ``CAPACITY`` item. ``eligibility_window_secs`` defaults
        to the event's ``CONFIG`` value.

        Requirements: 5.8, 10.5.
        """
        if eligibility_window_secs is None:
            eligibility_window_secs = self._load_config(event_id)[
                "eligibility_window_secs"
            ]

        now = self.clock()
        # Promotion_Time is epoch milliseconds (see promote_cycle), so the
        # expiry cutoff must be computed in milliseconds too.
        cutoff = int(now * 1000) - int(eligibility_window_secs) * 1000

        candidates = self._query_expired_eligible(event_id, cutoff, limit)

        expired = 0
        for write_shard, ordering_key in candidates:
            result = self._lifecycle.apply_transition(
                pk=entry_pk(event_id, write_shard),
                sk=entry_sk(ordering_key),
                from_status=EligibilityStatus.ELIGIBLE,
                to_status=EligibilityStatus.EXPIRED,
                extra_set={
                    _ELIG_PK_ATTR: {"S": elig_pk(event_id, EligibilityStatus.EXPIRED)},
                },
            )
            if result.committed:
                # Free the ELIGIBLE slot (Promoted_Total is monotonic and is
                # deliberately not decremented on expiry).
                self._release_eligible_slot(event_id, 1)
                expired += 1

        return ExpireResult(expired_count=expired, scanned=len(candidates))

    # ------------------------------------------------------------------ #
    # Capacity reservation / release (CAPACITY item)
    # ------------------------------------------------------------------ #
    def _reserve(self, event_id: str, max_batch_size: int) -> int:
        """Reserve ``min(max_batch_size, remaining)`` slots atomically.

        Optimistic compare-and-set on ``Version``: read the counters, compute
        the grant bounded by observed remaining capacity, and commit only if
        ``Version`` is unchanged. Retries on a racing mutation, grants a partial
        amount when only some capacity is free, and returns ``0`` when full or
        after exhausting :attr:`reserve_retries` (Req 6.1, 6.4).
        """
        pk = event_pk(event_id)
        for _ in range(max(self.reserve_retries, 1)):
            state = self._read_capacity(pk)
            granted = compute_batch_size(
                waiting_count=max_batch_size,
                max_batch_size=max_batch_size,
                downstream_capacity=state["downstream_capacity"],
                eligible_count=state["eligible_count"],
                active_count=state["active_count"],
            )
            if granted <= 0:
                return 0
            if self._commit_reserve(pk, granted, state["version"]):
                return granted
            # Version changed under us: re-read and retry (optimistic).
        return 0

    def _read_capacity(self, pk: str) -> dict[str, int]:
        """Read the CAPACITY counters via a strongly-consistent ``GetItem``."""
        resp = self.client.get_item(
            TableName=self.table_name,
            Key={"PK": {"S": pk}, "SK": {"S": CAPACITY_SK}},
            ConsistentRead=True,
        )
        item = resp.get("Item")
        if not item:
            raise KeyError(f"CAPACITY item missing for {pk!r}")
        return {
            "downstream_capacity": int(item[_CAPACITY_ATTR]["N"]),
            "eligible_count": int(item.get(_ELIGIBLE_ATTR, {"N": "0"})["N"]),
            "active_count": int(item.get(_ACTIVE_ATTR, {"N": "0"})["N"]),
            "version": int(item.get(_VERSION_ATTR, {"N": "0"})["N"]),
        }

    def _commit_reserve(self, pk: str, granted: int, expected_version: int) -> bool:
        """Commit a reservation iff ``Version`` still equals ``expected_version``.

        Applies ``ADD Eligible_Count :n, Promoted_Total :n, Version :1`` under
        ``ConditionExpression: Version = :ver``. Returns ``True`` on commit,
        ``False`` on the conditional-check failure that signals a concurrent
        mutation (the caller re-plans).
        """
        try:
            self.client.update_item(
                TableName=self.table_name,
                Key={"PK": {"S": pk}, "SK": {"S": CAPACITY_SK}},
                UpdateExpression="ADD #ec :n, #pt :n, #v :one",
                ConditionExpression="#v = :ver",
                ExpressionAttributeNames={
                    "#ec": _ELIGIBLE_ATTR,
                    "#pt": _PROMOTED_ATTR,
                    "#v": _VERSION_ATTR,
                },
                ExpressionAttributeValues={
                    ":n": {"N": str(granted)},
                    ":one": {"N": "1"},
                    ":ver": {"N": str(expected_version)},
                },
            )
            return True
        except ClientError as exc:
            if _is_conditional_failure(exc):
                return False
            raise

    def _release_reservation(self, event_id: str, count: int) -> None:
        """Undo an unused reservation: decrement ``Eligible_Count`` and ``Promoted_Total``.

        Used when a reserved slot is never consumed (a racing cycle already
        promoted the entry, or fewer WAITING entries existed than were granted),
        so the speculative ``ADD`` from :meth:`_commit_reserve` is fully undone
        and no capacity leaks (Req 10.5).
        """
        if count <= 0:
            return
        self.client.update_item(
            TableName=self.table_name,
            Key={"PK": {"S": event_pk(event_id)}, "SK": {"S": CAPACITY_SK}},
            UpdateExpression="ADD #ec :neg, #pt :neg, #v :one",
            ExpressionAttributeNames={
                "#ec": _ELIGIBLE_ATTR,
                "#pt": _PROMOTED_ATTR,
                "#v": _VERSION_ATTR,
            },
            ExpressionAttributeValues={
                ":neg": {"N": str(-count)},
                ":one": {"N": "1"},
            },
        )

    def _release_eligible_slot(self, event_id: str, count: int) -> None:
        """Free ``count`` ELIGIBLE slots on expiry: decrement ``Eligible_Count`` only.

        ``Promoted_Total`` is a monotonic historical counter and is deliberately
        not decremented when a genuinely-promoted fan expires (Req 5.8, 7.5).
        """
        if count <= 0:
            return
        self.client.update_item(
            TableName=self.table_name,
            Key={"PK": {"S": event_pk(event_id)}, "SK": {"S": CAPACITY_SK}},
            UpdateExpression="ADD #ec :neg, #v :one",
            ExpressionAttributeNames={"#ec": _ELIGIBLE_ATTR, "#v": _VERSION_ATTR},
            ExpressionAttributeValues={":neg": {"N": str(-count)}, ":one": {"N": "1"}},
        )

    # ------------------------------------------------------------------ #
    # WAITING selection (sparse WaitingIndex, k-way merge)
    # ------------------------------------------------------------------ #
    def _select_next_waiting(
        self, event_id: str, shard_count: int, limit: int
    ) -> list[tuple[str, str]]:
        """Return the global first ``limit`` WAITING entries in Ordering_Key order.

        Queries each shard partition of the sparse ``WaitingIndex`` ascending by
        ``Ordering_Key`` with ``Limit = limit`` (each shard is already sorted, so
        its head is its earliest ``limit`` entries), then k-way merges the heads
        on ``Ordering_Key`` and takes the global first ``limit``.

        Returns a list of ``(waiting_shard_value, ordering_key)`` tuples - the
        ``waiting_shard_value`` doubles as the base-table ``PK`` for the
        subsequent conditional transition (Req 5.1, 5.6).
        """
        heads: list[tuple[tuple[str, str], str, str]] = []
        for shard in range(shard_count):
            # Zero-pad the shard to match the width the Admission_Writer used
            # when it wrote Waiting_Shard (format_shard). Without this, any
            # shard_count > 9 produces an unpadded key (e.g. ...#SH#42) that
            # never matches the stored padded key (...#SH#042), so the promoter
            # would silently find nothing to promote.
            ws_value = waiting_shard(event_id, format_shard(shard, shard_count))
            resp = self.client.query(
                TableName=self.table_name,
                IndexName=WAITING_INDEX_NAME,
                KeyConditionExpression="#ws = :ws",
                ExpressionAttributeNames={"#ws": _WAITING_SHARD_ATTR},
                ExpressionAttributeValues={":ws": {"S": ws_value}},
                ScanIndexForward=True,
                Limit=limit,
            )
            for item in resp.get("Items", []):
                ordering_key = item[_ORDERING_KEY_ATTR]["S"]
                heads.append(
                    (ordering_key_sort_key(ordering_key), ws_value, ordering_key)
                )

        # k-way merge == sort the combined shard heads by the total order.
        heads.sort(key=lambda entry: entry[0])
        return [(ws_value, ordering_key) for (_sort, ws_value, ordering_key) in heads[:limit]]

    # ------------------------------------------------------------------ #
    # Expiry candidate query (EligibilityIndex)
    # ------------------------------------------------------------------ #
    def _query_expired_eligible(
        self, event_id: str, cutoff: int, limit: int | None
    ) -> list[tuple[str, str]]:
        """Return ``(write_shard, ordering_key)`` for ELIGIBLE entries past ``cutoff``.

        Queries ``EligibilityIndex`` where ``Elig_PK = EVT#<id>#ELIGIBLE`` and
        ``Promotion_Time < cutoff``, ascending (oldest first), paginating until
        exhausted or ``limit`` candidates are collected.
        """
        results: list[tuple[str, str]] = []
        start_key: dict[str, Any] | None = None
        while True:
            kwargs: dict[str, Any] = {
                "TableName": self.table_name,
                "IndexName": ELIGIBILITY_INDEX_NAME,
                "KeyConditionExpression": "#ep = :ep AND #pt < :cutoff",
                "ExpressionAttributeNames": {
                    "#ep": _ELIG_PK_ATTR,
                    "#pt": _PROMOTION_TIME_ATTR,
                },
                "ExpressionAttributeValues": {
                    ":ep": {"S": elig_pk(event_id, EligibilityStatus.ELIGIBLE)},
                    ":cutoff": {"N": str(cutoff)},
                },
                "ScanIndexForward": True,
            }
            if start_key is not None:
                kwargs["ExclusiveStartKey"] = start_key
            resp = self.client.query(**kwargs)
            for item in resp.get("Items", []):
                write_shard = item[_WRITE_SHARD_ATTR]["S"]
                ordering_key = item[_ORDERING_KEY_ATTR]["S"]
                results.append((write_shard, ordering_key))
                if limit is not None and len(results) >= limit:
                    return results
            start_key = resp.get("LastEvaluatedKey")
            if not start_key:
                return results

    # ------------------------------------------------------------------ #
    # Config
    # ------------------------------------------------------------------ #
    def _load_config(self, event_id: str) -> dict[str, int]:
        """Read shard/batch/window tunables from the event's ``CONFIG`` item."""
        resp = self.client.get_item(
            TableName=self.table_name,
            Key={"PK": {"S": event_pk(event_id)}, "SK": {"S": CONFIG_SK}},
            ConsistentRead=True,
        )
        item = resp.get("Item")
        if not item:
            raise KeyError(f"CONFIG item missing for event {event_id!r}")
        return {
            "shard_count": int(item[_SHARD_COUNT_ATTR]["N"]),
            "max_batch_size": int(item[_MAX_BATCH_ATTR]["N"]),
            "eligibility_window_secs": int(item[_ELIG_WINDOW_ATTR]["N"]),
        }


# --------------------------------------------------------------------------- #
# Module-level helpers
# --------------------------------------------------------------------------- #
def _is_conditional_failure(exc: ClientError) -> bool:
    """Return whether ``exc`` is a DynamoDB ConditionalCheckFailedException."""
    return (
        exc.response.get("Error", {}).get("Code")
        == "ConditionalCheckFailedException"
    )
