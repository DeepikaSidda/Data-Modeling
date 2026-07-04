"""Admission_Writer data-access layer for the Virtual Waiting Room.

This is the burst-path component that admits an arriving fan and writes their
Queue_Entry. It wires the pure-logic layer (Ordering_Key allocation, write
sharding, Entry_Token signing, backoff schedule) to DynamoDB, honoring the
design's *Admission (burst path)* flow and the exactly-once guarantee.

## What ``admit`` does (design "Admission (burst path)")

1. **Gate on event-open.** ``GetItem`` the ``EVT#<Event_Id> / CONFIG`` item and
   reject with :class:`AdmissionError` ``EVENT_NOT_OPEN`` when the event is
   missing or ``Event_Status != OPEN`` (Req 1.5).
2. **Gate on queue-full (approximate).** Compare an injected/aggregated total
   admitted count against the event's ``Max_Queue_Size`` and reject with
   ``QUEUE_FULL`` when the queue is full (Req 1.6). This is intentionally an
   *approximation* fed by the sharded ``ADMIT_COUNT`` aggregate - a soft safety
   valve, never a hard boundary, because a hard global counter would be the
   very hot partition the sharding design exists to avoid.
3. **Assign server-authoritative values.** ``Write_Shard = hash(Fan_Id) %
   Shard_Count`` (:func:`waiting_room.sharding.assign_shard`) and a fresh
   ``Ordering_Key`` from the injected :class:`~waiting_room.ordering.OrderingKeyAllocator`
   - both server-controlled and independent of any client input (Req 3.1, 4.1).
4. **Write exactly-once with a transaction.** A single ``TransactWriteItems``
   atomically puts:

   * the **queue entry** (``PK = EVT#<Event_Id>#SH#<shard>``,
     ``SK = ENTRY#<Ordering_Key>``) under an ``attribute_not_exists(PK)``
     condition, carrying ``Fan_Id``, ``Event_Id``, ``Write_Shard``,
     ``Entry_Timestamp``, ``Eligibility_Status = WAITING``, ``Ordering_Key``,
     ``Waiting_Shard`` (so it appears in the sparse ``WaitingIndex``), no
     ``Batch_Id``, and a ``ttl``; and
   * the **fan dedupe guard** (``PK = EVT#<Event_Id>#FAN#<Fan_Id>``,
     ``SK = ADMISSION``) under an ``attribute_not_exists(PK)`` condition,
     storing ``Ordering_Key``, ``Write_Shard``, ``Fan_Id``, ``Event_Id``.

   The two puts commit together or not at all, so a given ``(Event_Id, Fan_Id)``
   can never yield two entries (Req 1.1, 2.5, 9.1).
5. **Increment the sharded admit counter.** After the transaction commits, an
   atomic ``ADD Admitted_Count :1`` bumps the ``ADMIT_COUNT`` item on the
   *same* shard partition as the entry (Req 1.6). See the note below on why
   this is a separate ``UpdateItem`` rather than a third transaction item.
6. **Issue the Entry_Token.** The claim set
   ``{Fan_Id, Event_Id, Ordering_Key, Write_Shard}`` is signed with the
   provided secret and returned (Req 1.4).

## Throttle handling (Req 2.6, 2.7)

Throttled writes - ``ProvisionedThroughputExceededException``, on-demand
``ThrottlingException``, or a ``TransactionCanceledException`` whose cancellation
reasons are throttle/conflict - are retried using the bounded
exponential-backoff-with-jitter schedule in :mod:`waiting_room.backoff`. Because
``TransactWriteItems`` is atomic, a throttled attempt writes **nothing**, so a
retry re-submits the identical items with no risk of a partial write. When the
retry budget is exhausted the writer raises :class:`AdmissionError`
``WRITE_RETRYABLE`` and has persisted no partial entry (Req 2.7).

## Idempotent duplicates (Req 1.3)

If the transaction is cancelled because the **dedupe guard** condition failed
(the fan already holds an entry for the event), the writer treats it as an
idempotent duplicate: it reads the existing guard item and returns that entry's
``Ordering_Key`` and a freshly signed token for it, rather than creating a
duplicate. Every duplicate request therefore returns the same Ordering_Key.

## Why increment ``ADMIT_COUNT`` outside the transaction

``TransactWriteItems`` is capped at a modest number of items and every item in
it shares one all-or-nothing fate. Folding the counter ``ADD`` into the
transaction would (a) consume a transaction slot, (b) couple the counter's
success to the entry write for no correctness benefit, and (c) offer nothing -
the counter is a *monotonic approximate aggregate* for the soft queue-full
gate, not an authority. A duplicate admission never reaches the increment
because the guard condition fails first, so the counter is not double-counted.
If the post-commit increment itself fails, the admission has still succeeded
and is durable; the aggregate simply lags by one until reconciled by the
Streams aggregator. The increment is therefore best-effort and its failure is
swallowed rather than surfaced to the fan.

Everything DynamoDB-facing is **injected** (the ``boto3`` client, table name,
signing secret, clock, Fan_Id generator, Ordering_Key allocator, backoff
randomness/sleep, and the queue-full count provider) so the writer runs
end-to-end against ``moto`` / DynamoDB Local with no global state.

Requirements: 1.1, 1.2, 1.3, 1.5, 1.6, 2.5, 2.6, 2.7, 9.1.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Optional

from botocore.exceptions import ClientError

from waiting_room.backoff import RetryOutcome, next_retry
from waiting_room.config import EligibilityStatus, EventStatus, WaitingRoomConfig
from waiting_room.ordering import OrderingKeyAllocator
from waiting_room.provisioning import (
    CONFIG_SK,
    TABLE_NAME,
    event_pk,
)
from waiting_room.sharding import assign_shard
from waiting_room.token import sign

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mypy_boto3_dynamodb.client import DynamoDBClient

__all__ = [
    "ADMISSION_SK",
    "ADMIT_COUNT_SK",
    "ENTRY_SK_PREFIX",
    "WAITING_STATUS",
    "AdmissionError",
    "AdmissionResult",
    "AdmittedCountProvider",
    "FanIdGenerator",
    "Admission_Writer",
]


# --------------------------------------------------------------------------- #
# Item-model constants (mirror the design's WaitingRoom key/attribute schema)
# --------------------------------------------------------------------------- #
#: Sort key of the fan dedupe guard item (``PK = EVT#<id>#FAN#<fan>``).
ADMISSION_SK: str = "ADMISSION"
#: Sort key of the per-shard sharded admit counter item.
ADMIT_COUNT_SK: str = "ADMIT_COUNT"
#: Sort-key prefix of a queue-entry item.
ENTRY_SK_PREFIX: str = "ENTRY#"
#: The lifecycle status a freshly admitted entry holds.
WAITING_STATUS: str = EligibilityStatus.WAITING.value

#: Numeric attribute accumulating a shard's cumulative admissions.
_ADMITTED_COUNT_ATTR: str = "Admitted_Count"

#: DynamoDB error codes (top-level) that are safe to retry with backoff.
_RETRYABLE_ERROR_CODES: frozenset[str] = frozenset(
    {
        "ProvisionedThroughputExceededException",
        "ThrottlingException",
        "RequestLimitExceeded",
        "InternalServerError",
    }
)
#: Per-item ``CancellationReasons`` codes (inside a TransactionCanceled) that
#: are safe to retry - transient throttling/conflict, not a real condition miss.
_RETRYABLE_CANCEL_CODES: frozenset[str] = frozenset(
    {
        "ThrottlingError",
        "ProvisionedThroughputExceeded",
        "TransactionConflict",
    }
)
#: The DynamoDB code signalling a whole transaction was cancelled.
_TRANSACTION_CANCELLED: str = "TransactionCanceledException"
#: The per-item cancellation code for a failed ``ConditionExpression``.
_CONDITIONAL_CHECK_FAILED: str = "ConditionalCheckFailed"

#: Index of the queue-entry put within the TransactItems list.
_ENTRY_ITEM_INDEX: int = 0
#: Index of the dedupe-guard put within the TransactItems list.
_GUARD_ITEM_INDEX: int = 1

#: Default retention added to the entry ``ttl`` (seconds). TTL is best-effort
#: garbage collection only - never authoritative for eligibility/expiry.
DEFAULT_TTL_SECS: int = 86_400


# --------------------------------------------------------------------------- #
# Injected-collaborator contracts
# --------------------------------------------------------------------------- #
FanIdGenerator = Callable[[], str]
"""Zero-arg callable minting a fresh, server-controlled ``Fan_Id``."""

AdmittedCountProvider = Callable[[str], int]
"""Callable returning the (approximate) total admitted count for an event.

Backed in production by the Streams-fed ``Σ ADMIT_COUNT`` aggregate in
ElastiCache; injected here so the queue-full gate stays off any hot counting
path. Receives the ``event_id`` and returns a non-negative admitted count.
"""


def _default_fan_id() -> str:
    """Mint a fresh opaque ``Fan_Id`` (unguessable, server-issued)."""
    return f"f_{uuid.uuid4().hex}"


# --------------------------------------------------------------------------- #
# Errors / result
# --------------------------------------------------------------------------- #
class AdmissionError(Exception):
    """Raised when admission is rejected for a well-defined reason.

    The ``code`` mirrors the design's rejection outcomes so an API layer can
    map it to an HTTP status:

    * ``EVENT_NOT_OPEN`` - the event's queue is closed/absent (Req 1.5).
    * ``QUEUE_FULL`` - the event has reached ``Max_Queue_Size`` (Req 1.6).
    * ``WRITE_RETRYABLE`` - throttling exhausted the retry budget; **no**
      partial entry was written and the fan should retry (Req 2.7).
    """

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    """The outcome of a successful (or idempotently duplicate) admission.

    Attributes:
        entry_token: The signed Entry_Token encoding
            ``{Fan_Id, Event_Id, Ordering_Key, Write_Shard}`` (Req 1.4).
        ordering_key: The server-assigned Ordering_Key for the fan's entry.
        fan_id: The server-controlled Fan_Id for the entry.
        event_id: The event the fan was admitted to.
        write_shard: The integer Write_Shard the entry lives on.
        eligibility_status: Always ``WAITING`` for a fresh admission; the
            existing entry's status is not re-read on the duplicate path (the
            guard only records the Ordering_Key), so it is reported as
            ``WAITING`` there too.
        duplicate: ``True`` when the fan already held an entry and this call
            returned that existing entry idempotently (Req 1.3).
    """

    entry_token: str
    ordering_key: str
    fan_id: str
    event_id: str
    write_shard: int
    eligibility_status: EligibilityStatus = EligibilityStatus.WAITING
    duplicate: bool = False

    def as_dict(self) -> dict[str, Any]:
        """Return the result as a plain, JSON-serializable dict."""
        return {
            "entry_token": self.entry_token,
            "ordering_key": self.ordering_key,
            "fan_id": self.fan_id,
            "event_id": self.event_id,
            "write_shard": self.write_shard,
            "eligibility_status": self.eligibility_status.value,
            "duplicate": self.duplicate,
        }


# --------------------------------------------------------------------------- #
# Admission_Writer
# --------------------------------------------------------------------------- #
class Admission_Writer:
    """Admits fans and writes their Queue_Entry with exactly-once semantics.

    Args:
        client: A low-level ``boto3`` DynamoDB client (real, DynamoDB Local, or
            ``moto``).
        secret: The server-held signing key used to issue Entry_Tokens.
        config: The :class:`WaitingRoomConfig`; its ``event.shard_count`` sizes
            write sharding and must match the value the Status_Reader uses to
            reconstruct keys, and its ``backoff`` drives throttle retries.
        allocator: The stateful :class:`OrderingKeyAllocator` producing
            monotonic, totally-ordered Ordering_Keys. One allocator per
            admission node; injected so tests can pin the clock/tie-breaker.
        table_name: The DynamoDB table name (defaults to ``WaitingRoom``).
        fan_id_generator: Zero-arg callable minting a fresh ``Fan_Id`` when
            ``admit`` is called without one. A caller that already knows the
            authenticated identity's stable Fan_Id passes it explicitly so
            duplicate detection works across requests.
        admitted_count_provider: Optional callable returning the approximate
            total admitted count for the queue-full gate. When ``None`` the
            queue-full gate is skipped.
        clock: Zero-arg callable returning the current time in **epoch
            seconds** (defaults to :func:`time.time`). Drives the server
            ``Entry_Timestamp`` (stored as epoch milliseconds) and the ``ttl``.
        sleep: Callable used to wait between throttle retries (defaults to
            :func:`time.sleep`); injected so tests never actually block.
        rand: Zero-arg callable in ``[0.0, 1.0)`` supplying backoff jitter
            (defaults to :func:`random.random` inside the backoff module).
        ttl_secs: Retention added to ``Entry_Timestamp`` to compute ``ttl``.
    """

    def __init__(
        self,
        *,
        client: "DynamoDBClient",
        secret: str | bytes,
        config: WaitingRoomConfig | None = None,
        allocator: OrderingKeyAllocator | None = None,
        table_name: str = TABLE_NAME,
        fan_id_generator: FanIdGenerator = _default_fan_id,
        admitted_count_provider: Optional[AdmittedCountProvider] = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        rand: Optional[Callable[[], float]] = None,
        ttl_secs: int = DEFAULT_TTL_SECS,
    ) -> None:
        self._client = client
        self._secret = secret
        self._config = config or WaitingRoomConfig()
        self._allocator = allocator or OrderingKeyAllocator()
        self._table_name = table_name
        self._fan_id_generator = fan_id_generator
        self._admitted_count_provider = admitted_count_provider
        self._clock = clock
        self._sleep = sleep
        self._rand = rand
        self._ttl_secs = ttl_secs

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def admit(self, event_id: str, fan_id: str | None = None) -> AdmissionResult:
        """Admit a fan to ``event_id``'s queue, returning their entry token.

        Args:
            event_id: The event whose queue the fan is entering.
            fan_id: The server-controlled Fan_Id for the fan. When ``None`` a
                fresh one is minted via the injected generator. Pass the same
                ``fan_id`` again to observe idempotent-duplicate behaviour.

        Returns:
            An :class:`AdmissionResult`. ``duplicate`` is ``True`` when the fan
            already held an entry (Req 1.3).

        Raises:
            AdmissionError: ``EVENT_NOT_OPEN`` (Req 1.5), ``QUEUE_FULL``
                (Req 1.6), or ``WRITE_RETRYABLE`` on exhausted retries with no
                partial write (Req 2.7).
        """
        if not event_id:
            raise ValueError("event_id must be a non-empty string")

        # (1) Event-open gate + authoritative queue-full bound (Req 1.5, 1.6).
        max_queue_size = self._gate_event_open(event_id)

        # (2) Approximate queue-full gate from the injected aggregate (Req 1.6).
        self._gate_queue_full(event_id, max_queue_size)

        fan = fan_id if fan_id is not None else self._fan_id_generator()
        if not fan:
            raise ValueError("fan_id must be a non-empty string")

        # (3) Server-authoritative shard assignment (Req 2.2, 4.1).
        shard_int, shard_str = assign_shard(fan, self._config.event.shard_count)
        entry_pk = self._entry_pk(event_id, shard_str)

        # (4) Transactional exactly-once write with bounded throttle retry.
        ordering_key = self._allocator.next_ordering_key()
        attempt = 0
        while True:
            entry_item = self._build_entry_item(
                entry_pk=entry_pk,
                event_id=event_id,
                fan_id=fan,
                shard_str=shard_str,
                ordering_key=ordering_key,
            )
            guard_item = self._build_guard_item(
                event_id=event_id,
                fan_id=fan,
                shard_str=shard_str,
                ordering_key=ordering_key,
            )
            try:
                self._client.transact_write_items(
                    TransactItems=[
                        {
                            "Put": {
                                "TableName": self._table_name,
                                "Item": entry_item,
                                "ConditionExpression": "attribute_not_exists(PK)",
                            }
                        },
                        {
                            "Put": {
                                "TableName": self._table_name,
                                "Item": guard_item,
                                "ConditionExpression": "attribute_not_exists(PK)",
                            }
                        },
                    ]
                )
            except ClientError as exc:
                decision = self._classify_write_error(exc)
                if decision == "duplicate":
                    # Idempotent duplicate: return the existing entry (Req 1.3).
                    return self._existing_admission(event_id, fan, shard_int)
                if decision == "reallocate":
                    # Astronomically-unlikely Ordering_Key collision on a new
                    # fan: mint a fresh key and retry immediately (no backoff).
                    ordering_key = self._allocator.next_ordering_key()
                    continue
                if decision == "retry":
                    outcome, delay = self._next_retry(attempt)
                    if outcome is RetryOutcome.RETRY:
                        self._sleep(delay or 0.0)
                        attempt += 1
                        continue
                    # Retry budget exhausted - retryable error, no partial write
                    # (TransactWriteItems is atomic), per Req 2.7.
                    raise AdmissionError(
                        "WRITE_RETRYABLE",
                        "admission write throttled; retry budget exhausted",
                    ) from exc
                raise  # unexpected error - surface it.
            else:
                break  # transaction committed.

        # (5) Best-effort sharded admit-counter increment (Req 1.6).
        self._increment_admit_count(entry_pk)

        # (6) Issue the Entry_Token (Req 1.4).
        token = self._issue_token(fan, event_id, ordering_key, shard_int)
        return AdmissionResult(
            entry_token=token,
            ordering_key=ordering_key,
            fan_id=fan,
            event_id=event_id,
            write_shard=shard_int,
            eligibility_status=EligibilityStatus.WAITING,
            duplicate=False,
        )

    # ------------------------------------------------------------------ #
    # Gating
    # ------------------------------------------------------------------ #
    def _gate_event_open(self, event_id: str) -> Optional[int]:
        """Reject a closed/absent event; return the event's ``Max_Queue_Size``.

        Reads the ``EVT#<Event_Id> / CONFIG`` item with a single ``GetItem``.
        Raises :class:`AdmissionError` ``EVENT_NOT_OPEN`` when the item is
        missing or ``Event_Status != OPEN`` (Req 1.5). Returns the configured
        ``Max_Queue_Size`` (or ``None`` when not present) for the queue-full
        gate.
        """
        response = self._client.get_item(
            TableName=self._table_name,
            Key={"PK": {"S": event_pk(event_id)}, "SK": {"S": CONFIG_SK}},
            ConsistentRead=True,
        )
        item = response.get("Item")
        if not item:
            raise AdmissionError("EVENT_NOT_OPEN", f"event {event_id!r} is not open")

        status = _attr_str(item, "Event_Status")
        if status != EventStatus.OPEN.value:
            raise AdmissionError("EVENT_NOT_OPEN", f"event {event_id!r} is not open")

        return _attr_int(item, "Max_Queue_Size")

    def _gate_queue_full(self, event_id: str, max_queue_size: Optional[int]) -> None:
        """Reject when the (approximate) admitted count has reached the cap.

        Uses the injected ``admitted_count_provider`` (backed by the Streams
        aggregate) so no hot counting happens on the admission path. Skipped
        when no provider or no ``Max_Queue_Size`` is configured (Req 1.6).
        """
        if self._admitted_count_provider is None or max_queue_size is None:
            return
        admitted = self._admitted_count_provider(event_id)
        if admitted >= max_queue_size:
            raise AdmissionError(
                "QUEUE_FULL",
                f"event {event_id!r} queue is full ({admitted}/{max_queue_size})",
            )

    # ------------------------------------------------------------------ #
    # Item builders
    # ------------------------------------------------------------------ #
    def _entry_pk(self, event_id: str, shard_str: str) -> str:
        """Return the queue-entry / ``Waiting_Shard`` partition key."""
        return f"EVT#{event_id}#SH#{shard_str}"

    def _build_entry_item(
        self,
        *,
        entry_pk: str,
        event_id: str,
        fan_id: str,
        shard_str: str,
        ordering_key: str,
    ) -> dict[str, dict[str, str]]:
        """Build the queue-entry item in low-level AttributeValue form.

        Carries every attribute Req 1.2 mandates: ``Fan_Id``, ``Event_Id``, a
        server ``Entry_Timestamp``, ``Ordering_Key``, ``Eligibility_Status =
        WAITING`` and a null ``Batch_Id`` (omitted, since DynamoDB has no null
        placeholder and downstream code treats absent as null). ``Waiting_Shard``
        equals the entry PK so the item is visible in the sparse
        ``WaitingIndex`` while ``WAITING``. ``Elig_PK`` is intentionally absent
        (set only on promotion) so the entry stays out of the ``EligibilityIndex``.
        """
        now_secs = self._clock()
        entry_ts_ms = int(now_secs * 1000)
        ttl = int(now_secs) + self._ttl_secs
        return {
            "PK": {"S": entry_pk},
            "SK": {"S": f"{ENTRY_SK_PREFIX}{ordering_key}"},
            "Fan_Id": {"S": fan_id},
            "Event_Id": {"S": event_id},
            "Write_Shard": {"S": shard_str},
            "Entry_Timestamp": {"N": str(entry_ts_ms)},
            "Eligibility_Status": {"S": WAITING_STATUS},
            "Ordering_Key": {"S": ordering_key},
            # Sparse WaitingIndex PK: present only while WAITING (removed on
            # promotion/expiry), so the promoter reads a shrinking WAITING set.
            "Waiting_Shard": {"S": entry_pk},
            "ttl": {"N": str(ttl)},
        }

    def _build_guard_item(
        self,
        *,
        event_id: str,
        fan_id: str,
        shard_str: str,
        ordering_key: str,
    ) -> dict[str, dict[str, str]]:
        """Build the fan dedupe guard item in low-level AttributeValue form.

        Keyed by ``EVT#<Event_Id>#FAN#<Fan_Id> / ADMISSION`` (extremely high
        cardinality - one per fan, never hot). Stores enough to answer a
        dedupe/lookup ``GetItem`` and to recover the existing Ordering_Key on
        the idempotent-duplicate path.
        """
        return {
            "PK": {"S": self._guard_pk(event_id, fan_id)},
            "SK": {"S": ADMISSION_SK},
            "Ordering_Key": {"S": ordering_key},
            "Write_Shard": {"S": shard_str},
            "Fan_Id": {"S": fan_id},
            "Event_Id": {"S": event_id},
        }

    def _guard_pk(self, event_id: str, fan_id: str) -> str:
        """Return the dedupe-guard partition key ``EVT#<Event_Id>#FAN#<Fan_Id>``."""
        return f"EVT#{event_id}#FAN#{fan_id}"

    # ------------------------------------------------------------------ #
    # Post-commit side effects
    # ------------------------------------------------------------------ #
    def _increment_admit_count(self, entry_pk: str) -> None:
        """Atomically ``ADD Admitted_Count :1`` on the entry's shard partition.

        Best-effort (see the module docstring): a failure here does not undo the
        durable admission, so it is swallowed. The counter rides the same
        evenly-distributed shard key as the entry, so it never becomes hot.
        """
        try:
            self._client.update_item(
                TableName=self._table_name,
                Key={"PK": {"S": entry_pk}, "SK": {"S": ADMIT_COUNT_SK}},
                UpdateExpression="ADD #ac :one",
                ExpressionAttributeNames={"#ac": _ADMITTED_COUNT_ATTR},
                ExpressionAttributeValues={":one": {"N": "1"}},
            )
        except ClientError:
            # Aggregate lag only; the admission is already durable.
            pass

    def _issue_token(
        self, fan_id: str, event_id: str, ordering_key: str, shard_int: int
    ) -> str:
        """Sign and return the Entry_Token for the admitted entry (Req 1.4)."""
        return sign(
            {
                "Fan_Id": fan_id,
                "Event_Id": event_id,
                "Ordering_Key": ordering_key,
                "Write_Shard": shard_int,
            },
            self._secret,
        )

    # ------------------------------------------------------------------ #
    # Idempotent-duplicate path (Req 1.3)
    # ------------------------------------------------------------------ #
    def _existing_admission(
        self, event_id: str, fan_id: str, shard_int: int
    ) -> AdmissionResult:
        """Return the fan's existing entry after a dedupe-guard condition miss.

        Reads the guard item (``GetItem``), recovers its ``Ordering_Key``, and
        returns a result carrying that key and a freshly signed token for it -
        so every duplicate request yields the same Ordering_Key (Req 1.3). If
        the guard cannot be read (a rare race where it was removed between the
        cancelled transaction and this read), the write is reported as
        retryable rather than fabricating an entry.
        """
        response = self._client.get_item(
            TableName=self._table_name,
            Key={"PK": {"S": self._guard_pk(event_id, fan_id)}, "SK": {"S": ADMISSION_SK}},
            ConsistentRead=True,
        )
        item = response.get("Item")
        ordering_key = _attr_str(item or {}, "Ordering_Key")
        if not item or ordering_key is None:
            raise AdmissionError(
                "WRITE_RETRYABLE",
                "dedupe guard reported a duplicate but no entry was found; retry",
            )
        token = self._issue_token(fan_id, event_id, ordering_key, shard_int)
        return AdmissionResult(
            entry_token=token,
            ordering_key=ordering_key,
            fan_id=fan_id,
            event_id=event_id,
            write_shard=shard_int,
            eligibility_status=EligibilityStatus.WAITING,
            duplicate=True,
        )

    # ------------------------------------------------------------------ #
    # Error classification / retry
    # ------------------------------------------------------------------ #
    def _classify_write_error(self, exc: ClientError) -> str:
        """Classify a failed ``transact_write_items`` into a control decision.

        Returns one of:

        * ``"duplicate"`` - the dedupe guard's condition failed; the fan already
          has an entry (Req 1.3).
        * ``"reallocate"`` - only the entry key's condition failed (an
          Ordering_Key collision on an otherwise-new fan); retry with a fresh
          Ordering_Key.
        * ``"retry"`` - a transient throttle/conflict; retry with backoff
          (Req 2.6).
        * ``"raise"`` - anything else; the caller re-raises.
        """
        code = _error_code(exc)

        if code == _TRANSACTION_CANCELLED:
            reasons = _cancellation_reasons(exc)
            guard_reason = _reason_code(reasons, _GUARD_ITEM_INDEX)
            entry_reason = _reason_code(reasons, _ENTRY_ITEM_INDEX)

            # Guard condition miss => idempotent duplicate. Checked first so a
            # genuine duplicate is never mistaken for a retryable conflict.
            if guard_reason == _CONDITIONAL_CHECK_FAILED:
                return "duplicate"
            # Any transient reason on either item => retry the whole transaction.
            if any(r.get("Code") in _RETRYABLE_CANCEL_CODES for r in reasons):
                return "retry"
            # Only the entry key collided (guard was fine) => new Ordering_Key.
            if entry_reason == _CONDITIONAL_CHECK_FAILED:
                return "reallocate"
            return "raise"

        if code in _RETRYABLE_ERROR_CODES:
            return "retry"

        return "raise"

    def _next_retry(self, attempt: int) -> tuple[RetryOutcome, Optional[float]]:
        """Compute the retry outcome/delay for ``attempt`` from the backoff config."""
        if self._rand is None:
            return next_retry(attempt, self._config.backoff)
        return next_retry(attempt, self._config.backoff, self._rand)


# --------------------------------------------------------------------------- #
# Module-level helpers
# --------------------------------------------------------------------------- #
def _error_code(exc: ClientError) -> str | None:
    """Return the top-level DynamoDB error code from a ``ClientError``."""
    return exc.response.get("Error", {}).get("Code")


def _cancellation_reasons(exc: ClientError) -> list[dict[str, Any]]:
    """Return the per-item ``CancellationReasons`` list (empty if absent)."""
    reasons = exc.response.get("CancellationReasons")
    return reasons if isinstance(reasons, list) else []


def _reason_code(reasons: list[dict[str, Any]], index: int) -> str | None:
    """Return the cancellation ``Code`` at ``index``, or ``None`` if out of range."""
    if 0 <= index < len(reasons):
        return reasons[index].get("Code")
    return None


def _attr_str(item: dict[str, Any], attr: str) -> str | None:
    """Extract a string attribute value from a low-level item mapping."""
    value = item.get(attr)
    if not value:
        return None
    return value.get("S")


def _attr_int(item: dict[str, Any], attr: str) -> int | None:
    """Extract a numeric attribute value from a low-level item mapping as int."""
    value = item.get(attr)
    if not value:
        return None
    raw = value.get("N")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
