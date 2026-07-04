"""Streams aggregator logic for the Virtual Waiting Room (near-pure logic).

The design's **Aggregator** consumes DynamoDB Streams records emitted by the
``WaitingRoom`` table and maintains the derived aggregates that the hot status
path serves out of ElastiCache, so that millions of pollers never trigger any
per-request cross-shard work. This module holds that aggregation as a
**self-contained, in-memory, deterministic** state machine - it performs no
DynamoDB access and no I/O beyond reading an *injectable clock* (the promotion
rate is a function of time, so the clock must be injectable for testability).
Stream records are represented as plain dataclasses / dicts, so nothing here
depends on a live DynamoDB Streams feed.

It maintains three aggregates, each mapping to a design responsibility:

* **Summed admitted total** (``Σ ADMIT_COUNT`` across shards). The design
  admits fans behind a **sharded** admit counter - one ``ADMIT_COUNT`` item
  per write shard, incremented with an atomic ``ADD Admitted_Count :1`` on the
  *same* partition as the entry it counts (never a hot global counter). The
  Aggregator observes each shard counter's stream events and keeps the latest
  ``Admitted_Count`` per shard; the total admitted is their sum. That total
  drives the approximate **queue-full gate** (``total_admitted`` vs
  ``Max_Queue_Size``, Req 1.6) and is the baseline for a fan's
  ``admission_sequence_rank``.

* **Global ``Promoted_Total`` progress** (count of entries promoted out of
  ``WAITING``). Promotion applies a conditional ``WAITING -> ELIGIBLE``
  transition per entry, which the stream surfaces as an entry ``MODIFY`` event
  whose image leaves ``WAITING``. Because promotion is strictly in
  ``Ordering_Key`` order, ``Promoted_Total`` is exactly how many entries have
  left the front, so a fan's **approximate position** is
  ``admission_sequence_rank - Promoted_Total`` (Req 8.3) - served here by
  reusing :func:`waiting_room.position.approximate_position`.

* **Observed promotion rate** (a moving average of promotions/sec). Each
  counted promotion is timestamped; the rate is the number of promotions in a
  trailing window divided by the window length. This is the ``rho`` that the
  status path divides ``Queue_Position`` by to derive ``Estimated_Wait_Time``
  (Req 8.4, see :func:`waiting_room.status_logic.estimated_wait`).

Determinism / idempotency notes:

* The clock and every record timestamp share one time base (seconds). Tests
  inject a controlled clock so the rate is fully reproducible.
* DynamoDB Streams delivers **at-least-once**, so promotions are counted at
  most once per entry key: a redelivered ``WAITING -> ELIGIBLE`` record does
  not double-count, and later non-promotion transitions (e.g.
  ``ELIGIBLE -> ACTIVE``) are ignored.
* Shard admit counts are reconciled with ``max`` so a redelivered or
  out-of-order counter image can never move a shard's total backward
  (``Admitted_Count`` is monotonic).

Requirements: 8.3, 8.4, 1.6.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional

from waiting_room.config import EligibilityStatus
from waiting_room.position import approximate_position

__all__ = [
    "PK_ATTR",
    "SK_ATTR",
    "ADMIT_COUNT_SK",
    "CAPACITY_SK",
    "ENTRY_SK_PREFIX",
    "ADMITTED_COUNT_ATTR",
    "ELIGIBILITY_STATUS_ATTR",
    "DEFAULT_RATE_WINDOW_SECS",
    "StreamRecord",
    "AggregateSnapshot",
    "StreamAggregator",
]


# --------------------------------------------------------------------------- #
# Item-model constants (mirror the design's WaitingRoom key/attribute schema)
# --------------------------------------------------------------------------- #
#: Partition-key attribute name on every ``WaitingRoom`` item.
PK_ATTR: str = "PK"
#: Sort-key attribute name on every ``WaitingRoom`` item.
SK_ATTR: str = "SK"

#: Sort key of a per-shard sharded admit counter item
#: (``PK = EVT#<id>#SH#<shard>``, ``SK = ADMIT_COUNT``).
ADMIT_COUNT_SK: str = "ADMIT_COUNT"
#: Sort key of the per-event capacity counter item (``SK = CAPACITY``).
CAPACITY_SK: str = "CAPACITY"
#: Sort-key prefix of a queue-entry item (``SK = ENTRY#<Ordering_Key>``).
ENTRY_SK_PREFIX: str = "ENTRY#"

#: Numeric attribute holding a shard's cumulative admissions.
ADMITTED_COUNT_ATTR: str = "Admitted_Count"
#: String attribute holding an entry's lifecycle status.
ELIGIBILITY_STATUS_ATTR: str = "Eligibility_Status"

#: Default trailing window (seconds) for the moving-average promotion rate.
DEFAULT_RATE_WINDOW_SECS: float = 10.0


# --------------------------------------------------------------------------- #
# Stream record representation
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class StreamRecord:
    """A minimal, DynamoDB-Streams-shaped record consumed by the aggregator.

    Carries only what aggregation needs: the event kind and the item images.
    ``new_image``/``old_image`` are attribute maps keyed by attribute name.
    Each value may be a plain Python value (``"WAITING"``, ``5``) *or* a
    DynamoDB typed ``AttributeValue`` (``{"S": "WAITING"}``, ``{"N": "5"}``);
    both are accepted so the same type works for hand-built test records and
    for images lifted straight off a real stream.

    ``timestamp`` is the record's creation time in seconds (the analogue of a
    stream record's ``ApproximateCreationDateTime``); when ``None`` the
    aggregator falls back to its injected clock. It must share a time base with
    that clock.
    """

    event_name: str
    new_image: Optional[Mapping[str, Any]] = None
    old_image: Optional[Mapping[str, Any]] = None
    timestamp: Optional[float] = None


@dataclass(frozen=True, slots=True)
class AggregateSnapshot:
    """An immutable snapshot of the three aggregates at a single instant.

    This is the shape the data-access layer would push into ElastiCache: a
    consistent ``(admitted_total, promoted_total, promotion_rate)`` triple that
    the status path reads to answer position and estimated-wait queries.
    """

    admitted_total: int
    promoted_total: int
    promotion_rate: float


# --------------------------------------------------------------------------- #
# Aggregator
# --------------------------------------------------------------------------- #
class StreamAggregator:
    """In-memory aggregator of admitted total, promoted total, and rate.

    Feed it records with :meth:`apply` / :meth:`apply_records`; read the
    derived aggregates via :attr:`admitted_total`, :attr:`promoted_total`, and
    :meth:`promotion_rate`. The instance is *not* thread-safe; a single stream
    consumer owns one aggregator (Streams shards are processed serially).

    Args:
        clock: Zero-arg callable returning the current time in seconds. Used to
            timestamp promotions that arrive without a record timestamp and to
            evaluate the trailing rate window. Defaults to :func:`time.time`.
            Inject a deterministic clock in tests.
        rate_window_secs: Length of the trailing window over which the
            promotion rate is averaged. Must be a positive real number.

    Requirements: 8.3, 8.4, 1.6.
    """

    __slots__ = (
        "_clock",
        "_rate_window_secs",
        "_shard_admitted",
        "_promoted_total",
        "_promoted_keys",
        "_promotion_times",
    )

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.time,
        rate_window_secs: float = DEFAULT_RATE_WINDOW_SECS,
    ) -> None:
        if not callable(clock):
            raise TypeError("clock must be a zero-arg callable returning seconds")
        if isinstance(rate_window_secs, bool) or not isinstance(
            rate_window_secs, (int, float)
        ):
            raise TypeError("rate_window_secs must be a real number")
        if rate_window_secs <= 0:
            raise ValueError("rate_window_secs must be positive")

        self._clock = clock
        self._rate_window_secs = float(rate_window_secs)
        # Latest observed Admitted_Count per shard partition key. Summed on read.
        self._shard_admitted: dict[str, int] = {}
        # Monotonic count of entries promoted out of WAITING.
        self._promoted_total: int = 0
        # Entry keys already counted as promoted (at-least-once dedupe).
        self._promoted_keys: set[tuple[Any, Any]] = set()
        # Timestamps of counted promotions, for the moving-average rate.
        self._promotion_times: deque[float] = deque()

    # ---- ingestion ------------------------------------------------------- #
    def apply(self, record: StreamRecord | Mapping[str, Any]) -> None:
        """Apply a single stream record, updating the aggregates.

        Classifies the record by its item's sort key and routes it:

        * ``SK == ADMIT_COUNT`` -> update that shard's admitted count.
        * ``SK`` starts with ``ENTRY#`` -> detect a ``WAITING -> non-WAITING``
          promotion transition and, if new, count it.
        * anything else (capacity item, dedupe guard, config, ``REMOVE``
          events, unrelated items) is ignored - none of them change the
          admitted or promoted aggregates.

        Malformed or unrelated records are skipped rather than raising, so a
        mixed real stream can be replayed wholesale.
        """
        rec = _coerce_record(record)
        image = rec.new_image
        if image is None:
            # REMOVE (or an image-less record): nothing to aggregate. Admit
            # counts and promoted totals are monotonic and never react to
            # deletions.
            return

        sk = _attr_value(image.get(SK_ATTR))
        if sk == ADMIT_COUNT_SK:
            self._apply_admit_count(image)
        elif isinstance(sk, str) and sk.startswith(ENTRY_SK_PREFIX):
            self._apply_entry(rec, image)
        # else: CAPACITY / ADMISSION / CONFIG / unknown -> ignore.

    def apply_records(self, records: Iterable[StreamRecord | Mapping[str, Any]]) -> None:
        """Apply an iterable of stream records in order."""
        for record in records:
            self.apply(record)

    def _apply_admit_count(self, image: Mapping[str, Any]) -> None:
        """Record the latest ``Admitted_Count`` for a shard counter item.

        Keyed by the shard's partition key so distinct shards accumulate
        independently. Reconciled with ``max`` because ``Admitted_Count`` is
        monotonic: a redelivered or out-of-order image must never lower a
        shard's contribution to the sum.
        """
        shard_pk = _attr_value(image.get(PK_ATTR))
        if shard_pk is None:
            return
        count = _as_int(_attr_value(image.get(ADMITTED_COUNT_ATTR)))
        if count is None or count < 0:
            return
        previous = self._shard_admitted.get(shard_pk, 0)
        if count > previous:
            self._shard_admitted[shard_pk] = count

    def _apply_entry(self, rec: StreamRecord, image: Mapping[str, Any]) -> None:
        """Count a genuine ``WAITING -> non-WAITING`` promotion, once per entry.

        A promotion is recognized only when the entry *left* ``WAITING`` on this
        record - i.e. the old image was ``WAITING`` and the new image is not.
        This precisely captures "promoted out of WAITING": a fresh ``INSERT``
        (new fan, still ``WAITING``) is not a promotion, and a later
        ``ELIGIBLE -> ACTIVE`` transition is not a *second* promotion. The entry
        key guards against at-least-once redelivery double-counting.
        """
        new_status = _normalize_status(_attr_value(image.get(ELIGIBILITY_STATUS_ATTR)))
        if new_status is EligibilityStatus.WAITING or new_status is None:
            # Still waiting (or status absent): not a promotion out of WAITING.
            return

        old_image = rec.old_image
        old_status = (
            _normalize_status(_attr_value(old_image.get(ELIGIBILITY_STATUS_ATTR)))
            if old_image is not None
            else None
        )
        if old_status is not EligibilityStatus.WAITING:
            # Only a WAITING -> non-WAITING edge is a promotion. Records whose
            # old state was already promoted (ELIGIBLE/ACTIVE/...) are ignored.
            return

        key = (_attr_value(image.get(PK_ATTR)), _attr_value(image.get(SK_ATTR)))
        if key in self._promoted_keys:
            return  # already counted this entry's promotion.

        self._promoted_keys.add(key)
        self._promoted_total += 1
        when = rec.timestamp if rec.timestamp is not None else self._clock()
        self._promotion_times.append(float(when))

    # ---- accessors ------------------------------------------------------- #
    @property
    def admitted_total(self) -> int:
        """Summed admitted total ``Σ ADMIT_COUNT`` across all observed shards.

        The baseline for a fan's ``admission_sequence_rank`` and the quantity
        compared against ``Max_Queue_Size`` by the queue-full gate (Req 1.6).
        """
        return sum(self._shard_admitted.values())

    @property
    def promoted_total(self) -> int:
        """Global count of entries promoted out of ``WAITING`` (monotonic).

        Feeds the approximate position ``admission_sequence_rank -
        promoted_total`` (Req 8.3).
        """
        return self._promoted_total

    @property
    def shard_count_seen(self) -> int:
        """Number of distinct shard admit counters observed so far."""
        return len(self._shard_admitted)

    def per_shard_admitted(self) -> dict[str, int]:
        """Return a copy of the latest ``Admitted_Count`` per shard partition."""
        return dict(self._shard_admitted)

    def promotion_rate(self, now: Optional[float] = None) -> float:
        """Return the observed promotion rate (promotions/sec) as a moving average.

        Computed as the number of promotions counted within the trailing
        ``rate_window_secs`` window ending at ``now``, divided by the window
        length. ``now`` defaults to the injected clock. Promotions older than
        the window are evicted so the rate tracks recent throughput; with no
        recent promotions the rate is ``0.0``.

        This is the ``rho`` the status path feeds into
        :func:`waiting_room.status_logic.estimated_wait` (Req 8.4).
        """
        current = self._clock() if now is None else now
        cutoff = current - self._rate_window_secs
        times = self._promotion_times
        # Evict promotions that have fallen out of the trailing window.
        while times and times[0] <= cutoff:
            times.popleft()
        return len(times) / self._rate_window_secs

    def snapshot(self, now: Optional[float] = None) -> AggregateSnapshot:
        """Return a consistent ``(admitted, promoted, rate)`` triple.

        Convenience for publishing all three aggregates together (the payload
        the data-access layer would write to ElastiCache).
        """
        return AggregateSnapshot(
            admitted_total=self.admitted_total,
            promoted_total=self._promoted_total,
            promotion_rate=self.promotion_rate(now),
        )

    # ---- derived helpers (reuse the pure-logic layer) -------------------- #
    def is_queue_full(self, max_queue_size: int) -> bool:
        """Return whether the summed admitted total has reached ``max_queue_size``.

        The approximate queue-full gate (Req 1.6): the admit counter aggregate
        lags by at most one in-flight batch per shard, so this is a soft safety
        valve, not a hard boundary. ``max_queue_size`` must be a positive int.
        """
        if isinstance(max_queue_size, bool) or not isinstance(max_queue_size, int):
            raise TypeError("max_queue_size must be an int")
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be positive")
        return self.admitted_total >= max_queue_size

    def approximate_position(self, admission_sequence_rank: int) -> int:
        """Return the approximate queue position for a fan's admission rank.

        Delegates to :func:`waiting_room.position.approximate_position` using
        the current ``promoted_total``: ``max(admission_sequence_rank -
        promoted_total, 1)`` (Req 8.3).
        """
        return approximate_position(admission_sequence_rank, self._promoted_total)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"{type(self).__name__}(admitted_total={self.admitted_total}, "
            f"promoted_total={self._promoted_total}, "
            f"shards={len(self._shard_admitted)})"
        )


# --------------------------------------------------------------------------- #
# Record / value coercion helpers
# --------------------------------------------------------------------------- #
def _coerce_record(record: StreamRecord | Mapping[str, Any]) -> StreamRecord:
    """Return a :class:`StreamRecord` from a record instance or a plain mapping.

    Accepts a :class:`StreamRecord` directly, or a mapping using either
    snake_case keys (``event_name``, ``new_image``, ``old_image``,
    ``timestamp``) or DynamoDB-Streams-style keys (``eventName``, ``NewImage``,
    ``OldImage``, ``ApproximateCreationDateTime``).
    """
    if isinstance(record, StreamRecord):
        return record
    if not isinstance(record, Mapping):
        raise TypeError(
            "record must be a StreamRecord or a mapping, got "
            f"{type(record).__name__}"
        )
    event_name = record.get("event_name", record.get("eventName", ""))
    new_image = record.get("new_image", record.get("NewImage"))
    old_image = record.get("old_image", record.get("OldImage"))
    timestamp = record.get(
        "timestamp", record.get("ApproximateCreationDateTime")
    )
    return StreamRecord(
        event_name=event_name,
        new_image=new_image,
        old_image=old_image,
        timestamp=timestamp,
    )


def _attr_value(value: Any) -> Any:
    """Unwrap a DynamoDB typed ``AttributeValue`` to its Python value.

    A DynamoDB stream image renders attributes as single-key type descriptors,
    e.g. ``{"S": "WAITING"}`` or ``{"N": "5"}``. This returns the inner value
    for the common scalar descriptors and passes plain values through
    unchanged, so both hand-built and real-stream images work.
    """
    if isinstance(value, Mapping) and len(value) == 1:
        (tag, inner), = value.items()
        if tag in ("S", "N", "B", "BOOL"):
            return inner
        if tag == "NULL":
            return None
    return value


def _as_int(value: Any) -> Optional[int]:
    """Coerce a stream attribute value to ``int``, or ``None`` if not possible.

    DynamoDB numbers arrive as strings (``"5"``) or ``Decimal``; both convert
    cleanly. ``bool`` is treated as absent because a boolean count is a
    malformed image rather than a real admission total.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_status(value: Any) -> Optional[EligibilityStatus]:
    """Coerce a status value to :class:`EligibilityStatus`, or ``None``.

    Accepts an :class:`EligibilityStatus` instance or its string value
    (``"WAITING"``); returns ``None`` for anything unrecognized so an entry
    with a missing/garbled status is simply treated as "not a promotion".
    """
    if isinstance(value, EligibilityStatus):
        return value
    if isinstance(value, str):
        try:
            return EligibilityStatus(value)
        except ValueError:
            return None
    return None
