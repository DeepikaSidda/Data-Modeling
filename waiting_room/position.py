"""Queue-position computation for the Virtual Waiting Room (pure logic).

A fan's ``Queue_Position`` is their ordinal rank within an event queue, with
position ``1`` at the front. This module provides the two ways the design
computes it, both free of any I/O or DynamoDB dependency:

* :func:`queue_position` - the **exact** count path used for audit and
  verification. It computes
  ``Queue_Position = 1 + count(WAITING entries with Ordering_Key < mine)``
  using the strict total order defined by
  :func:`waiting_room.ordering.compare_ordering_keys`. Only entries whose
  ``Eligibility_Status`` is ``WAITING`` are counted, because promoted entries
  (``ELIGIBLE``/``ACTIVE``/``EXPIRED``/``COMPLETED``) have left the front of
  the line. The front-most ``WAITING`` entry therefore holds position ``1``,
  and the number of fans ahead equals ``Queue_Position - 1`` (Req 8.3).

* :func:`approximate_position` - the **hot-path** estimate served from cached
  aggregates. Because promotion is strictly in ``Ordering_Key`` order,
  ``Promoted_Total`` is exactly how many entries have left the front, so a
  fan's approximate position is ``admission_sequence_rank - Promoted_Total``,
  floored at ``1`` (a fan never reports a position better than the front).

Keeping this pure lets the property test (task 4.2) exercise
:func:`queue_position` across the whole input space and assert
``Queue_Position(f) = 1 + |{g : g WAITING AND g.Ordering_Key < f.Ordering_Key}|``.

Requirements: 8.3.
"""

from __future__ import annotations

from typing import Iterable, NamedTuple, Union

from waiting_room.config import EligibilityStatus
from waiting_room.ordering import compare_ordering_keys

__all__ = [
    "QueueEntryView",
    "EntryLike",
    "queue_position",
    "fans_ahead",
    "approximate_position",
]


class QueueEntryView(NamedTuple):
    """A lightweight, ordering-relevant view of a Queue_Entry.

    Carries only the two attributes position computation needs: the entry's
    ``Ordering_Key`` and its ``Eligibility_Status``. Being a ``NamedTuple`` it
    is also a plain ``(ordering_key, status)`` 2-tuple, so callers may pass
    either these views or bare 2-tuples to :func:`queue_position`.
    """

    ordering_key: str
    status: EligibilityStatus


#: An entry accepted by :func:`queue_position`: either a :class:`QueueEntryView`
#: (or any object exposing ``ordering_key`` and ``status``) or a bare
#: ``(ordering_key, status)`` 2-tuple.
EntryLike = Union["QueueEntryView", tuple]


def _coerce(entry: EntryLike) -> tuple[str, object]:
    """Return ``(ordering_key, status)`` from an entry view, object, or tuple.

    Accepts anything exposing ``.ordering_key`` and ``.status`` attributes, or
    a 2-element ``(ordering_key, status)`` sequence. Raises ``TypeError`` for
    anything else so malformed inputs fail loudly rather than miscount.
    """
    ordering_key = getattr(entry, "ordering_key", None)
    status = getattr(entry, "status", None)
    if ordering_key is not None or status is not None:
        return ordering_key, status
    # Fall back to a 2-element sequence (e.g. a plain tuple).
    try:
        ordering_key, status = entry  # type: ignore[misc]
    except (TypeError, ValueError) as exc:
        raise TypeError(
            "entry must expose 'ordering_key' and 'status' or be a "
            "(ordering_key, status) 2-tuple"
        ) from exc
    return ordering_key, status


def fans_ahead(entry_ordering_key: str, entries: Iterable[EntryLike]) -> int:
    """Return the number of ``WAITING`` fans strictly ahead of the given key.

    Counts entries whose ``Eligibility_Status`` is ``WAITING`` and whose
    ``Ordering_Key`` is strictly less than ``entry_ordering_key`` under the
    ordering comparator. This is exactly ``Queue_Position - 1`` (Req 8.3).

    The querying entry itself is naturally excluded: an equal ``Ordering_Key``
    does not compare "less than", so an entry never counts as ahead of itself.
    """
    ahead = 0
    for entry in entries:
        ordering_key, status = _coerce(entry)
        if status is not EligibilityStatus.WAITING:
            continue
        if compare_ordering_keys(ordering_key, entry_ordering_key) < 0:
            ahead += 1
    return ahead


def queue_position(entry_ordering_key: str, entries: Iterable[EntryLike]) -> int:
    """Return the exact ``Queue_Position`` for the given ``Ordering_Key``.

    Computes ``1 + count(WAITING entries with Ordering_Key < mine)`` using the
    ordering comparator, so:

    * the front-most ``WAITING`` entry holds position ``1``,
    * the number of fans ahead equals ``Queue_Position - 1``,
    * only ``WAITING`` entries are counted (promoted entries have left the
      front and do not contribute), and
    * recomputing over the same set is idempotent because the comparator is a
      strict total order.

    ``entries`` may include the querying entry itself and entries of any
    status; both are handled correctly (the querying entry never counts as
    ahead of itself, and non-``WAITING`` entries are ignored).

    Requirements: 8.3.
    """
    return 1 + fans_ahead(entry_ordering_key, entries)


def approximate_position(admission_sequence_rank: int, promoted_total: int) -> int:
    """Return the cached-aggregate approximate position, floored at ``1``.

    ``admission_sequence_rank`` is the fan's 1-based ordinal at admission time
    (how many fans, including this one, had been admitted). ``promoted_total``
    is the number of entries promoted out of ``WAITING`` so far. Because
    promotion is strictly in ``Ordering_Key`` order, ``Promoted_Total`` is
    exactly how many entries have left the front, so::

        approximate_position = admission_sequence_rank - promoted_total

    The result is floored at ``1``: a fan never reports a position ahead of the
    front of the line, even if the cached ``promoted_total`` transiently runs
    ahead of this fan's rank (bounded staleness on the hot path). Both inputs
    must be non-negative integers.

    Requirements: 8.3.
    """
    _require_non_negative(admission_sequence_rank=admission_sequence_rank)
    _require_non_negative(promoted_total=promoted_total)
    return max(admission_sequence_rank - promoted_total, 1)


def _require_non_negative(**named_values: int) -> None:
    """Validate that each named argument is a non-negative integer.

    ``bool`` is rejected explicitly: although ``bool`` is a subclass of
    ``int`` in Python, a boolean rank/total is almost certainly a mistake.
    """
    for name, value in named_values.items():
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an int, got {type(value).__name__}")
        if value < 0:
            raise ValueError(f"{name} must be non-negative, got {value}")
