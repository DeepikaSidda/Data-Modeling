"""Batch-size computation for the Virtual Waiting Room (pure logic).

A promotion cycle may only move fans from ``WAITING`` to ``ELIGIBLE`` up to
three simultaneous limits:

* ``w`` - the number of ``WAITING`` entries actually available to promote
  (can't promote more fans than are waiting),
* ``m`` - the configured ``Max_Batch_Size`` (an operational cap per cycle),
* ``r`` - the *remaining* downstream capacity,
  ``r = Downstream_Capacity - (Eligible_Count + Active_Count)`` (never
  over-saturate the downstream purchasing service).

The batch size is therefore ``min(w, m, max(r, 0))``. The ``max(r, 0)`` guard
makes the result zero whenever remaining capacity is non-positive - i.e. the
``ELIGIBLE`` + ``ACTIVE`` population already meets or exceeds
``Downstream_Capacity`` - so a full or over-subscribed downstream promotes
nobody.

This module is intentionally free of any I/O or DynamoDB dependency: it is the
pure decision function that the ``Batch_Promoter`` data-access layer wraps
around an atomic capacity reservation. Keeping it pure lets the property test
(task 5.2) exercise it across the whole input space.

Requirements: 5.3, 5.4, 6.1, 6.2, 6.3, 6.5.
"""

from __future__ import annotations

__all__ = ["remaining_capacity", "compute_batch_size"]


def remaining_capacity(
    downstream_capacity: int,
    eligible_count: int,
    active_count: int,
) -> int:
    """Return remaining downstream capacity ``r`` (may be negative).

    Computes ``r = Downstream_Capacity - (Eligible_Count + Active_Count)``,
    accounting for both ``ELIGIBLE`` fans awaiting activation and ``ACTIVE``
    fans currently purchasing (Requirement 6.3). The raw value is returned
    without clamping so callers can distinguish "exactly full" (``r == 0``)
    from "over-subscribed" (``r < 0``); :func:`compute_batch_size` applies the
    ``max(r, 0)`` guard.

    All inputs must be non-negative.

    Requirements: 6.3, 6.5.
    """
    _require_non_negative(downstream_capacity=downstream_capacity)
    _require_non_negative(eligible_count=eligible_count)
    _require_non_negative(active_count=active_count)
    return downstream_capacity - (eligible_count + active_count)


def compute_batch_size(
    waiting_count: int,
    max_batch_size: int,
    downstream_capacity: int,
    eligible_count: int,
    active_count: int,
) -> int:
    """Return the number of entries to promote this cycle.

    The result is ``min(w, m, max(r, 0))`` where:

    * ``w`` = ``waiting_count`` - number of ``WAITING`` entries available,
    * ``m`` = ``max_batch_size`` - configured per-cycle maximum,
    * ``r`` = ``downstream_capacity - (eligible_count + active_count)`` -
      remaining downstream capacity.

    Returns ``0`` when ``r <= 0`` (downstream is full or over-subscribed), when
    there are no ``WAITING`` entries, or when the configured batch cap is zero.

    All arguments must be non-negative integers.

    Requirements: 5.3, 5.4, 6.1, 6.2, 6.3, 6.5.
    """
    _require_non_negative(waiting_count=waiting_count)
    _require_non_negative(max_batch_size=max_batch_size)

    remaining = remaining_capacity(
        downstream_capacity=downstream_capacity,
        eligible_count=eligible_count,
        active_count=active_count,
    )
    return min(waiting_count, max_batch_size, max(remaining, 0))


def _require_non_negative(**named_values: int) -> None:
    """Validate that each named argument is a non-negative integer.

    ``bool`` is rejected explicitly: although ``bool`` is a subclass of
    ``int`` in Python, a boolean count is almost certainly a caller mistake.
    """
    for name, value in named_values.items():
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an int, got {type(value).__name__}")
        if value < 0:
            raise ValueError(f"{name} must be non-negative, got {value}")
