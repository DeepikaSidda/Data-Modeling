"""Active-pool regulation for the Virtual Waiting Room (pure logic).

This module is the **pure decision function** behind the optional active-pool
regulation stretch goal (Requirement 7). When enabled, the ``Batch_Promoter``
treats ``Active_Target`` (~1,000) as a steady-state goal and keeps the pool of
committed fans near that target within a tolerance band, *always* subordinate
to ``Downstream_Capacity``.

It builds on :mod:`waiting_room.capacity` (the atomic capacity counter) but
deliberately does **not** modify it: regulation is layered on top so the
capacity invariant (``Eligible_Count + Active_Count <= Downstream_Capacity``)
is untouched. This module contains no I/O or ``boto3`` dependency, so the
property test (task 6.5) can exercise it across the whole input space.

Design mapping (see design.md - "Active-Pool Regulation (Stretch)"):

* **The pool being regulated is the *committed* pool** - fans that are already
  ``ACTIVE`` *plus* fans that are ``ELIGIBLE`` and therefore in-flight toward
  becoming ``ACTIVE``. We call this the *projected active* count,
  ``projected = active_count + eligible_count``. Counting ``ELIGIBLE`` fans is
  what prevents a cycle from over-promoting: if we ignored them we would keep
  requesting ``target - active`` every cycle even though a batch is already
  queued to activate.
* **Incremental refill (Req 7.2, 7.3).** As slots free (``COMPLETED`` /
  ``EXPIRED`` decrement the counter), the projected count falls and the next
  cycle refills back toward ``target`` - it never waits for the pool to fully
  drain.
* **Tolerance band ``[target - tolerance, target]`` (Req 7.4).** To avoid
  thrashing, a refill is only requested once the projected count falls *below*
  the band's lower edge (``target - tolerance``); when it does, the pool is
  refilled up toward ``target`` (the top of the band). Within the band the
  regulator is quiescent.
* **Capacity always dominates (Req 7.6).** The refill is clamped to the
  remaining capacity headroom ``Downstream_Capacity - projected`` so that
  honoring the band can never push the pool over ``Downstream_Capacity``. When
  ``target`` (or the band) would require exceeding capacity, capacity wins and
  the band is intentionally broken.

Requirements: 7.1, 7.2, 7.3, 7.4, 7.6.
"""

from __future__ import annotations

__all__ = [
    "projected_active",
    "capacity_headroom",
    "band_lower_bound",
    "needs_refill",
    "refill_target",
]


def projected_active(active_count: int, eligible_count: int) -> int:
    """Return the *committed* pool size ``active_count + eligible_count``.

    ``ELIGIBLE`` fans are in-flight toward ``ACTIVE`` (they already hold a
    downstream slot), so both counts are committed to the active pool. This is
    the quantity the regulator drives toward ``Active_Target`` and the quantity
    capped by ``Downstream_Capacity`` (mirroring
    :attr:`waiting_room.capacity.CapacityState.occupied`).

    Requirements: 7.1, 7.4.
    """
    _require_non_negative(active_count=active_count, eligible_count=eligible_count)
    return active_count + eligible_count


def capacity_headroom(
    active_count: int,
    eligible_count: int,
    downstream_capacity: int,
) -> int:
    """Return remaining capacity ``Downstream_Capacity - projected``, floored at 0.

    This is the hard ceiling on any refill: promoting more than this would push
    ``Eligible_Count + Active_Count`` over ``Downstream_Capacity``. Floored at
    zero so an already-full (or over-subscribed) pool reports no headroom.

    Requirements: 7.6.
    """
    _require_non_negative(downstream_capacity=downstream_capacity)
    projected = projected_active(active_count, eligible_count)
    return max(downstream_capacity - projected, 0)


def band_lower_bound(active_target: int, tolerance: int) -> int:
    """Return the tolerance band's lower edge ``active_target - tolerance``.

    The regulation band is ``[active_target - tolerance, active_target]``. The
    value may be negative when ``tolerance > active_target``; callers compare
    the projected count against it directly, so a negative lower edge simply
    means the regulator never triggers a refill.

    Requirements: 7.4.
    """
    _require_non_negative(active_target=active_target, tolerance=tolerance)
    return active_target - tolerance


def needs_refill(
    active_count: int,
    eligible_count: int,
    active_target: int,
    tolerance: int,
) -> bool:
    """Return whether the projected pool has fallen below the band.

    ``True`` iff ``projected < active_target - tolerance`` - i.e. the committed
    ``ELIGIBLE + ACTIVE`` population has dropped below the band's lower edge and
    a refill toward ``active_target`` should be requested. While the projected
    count sits anywhere within ``[active_target - tolerance, active_target]``
    (or above it) the regulator stays quiescent to avoid thrashing.

    Requirements: 7.2, 7.4.
    """
    projected = projected_active(active_count, eligible_count)
    return projected < band_lower_bound(active_target, tolerance)


def refill_target(
    active_count: int,
    eligible_count: int,
    downstream_capacity: int,
    active_target: int,
    tolerance: int,
) -> int:
    """Return how many additional promotions to request this cycle.

    Drives the committed pool (``ACTIVE + ELIGIBLE``) toward ``active_target``:

    #. Do nothing unless the projected count has fallen *below* the band's
       lower edge ``active_target - tolerance`` (anti-thrash hysteresis).
       Returns ``0`` while the pool is within band or above target
       (Req 7.2, 7.4).
    #. When below the band, aim to refill back up to ``active_target`` - a
       ``want = active_target - projected`` shortfall (Req 7.1, 7.2).
    #. Clamp the request to the remaining capacity headroom
       ``Downstream_Capacity - projected`` so the pool can never exceed
       ``Downstream_Capacity``; the capacity limit always dominates the band,
       even when honoring the band would require exceeding capacity
       (Req 7.3, 7.6).

    The result is therefore::

        min(max(active_target - projected, 0), max(downstream_capacity - projected, 0))
        # ... but only when projected < active_target - tolerance, else 0

    and satisfies ``projected + refill_target(...) <= Downstream_Capacity`` and
    ``projected + refill_target(...) <= max(active_target, projected)`` for all
    inputs.

    All arguments must be non-negative integers.

    Requirements: 7.1, 7.2, 7.3, 7.4, 7.6.
    """
    _require_non_negative(downstream_capacity=downstream_capacity)

    if not needs_refill(active_count, eligible_count, active_target, tolerance):
        return 0

    projected = projected_active(active_count, eligible_count)
    want = active_target - projected  # > 0 here: projected < target - tolerance <= target
    headroom = max(downstream_capacity - projected, 0)
    return min(want, headroom)


def _require_non_negative(**named_values: int) -> None:
    """Validate that each named argument is a non-negative integer.

    ``bool`` is rejected explicitly: although ``bool`` is a subclass of ``int``
    in Python, a boolean count is almost certainly a caller mistake. Mirrors the
    validation used by :mod:`waiting_room.batching` for a consistent contract.
    """
    for name, value in named_values.items():
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an int, got {type(value).__name__}")
        if value < 0:
            raise ValueError(f"{name} must be non-negative, got {value}")
