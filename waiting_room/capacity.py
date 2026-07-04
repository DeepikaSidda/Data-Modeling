"""Atomic capacity-reservation model for the Virtual Waiting Room.

This module is a **pure, in-memory model** of the single DynamoDB ``CAPACITY``
item (``PK = EVT#<Event_Id>``, ``SK = CAPACITY``) that guards the downstream
purchasing service against over-promotion. It contains no ``boto3`` / I/O and
is the logic exercised by the concurrency property tests (Property 11 and
Property 14) via a deterministic interleaving explorer.

Design mapping (see design.md - "Downstream Over-Promotion Prevention"):

* The ``CAPACITY`` item holds ``Downstream_Capacity``, ``Eligible_Count``,
  ``Active_Count``, ``Promoted_Total`` and ``Version``.
* A reservation is the analogue of the conditional ``UpdateItem``::

      UpdateExpression: ADD Eligible_Count :n, Promoted_Total :n
      ConditionExpression: Eligible_Count + Active_Count + :n <= Downstream_Capacity

  The promoter reads ``remaining = Downstream_Capacity - (Eligible_Count +
  Active_Count)`` and grants ``:n = min(requested, max(remaining, 0))``. If a
  concurrent grant changed the item, the conditional write fails and the
  promoter re-reads and retries (optimistic concurrency).

To make that optimistic-concurrency behavior *deterministically* reproducible
for property tests, the model exposes an explicit compare-and-set flow:

    state = counter.read()                     # GetItem
    plan  = counter.plan_reserve(requested, state=state)
    ok    = counter.try_commit_reserve(plan)   # conditional UpdateItem

``try_commit_reserve`` applies the grant **only if** the item's ``Version`` is
unchanged since ``plan`` was computed; otherwise it fails (returns ``False``)
exactly as a conditional ``UpdateItem`` would, and the caller re-plans. An
interleaving explorer can therefore drive any ordering of ``read`` / ``commit``
steps across concurrent promoters and observe that the invariant

    Eligible_Count + Active_Count <= Downstream_Capacity

is *never* violated (Requirements 6.1-6.5).

Terminal / lifecycle transitions are modeled as atomic decrements/moves that
free capacity by exactly the moved count (Requirement 7.5):

* ``activate``  - ELIGIBLE -> ACTIVE (moves a unit; net zero to the sum).
* ``expire``    - ELIGIBLE -> EXPIRED (frees a slot).
* ``complete``  - ACTIVE -> COMPLETED (frees a slot).

Active-pool regulation (driving ``Active_Count`` toward ``Active_Target``
within a tolerance band) is intentionally **not** implemented here - that is a
separate task (6.4). This module is structured so that regulation logic can be
layered on top of :class:`CapacityCounter` without changing it.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 7.5.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from waiting_room.config import EventConfig

__all__ = [
    "CapacityPool",
    "CapacityState",
    "ReservationPlan",
    "CapacityCounter",
]


class CapacityPool(str, Enum):
    """A counted pool on the capacity item that occupies downstream capacity.

    Both ``ELIGIBLE`` and ``ACTIVE`` fans hold a downstream slot, so both count
    against ``Downstream_Capacity`` (Requirement 6.3). Backed by ``str`` so it
    aligns with the corresponding ``Eligibility_Status`` values.
    """

    ELIGIBLE = "ELIGIBLE"
    ACTIVE = "ACTIVE"


@dataclass(frozen=True, slots=True)
class CapacityState:
    """An immutable snapshot of the ``CAPACITY`` item.

    This is what :meth:`CapacityCounter.read` returns - the analogue of a
    ``GetItem`` on the capacity item. It is frozen so a promoter can hold a
    consistent view while it computes a reservation, then attempt to commit it
    against a possibly-changed live counter.

    Requirements: 6.1, 6.3, 6.5.
    """

    #: Maximum concurrently ELIGIBLE + ACTIVE fans the downstream can serve.
    downstream_capacity: int
    #: Fans promoted to ELIGIBLE and awaiting activation.
    eligible_count: int = 0
    #: Fans currently ACTIVE (holding a purchasing slot).
    active_count: int = 0
    #: Monotonic count of all promotions ever granted (never decremented).
    promoted_total: int = 0
    #: Optimistic-concurrency version; bumped on every mutation.
    version: int = 0

    @property
    def occupied(self) -> int:
        """Slots currently occupied: ``Eligible_Count + Active_Count``.

        This is the quantity capped by ``Downstream_Capacity`` (Requirement
        6.3 - capacity accounts for both ELIGIBLE and ACTIVE fans).
        """
        return self.eligible_count + self.active_count

    @property
    def remaining(self) -> int:
        """Available capacity: ``Downstream_Capacity - occupied``, floored at 0.

        Requirement 6.5 - remaining capacity is the configured capacity minus
        the combined ELIGIBLE + ACTIVE count. Floored at zero so an
        over-subscribed counter never reports negative headroom.
        """
        return max(self.downstream_capacity - self.occupied, 0)

    @property
    def invariant_holds(self) -> bool:
        """Whether ``Eligible_Count + Active_Count <= Downstream_Capacity``.

        The core over-promotion-prevention invariant (Requirement 6.1, 6.2).
        """
        return self.occupied <= self.downstream_capacity


@dataclass(frozen=True, slots=True)
class ReservationPlan:
    """A planned reservation computed against a specific counter snapshot.

    Produced by :meth:`CapacityCounter.plan_reserve` and consumed by
    :meth:`CapacityCounter.try_commit_reserve`. It records the ``granted``
    amount (already clamped to the snapshot's remaining capacity) and the
    ``expected_version`` the grant was computed against, so the commit can
    enforce compare-and-set semantics.

    Requirements: 6.4 (concurrency coordination).
    """

    #: The amount the promoter asked to reserve.
    requested: int
    #: The amount that may actually be granted against the planned snapshot,
    #: i.e. ``min(requested, max(remaining, 0))``.
    granted: int
    #: The counter ``Version`` this plan was computed against. The commit only
    #: succeeds if the live version still matches.
    expected_version: int


class CapacityCounter:
    """Mutable in-memory model of the atomic DynamoDB ``CAPACITY`` item.

    All mutations bump :attr:`version`, mirroring a write to the underlying
    item. Reservations may be applied either directly (:meth:`reserve`, the
    uncontended convenience path) or via the explicit compare-and-set flow
    (:meth:`plan_reserve` + :meth:`try_commit_reserve`) used by the
    deterministic interleaving explorer in the concurrency property tests.

    The invariant ``Eligible_Count + Active_Count <= Downstream_Capacity`` is
    enforced on every path and can never be broken by :meth:`reserve`
    (Requirements 6.1, 6.2, 6.4).
    """

    __slots__ = (
        "_downstream_capacity",
        "_eligible_count",
        "_active_count",
        "_promoted_total",
        "_version",
    )

    def __init__(
        self,
        downstream_capacity: int,
        *,
        eligible_count: int = 0,
        active_count: int = 0,
        promoted_total: int = 0,
        version: int = 0,
    ) -> None:
        if downstream_capacity <= 0:
            raise ValueError("downstream_capacity must be positive")
        if eligible_count < 0 or active_count < 0:
            raise ValueError("counts must be non-negative")
        if promoted_total < 0:
            raise ValueError("promoted_total must be non-negative")
        if version < 0:
            raise ValueError("version must be non-negative")
        if eligible_count + active_count > downstream_capacity:
            raise ValueError(
                "initial Eligible_Count + Active_Count exceeds Downstream_Capacity"
            )
        self._downstream_capacity = downstream_capacity
        self._eligible_count = eligible_count
        self._active_count = active_count
        self._promoted_total = promoted_total
        self._version = version

    # ------------------------------------------------------------------ #
    # Constructors
    # ------------------------------------------------------------------ #
    @classmethod
    def for_event(cls, config: EventConfig) -> "CapacityCounter":
        """Create an empty counter sized from an :class:`EventConfig`."""
        return cls(downstream_capacity=config.downstream_capacity)

    @classmethod
    def from_state(cls, state: CapacityState) -> "CapacityCounter":
        """Rehydrate a counter from a :class:`CapacityState` snapshot."""
        return cls(
            downstream_capacity=state.downstream_capacity,
            eligible_count=state.eligible_count,
            active_count=state.active_count,
            promoted_total=state.promoted_total,
            version=state.version,
        )

    # ------------------------------------------------------------------ #
    # Read-only accessors
    # ------------------------------------------------------------------ #
    @property
    def version(self) -> int:
        """Current optimistic-concurrency version."""
        return self._version

    @property
    def remaining(self) -> int:
        """Available capacity right now (see :attr:`CapacityState.remaining`)."""
        return max(self._downstream_capacity - self.occupied, 0)

    @property
    def occupied(self) -> int:
        """Currently occupied slots: ``Eligible_Count + Active_Count``."""
        return self._eligible_count + self._active_count

    @property
    def invariant_holds(self) -> bool:
        """Whether the over-promotion invariant currently holds."""
        return self.occupied <= self._downstream_capacity

    def read(self) -> CapacityState:
        """Return an immutable snapshot of the counter (models ``GetItem``)."""
        return CapacityState(
            downstream_capacity=self._downstream_capacity,
            eligible_count=self._eligible_count,
            active_count=self._active_count,
            promoted_total=self._promoted_total,
            version=self._version,
        )

    # ------------------------------------------------------------------ #
    # Reservation - optimistic compare-and-set flow (for interleaving)
    # ------------------------------------------------------------------ #
    def plan_reserve(
        self, requested: int, *, state: CapacityState | None = None
    ) -> ReservationPlan:
        """Compute a reservation against a snapshot without applying it.

        Pure with respect to the counter: it reads (or accepts) a snapshot and
        returns the amount that *could* be granted, clamped so that applying it
        would keep ``Eligible_Count + Active_Count <= Downstream_Capacity``::

            granted = min(requested, max(remaining, 0))

        The returned :class:`ReservationPlan` carries the snapshot ``Version``
        so :meth:`try_commit_reserve` can enforce compare-and-set semantics.

        Requirements: 6.1, 6.2, 6.5.
        """
        if requested < 0:
            raise ValueError("requested must be non-negative")
        snapshot = state if state is not None else self.read()
        granted = min(requested, snapshot.remaining)
        return ReservationPlan(
            requested=requested,
            granted=granted,
            expected_version=snapshot.version,
        )

    def try_commit_reserve(self, plan: ReservationPlan) -> bool:
        """Attempt to apply a planned reservation with compare-and-set.

        Models the conditional ``UpdateItem``: the grant is applied **only if**
        the live ``Version`` still equals ``plan.expected_version``. If a
        concurrent mutation bumped the version, the commit fails (returns
        ``False``) and the caller must re-read and re-plan - exactly the
        optimistic retry the design prescribes. This is what guarantees that no
        interleaving of concurrent reservations can ever over-promote
        (Requirement 6.4).

        On success the counter applies ``Eligible_Count += granted`` and
        ``Promoted_Total += granted`` and bumps ``Version``; a zero grant is a
        successful no-op that still confirms the version matched.
        """
        if plan.expected_version != self._version:
            return False
        # Version matched => the snapshot the plan was computed against is still
        # live, so the clamped grant is guaranteed to preserve the invariant.
        # Defensive re-check keeps the invariant true even if callers hand-craft
        # a plan.
        if self.occupied + plan.granted > self._downstream_capacity:
            return False
        if plan.granted:
            self._eligible_count += plan.granted
            self._promoted_total += plan.granted
        self._version += 1
        return True

    # ------------------------------------------------------------------ #
    # Reservation - uncontended convenience path
    # ------------------------------------------------------------------ #
    def reserve(self, requested: int) -> int:
        """Atomically reserve up to ``requested`` slots and return the grant.

        Equivalent to a single uncontended ``plan_reserve`` +
        ``try_commit_reserve`` cycle. Returns ``granted = min(requested,
        max(remaining, 0))``; grants ``0`` when no capacity remains
        (Requirement 6.2). The invariant
        ``Eligible_Count + Active_Count <= Downstream_Capacity`` always holds
        afterwards (Requirement 6.1).
        """
        plan = self.plan_reserve(requested)
        committed = self.try_commit_reserve(plan)
        # Uncontended: version cannot have changed between plan and commit.
        assert committed, "uncontended reservation must commit"
        return plan.granted

    # ------------------------------------------------------------------ #
    # Lifecycle transitions that move / free capacity
    # ------------------------------------------------------------------ #
    def activate(self, count: int = 1) -> None:
        """Move ``count`` fans ELIGIBLE -> ACTIVE (net zero to occupied slots).

        Requirement 6.3 - both ELIGIBLE and ACTIVE fans occupy capacity, so an
        activation is a transfer that leaves ``occupied`` unchanged.
        """
        if count < 0:
            raise ValueError("count must be non-negative")
        if count > self._eligible_count:
            raise ValueError("cannot activate more fans than are ELIGIBLE")
        if count:
            self._eligible_count -= count
            self._active_count += count
        self._version += 1

    def release(self, count: int = 1, *, pool: CapacityPool = CapacityPool.ACTIVE) -> None:
        """Free ``count`` occupied slots from the given ``pool``.

        This is the primitive behind terminal transitions: an ELIGIBLE fan that
        EXPIRES frees an ELIGIBLE slot; an ACTIVE fan that COMPLETES frees an
        ACTIVE slot. Freeing ``count`` slots increases available capacity by
        exactly ``count`` (Requirement 7.5). ``Promoted_Total`` is monotonic and
        deliberately not decremented.
        """
        if count < 0:
            raise ValueError("count must be non-negative")
        if pool is CapacityPool.ACTIVE:
            if count > self._active_count:
                raise ValueError("cannot release more slots than are ACTIVE")
            self._active_count -= count
        else:  # CapacityPool.ELIGIBLE
            if count > self._eligible_count:
                raise ValueError("cannot release more slots than are ELIGIBLE")
            self._eligible_count -= count
        self._version += 1

    def complete(self, count: int = 1) -> None:
        """Transition ``count`` fans ACTIVE -> COMPLETED, freeing their slots.

        Requirement 7.5 - a terminal completion frees exactly ``count`` slots.
        """
        self.release(count, pool=CapacityPool.ACTIVE)

    def expire(self, count: int = 1) -> None:
        """Transition ``count`` fans ELIGIBLE -> EXPIRED, freeing their slots.

        Requirement 7.5 (and 5.8) - an expiry frees exactly ``count`` slots.
        """
        self.release(count, pool=CapacityPool.ELIGIBLE)

    # ------------------------------------------------------------------ #
    # Debug / repr
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            "CapacityCounter("
            f"downstream_capacity={self._downstream_capacity}, "
            f"eligible_count={self._eligible_count}, "
            f"active_count={self._active_count}, "
            f"promoted_total={self._promoted_total}, "
            f"version={self._version})"
        )
