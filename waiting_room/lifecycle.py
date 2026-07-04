"""Eligibility lifecycle state machine for the Virtual Waiting Room.

This module is a **pure, dependency-free** model of the Queue_Entry lifecycle.
It contains no ``boto3`` / I/O and no persistence concern: it answers a single
question - *is a requested* ``(from_status, to_status)`` *transition permitted,
and if so what is the resulting status?* The DynamoDB data-access layer (a
later task) layers conditional ``UpdateItem`` writes on top of this validator
so that concurrent transitions cannot corrupt state; the *rules* those writes
enforce live here.

Design mapping (see design.md - "Eligibility Lifecycle Integrity")::

    [*] --> WAITING
    WAITING  --> ELIGIBLE
    ELIGIBLE --> ACTIVE
    ELIGIBLE --> EXPIRED
    ACTIVE   --> COMPLETED
    EXPIRED  --> [*]
    COMPLETED --> [*]

All five :class:`~waiting_room.config.EligibilityStatus` members are valid
*states* an entry may hold; only the four transitions above are permitted
*edges*. ``EXPIRED`` and ``COMPLETED`` are terminal - no outbound transition
leaves them.

Public surface:

* :data:`ALLOWED_TRANSITIONS` - the frozen set of permitted ``(from, to)`` edges.
* :func:`is_allowed` - total predicate over any ``(from, to)`` pair.
* :func:`transition` - returns the resulting status for a permitted transition,
  or raises :class:`IllegalTransitionError` (leaving state unchanged) otherwise.
* :func:`validate` - raises :class:`IllegalTransitionError` on a disallowed
  transition, returns ``None`` on success (assertion-style helper).
* :func:`is_terminal` - whether a status has no outbound transitions.

Requirements: 10.1, 10.2, 10.4.
"""

from __future__ import annotations

from waiting_room.config import EligibilityStatus

__all__ = [
    "ALLOWED_TRANSITIONS",
    "IllegalTransitionError",
    "is_allowed",
    "transition",
    "validate",
    "is_terminal",
    "allowed_targets",
]


#: The complete set of permitted lifecycle edges ``(from_status, to_status)``.
#: Every one of the five :class:`EligibilityStatus` members is a valid state,
#: but only these four transitions are permitted (Req 10.1). The set is frozen
#: so it cannot be mutated at runtime.
ALLOWED_TRANSITIONS: frozenset[tuple[EligibilityStatus, EligibilityStatus]] = frozenset(
    {
        (EligibilityStatus.WAITING, EligibilityStatus.ELIGIBLE),
        (EligibilityStatus.ELIGIBLE, EligibilityStatus.ACTIVE),
        (EligibilityStatus.ELIGIBLE, EligibilityStatus.EXPIRED),
        (EligibilityStatus.ACTIVE, EligibilityStatus.COMPLETED),
    }
)


class IllegalTransitionError(ValueError):
    """Raised when a lifecycle transition ``(from, to)`` is not permitted.

    Carries the offending ``from_status`` and ``to_status`` so callers can log
    or reconcile. Because :func:`transition` raises *before* producing any new
    status, a rejected transition leaves the caller's state unchanged
    (Req 10.2).
    """

    def __init__(
        self,
        from_status: EligibilityStatus,
        to_status: EligibilityStatus,
    ) -> None:
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(
            f"illegal eligibility transition: "
            f"{from_status.value} -> {to_status.value}"
        )


def is_allowed(
    from_status: EligibilityStatus,
    to_status: EligibilityStatus,
) -> bool:
    """Return whether transitioning ``from_status`` to ``to_status`` is permitted.

    This is a *total* predicate: it is defined for every pair of valid statuses
    (including self-loops and transitions out of terminal states, both of which
    are simply not in the allowed set and therefore return ``False``). It never
    raises and never mutates state.

    Requirements: 10.1, 10.2.
    """
    return (from_status, to_status) in ALLOWED_TRANSITIONS


def transition(
    from_status: EligibilityStatus,
    to_status: EligibilityStatus,
) -> EligibilityStatus:
    """Validate and apply a lifecycle transition.

    On success returns the resulting status (``to_status``). On a disallowed
    transition raises :class:`IllegalTransitionError` *without* producing a new
    status, so the caller's state is left unchanged (Req 10.2). Because the
    only successful outcome is one of the four permitted edges, an entry driven
    exclusively through this function always holds exactly one permitted status
    (Req 10.4).

    Requirements: 10.1, 10.2, 10.4.
    """
    if (from_status, to_status) not in ALLOWED_TRANSITIONS:
        raise IllegalTransitionError(from_status, to_status)
    return to_status


def validate(
    from_status: EligibilityStatus,
    to_status: EligibilityStatus,
) -> None:
    """Raise :class:`IllegalTransitionError` if ``(from, to)`` is not permitted.

    Assertion-style companion to :func:`is_allowed` for call sites that want to
    guard a transition without consuming a return value. Returns ``None`` when
    the transition is permitted.

    Requirements: 10.1, 10.2.
    """
    if (from_status, to_status) not in ALLOWED_TRANSITIONS:
        raise IllegalTransitionError(from_status, to_status)


def allowed_targets(from_status: EligibilityStatus) -> frozenset[EligibilityStatus]:
    """Return the set of statuses reachable from ``from_status`` in one step.

    Empty for terminal states (``EXPIRED``, ``COMPLETED``).
    """
    return frozenset(
        to_status
        for (src, to_status) in ALLOWED_TRANSITIONS
        if src is from_status
    )


def is_terminal(status: EligibilityStatus) -> bool:
    """Return whether ``status`` has no permitted outbound transitions.

    ``EXPIRED`` and ``COMPLETED`` are terminal; the other three are not.
    """
    return not any(src is status for (src, _to) in ALLOWED_TRANSITIONS)
