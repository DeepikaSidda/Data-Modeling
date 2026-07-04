"""Status-query decision logic for the Virtual Waiting Room (pure logic).

The ``Status_Reader`` data-access layer (a later task) verifies an
``Entry_Token``, reads the entry with a single ``GetItem``, and then must
answer a fan's status query: *may I browse yet, and if not, why?* This module
holds the **pure** decision functions behind that answer - no I/O, no DynamoDB,
no clock of its own (the current time is always passed in) - so they can be
exercised exhaustively by property tests.

This file is organized into sections so the remaining status-logic pieces can
be appended without reworking what is here:

* **Browse gating** (task 10.1) - :func:`evaluate_browse`.
* **Estimated wait time** (task 10.3) - :func:`estimated_wait`.
* **Caching directive** (task 10.5) - :func:`max_age`, :func:`cache_directive`.

Browse gating implements Requirements 8.6 and 8.7:

    may_browse = (Eligibility_Status == ELIGIBLE
                  AND the eligibility window has not expired
                  AND downstream browsing is available)

When ``may_browse`` is ``False`` the decision carries a ``reason`` explaining
why. For an otherwise-``ELIGIBLE`` fan the reason is ``EXPIRED`` (the
eligibility window elapsed) or ``DOWNSTREAM_UNAVAILABLE`` (downstream browsing
is offline) per Requirement 8.7. A fan who is not ``ELIGIBLE`` at all (e.g.
still ``WAITING``) simply cannot browse; that carries the ``NOT_ELIGIBLE``
reason.

Requirements: 8.6, 8.7.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from waiting_room.config import EligibilityStatus, WaitingRoomConfig

__all__ = [
    "BrowseReason",
    "BrowseDecision",
    "evaluate_browse",
    "estimated_wait",
    "max_age",
    "cache_directive",
]


# --------------------------------------------------------------------------- #
# Browse gating (task 10.1)
# --------------------------------------------------------------------------- #
class BrowseReason(str, Enum):
    """Why a fan may *not* proceed to browse.

    Backed by ``str`` so a reason serializes directly into the status
    response's ``reason`` field. ``None`` (rather than a member of this enum)
    is used when the fan *may* browse.

    * :attr:`EXPIRED` - the fan was ``ELIGIBLE`` but the eligibility window
      elapsed before they began browsing (Requirement 8.7).
    * :attr:`DOWNSTREAM_UNAVAILABLE` - the fan is ``ELIGIBLE`` within the
      window, but downstream browsing is currently unavailable
      (Requirement 8.7).
    * :attr:`NOT_ELIGIBLE` - the fan's ``Eligibility_Status`` is not
      ``ELIGIBLE`` (e.g. still ``WAITING``), so browsing has not been granted
      yet.
    """

    EXPIRED = "EXPIRED"
    DOWNSTREAM_UNAVAILABLE = "DOWNSTREAM_UNAVAILABLE"
    NOT_ELIGIBLE = "NOT_ELIGIBLE"


@dataclass(frozen=True, slots=True)
class BrowseDecision:
    """Outcome of a browse-gating evaluation.

    ``may_browse`` is ``True`` only when the fan is ``ELIGIBLE``, still within
    the eligibility window, and downstream browsing is available; in that case
    ``reason`` is ``None``. Otherwise ``may_browse`` is ``False`` and
    ``reason`` explains why (see :class:`BrowseReason`).

    Frozen so a decision can be shared/cached safely.
    """

    may_browse: bool
    reason: BrowseReason | None = None

    def __post_init__(self) -> None:
        # Invariant: browsing is permitted iff there is no blocking reason.
        if self.may_browse and self.reason is not None:
            raise ValueError("may_browse=True must not carry a reason")
        if not self.may_browse and self.reason is None:
            raise ValueError("may_browse=False must carry a reason")


def evaluate_browse(
    status: EligibilityStatus,
    promotion_time: float | int | None,
    now: float | int,
    eligibility_window_secs: int,
    downstream_available: bool,
) -> BrowseDecision:
    """Decide whether a fan may proceed to browse.

    Returns a :class:`BrowseDecision` where ``may_browse`` is ``True`` **iff**
    all three conditions hold (Requirement 8.6):

    #. ``status`` is :attr:`EligibilityStatus.ELIGIBLE`,
    #. the eligibility window has **not** expired - i.e.
       ``now - promotion_time < eligibility_window_secs``, and
    #. ``downstream_available`` is ``True``.

    When ``may_browse`` is ``False`` the returned ``reason`` is (Requirement
    8.7):

    * :attr:`BrowseReason.NOT_ELIGIBLE` when ``status`` is not ``ELIGIBLE``;
    * :attr:`BrowseReason.EXPIRED` when the fan is ``ELIGIBLE`` but the window
      has elapsed;
    * :attr:`BrowseReason.DOWNSTREAM_UNAVAILABLE` when the fan is ``ELIGIBLE``
      and within the window but downstream browsing is unavailable.

    If a fan is ``ELIGIBLE`` yet *both* expired and downstream-unavailable,
    ``EXPIRED`` takes precedence: an elapsed window is a terminal condition
    (the entry is bound for ``EXPIRED``), whereas downstream availability is
    transient.

    Args:
        status: The entry's current ``Eligibility_Status``.
        promotion_time: When the fan was promoted to ``ELIGIBLE`` (same time
            base as ``now``). Only consulted when ``status`` is ``ELIGIBLE``;
            may be ``None`` for other statuses.
        now: The current time, in the same units/epoch as ``promotion_time``.
        eligibility_window_secs: The configured eligibility window; must be a
            positive integer.
        downstream_available: Whether downstream browsing is currently
            available.

    Returns:
        A :class:`BrowseDecision`.

    Raises:
        TypeError: If ``status`` is not an :class:`EligibilityStatus`,
            ``downstream_available`` is not a ``bool``, ``eligibility_window_secs``
            is not an ``int``, or ``promotion_time`` is missing/non-numeric for
            an ``ELIGIBLE`` fan.
        ValueError: If ``eligibility_window_secs`` is not positive.

    Requirements: 8.6, 8.7.
    """
    if not isinstance(status, EligibilityStatus):
        raise TypeError(
            f"status must be an EligibilityStatus, got {type(status).__name__}"
        )
    if isinstance(eligibility_window_secs, bool) or not isinstance(
        eligibility_window_secs, int
    ):
        raise TypeError(
            "eligibility_window_secs must be an int, got "
            f"{type(eligibility_window_secs).__name__}"
        )
    if eligibility_window_secs <= 0:
        raise ValueError("eligibility_window_secs must be positive")
    if not isinstance(downstream_available, bool):
        raise TypeError(
            "downstream_available must be a bool, got "
            f"{type(downstream_available).__name__}"
        )

    # Condition 1: only ELIGIBLE fans are ever candidates to browse.
    if status is not EligibilityStatus.ELIGIBLE:
        return BrowseDecision(may_browse=False, reason=BrowseReason.NOT_ELIGIBLE)

    # Condition 2: the eligibility window must not have elapsed. The window is
    # "expired" once at least ``eligibility_window_secs`` have passed since
    # promotion (elapsed >= window), matching the expiry sweep's boundary.
    if promotion_time is None or (
        isinstance(promotion_time, bool) or not isinstance(promotion_time, (int, float))
    ):
        raise TypeError(
            "promotion_time must be numeric for an ELIGIBLE fan, got "
            f"{type(promotion_time).__name__}"
        )
    elapsed = now - promotion_time
    if elapsed >= eligibility_window_secs:
        return BrowseDecision(may_browse=False, reason=BrowseReason.EXPIRED)

    # Condition 3: downstream browsing must be available.
    if not downstream_available:
        return BrowseDecision(
            may_browse=False, reason=BrowseReason.DOWNSTREAM_UNAVAILABLE
        )

    return BrowseDecision(may_browse=True, reason=None)


# --------------------------------------------------------------------------- #
# Estimated wait time (task 10.3)
# --------------------------------------------------------------------------- #
def estimated_wait(position: int | float, rho: int | float) -> float:
    """Compute a fan's ``Estimated_Wait_Time`` from position and promotion rate.

    Per Requirement 8.4, the status path derives estimated wait time from the
    fan's ``Queue_Position`` and the observed promotion rate ``rho`` (fans
    promoted per second). With ``position`` fans ahead-or-at the front and a
    steady promotion rate of ``rho`` fans/second, the projected time to
    promotion is simply::

        Estimated_Wait_Time = position / rho   (seconds)

    A ``position`` of ``0`` yields ``0.0`` (the fan is at the very front and
    waits no time). The result is always a ``float`` number of seconds.

    Args:
        position: The fan's ``Queue_Position`` (number of fans ahead-or-at the
            front). Must be non-negative. ``bool`` is rejected.
        rho: The observed promotion rate in fans per second. Must be strictly
            positive - a non-positive rate has no finite projection and, in the
            ``rho == 0`` case, would divide by zero. ``bool`` is rejected.

    Returns:
        The estimated wait time in seconds as a ``float``.

    Raises:
        TypeError: If ``position`` or ``rho`` is not a real number (or is a
            ``bool``).
        ValueError: If ``rho <= 0`` or ``position < 0``.

    Requirements: 8.4.
    """
    if isinstance(position, bool) or not isinstance(position, (int, float)):
        raise TypeError(
            f"position must be a real number, got {type(position).__name__}"
        )
    if isinstance(rho, bool) or not isinstance(rho, (int, float)):
        raise TypeError(f"rho must be a real number, got {type(rho).__name__}")
    if rho <= 0:
        raise ValueError("rho must be positive")
    if position < 0:
        raise ValueError("position must be non-negative")

    return position / rho


# --------------------------------------------------------------------------- #
# Caching directive (task 10.5)
# --------------------------------------------------------------------------- #
def _resolve_staleness_bound(staleness_bound: int | WaitingRoomConfig) -> int:
    """Coerce a caching input into a validated positive-int staleness bound.

    Accepts either a raw integer number of seconds or a
    :class:`~waiting_room.config.WaitingRoomConfig`, from which the
    ``staleness_bound_secs`` setting (task 1.1) is read. The bound must be a
    positive integer; ``bool`` is rejected because ``True``/``False`` are not
    meaningful cache ages even though ``bool`` is a subclass of ``int``.

    Raises:
        TypeError: If ``staleness_bound`` is neither a ``WaitingRoomConfig``
            nor an ``int`` (or is a ``bool``).
        ValueError: If the resolved bound is not positive.
    """
    if isinstance(staleness_bound, WaitingRoomConfig):
        # WaitingRoomConfig already validates staleness_bound_secs > 0 in its
        # __post_init__, but re-check defensively below for a single code path.
        bound = staleness_bound.staleness_bound_secs
    elif isinstance(staleness_bound, bool) or not isinstance(staleness_bound, int):
        raise TypeError(
            "staleness_bound must be a positive int or WaitingRoomConfig, got "
            f"{type(staleness_bound).__name__}"
        )
    else:
        bound = staleness_bound

    if bound <= 0:
        raise ValueError("staleness_bound must be positive")
    return bound


def max_age(staleness_bound: int | WaitingRoomConfig) -> int:
    """Compute the integer ``max-age`` for a cacheable status response.

    Per Requirement 8.8, a cacheable status response carries a caching
    directive whose ``max-age`` bounds staleness to a configured maximum, so
    that repeated polling by millions of fans is absorbed at the edge without
    per-request queue recomputation. The returned value ``n`` always satisfies
    the invariant::

        0 < n <= staleness_bound

    We emit the largest permissible age (``n == staleness_bound``): a larger
    ``max-age`` maximizes edge cache reuse (fewer origin recomputations) while
    still never letting a fan see data staler than the configured bound.

    Args:
        staleness_bound: Either the configured staleness bound in seconds (a
            positive ``int``) or a :class:`~waiting_room.config.WaitingRoomConfig`
            whose ``staleness_bound_secs`` is used.

    Returns:
        The ``max-age`` in seconds as a positive ``int`` not exceeding the
        staleness bound.

    Raises:
        TypeError: If ``staleness_bound`` is not a ``WaitingRoomConfig`` or an
            ``int`` (``bool`` is rejected).
        ValueError: If the resolved staleness bound is not positive.

    Requirements: 8.8.
    """
    bound = _resolve_staleness_bound(staleness_bound)
    # Largest value satisfying 0 < n <= bound.
    return bound


def cache_directive(staleness_bound: int | WaitingRoomConfig) -> str:
    """Build the ``Cache-Control`` directive for a cacheable status response.

    Returns a well-formed ``max-age`` directive string of the form
    ``"max-age=<n>"`` (e.g. ``"max-age=5"``), where ``n`` is computed by
    :func:`max_age` and therefore satisfies ``0 < n <= staleness_bound``. This
    is the value the ``Status_Reader`` attaches so millions of repeat polls are
    served from the edge cache without recomputation (Requirement 8.8).

    Args:
        staleness_bound: Either the configured staleness bound in seconds (a
            positive ``int``) or a :class:`~waiting_room.config.WaitingRoomConfig`
            whose ``staleness_bound_secs`` is used.

    Returns:
        The directive string ``"max-age=<n>"``.

    Raises:
        TypeError: If ``staleness_bound`` is not a ``WaitingRoomConfig`` or an
            ``int`` (``bool`` is rejected).
        ValueError: If the resolved staleness bound is not positive.

    Requirements: 8.8.
    """
    return f"max-age={max_age(staleness_bound)}"
