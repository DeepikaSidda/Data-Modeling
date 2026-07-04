"""Bounded exponential-backoff-with-jitter schedule (pure logic).

When a DynamoDB write is throttled, the ``Admission_Writer`` retries it using
exponential backoff with jitter, up to a bounded retry limit. This module is
the pure decision layer for that policy: it computes *how long* attempt ``k``
should wait and *whether* another attempt is even allowed, but it never sleeps
and performs no I/O. The data-access layer wraps it around the actual sleep +
retry loop.

Delay schedule
--------------
For a 0-based attempt index ``k`` the *ideal* (un-jittered) delay grows
geometrically::

    ideal = base_delay_secs * 2 ** k

Jitter shaves off up to ``jitter_fraction`` of that ideal, so the delay lands
somewhere in the half-open band ``[ideal * (1 - jitter), ideal]`` depending on
the injected random draw. Concretely, for ``rand`` in ``[0.0, 1.0)``::

    delay = ideal * (1 - jitter_fraction * rand)

so ``rand == 0`` yields the upper bound (``ideal``) and ``rand`` approaching
``1`` approaches the lower bound (``ideal * (1 - jitter_fraction)``). This is
"full-ish" jitter that only ever *reduces* the delay, so the geometric ceiling
is preserved.

The delay is finally clamped to ``max_delay_secs``. Because both the ideal
ceiling and floor are clamped by the same ceiling, a computed delay always
lies within the (possibly clamped) bounds reported by :func:`delay_bounds`.

Randomness is injected via the ``rand`` callable (defaulting to
``random.random``) so the property test (task 9.2) can pin it to the band
endpoints and assert the bounds deterministically.

Retry limit
-----------
Retries stop once ``max_retries`` attempts have been made: attempt indices
``0 .. max_retries - 1`` are permitted, and any attempt at or beyond
``max_retries`` is exhausted. On exhaustion the policy reports a *retryable*
outcome (the caller returns a retryable error and persists no partial write),
per Requirement 2.7.

Requirements: 2.6, 2.7.
"""

from __future__ import annotations

import random
from enum import Enum
from typing import Callable, Iterator, Optional, Tuple

from waiting_room.config import BackoffConfig

__all__ = [
    "RetryOutcome",
    "delay_bounds",
    "backoff_delay",
    "should_retry",
    "next_retry",
    "backoff_schedule",
]


class RetryOutcome(str, Enum):
    """Result of asking whether another retry should occur.

    ``RETRY`` means the attempt is within the bounded limit and carries a
    delay. ``RETRYABLE_EXHAUSTED`` means the retry budget is spent; the caller
    should surface a *retryable* error and record no partial write
    (Requirement 2.7).
    """

    RETRY = "RETRY"
    RETRYABLE_EXHAUSTED = "RETRYABLE_EXHAUSTED"


def delay_bounds(attempt: int, config: BackoffConfig) -> Tuple[float, float]:
    """Return the ``(lower, upper)`` delay bounds for a 0-based ``attempt``.

    The unclamped bounds are ``base * 2**attempt * (1 - jitter)`` and
    ``base * 2**attempt``. Each bound is then clamped to
    ``config.max_delay_secs``, so the returned interval is exactly the range
    within which :func:`backoff_delay` is guaranteed to fall for this attempt.

    Requirements: 2.6.
    """
    _require_non_negative_attempt(attempt)

    ideal_upper = config.base_delay_secs * (2 ** attempt)
    ideal_lower = ideal_upper * (1.0 - config.jitter_fraction)

    upper = min(ideal_upper, config.max_delay_secs)
    lower = min(ideal_lower, config.max_delay_secs)
    return (lower, upper)


def backoff_delay(
    attempt: int,
    config: BackoffConfig,
    rand: Callable[[], float] = random.random,
) -> float:
    """Return the delay in seconds for a 0-based ``attempt`` index.

    The delay lies within the band reported by :func:`delay_bounds`:
    ``base * 2**attempt * (1 - jitter)`` .. ``base * 2**attempt``, clamped to
    ``config.max_delay_secs``. ``rand`` supplies a value in ``[0.0, 1.0)``
    (``random.random`` by default); ``rand() == 0`` yields the upper bound and
    values approaching ``1`` approach the lower bound.

    This function does not sleep - it only computes the delay a caller should
    wait before the next retry.

    Requirements: 2.6.
    """
    _require_non_negative_attempt(attempt)

    draw = rand()
    if not 0.0 <= draw < 1.0 + 1e-9:  # tolerate a rand() that returns exactly 1.0
        raise ValueError(f"rand() must yield a value in [0.0, 1.0], got {draw}")
    # Clamp the draw into [0, 1] so a jitter of 0..jitter_fraction is applied.
    draw = min(max(draw, 0.0), 1.0)

    ideal_upper = config.base_delay_secs * (2 ** attempt)
    delay = ideal_upper * (1.0 - config.jitter_fraction * draw)
    return min(delay, config.max_delay_secs)


def should_retry(attempt: int, config: BackoffConfig) -> bool:
    """Return whether a 0-based ``attempt`` is within the bounded retry limit.

    ``True`` for attempts ``0 .. max_retries - 1``; ``False`` once the budget
    is exhausted (``attempt >= max_retries``). When this returns ``False`` the
    caller should stop retrying and surface a retryable error
    (Requirement 2.7).

    Requirements: 2.6, 2.7.
    """
    _require_non_negative_attempt(attempt)
    return attempt < config.max_retries


def next_retry(
    attempt: int,
    config: BackoffConfig,
    rand: Callable[[], float] = random.random,
) -> Tuple[RetryOutcome, Optional[float]]:
    """Decide the outcome for a 0-based ``attempt`` and its delay if retrying.

    Returns ``(RetryOutcome.RETRY, delay)`` when the attempt is within the
    limit, or ``(RetryOutcome.RETRYABLE_EXHAUSTED, None)`` once the retry
    budget is spent. This is the single call a retry loop makes per attempt to
    learn both *whether* to wait and *how long*.

    Requirements: 2.6, 2.7.
    """
    if not should_retry(attempt, config):
        return (RetryOutcome.RETRYABLE_EXHAUSTED, None)
    return (RetryOutcome.RETRY, backoff_delay(attempt, config, rand))


def backoff_schedule(
    config: BackoffConfig,
    rand: Callable[[], float] = random.random,
) -> Iterator[float]:
    """Yield the delay for each permitted attempt, in order.

    Produces exactly ``config.max_retries`` delays (for attempts
    ``0 .. max_retries - 1``) and then stops. The generator finishing *is* the
    exhaustion signal; the caller treats a schedule that runs out without a
    successful write as a retryable failure with no partial write
    (Requirement 2.7).

    Requirements: 2.6, 2.7.
    """
    for attempt in range(config.max_retries):
        yield backoff_delay(attempt, config, rand)


def _require_non_negative_attempt(attempt: int) -> None:
    """Validate ``attempt`` is a non-negative integer (``bool`` rejected)."""
    if isinstance(attempt, bool) or not isinstance(attempt, int):
        raise TypeError(f"attempt must be an int, got {type(attempt).__name__}")
    if attempt < 0:
        raise ValueError(f"attempt must be non-negative, got {attempt}")
