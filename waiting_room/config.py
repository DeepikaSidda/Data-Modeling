"""Event-configuration schema for the Virtual Waiting Room.

This module defines the typed configuration objects that drive admission,
sharding, promotion, capacity regulation, retry/backoff, and status-caching
behavior. Everything here is a plain, dependency-free dataclass so it can be
imported by both the pure-logic layer and the DynamoDB data-access layer
without pulling in ``boto3`` or any AWS runtime.

The defaults mirror the ``CONFIG`` item described in the design document. Two
values deserve a note:

* ``shard_count`` defaults to ``1000`` purely for illustration (this is what
  the NoSQL Workbench sample ships). Production sizing is burst-driven and
  should be ~4000 for a 10M-arrival / 10s burst, because the sparse
  ``WaitingIndex`` GSI - not the base table - is the binding constraint.
* ``max_queue_size`` defaults to ``10_000_000`` to match the headline burst
  target.

Requirements: 2.1, 2.2.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

__all__ = [
    "EventStatus",
    "EligibilityStatus",
    "BackoffConfig",
    "EventConfig",
    "WaitingRoomConfig",
    # Default constants (exported for reuse by later tasks / tests).
    "DEFAULT_SHARD_COUNT",
    "PRODUCTION_SHARD_COUNT",
    "DEFAULT_MAX_QUEUE_SIZE",
    "DEFAULT_ELIGIBILITY_WINDOW_SECS",
    "DEFAULT_MAX_BATCH_SIZE",
    "DEFAULT_ACTIVE_TARGET",
    "DEFAULT_DOWNSTREAM_CAPACITY",
]


# --------------------------------------------------------------------------- #
# Enumerations
# --------------------------------------------------------------------------- #
class EventStatus(str, Enum):
    """Whether an event's queue is accepting new admissions.

    Backed by ``str`` so instances serialize directly to the DynamoDB
    ``CONFIG.Event_Status`` attribute value.
    """

    OPEN = "OPEN"
    CLOSED = "CLOSED"


class EligibilityStatus(str, Enum):
    """Lifecycle state of a Queue_Entry.

    The permitted transitions (enforced by the lifecycle state machine in a
    later task) are::

        WAITING  -> ELIGIBLE
        ELIGIBLE -> ACTIVE
        ELIGIBLE -> EXPIRED
        ACTIVE   -> COMPLETED
    """

    WAITING = "WAITING"
    ELIGIBLE = "ELIGIBLE"
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    COMPLETED = "COMPLETED"


# --------------------------------------------------------------------------- #
# Default constants
# --------------------------------------------------------------------------- #
#: Illustrative shard count shipped with the NoSQL Workbench sample.
DEFAULT_SHARD_COUNT: int = 1000

#: Burst-driven production sizing for a 10M-arrival / 10s burst. The sparse
#: ``WaitingIndex`` GSI partitions only by shard, so it is the binding
#: constraint; ~4000 shards keeps each GSI partition well under the
#: per-partition write ceiling. See the design's Scalability analysis.
PRODUCTION_SHARD_COUNT: int = 4000

DEFAULT_MAX_QUEUE_SIZE: int = 10_000_000
DEFAULT_ELIGIBILITY_WINDOW_SECS: int = 120
DEFAULT_MAX_BATCH_SIZE: int = 500
DEFAULT_ACTIVE_TARGET: int = 1000
DEFAULT_DOWNSTREAM_CAPACITY: int = 1000


# --------------------------------------------------------------------------- #
# Retry / backoff configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class BackoffConfig:
    """Exponential-backoff-with-jitter settings for throttled writes.

    The delay for attempt ``k`` (0-indexed) lies within
    ``[base_delay_secs * 2**k * (1 - jitter_fraction), base_delay_secs * 2**k]``
    and retries stop once ``max_retries`` is reached, signaling a retryable
    outcome on exhaustion.

    Requirements: 2.6, 2.7.
    """

    #: Base delay in seconds for the first attempt (k = 0).
    base_delay_secs: float = 0.05
    #: Maximum number of retries before signaling a retryable failure.
    max_retries: int = 6
    #: Fraction of the computed delay that may be shaved off as jitter,
    #: in ``[0.0, 1.0]``. ``0.2`` means "up to 20% jitter".
    jitter_fraction: float = 0.2
    #: Optional hard ceiling on any single delay, in seconds.
    max_delay_secs: float = 5.0

    def __post_init__(self) -> None:
        if self.base_delay_secs <= 0:
            raise ValueError("base_delay_secs must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if not 0.0 <= self.jitter_fraction <= 1.0:
            raise ValueError("jitter_fraction must be within [0.0, 1.0]")
        if self.max_delay_secs <= 0:
            raise ValueError("max_delay_secs must be positive")


# --------------------------------------------------------------------------- #
# Per-event configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class EventConfig:
    """Configuration for a single ticket-sale event's queue.

    Mirrors the ``EVT#<Event_Id> / CONFIG`` item in the ``WaitingRoom`` table
    plus the derived capacity target. Frozen so a loaded config can be shared
    safely across concurrent workers.

    Requirements: 2.1, 2.2.
    """

    #: Whether the queue is accepting new admissions.
    event_status: EventStatus = EventStatus.OPEN
    #: Number of write shards to spread the admission burst across. Default is
    #: illustrative (see :data:`DEFAULT_SHARD_COUNT`); production is
    #: burst-driven (~:data:`PRODUCTION_SHARD_COUNT`).
    shard_count: int = DEFAULT_SHARD_COUNT
    #: Maximum number of fans admitted before the queue is full.
    max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE
    #: Seconds an ELIGIBLE fan has to begin purchasing before EXPIRED.
    eligibility_window_secs: int = DEFAULT_ELIGIBILITY_WINDOW_SECS
    #: Upper bound on the number of entries promoted in one cycle.
    max_batch_size: int = DEFAULT_MAX_BATCH_SIZE
    #: Target size of the ACTIVE purchasing pool (optional stretch goal).
    active_target: int = DEFAULT_ACTIVE_TARGET
    #: Maximum concurrently ELIGIBLE + ACTIVE fans the downstream can serve.
    downstream_capacity: int = DEFAULT_DOWNSTREAM_CAPACITY

    def __post_init__(self) -> None:
        if self.shard_count <= 0:
            raise ValueError("shard_count must be positive")
        if self.max_queue_size <= 0:
            raise ValueError("max_queue_size must be positive")
        if self.eligibility_window_secs <= 0:
            raise ValueError("eligibility_window_secs must be positive")
        if self.max_batch_size <= 0:
            raise ValueError("max_batch_size must be positive")
        if self.active_target < 0:
            raise ValueError("active_target must be non-negative")
        if self.downstream_capacity <= 0:
            raise ValueError("downstream_capacity must be positive")

    @property
    def is_open(self) -> bool:
        """Whether the event is currently accepting admissions."""
        return self.event_status is EventStatus.OPEN


# --------------------------------------------------------------------------- #
# Top-level runtime configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class WaitingRoomConfig:
    """Runtime configuration bundling per-event, backoff, and cache settings.

    This is the object most components accept. It composes an
    :class:`EventConfig`, a :class:`BackoffConfig`, and the status-cache
    ``staleness_bound`` used to bound how stale a cached status response may
    be (drives the ``Cache-Control: max-age`` directive).

    Requirements: 2.1, 2.2.
    """

    #: Per-event queue configuration.
    event: EventConfig = field(default_factory=EventConfig)
    #: Throttle retry / backoff settings.
    backoff: BackoffConfig = field(default_factory=BackoffConfig)
    #: Maximum age, in seconds, a cached status response may report. The
    #: emitted ``max-age`` directive satisfies ``0 < max-age <= staleness_bound``.
    staleness_bound_secs: int = 5

    def __post_init__(self) -> None:
        if self.staleness_bound_secs <= 0:
            raise ValueError("staleness_bound_secs must be positive")
