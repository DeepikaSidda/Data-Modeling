"""Virtual Waiting Room.

A DynamoDB-backed system that fairly queues up to 10,000,000 fans arriving
within seconds for a high-demand ticket sale, assigns each fan a verifiable
queue position, promotes fans from ``WAITING`` to ``ELIGIBLE`` in batches
bounded by downstream purchasing capacity, and serves low-latency status
updates to millions of concurrent pollers.

This package is organized bottom-up:

* :mod:`waiting_room.config` - typed configuration schema (this task).

Subsequent tasks add the pure-logic layer (ordering, sharding, position,
batching, capacity, lifecycle, tokens, backoff, status) and the DynamoDB
data-access layer (admission, promotion, status reads, aggregation).
"""

from waiting_room.backoff import (
    RetryOutcome,
    backoff_delay,
    backoff_schedule,
    delay_bounds,
    next_retry,
    should_retry,
)
from waiting_room.active_pool import (
    band_lower_bound,
    capacity_headroom,
    needs_refill,
    projected_active,
    refill_target,
)
from waiting_room.capacity import (
    CapacityCounter,
    CapacityPool,
    CapacityState,
    ReservationPlan,
)
from waiting_room.config import (
    BackoffConfig,
    EligibilityStatus,
    EventConfig,
    EventStatus,
    WaitingRoomConfig,
)
from waiting_room.position import (
    EntryLike,
    QueueEntryView,
    approximate_position,
    fans_ahead,
    queue_position,
)
from waiting_room.lifecycle import (
    ALLOWED_TRANSITIONS,
    IllegalTransitionError,
    allowed_targets,
    is_allowed,
    is_terminal,
    transition,
    validate,
)
from waiting_room.status_logic import (
    BrowseDecision,
    BrowseReason,
    cache_directive,
    estimated_wait,
    evaluate_browse,
    max_age,
)
from waiting_room.token import (
    EntryClaims,
    InvalidTokenError,
    sign,
    verify,
)
from waiting_room.admission import (
    Admission_Writer,
    AdmissionError,
    AdmissionResult,
)
from waiting_room.promoter import (
    BatchPromoter,
    ExpireResult,
    PromotionResult,
)
from waiting_room.status_reader import (
    EntryNotFoundError,
    PositionAggregates,
    StatusAuthError,
    StatusResult,
    Status_Reader,
)
from waiting_room.lifecycle_manager import (
    LifecycleManager,
    ReconciliationResult,
    TransitionOutcome,
    TransitionResult,
)
from waiting_room.aggregator import (
    AggregateSnapshot,
    StreamAggregator,
    StreamRecord,
)

__all__ = [
    "BackoffConfig",
    "EligibilityStatus",
    "EventConfig",
    "EventStatus",
    "WaitingRoomConfig",
    "CapacityCounter",
    "CapacityPool",
    "CapacityState",
    "ReservationPlan",
    "EntryLike",
    "QueueEntryView",
    "approximate_position",
    "fans_ahead",
    "queue_position",
    "band_lower_bound",
    "capacity_headroom",
    "needs_refill",
    "projected_active",
    "refill_target",
    "ALLOWED_TRANSITIONS",
    "IllegalTransitionError",
    "allowed_targets",
    "is_allowed",
    "is_terminal",
    "transition",
    "validate",
    "RetryOutcome",
    "backoff_delay",
    "backoff_schedule",
    "delay_bounds",
    "next_retry",
    "should_retry",
    "EntryClaims",
    "InvalidTokenError",
    "sign",
    "verify",
    "BrowseDecision",
    "BrowseReason",
    "cache_directive",
    "estimated_wait",
    "evaluate_browse",
    "max_age",
    "Admission_Writer",
    "AdmissionError",
    "AdmissionResult",
    "BatchPromoter",
    "ExpireResult",
    "PromotionResult",
    "EntryNotFoundError",
    "PositionAggregates",
    "StatusAuthError",
    "StatusResult",
    "Status_Reader",
    "LifecycleManager",
    "ReconciliationResult",
    "TransitionOutcome",
    "TransitionResult",
    "AggregateSnapshot",
    "StreamAggregator",
    "StreamRecord",
]

__version__ = "0.1.0"
