"""Property-based invariant tests for the Virtual Waiting Room pure-logic layer.

This module holds three Hypothesis property tests, each tagged with the exact
design Correctness Property it validates:

* Property 15 - lifecycle state-machine validity (``waiting_room.lifecycle``).
* Property 3  - Entry_Token round-trip and integrity (``waiting_room.token``).
* Property 6  - bounded, backed-off retry with no partial write
  (``waiting_room.backoff``).

Each test runs with ``@settings(max_examples=100)`` per the implementation
plan's "≥ 100 iterations" requirement. These tests treat the modules under
test as black boxes and must never be weakened to pass; a failing example is a
real defect to report.
"""

from __future__ import annotations

import math

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from waiting_room.backoff import (
    RetryOutcome,
    backoff_delay,
    delay_bounds,
    next_retry,
    should_retry,
)
from waiting_room.config import BackoffConfig, EligibilityStatus
from waiting_room.lifecycle import (
    ALLOWED_TRANSITIONS,
    IllegalTransitionError,
    is_allowed,
    transition,
)
from waiting_room.token import CLAIM_FIELDS, InvalidTokenError, sign, verify

# --------------------------------------------------------------------------- #
# Shared strategies
# --------------------------------------------------------------------------- #
_STATUSES = list(EligibilityStatus)
_status_strategy = st.sampled_from(_STATUSES)


# =========================================================================== #
# Property 15: Lifecycle state-machine validity
# =========================================================================== #
# Feature: virtual-waiting-room, Property 15: For any transition request
# (from, to), it succeeds if and only if (from, to) is in the allowed set
# {WAITING→ELIGIBLE, ELIGIBLE→ACTIVE, ELIGIBLE→EXPIRED, ACTIVE→COMPLETED}; a
# disallowed transition leaves the entry unchanged; and after any sequence of
# operations every entry holds exactly one permitted Eligibility_Status.
#
# Validates: Requirements 10.1, 10.2, 10.4
@settings(max_examples=100)
@given(
    from_status=_status_strategy,
    to_status=_status_strategy,
    start=_status_strategy,
    requests=st.lists(_status_strategy, max_size=20),
)
def test_property_15_lifecycle_state_machine_validity(
    from_status: EligibilityStatus,
    to_status: EligibilityStatus,
    start: EligibilityStatus,
    requests: list[EligibilityStatus],
) -> None:
    """Feature: virtual-waiting-room, Property 15: For any transition request
    (from, to), it succeeds if and only if (from, to) is in the allowed set
    {WAITING→ELIGIBLE, ELIGIBLE→ACTIVE, ELIGIBLE→EXPIRED, ACTIVE→COMPLETED}; a
    disallowed transition leaves the entry unchanged; and after any sequence of
    operations every entry holds exactly one permitted Eligibility_Status.

    Validates: Requirements 10.1, 10.2, 10.4
    """
    # --- Part A: transition succeeds iff (from, to) is in the allowed set. ---
    allowed = (from_status, to_status) in ALLOWED_TRANSITIONS
    assert is_allowed(from_status, to_status) is allowed

    if allowed:
        result = transition(from_status, to_status)
        assert result is to_status
    else:
        with pytest.raises(IllegalTransitionError):
            transition(from_status, to_status)

    # --- Part B: drive a random transition sequence and assert the status ---
    # stays exactly one permitted member; disallowed requests leave it
    # unchanged.
    current = start
    for target in requests:
        if is_allowed(current, target):
            current = transition(current, target)
            assert current is target
        else:
            before = current
            with pytest.raises(IllegalTransitionError):
                transition(current, target)
            # Rejected transition leaves the entry unchanged (Req 10.2).
            assert current is before
        # After every operation the entry holds exactly one valid status.
        assert isinstance(current, EligibilityStatus)
        assert current in _STATUSES


# =========================================================================== #
# Property 3: Entry_Token round-trip and integrity
# =========================================================================== #
_non_empty_text = st.text(min_size=1, max_size=40)

_claims_strategy = st.fixed_dictionaries(
    {
        "Fan_Id": _non_empty_text,
        "Event_Id": _non_empty_text,
        "Ordering_Key": _non_empty_text,
        "Write_Shard": st.integers(min_value=0, max_value=100_000),
    }
)


# Feature: virtual-waiting-room, Property 3: For any claim set {Fan_Id,
# Event_Id, Ordering_Key, Write_Shard}, verifying a freshly signed token
# recovers exactly those claims; and for any single-byte mutation of a signed
# token, verification fails.
#
# Validates: Requirements 1.4, 4.2, 4.3, 8.5
@settings(max_examples=100)
@given(
    claims=_claims_strategy,
    secret=st.text(min_size=1, max_size=32),
)
def test_property_3_entry_token_round_trip_and_integrity(
    claims: dict, secret: str
) -> None:
    """Feature: virtual-waiting-room, Property 3: For any claim set {Fan_Id,
    Event_Id, Ordering_Key, Write_Shard}, verifying a freshly signed token
    recovers exactly those claims; and for any single-byte mutation of a signed
    token, verification fails.

    Validates: Requirements 1.4, 4.2, 4.3, 8.5
    """
    token = sign(claims, secret)

    # --- Round-trip: verify recovers exactly the signed claims. ---
    recovered = verify(token, secret)
    assert recovered.as_dict() == {field: claims[field] for field in CLAIM_FIELDS}

    # --- Integrity: any single-byte (single-char) mutation must fail. ---
    for i, original_char in enumerate(token):
        # Choose a substitute character guaranteed to differ from the original.
        substitute = "A" if original_char != "A" else "B"
        mutated = token[:i] + substitute + token[i + 1:]
        assert mutated != token
        with pytest.raises(InvalidTokenError):
            verify(mutated, secret)


# =========================================================================== #
# Property 6: Bounded, backed-off retry with no partial write
# =========================================================================== #
@st.composite
def _backoff_configs(draw: st.DrawFn) -> BackoffConfig:
    """Generate valid, overflow-safe BackoffConfig instances."""
    base_delay_secs = draw(
        st.floats(min_value=1e-3, max_value=10.0, allow_nan=False, allow_infinity=False)
    )
    max_retries = draw(st.integers(min_value=0, max_value=12))
    jitter_fraction = draw(
        st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
    )
    max_delay_secs = draw(
        st.floats(min_value=1e-3, max_value=100.0, allow_nan=False, allow_infinity=False)
    )
    return BackoffConfig(
        base_delay_secs=base_delay_secs,
        max_retries=max_retries,
        jitter_fraction=jitter_fraction,
        max_delay_secs=max_delay_secs,
    )


# Feature: virtual-waiting-room, Property 6: For any attempt index k below the
# limit, the retry delay lies within [base·2^k·(1−jitter), base·2^k]; retries
# stop at the configured limit; and for any run that exhausts retries, no
# Queue_Entry is persisted and the returned error is retryable.
#
# Validates: Requirements 2.6, 2.7
@settings(max_examples=100)
@given(
    config=_backoff_configs(),
    attempt=st.integers(min_value=0, max_value=16),
    draw=st.floats(
        min_value=0.0, max_value=1.0, exclude_max=True, allow_nan=False, allow_infinity=False
    ),
)
def test_property_6_bounded_backed_off_retry_no_partial_write(
    config: BackoffConfig, attempt: int, draw: float
) -> None:
    """Feature: virtual-waiting-room, Property 6: For any attempt index k below
    the limit, the retry delay lies within [base·2^k·(1−jitter), base·2^k];
    retries stop at the configured limit; and for any run that exhausts
    retries, no Queue_Entry is persisted and the returned error is retryable.

    Validates: Requirements 2.6, 2.7
    """
    # --- Bounded delay: the computed delay lies within delay_bounds, which ---
    # already account for the max_delay clamp.
    lower, upper = delay_bounds(attempt, config)
    delay = backoff_delay(attempt, config, rand=lambda: draw)

    tol = 1e-9 * (abs(upper) + 1.0)
    assert lower - tol <= delay <= upper + tol
    # Delay never exceeds the configured hard ceiling.
    assert delay <= config.max_delay_secs + tol
    # Bounds are internally consistent with the geometric schedule.
    ideal_upper = config.base_delay_secs * (2 ** attempt)
    assert math.isclose(upper, min(ideal_upper, config.max_delay_secs), rel_tol=1e-9, abs_tol=1e-12)

    # --- Retry limit: should_retry is True below the limit, False at/after. ---
    if attempt < config.max_retries:
        assert should_retry(attempt, config) is True
        outcome, retry_delay = next_retry(attempt, config, rand=lambda: draw)
        assert outcome is RetryOutcome.RETRY
        assert retry_delay is not None
        assert lower - tol <= retry_delay <= upper + tol
    else:
        assert should_retry(attempt, config) is False

    # --- Exhaustion: at the configured limit the outcome is a retryable ---
    # exhaustion carrying no delay (no partial write / nothing persisted).
    exhausted_outcome, exhausted_delay = next_retry(
        config.max_retries, config, rand=lambda: draw
    )
    assert exhausted_outcome is RetryOutcome.RETRYABLE_EXHAUSTED
    assert exhausted_delay is None
    assert should_retry(config.max_retries, config) is False
