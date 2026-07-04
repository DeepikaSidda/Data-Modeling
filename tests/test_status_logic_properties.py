"""Property-based tests for the status-query decision logic (pure logic).

Covers three design Correctness Properties for ``waiting_room/status_logic.py``:

* Property 17 - browse gating (tasks 10.1 / 10.2), Requirements 8.6, 8.7.
* Property 18 - estimated wait time (tasks 10.3 / 10.4), Requirement 8.4.
* Property 19 - cacheable status staleness bound (tasks 10.5 / 10.6),
  Requirement 8.8.

Each property runs with Hypothesis at >= 100 examples and asserts against the
module's *documented* behavior, including the browse-gating reason precedence
(NOT_ELIGIBLE > EXPIRED > DOWNSTREAM_UNAVAILABLE) and the expiry boundary
``elapsed >= window``.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from waiting_room.config import EligibilityStatus
from waiting_room.status_logic import (
    BrowseDecision,
    BrowseReason,
    cache_directive,
    estimated_wait,
    evaluate_browse,
    max_age,
)


# --------------------------------------------------------------------------- #
# Property 17: Browse gating logic
# --------------------------------------------------------------------------- #
@settings(max_examples=100)
@given(
    status=st.sampled_from(list(EligibilityStatus)),
    promotion_time=st.floats(
        min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False
    ),
    # Elapsed time may be negative (clock skew) up to well past the window so we
    # exercise both the "within window" and "expired" branches.
    elapsed=st.floats(
        min_value=-50.0, max_value=500.0, allow_nan=False, allow_infinity=False
    ),
    eligibility_window_secs=st.integers(min_value=1, max_value=300),
    downstream_available=st.booleans(),
)
def test_property_17_browse_gating_logic(
    status: EligibilityStatus,
    promotion_time: float,
    elapsed: float,
    eligibility_window_secs: int,
    downstream_available: bool,
) -> None:
    """Feature: virtual-waiting-room, Property 17: For any status query on an entry, may_browse is true if and only if Eligibility_Status = ELIGIBLE AND the eligibility window has not expired AND downstream browsing is available; when false, a reason (EXPIRED or DOWNSTREAM_UNAVAILABLE) is returned.

    Validates: Requirements 8.6, 8.7
    """
    now = promotion_time + elapsed

    decision = evaluate_browse(
        status=status,
        promotion_time=promotion_time,
        now=now,
        eligibility_window_secs=eligibility_window_secs,
        downstream_available=downstream_available,
    )

    assert isinstance(decision, BrowseDecision)

    # The core biconditional (Requirement 8.6). The module treats the window as
    # expired once elapsed >= window, so "not expired" is elapsed < window.
    within_window = (now - promotion_time) < eligibility_window_secs
    expected_may_browse = (
        status is EligibilityStatus.ELIGIBLE and within_window and downstream_available
    )
    assert decision.may_browse is expected_may_browse

    if decision.may_browse:
        # A permitting decision carries no reason.
        assert decision.reason is None
    else:
        # A blocking decision always carries a reason (Requirement 8.7), and it
        # matches the module's documented precedence:
        #   1. NOT_ELIGIBLE   - status is not ELIGIBLE
        #   2. EXPIRED        - ELIGIBLE but window elapsed
        #   3. DOWNSTREAM_UNAVAILABLE - ELIGIBLE, within window, downstream down
        assert decision.reason is not None
        assert isinstance(decision.reason, BrowseReason)
        if status is not EligibilityStatus.ELIGIBLE:
            expected_reason = BrowseReason.NOT_ELIGIBLE
        elif not within_window:
            expected_reason = BrowseReason.EXPIRED
        else:
            expected_reason = BrowseReason.DOWNSTREAM_UNAVAILABLE
        assert decision.reason is expected_reason


# --------------------------------------------------------------------------- #
# Property 18: Estimated wait time formula
# --------------------------------------------------------------------------- #
@settings(max_examples=100)
@given(
    position=st.one_of(
        st.integers(min_value=0, max_value=10_000_000),
        st.floats(
            min_value=0.0, max_value=10_000_000.0, allow_nan=False, allow_infinity=False
        ),
    ),
    rho=st.floats(
        min_value=1e-6,
        max_value=1_000_000.0,
        allow_nan=False,
        allow_infinity=False,
        exclude_min=False,
    ),
)
def test_property_18_estimated_wait_time_formula(
    position: int | float, rho: float
) -> None:
    """Feature: virtual-waiting-room, Property 18: For any Queue_Position p and observed promotion rate ρ > 0, the returned Estimated_Wait_Time equals p / ρ.

    Validates: Requirements 8.4
    """
    result = estimated_wait(position, rho)
    assert result == pytest.approx(position / rho)


# --------------------------------------------------------------------------- #
# Property 19: Cacheable status responses bound staleness
# --------------------------------------------------------------------------- #
@settings(max_examples=100)
@given(staleness_bound=st.integers(min_value=1, max_value=86_400))
def test_property_19_cacheable_status_bounds_staleness(staleness_bound: int) -> None:
    """Feature: virtual-waiting-room, Property 19: For any cacheable status response, the attached caching directive's max-age is greater than zero and does not exceed the configured staleness bound.

    Validates: Requirements 8.8
    """
    n = max_age(staleness_bound)
    # max-age is strictly positive and never exceeds the configured bound.
    assert 0 < n <= staleness_bound

    directive = cache_directive(staleness_bound)
    # Well-formed "max-age=<n>" string carrying exactly that n.
    assert directive == f"max-age={n}"
    prefix, _, value = directive.partition("=")
    assert prefix == "max-age"
    parsed = int(value)
    assert parsed == n
    assert 0 < parsed <= staleness_bound
