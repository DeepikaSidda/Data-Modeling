"""Property-based tests for the capacity-reservation model and active-pool
regulation.

Modules under test:
* ``waiting_room/capacity.py`` — the atomic capacity counter and its
  compare-and-set reservation flow plus lifecycle transitions.
* ``waiting_room/active_pool.py`` — the pure active-pool regulation decision
  functions.

These tests implement spec tasks 6.2, 6.3 and 6.5 and validate design.md
Correctness Properties 11, 14 and 16. Each property runs at least 100 examples.

Concurrency (Property 11) is modeled with a *deterministic* interleaving
explorer over the counter's compare-and-set flow — Hypothesis generates the
ordering of read/plan/commit steps across several "promoters", so no real
threads are used and every race is reproducible.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from waiting_room.active_pool import (
    band_lower_bound,
    capacity_headroom,
    needs_refill,
    projected_active,
    refill_target,
)
from waiting_room.capacity import CapacityCounter, CapacityPool


# --------------------------------------------------------------------------- #
# Property 11 — No over-promotion under concurrency (task 6.2)
# --------------------------------------------------------------------------- #
@st.composite
def _concurrency_scenario(draw):
    """Generate an initial capacity state plus several promoters and a
    deterministic interleaving (schedule) of their read/plan/commit steps."""
    capacity = draw(st.integers(min_value=1, max_value=16))
    eligible = draw(st.integers(min_value=0, max_value=capacity))
    active = draw(st.integers(min_value=0, max_value=capacity - eligible))
    n = draw(st.integers(min_value=2, max_value=5))
    requests = draw(
        st.lists(
            st.integers(min_value=0, max_value=capacity + 2),
            min_size=n,
            max_size=n,
        )
    )
    # Arbitrary interleaving: each entry advances that promoter by one step.
    schedule = draw(
        st.lists(st.integers(min_value=0, max_value=n - 1), min_size=0, max_size=4 * n)
    )
    return capacity, eligible, active, requests, schedule


@settings(max_examples=100, deadline=None)
@given(_concurrency_scenario())
def test_property_11_no_over_promotion_under_concurrency(scenario):
    """Feature: virtual-waiting-room, Property 11: For any interleaving of concurrent promotion cycles issuing atomic capacity reservations, the combined count of ELIGIBLE and ACTIVE entries never exceeds Downstream_Capacity at any point, and any reservation whose per-entry transition later fails releases its slot so no capacity is leaked.

    Validates: Requirements 6.4, 10.5
    """
    capacity, eligible0, active0, requests, schedule = scenario
    counter = CapacityCounter(capacity, eligible_count=eligible0, active_count=active0)
    n = len(requests)

    # Promoter state: phase 0=needs read, 1=needs plan, 2=needs commit, 3=done.
    phase = [0] * n
    snap = [None] * n
    plan = [None] * n
    granted = [0] * n

    def step(i: int) -> None:
        if phase[i] == 0:
            snap[i] = counter.read()  # models GetItem
            phase[i] = 1
        elif phase[i] == 1:
            plan[i] = counter.plan_reserve(requests[i], state=snap[i])
            phase[i] = 2
        elif phase[i] == 2:
            version_at_commit = counter.version
            expected = plan[i].expected_version
            ok = counter.try_commit_reserve(plan[i])  # models conditional UpdateItem
            # Optimistic CAS: a fresh plan (version unchanged since read) MUST
            # commit; a stale plan (a concurrent grant bumped the version) MUST
            # be rejected, forcing the promoter to re-read and re-plan.
            assert ok == (expected == version_at_commit)
            if ok:
                granted[i] = plan[i].granted
                phase[i] = 3
            else:
                phase[i] = 0  # stale -> retry from a fresh read
        # The over-promotion invariant holds at *every* observable step.
        assert counter.occupied <= capacity
        assert counter.invariant_holds

    for i in schedule:
        step(i)

    # Drain: run each unfinished promoter to completion uninterrupted. With no
    # interleaving the version cannot change between plan and commit, so every
    # remaining reservation resolves in a bounded number of steps.
    for i in range(n):
        guard = 0
        while phase[i] != 3:
            step(i)
            guard += 1
            assert guard < 16

    total_granted = sum(granted)
    assert counter.occupied == eligible0 + active0 + total_granted
    assert counter.occupied <= capacity
    # Promoted_Total advanced by exactly the granted amount (monotonic).
    assert counter.read().promoted_total == total_granted

    # No capacity leak: a reservation whose per-entry transition later fails
    # releases its slot, restoring available capacity by exactly that amount.
    reserved_eligible = counter.read().eligible_count
    remaining_before = counter.remaining
    occupied_before = counter.occupied
    promoted_before = counter.read().promoted_total
    counter.release(reserved_eligible, pool=CapacityPool.ELIGIBLE)
    assert counter.remaining == remaining_before + reserved_eligible
    assert counter.occupied == occupied_before - reserved_eligible
    # Releasing never decrements the monotonic Promoted_Total.
    assert counter.read().promoted_total == promoted_before


# --------------------------------------------------------------------------- #
# Property 14 — Terminal transitions free capacity (task 6.3)
# --------------------------------------------------------------------------- #
@st.composite
def _terminal_scenario(draw):
    """Generate a valid counter state plus a sequence of lifecycle ops."""
    capacity = draw(st.integers(min_value=1, max_value=20))
    eligible = draw(st.integers(min_value=0, max_value=capacity))
    active = draw(st.integers(min_value=0, max_value=capacity - eligible))
    promoted_total = draw(
        st.integers(min_value=eligible + active, max_value=eligible + active + 50)
    )
    ops = draw(
        st.lists(
            st.tuples(
                st.sampled_from(["complete", "expire", "activate"]),
                st.integers(min_value=0, max_value=capacity),
            ),
            min_size=0,
            max_size=12,
        )
    )
    return capacity, eligible, active, promoted_total, ops


@settings(max_examples=100, deadline=None)
@given(_terminal_scenario())
def test_property_14_terminal_transitions_free_capacity(scenario):
    """Feature: virtual-waiting-room, Property 14: For any ELIGIBLE entry whose eligibility window has elapsed, an expiry sweep transitions it to EXPIRED; and for any ACTIVE → COMPLETED or ELIGIBLE → EXPIRED transition, the freed slot increases available capacity by exactly one.

    Validates: Requirements 5.8, 7.5
    """
    capacity, eligible, active, promoted_total, ops = scenario
    counter = CapacityCounter(
        capacity,
        eligible_count=eligible,
        active_count=active,
        promoted_total=promoted_total,
    )
    prev_promoted = counter.read().promoted_total

    for op, raw in ops:
        state = counter.read()
        remaining_before = counter.remaining
        occupied_before = counter.occupied

        if op == "complete":  # ACTIVE -> COMPLETED, frees `count` slots
            count = min(raw, state.active_count)
            counter.complete(count)
            freed = count
        elif op == "expire":  # ELIGIBLE -> EXPIRED, frees `count` slots
            count = min(raw, state.eligible_count)
            counter.expire(count)
            freed = count
        else:  # activate: ELIGIBLE -> ACTIVE, transfer with net-zero occupancy
            count = min(raw, state.eligible_count)
            counter.activate(count)
            freed = 0

        after = counter.read()
        # A terminal transition increases available capacity by exactly the
        # freed count (and a single freed slot increases it by exactly one).
        assert counter.remaining == remaining_before + freed
        assert counter.occupied == occupied_before - freed
        # Promoted_Total is monotonic: these transitions never decrement it.
        assert after.promoted_total >= prev_promoted
        assert after.promoted_total == prev_promoted
        prev_promoted = after.promoted_total
        assert counter.invariant_holds


# --------------------------------------------------------------------------- #
# Property 16 — Active-pool regulation bounded by capacity (task 6.5)
# --------------------------------------------------------------------------- #
@st.composite
def _regulation_scenario(draw):
    """Generate a committed-pool state, a target/tolerance band, and a sequence
    of frees (completions/expirations) and refill computations."""
    capacity = draw(st.integers(min_value=1, max_value=30))
    eligible = draw(st.integers(min_value=0, max_value=capacity))
    active = draw(st.integers(min_value=0, max_value=capacity - eligible))
    # Target may exceed capacity so the "capacity dominates the band" case is hit.
    target = draw(st.integers(min_value=0, max_value=40))
    tolerance = draw(st.integers(min_value=0, max_value=20))
    ops = draw(
        st.lists(
            st.tuples(
                st.sampled_from(["free_active", "free_eligible", "refill"]),
                st.integers(min_value=0, max_value=capacity),
            ),
            min_size=1,
            max_size=15,
        )
    )
    return capacity, eligible, active, target, tolerance, ops


@settings(max_examples=100, deadline=None)
@given(_regulation_scenario())
def test_property_16_active_pool_regulation_bounded_by_capacity(scenario):
    """Feature: virtual-waiting-room, Property 16: For any sequence of completions/expirations and refills with active-pool regulation enabled, the Active_Fan count is driven toward the configured target and kept within its tolerance band when capacity permits, and never exceeds Downstream_Capacity even when honoring the band would require it — the capacity limit always dominates.

    Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.6
    """
    capacity, active, eligible, target, tolerance = (
        scenario[0],
        scenario[2],
        scenario[1],
        scenario[3],
        scenario[4],
    )
    ops = scenario[5]

    for op, raw in ops:
        projected = projected_active(active, eligible)
        # The committed pool never exceeds capacity at any point in the sequence.
        assert projected <= capacity

        if op == "free_active":
            active -= min(raw, active)
        elif op == "free_eligible":
            eligible -= min(raw, eligible)
        else:  # refill: compute promotions to request this cycle
            refill = refill_target(active, eligible, capacity, target, tolerance)
            headroom = capacity_headroom(active, eligible, capacity)
            want = target - projected

            assert refill >= 0
            # Capacity always dominates: honoring the band can never push the
            # committed pool past Downstream_Capacity.
            assert projected + refill <= capacity
            # A refill never overshoots the band's top (target) — unless the
            # pool is already above target, in which case it stays put.
            assert projected + refill <= max(target, projected)

            if not needs_refill(active, eligible, target, tolerance):
                # Within band or above target -> quiescent (anti-thrash).
                assert refill == 0
            else:
                # Below the band: aim for target, clamped by capacity headroom.
                assert refill == min(want, headroom)
                if headroom >= want:
                    # Capacity permits -> driven exactly up to target, which
                    # lies inside the tolerance band.
                    assert projected + refill == target
                    assert (
                        band_lower_bound(target, tolerance)
                        <= projected + refill
                        <= target
                    )
                else:
                    # Capacity limits the refill: the band is intentionally
                    # broken and the pool fills exactly to capacity.
                    assert projected + refill == capacity
                    assert refill < want

            eligible += refill  # apply the promotions as new ELIGIBLE fans

        # Invariant maintained after every op.
        assert projected_active(active, eligible) <= capacity
