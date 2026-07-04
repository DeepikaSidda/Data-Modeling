"""Simulated shard-distribution smoke test (Task 19.1, non-PBT, non-AWS).

This is a *deterministic* load/burst smoke harness for the write-sharding
scheme in :mod:`waiting_room.sharding`. It is intentionally **not** a
Hypothesis property test and touches **no** AWS/DynamoDB resources - it simply
hashes a large synthetic population of ``Fan_Id``s through
:func:`compute_write_shard` for the two documented shard counts (the
illustrative ``1000`` and the burst-driven ``4000``) and checks that the
resulting per-shard load stays within a bounded deviation of the mean. If the
load is balanced, no single partition would breach DynamoDB's per-partition
write ceiling when the sample is extrapolated to the real burst.

Why this matters (see design.md - "Scalability and Cost Analysis"):

* The target burst is **10,000,000 fans in ~10 s => ~1,000,000 admissions/s**.
* DynamoDB's per-partition write ceiling is **1,000 WCU/s**.
* The **binding constraint is the sparse ``WaitingIndex`` GSI**, which
  partitions *only* by ``Waiting_Shard`` (there are exactly ``Shard_Count`` of
  them). Every admission produces one GSI write, so the GSI's per-partition
  write rate is ``(admissions/s) / Shard_Count`` - *assuming the shards are
  evenly loaded*. If the hash piled a disproportionate share onto one shard,
  that shard's GSI partition would throttle even though the aggregate looks
  fine. This test is the guard that the assignment stays balanced.
* At ``Shard_Count = 1000`` each GSI partition sees the mean ``1,000,000 /
  1000 = 1,000 WCU/s`` - exactly the ceiling with zero headroom, which is
  precisely why 1000 is *illustrative only*. At ``Shard_Count = 4000`` the mean
  falls to ``~250 WCU/s``, comfortably under the ceiling.

Scaling note: hashing the full 10M-fan burst here would be slow and adds no
signal (SHA-256 distribution does not change shape with N once N >> shards).
We use **200,000** distinct ``Fan_Id``s - large enough that even the
4000-shard case averages ~50 fans/shard, so the distribution is tight while
the run finishes in a couple of seconds. The *balance ratio* we measure is
population-independent, so it extrapolates directly to the real burst rate.

Requirements: 2.2, 9.4.
"""

from __future__ import annotations

import math
from collections import Counter
from functools import lru_cache

import pytest

from waiting_room.sharding import compute_write_shard

# --------------------------------------------------------------------------- #
# Scenario constants (documented, deterministic)
# --------------------------------------------------------------------------- #

# Scaled-down synthetic population. A proxy for the 10M-fan burst; chosen so the
# per-shard sample is large (>=50 fans/shard even at 4000 shards) yet the run
# finishes in ~1-2 s.
NUM_FANS = 200_000

# The two documented shard counts: the illustrative NoSQL Workbench value and
# the burst-driven production value for a 10M/10s burst (design.md 4a/4c).
SHARD_COUNTS = (1_000, 4_000)

# DynamoDB hard per-partition write ceiling.
PER_PARTITION_WCU_CEILING = 1_000

# Real target burst rate: 10,000,000 fans / ~10 s.
BURST_ADMISSIONS_PER_SEC = 1_000_000

# Robustness bound for per-shard balance: the busiest shard must stay within
# ``mean + SIGMA_BOUND * sqrt(mean)`` of the mean. For an even hash the shard
# populations are ~Poisson(mean) (std = sqrt(mean)); an 8-sigma allowance never
# flakes on ordinary hash variation yet fails loudly for a hot-shard
# assignment that collapses ids onto a few partitions.
SIGMA_BOUND = 8.0

# For the burst extrapolation we insist the busiest shard, scaled to the real
# 1M/s burst, stays under this fraction of the ceiling at Shard_Count=4000.
SAFE_CEILING_FRACTION = 0.6


def _fan_id(i: int) -> str:
    """Deterministic distinct synthetic Fan_Id (server-issued-style token)."""
    return f"FAN#{i:09d}"


@lru_cache(maxsize=None)
def _shard_counts(shard_count: int) -> Counter:
    """Assign every synthetic fan a shard and tally the per-shard population."""
    counts: Counter = Counter()
    for i in range(NUM_FANS):
        shard = compute_write_shard(_fan_id(i), shard_count)
        assert 0 <= shard < shard_count  # range invariant (Req 9.4)
        counts[shard] += 1
    return counts


# --------------------------------------------------------------------------- #
# Balance: per-shard load stays within a bounded deviation of the mean
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("shard_count", SHARD_COUNTS)
def test_shard_distribution_is_balanced_within_bound(shard_count: int) -> None:
    """No shard receives a disproportionate share of the synthetic burst.

    For each documented shard count, asserts every shard is populated and the
    busiest shard stays within ``mean + 8*sqrt(mean)`` - a bounded deviation of
    uniform. A balanced distribution means the per-partition load (and thus the
    sparse ``WaitingIndex`` GSI partition load) never spikes above the ceiling.

    Validates: Requirements 2.2, 9.4
    """
    counts = _shard_counts(shard_count)

    populated = len(counts)
    mean = NUM_FANS / shard_count
    max_count = max(counts.values())
    min_count = min(counts.values()) if populated == shard_count else 0

    # A broken assignment that collapses ids onto a few shards would leave most
    # shards empty; every shard must carry part of the load.
    assert populated == shard_count, (
        f"only {populated}/{shard_count} shards populated - assignment is not "
        f"spreading load across all shards"
    )

    upper_bound = mean + SIGMA_BOUND * math.sqrt(mean)
    assert max_count <= upper_bound, (
        f"busiest shard has {max_count} fans vs mean {mean:.1f} "
        f"(bound {upper_bound:.1f} = mean + {SIGMA_BOUND}*sqrt(mean)) - "
        f"distribution is too skewed"
    )

    print(
        "\n[shard-distribution smoke] "
        f"fans={NUM_FANS} shard_count={shard_count} "
        f"mean={mean:.2f} min={min_count} max={max_count} "
        f"max/mean={max_count / mean:.3f} bound={upper_bound:.1f} "
        f"populated={populated}/{shard_count}"
    )


# --------------------------------------------------------------------------- #
# Extrapolation: busiest shard stays under the partition ceiling at 4000
# --------------------------------------------------------------------------- #
def test_extrapolated_burst_stays_under_partition_ceiling() -> None:
    """Extrapolate the observed skew to the real burst and check the ceiling.

    Extrapolation math (design.md - Scalability and Cost Analysis 4c) at the
    production ``Shard_Count = 4000``:

      * Target burst:            1,000,000 admissions/s.
      * Expected per-partition:  1,000,000 / 4,000 = ~250 WCU/s (the mean),
                                 well under the 1,000 WCU/s ceiling.

    Each admission produces exactly one ``WaitingIndex`` GSI write, and the GSI
    partitions only by ``Waiting_Shard`` (Shard_Count partitions). So a shard
    holding a fraction ``f = max_count / total`` of the population would, under
    a uniform-rate burst, receive ``f * BURST_ADMISSIONS_PER_SEC`` GSI writes/s
    on its single partition. We take the *observed* busiest shard's share as
    the worst case and require it to stay under a safe fraction of the ceiling.

    Validates: Requirements 2.2, 9.4
    """
    shard_count = 4_000
    counts = _shard_counts(shard_count)
    total = sum(counts.values())
    max_count = max(counts.values())

    busiest_share = max_count / total
    observed_peak_wcu = busiest_share * BURST_ADMISSIONS_PER_SEC
    expected_mean_wcu = BURST_ADMISSIONS_PER_SEC / shard_count  # ~250

    safe_ceiling = SAFE_CEILING_FRACTION * PER_PARTITION_WCU_CEILING  # 600

    print(
        "\n[burst-extrapolation smoke] "
        f"expected_mean={expected_mean_wcu:.1f} WCU/s "
        f"observed_peak={observed_peak_wcu:.1f} WCU/s "
        f"ceiling={PER_PARTITION_WCU_CEILING} safe={safe_ceiling:.0f}"
    )

    # Sanity: the mean is the ~250 WCU/s the design predicts.
    assert expected_mean_wcu <= 300, (
        f"per-partition mean {expected_mean_wcu:.1f} WCU/s exceeds the "
        f"design's ~250 WCU/s expectation"
    )

    # Even the busiest GSI partition, scaled to 1M/s, stays under a safe
    # fraction of the 1,000 WCU/s per-partition ceiling.
    assert observed_peak_wcu < safe_ceiling, (
        f"busiest shard would see {observed_peak_wcu:.1f} WCU/s at the burst "
        f"rate, exceeding the safe {safe_ceiling:.0f} WCU/s budget"
    )
    assert observed_peak_wcu < PER_PARTITION_WCU_CEILING


def test_assignment_is_deterministic() -> None:
    """Shard assignment is stable per Fan_Id across repeated computation.

    The status-read path recomputes the shard from the token rather than
    consulting a directory, so the same Fan_Id must always map to the same
    shard, at every documented shard count.

    Validates: Requirements 9.4
    """
    sample = [_fan_id(i) for i in range(0, NUM_FANS, NUM_FANS // 200)]
    for shard_count in SHARD_COUNTS:
        first = {fid: compute_write_shard(fid, shard_count) for fid in sample}
        for fid in sample:
            assert compute_write_shard(fid, shard_count) == first[fid]
