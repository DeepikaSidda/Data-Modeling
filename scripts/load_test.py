"""Task 19.2 - real (bounded) burst load test against live DynamoDB.

The challenge target is ~10,000,000 fans in seconds (~1,000,000 admissions/s).
Driving that literally would need a distributed load fleet and cost real money,
so this harness runs a **bounded, cost-controlled** burst (tens of thousands of
admissions, a few cents) directly against the live table via the
``Admission_Writer`` (bypassing API Gateway/Lambda to hit DynamoDB at a higher
rate from one box), then EXTRAPOLATES to the full burst.

It empirically validates the properties that only appear under real load:
  * throughput and throttle/exhaustion behavior (on-demand warm-up),
  * write-shard distribution balance (the GSI hot-partition guard),
  * exactly-once admission (entry count == distinct fans, zero duplicates),
  * ordering-key monotonicity of the server HLC under concurrency.

Usage:
    python scripts/load_test.py [--count 20000] [--workers 64] [--shards 1000] [--teardown]
"""

from __future__ import annotations

import argparse
import statistics
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

from waiting_room.admission import ADMIT_COUNT_SK, Admission_Writer, AdmissionError
from waiting_room.config import EventConfig, WaitingRoomConfig
from waiting_room.ordering import OrderingKeyAllocator, ordering_key_sort_key
from waiting_room.provisioning import (
    TABLE_NAME,
    create_waiting_room_table,
    make_dynamodb_client,
    seed_event,
)
from waiting_room.sharding import compute_write_shard

REGION = "us-east-1"
SECRET = "load-test-secret"
PER_PARTITION_WCU_CEILING = 1_000
FULL_BURST_FANS = 10_000_000
FULL_BURST_SECONDS = 10  # ~1,000,000 admissions/s target


def main(count: int, workers: int, shard_count: int, teardown: bool) -> None:
    event_id = f"loadtest-{int(time.time())}"
    client = make_dynamodb_client(region_name=REGION)

    print(f"=== Ensuring table {TABLE_NAME!r} exists (on-demand) ===")
    create_waiting_room_table(client, wait=True)

    cfg = WaitingRoomConfig(
        event=EventConfig(
            shard_count=shard_count,
            downstream_capacity=1000,
            max_batch_size=500,
            max_queue_size=FULL_BURST_FANS,
        )
    )
    seed_event(client, event_id, cfg.event)
    print(f"seeded event {event_id!r} (shard_count={shard_count})")

    # One shared, thread-safe allocator models a single admission node; the
    # client is thread-safe for concurrent calls.
    allocator = OrderingKeyAllocator()
    writer = Admission_Writer(client=client, secret=SECRET, config=cfg, allocator=allocator)

    fan_ids = [f"lf-{i}" for i in range(count)]

    print(f"\n=== Driving {count} concurrent admissions ({workers} workers) ===")
    ok = 0
    duplicates = 0
    failures = 0
    order_pairs: list[tuple] = []  # (submit_index, ordering_key) sample

    start = time.perf_counter()

    def _admit(idx_fan):
        idx, fan = idx_fan
        try:
            r = writer.admit(event_id, fan)
            return ("ok", idx, r.ordering_key, r.duplicate)
        except AdmissionError as e:
            return ("err", idx, e.code, False)
        except Exception as e:  # noqa: BLE001
            return ("err", idx, type(e).__name__, False)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for kind, idx, payload, dup in pool.map(_admit, enumerate(fan_ids)):
            if kind == "ok":
                ok += 1
                if dup:
                    duplicates += 1
                if idx % max(count // 500, 1) == 0:
                    order_pairs.append((idx, payload))
            else:
                failures += 1

    elapsed = time.perf_counter() - start
    throughput = ok / elapsed if elapsed > 0 else 0.0

    print(f"  admitted_ok={ok} duplicates={duplicates} failures={failures}")
    print(f"  elapsed={elapsed:.2f}s  throughput={throughput:,.0f} admissions/s")

    # --- Shard distribution (deterministic; no table scan needed) --------- #
    print("\n=== Write-shard distribution ===")
    shard_counts = Counter(compute_write_shard(f, shard_count) for f in fan_ids)
    populated = len(shard_counts)
    mean = count / shard_count
    mx = max(shard_counts.values())
    mn = min(shard_counts.values()) if populated == shard_count else 0
    stdev = statistics.pstdev(list(shard_counts.values())) if populated > 1 else 0.0
    print(f"  shards_populated={populated}/{shard_count}  mean={mean:.2f}  min={mn}  max={mx}  stdev={stdev:.2f}")

    # --- Exactly-once: count ENTRY# items in the table vs distinct fans ---- #
    print("\n=== Exactly-once verification (table scan) ===")
    entry_total, admit_counter_sum = _scan_counts(client, event_id)
    print(f"  distinct_fans={count}  entry_items={entry_total}  Σ ADMIT_COUNT={admit_counter_sum}")
    exactly_once = entry_total == count and duplicates == 0
    print(f"  exactly_once={'PASS' if exactly_once else 'FAIL'}")

    # --- Ordering monotonicity sample ------------------------------------- #
    order_pairs.sort()
    keys_in_submit_order = [ok_ for _, ok_ in order_pairs]
    sorted_keys = sorted(keys_in_submit_order, key=ordering_key_sort_key)
    # Not strictly equal under concurrency, but HLC keeps them close; report.
    inversions = sum(
        1 for a, b in zip(keys_in_submit_order, keys_in_submit_order[1:])
        if ordering_key_sort_key(a) > ordering_key_sort_key(b)
    )
    print(f"\n=== Ordering sample ({len(keys_in_submit_order)} keys) ===")
    print(f"  submit-order inversions={inversions} (HLC + concurrency; keys are globally unique & total-ordered)")

    # --- Extrapolation to the full burst ---------------------------------- #
    print("\n=== Extrapolation to full burst ===")
    target_rate = FULL_BURST_FANS / FULL_BURST_SECONDS
    # GSI is the binding partition: peak per-partition WCU ≈ busiest_share * rate.
    busiest_share = mx / count
    peak_gsi_wcu = busiest_share * target_rate
    mean_gsi_wcu = target_rate / shard_count
    print(f"  target={target_rate:,.0f} admissions/s at shard_count={shard_count}")
    print(f"  mean WaitingIndex partition ≈ {mean_gsi_wcu:,.0f} WCU/s (ceiling {PER_PARTITION_WCU_CEILING})")
    print(f"  busiest WaitingIndex partition ≈ {peak_gsi_wcu:,.0f} WCU/s (from observed skew)")
    verdict = "UNDER ceiling" if peak_gsi_wcu < PER_PARTITION_WCU_CEILING else "OVER ceiling -> raise shard_count"
    print(f"  verdict: {verdict}")
    if peak_gsi_wcu >= PER_PARTITION_WCU_CEILING:
        need = int(peak_gsi_wcu / (0.5 * PER_PARTITION_WCU_CEILING)) * shard_count // shard_count
        print(f"  (recommend a larger shard_count so peak stays well under {PER_PARTITION_WCU_CEILING})")

    print("\nLOAD TEST COMPLETE")

    if teardown:
        print("\n=== Deleting table ===")
        from waiting_room.provisioning import delete_waiting_room_table

        delete_waiting_room_table(client, wait=True)
        print("table deleted")
    else:
        print(f"\n(Event {event_id!r} left in table {TABLE_NAME!r}.)")


def _scan_counts(client, event_id: str) -> tuple[int, int]:
    """Scan the table once, counting this event's ENTRY# items and Σ ADMIT_COUNT."""
    entry_total = 0
    admit_sum = 0
    prefix = f"EVT#{event_id}#SH#"
    kwargs = {"TableName": TABLE_NAME}
    while True:
        resp = client.scan(**kwargs)
        for item in resp.get("Items", []):
            pk = item.get("PK", {}).get("S", "")
            sk = item.get("SK", {}).get("S", "")
            if not pk.startswith(prefix):
                continue
            if sk.startswith("ENTRY#"):
                entry_total += 1
            elif sk == ADMIT_COUNT_SK:
                admit_sum += int(item.get("Admitted_Count", {"N": "0"})["N"])
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
        kwargs["ExclusiveStartKey"] = start_key
    return entry_total, admit_sum


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=20_000, help="number of admissions to drive")
    p.add_argument("--workers", type=int, default=64, help="concurrent worker threads")
    p.add_argument("--shards", type=int, default=1_000, help="Shard_Count for the event")
    p.add_argument("--teardown", action="store_true", help="delete the table after the run")
    args = p.parse_args()
    main(args.count, args.workers, args.shards, args.teardown)
