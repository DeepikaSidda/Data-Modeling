"""Live end-to-end smoke test against REAL AWS DynamoDB (us-east-1).

Provisions the WaitingRoom table + both GSIs (on-demand billing), then exercises
the full flow against real DynamoDB:

    seed event -> admit fans -> read status -> promote a batch ->
    read status again -> expire an eligible entry.

Run:  python scripts/live_smoke.py

Leaves the table in place. Pass --teardown to delete it afterwards.
"""

from __future__ import annotations

import argparse
import time

from waiting_room.admission import Admission_Writer
from waiting_room.aggregator import StreamAggregator
from waiting_room.config import EventConfig, WaitingRoomConfig
from waiting_room.promoter import BatchPromoter
from waiting_room.provisioning import (
    TABLE_NAME,
    create_waiting_room_table,
    delete_waiting_room_table,
    make_dynamodb_client,
    seed_event,
)
from waiting_room.status_reader import PositionAggregates, Status_Reader

REGION = "us-east-1"
SECRET = "live-smoke-secret-please-rotate"
EVENT_ID = f"live-{int(time.time())}"

# Small, fast config so the smoke test is cheap and quick.
CONFIG = WaitingRoomConfig(
    event=EventConfig(
        shard_count=8,
        downstream_capacity=3,
        max_batch_size=2,
        eligibility_window_secs=120,
        max_queue_size=1_000_000,
    )
)


def _banner(msg: str) -> None:
    print(f"\n=== {msg} ===")


def main(teardown: bool) -> None:
    client = make_dynamodb_client(region_name=REGION)

    _banner(f"Provisioning table {TABLE_NAME!r} + GSIs (on-demand) in {REGION}")
    desc = create_waiting_room_table(client, wait=True)
    print(f"table status: {desc.get('TableStatus')}")
    gsis = [g["IndexName"] for g in desc.get("GlobalSecondaryIndexes", [])]
    print(f"GSIs: {gsis}")

    _banner(f"Seeding event {EVENT_ID!r} (capacity={CONFIG.event.downstream_capacity}, "
            f"max_batch={CONFIG.event.max_batch_size})")
    seed_event(client, EVENT_ID, CONFIG.event)

    _banner("Admitting 6 fans")
    writer = Admission_Writer(client=client, secret=SECRET, config=CONFIG)
    results = {}
    for i in range(6):
        fan = f"fan-{i}"
        r = writer.admit(EVENT_ID, fan)
        results[fan] = r
        print(f"  {fan}: ordering_key={r.ordering_key}  shard={r.write_shard}  dup={r.duplicate}")

    # Idempotency check: re-admit fan-0.
    dup = writer.admit(EVENT_ID, "fan-0")
    print(f"  re-admit fan-0: dup={dup.duplicate} same_key={dup.ordering_key == results['fan-0'].ordering_key}")

    _banner("Status BEFORE promotion (fan-0)")
    # Simple aggregates: rank from admission order, promoted_total=0, rate=1/s.
    def aggregates_before(claims):
        return PositionAggregates(
            admission_sequence_rank=1, promoted_total=0, promotion_rate=1.0, downstream_available=True
        )

    reader = Status_Reader(
        client=client, secret=SECRET, aggregates_provider=aggregates_before, config=CONFIG
    )
    s = reader.read_status(results["fan-0"].entry_token)
    print(f"  status={s.eligibility_status.value} position={s.position} "
          f"eta={s.estimated_wait}s may_browse={s.may_browse} reason={s.reason} cache={s.cache_control}")

    _banner("Running one promotion cycle (capacity=3, max_batch=2 -> expect 2 promoted)")
    promoter = BatchPromoter(client=client)
    outcome = promoter.promote_cycle(EVENT_ID)
    print(f"  promoted={outcome.promoted_count} batch_id={outcome.batch_id} granted={outcome.granted}")

    _banner("Running a second promotion cycle (expect 1 more -> capacity reached at 3)")
    outcome2 = promoter.promote_cycle(EVENT_ID)
    print(f"  promoted={outcome2.promoted_count} (running total={outcome.promoted_count + outcome2.promoted_count})")

    _banner("Status AFTER promotion (fan-0, now expected ELIGIBLE)")
    def aggregates_after(claims):
        return PositionAggregates(
            admission_sequence_rank=1, promoted_total=3, promotion_rate=1.0, downstream_available=True
        )
    reader_after = Status_Reader(
        client=client, secret=SECRET, aggregates_provider=aggregates_after, config=CONFIG
    )
    s2 = reader_after.read_status(results["fan-0"].entry_token)
    print(f"  status={s2.eligibility_status.value} position={s2.position} "
          f"may_browse={s2.may_browse} reason={s2.reason}")

    _banner("Expiry sweep with window=0 (expire currently-eligible entries)")
    exp = promoter.expire_sweep(EVENT_ID, eligibility_window_secs=0)
    print(f"  expired={exp.expired_count} scanned={exp.scanned}")

    _banner("Aggregator: replay is optional; summary from counters")
    cap = client.get_item(
        TableName=TABLE_NAME,
        Key={"PK": {"S": f"EVT#{EVENT_ID}"}, "SK": {"S": "CAPACITY"}},
        ConsistentRead=True,
    )["Item"]
    print(f"  CAPACITY: eligible={cap['Eligible_Count']['N']} active={cap['Active_Count']['N']} "
          f"promoted_total={cap['Promoted_Total']['N']}")

    print("\nLIVE SMOKE TEST COMPLETE ✔")

    if teardown:
        _banner("Tearing down table")
        delete_waiting_room_table(client, wait=True)
        print("table deleted")
    else:
        print(f"\n(Table {TABLE_NAME!r} left in place. Re-run with --teardown to delete it.)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--teardown", action="store_true", help="Delete the table after the test")
    args = parser.parse_args()
    main(args.teardown)
