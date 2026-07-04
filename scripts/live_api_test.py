"""Exercise the DEPLOYED Virtual Waiting Room over its real HTTP API.

Steps:
  1. Seed the ``demo-event`` CONFIG/CAPACITY items on the deployed table
     (shard_count matches the Lambda default of 1000; small capacity so
     promotion is observable).
  2. POST /admit for several fans via the live API Gateway endpoint.
  3. GET /status for one fan (expect WAITING).
  4. Invoke the deployed promoter Lambda directly (so we don't wait for the
     1-minute schedule).
  5. GET /status again (expect ELIGIBLE / may_browse True).

Run:  python scripts/live_api_test.py
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

import boto3

from waiting_room.config import EventConfig
from waiting_room.provisioning import make_dynamodb_client, seed_event

REGION = "us-east-1"
STACK = "WaitingRoomStack"
# Fresh event id per run so CONFIG/CAPACITY start clean (the scheduled promoter
# targets "demo-event"; here we invoke the promoter Lambda directly with this
# id, which its handler honors via detail.event_id).
EVENT_ID = f"apitest-{int(time.time())}"


def _stack_outputs() -> dict[str, str]:
    cfn = boto3.client("cloudformation", region_name=REGION)
    outs = cfn.describe_stacks(StackName=STACK)["Stacks"][0]["Outputs"]
    return {o["OutputKey"]: o["OutputValue"] for o in outs}


def _promote_function_name() -> str:
    cfn = boto3.client("cloudformation", region_name=REGION)
    resources = cfn.describe_stack_resources(StackName=STACK)["StackResources"]
    for r in resources:
        if r["ResourceType"] == "AWS::Lambda::Function" and r["LogicalResourceId"].startswith("PromoteFunction"):
            return r["PhysicalResourceId"]
    raise RuntimeError("PromoteFunction not found in stack")


def _post_json(url: str, payload: dict) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


def _get(url: str, token: str) -> tuple[int, dict]:
    req = urllib.request.Request(url, method="GET",
                                 headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


def main() -> None:
    outputs = _stack_outputs()
    api = outputs["ApiUrl"].rstrip("/")
    print(f"API: {api}")
    print(f"Table: {outputs['TableName']}")

    print("\n=== Seeding demo-event (shard_count=1000, capacity=3, max_batch=2) ===")
    client = make_dynamodb_client(region_name=REGION)
    cfg = EventConfig(shard_count=1000, downstream_capacity=3, max_batch_size=2,
                      eligibility_window_secs=600, max_queue_size=1_000_000)
    seed_event(client, EVENT_ID, cfg)
    print("seeded CONFIG + CAPACITY")

    print("\n=== POST /admit for 5 fans ===")
    tokens = {}
    for i in range(5):
        fan = f"api-fan-{i}"
        status, body = _post_json(f"{api}/admit", {"event_id": EVENT_ID, "fan_id": fan})
        tokens[fan] = body.get("entry_token")
        print(f"  {fan}: HTTP {status} shard={body.get('write_shard')} dup={body.get('duplicate')}")

    fan0 = "api-fan-0"
    print(f"\n=== GET /status ({fan0}) BEFORE promotion ===")
    status, body = _get(f"{api}/status", tokens[fan0])
    print(f"  HTTP {status}: status={body.get('eligibility_status')} position={body.get('position')} "
          f"may_browse={body.get('may_browse')} reason={body.get('reason')} cache={body.get('cache_control')}")

    print("\n=== Invoking promoter Lambda twice (capacity 3, batch 2) ===")
    fn = _promote_function_name()
    lam = boto3.client("lambda", region_name=REGION)
    for n in range(2):
        r = lam.invoke(FunctionName=fn, Payload=json.dumps({"detail": {"event_id": EVENT_ID}}).encode())
        summary = json.loads(r["Payload"].read().decode())
        print(f"  cycle {n+1}: promoted={summary.get('promoted')} expired={summary.get('expired')} "
              f"skipped={summary.get('skipped')}")

    print(f"\n=== GET /status ({fan0}) AFTER promotion ===")
    status, body = _get(f"{api}/status", tokens[fan0])
    print(f"  HTTP {status}: status={body.get('eligibility_status')} position={body.get('position')} "
          f"may_browse={body.get('may_browse')} reason={body.get('reason')}")

    print("\nLIVE API TEST COMPLETE")


if __name__ == "__main__":
    main()
