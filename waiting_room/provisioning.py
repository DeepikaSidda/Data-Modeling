"""DynamoDB table provisioning for the Virtual Waiting Room.

This module creates the single ``WaitingRoom`` table and its two Global
Secondary Indexes exactly as specified in the design document and the frozen
NoSQL Workbench model (``submission/nosql-workbench-model.json``):

* **Base table** ``WaitingRoom`` - string ``PK`` / ``SK`` composite key,
  ``PAY_PER_REQUEST`` (on-demand) billing.
* **``WaitingIndex``** (sparse GSI) - ``PK = Waiting_Shard`` (S),
  ``SK = Ordering_Key`` (S), ``INCLUDE`` projection of ``Fan_Id`` and
  ``Entry_Timestamp``. An item appears here only while its ``Waiting_Shard``
  attribute is present (i.e. while ``Eligibility_Status = WAITING``), which is
  what lets the promoter read a shrinking front-of-line set in position order
  without a scan or filter.
* **``EligibilityIndex``** GSI - ``PK = Elig_PK`` (S),
  ``SK = Promotion_Time`` (N), ``INCLUDE`` projection of ``Fan_Id``,
  ``Batch_Id``, ``Write_Shard`` and ``Ordering_Key``. Serves capacity
  accounting, the expiry sweep, and status-by-status queries.

The client/resource factories accept an ``endpoint_url`` so the same code can
target a real AWS account, DynamoDB Local, or an in-process ``moto`` mock.

Two seed helpers write the per-event control items - the ``CONFIG`` item
(open-state and tunables) and the ``CAPACITY`` counter item - so an event's
queue is ready to accept admissions.

Requirements: 2.1, 2.3, 2.4.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3

from waiting_room.config import EventConfig

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource

__all__ = [
    "TABLE_NAME",
    "WAITING_INDEX_NAME",
    "ELIGIBILITY_INDEX_NAME",
    "CONFIG_SK",
    "CAPACITY_SK",
    "make_dynamodb_client",
    "make_dynamodb_resource",
    "event_pk",
    "build_table_definition",
    "create_waiting_room_table",
    "table_exists",
    "delete_waiting_room_table",
    "seed_event_config",
    "seed_capacity",
    "seed_event",
]


# --------------------------------------------------------------------------- #
# Names / constants (must match the frozen NoSQL Workbench model)
# --------------------------------------------------------------------------- #
TABLE_NAME: str = "WaitingRoom"
WAITING_INDEX_NAME: str = "WaitingIndex"
ELIGIBILITY_INDEX_NAME: str = "EligibilityIndex"

#: Sort-key literals for the per-event control items.
CONFIG_SK: str = "CONFIG"
CAPACITY_SK: str = "CAPACITY"


# --------------------------------------------------------------------------- #
# Client / resource factories
# --------------------------------------------------------------------------- #
def make_dynamodb_client(
    *,
    endpoint_url: str | None = None,
    region_name: str = "us-east-1",
    **kwargs: Any,
) -> "DynamoDBClient":
    """Return a low-level DynamoDB client.

    Pass ``endpoint_url`` (e.g. ``http://localhost:8000`` for DynamoDB Local)
    to target a local endpoint instead of the AWS service. Any additional
    keyword arguments are forwarded to :func:`boto3.client`.
    """
    return boto3.client(
        "dynamodb",
        endpoint_url=endpoint_url,
        region_name=region_name,
        **kwargs,
    )


def make_dynamodb_resource(
    *,
    endpoint_url: str | None = None,
    region_name: str = "us-east-1",
    **kwargs: Any,
) -> "DynamoDBServiceResource":
    """Return a DynamoDB service resource.

    Pass ``endpoint_url`` to target DynamoDB Local / moto. Additional keyword
    arguments are forwarded to :func:`boto3.resource`.
    """
    return boto3.resource(
        "dynamodb",
        endpoint_url=endpoint_url,
        region_name=region_name,
        **kwargs,
    )


# --------------------------------------------------------------------------- #
# Key helpers
# --------------------------------------------------------------------------- #
def event_pk(event_id: str) -> str:
    """Return the partition key for an event's control items (``EVT#<id>``)."""
    return f"EVT#{event_id}"


# --------------------------------------------------------------------------- #
# Table definition
# --------------------------------------------------------------------------- #
def build_table_definition(table_name: str = TABLE_NAME) -> dict[str, Any]:
    """Return the ``create_table`` keyword arguments for the ``WaitingRoom`` table.

    Kept as a pure function so it can be asserted against in tests without
    touching AWS. Only attributes that participate in a key (table or index)
    are declared in ``AttributeDefinitions``; every other attribute is
    schemaless, as DynamoDB requires.
    """
    return {
        "TableName": table_name,
        "AttributeDefinitions": [
            # Base-table composite key.
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            # WaitingIndex keys.
            {"AttributeName": "Waiting_Shard", "AttributeType": "S"},
            {"AttributeName": "Ordering_Key", "AttributeType": "S"},
            # EligibilityIndex keys.
            {"AttributeName": "Elig_PK", "AttributeType": "S"},
            {"AttributeName": "Promotion_Time", "AttributeType": "N"},
        ],
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": WAITING_INDEX_NAME,
                "KeySchema": [
                    {"AttributeName": "Waiting_Shard", "KeyType": "HASH"},
                    {"AttributeName": "Ordering_Key", "KeyType": "RANGE"},
                ],
                "Projection": {
                    "ProjectionType": "INCLUDE",
                    "NonKeyAttributes": ["Fan_Id", "Entry_Timestamp"],
                },
            },
            {
                "IndexName": ELIGIBILITY_INDEX_NAME,
                "KeySchema": [
                    {"AttributeName": "Elig_PK", "KeyType": "HASH"},
                    {"AttributeName": "Promotion_Time", "KeyType": "RANGE"},
                ],
                "Projection": {
                    "ProjectionType": "INCLUDE",
                    "NonKeyAttributes": [
                        "Fan_Id",
                        "Batch_Id",
                        "Write_Shard",
                        "Ordering_Key",
                    ],
                },
            },
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }


# --------------------------------------------------------------------------- #
# Table lifecycle
# --------------------------------------------------------------------------- #
def table_exists(client: "DynamoDBClient", table_name: str = TABLE_NAME) -> bool:
    """Return ``True`` if ``table_name`` already exists."""
    try:
        client.describe_table(TableName=table_name)
        return True
    except client.exceptions.ResourceNotFoundException:
        return False


def create_waiting_room_table(
    client: "DynamoDBClient",
    *,
    table_name: str = TABLE_NAME,
    wait: bool = True,
) -> dict[str, Any]:
    """Create the ``WaitingRoom`` table with both GSIs.

    Idempotent: if the table already exists, its current description is
    returned instead of raising. When ``wait`` is true the call blocks until
    the table reports ``ACTIVE``.

    Returns the ``TableDescription`` dict.
    """
    if table_exists(client, table_name):
        return client.describe_table(TableName=table_name)["Table"]

    response = client.create_table(**build_table_definition(table_name))

    if wait:
        waiter = client.get_waiter("table_exists")
        waiter.wait(TableName=table_name)
        return client.describe_table(TableName=table_name)["Table"]

    return response["TableDescription"]


def delete_waiting_room_table(
    client: "DynamoDBClient",
    *,
    table_name: str = TABLE_NAME,
    wait: bool = True,
) -> None:
    """Delete the table if it exists (useful for test teardown)."""
    if not table_exists(client, table_name):
        return
    client.delete_table(TableName=table_name)
    if wait:
        client.get_waiter("table_not_exists").wait(TableName=table_name)


# --------------------------------------------------------------------------- #
# Seed helpers
# --------------------------------------------------------------------------- #
def seed_event_config(
    client: "DynamoDBClient",
    event_id: str,
    config: EventConfig | None = None,
    *,
    table_name: str = TABLE_NAME,
) -> dict[str, Any]:
    """Write the ``CONFIG`` item for an event.

    Mirrors the ``EVT#<Event_Id> / CONFIG`` item: open-state plus the tunables
    that drive sharding, queue-full gating, promotion, and active-pool
    regulation. Uses the supplied :class:`EventConfig` (or defaults) as the
    source of values. Returns the item dict that was written.

    Requirements: 2.1, 2.3.
    """
    cfg = config or EventConfig()
    pk = event_pk(event_id)
    item = {
        "PK": {"S": pk},
        "SK": {"S": CONFIG_SK},
        "Event_Id": {"S": event_id},
        "Event_Status": {"S": cfg.event_status.value},
        "Shard_Count": {"N": str(cfg.shard_count)},
        "Max_Queue_Size": {"N": str(cfg.max_queue_size)},
        "Eligibility_Window_Secs": {"N": str(cfg.eligibility_window_secs)},
        "Max_Batch_Size": {"N": str(cfg.max_batch_size)},
        "Active_Target": {"N": str(cfg.active_target)},
    }
    client.put_item(TableName=table_name, Item=item)
    return item


def seed_capacity(
    client: "DynamoDBClient",
    event_id: str,
    config: EventConfig | None = None,
    *,
    table_name: str = TABLE_NAME,
    eligible_count: int = 0,
    active_count: int = 0,
    promoted_total: int = 0,
    version: int = 0,
) -> dict[str, Any]:
    """Write the ``CAPACITY`` counter item for an event.

    Mirrors the ``EVT#<Event_Id> / CAPACITY`` item. ``Downstream_Capacity`` is
    taken from the supplied :class:`EventConfig` (or defaults); the running
    counters start at zero unless overridden. Returns the item dict written.

    Requirements: 2.1, 2.3.
    """
    cfg = config or EventConfig()
    pk = event_pk(event_id)
    item = {
        "PK": {"S": pk},
        "SK": {"S": CAPACITY_SK},
        "Event_Id": {"S": event_id},
        "Downstream_Capacity": {"N": str(cfg.downstream_capacity)},
        "Eligible_Count": {"N": str(eligible_count)},
        "Active_Count": {"N": str(active_count)},
        "Promoted_Total": {"N": str(promoted_total)},
        "Version": {"N": str(version)},
    }
    client.put_item(TableName=table_name, Item=item)
    return item


def seed_event(
    client: "DynamoDBClient",
    event_id: str,
    config: EventConfig | None = None,
    *,
    table_name: str = TABLE_NAME,
) -> dict[str, dict[str, Any]]:
    """Seed both the ``CONFIG`` and ``CAPACITY`` items for an event.

    Convenience wrapper returning ``{"config": ..., "capacity": ...}`` with the
    two items that were written, so a freshly created table is immediately
    ready to accept admissions for ``event_id``.

    Requirements: 2.1, 2.3, 2.4.
    """
    cfg = config or EventConfig()
    return {
        "config": seed_event_config(client, event_id, cfg, table_name=table_name),
        "capacity": seed_capacity(client, event_id, cfg, table_name=table_name),
    }
