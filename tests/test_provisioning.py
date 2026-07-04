"""Task 1.3 - provisioning schema integration test (moto-backed).

Asserts that :func:`waiting_room.provisioning.create_waiting_room_table` creates
the ``WaitingRoom`` table with EXACTLY the key schema, both GSIs, and the
``INCLUDE`` projections the design (and the frozen NoSQL Workbench model)
require. A projection or key-schema regression here would let higher-level code
"work" in narrow tests yet read incomplete data in production, so this is the
explicit tripwire for the table definition.

Validates: Requirements 2.1, 2.4
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from waiting_room.config import EventConfig
from waiting_room.provisioning import (
    CAPACITY_SK,
    CONFIG_SK,
    ELIGIBILITY_INDEX_NAME,
    TABLE_NAME,
    WAITING_INDEX_NAME,
    create_waiting_room_table,
    event_pk,
    seed_event,
)


@pytest.fixture()
def client():
    with mock_aws():
        yield boto3.client("dynamodb", region_name="us-east-1")


def _key(schema: list[dict], key_type: str) -> str:
    """Return the attribute name for HASH/RANGE from a KeySchema list."""
    return next(k["AttributeName"] for k in schema if k["KeyType"] == key_type)


def _gsi(desc: dict, name: str) -> dict:
    return next(g for g in desc["GlobalSecondaryIndexes"] if g["IndexName"] == name)


def test_base_table_key_schema_and_billing(client):
    desc = create_waiting_room_table(client)

    assert desc["TableName"] == TABLE_NAME
    assert _key(desc["KeySchema"], "HASH") == "PK"
    assert _key(desc["KeySchema"], "RANGE") == "SK"

    attr_types = {a["AttributeName"]: a["AttributeType"] for a in desc["AttributeDefinitions"]}
    assert attr_types["PK"] == "S"
    assert attr_types["SK"] == "S"

    # On-demand billing.
    billing = desc.get("BillingModeSummary", {}).get("BillingMode")
    assert billing == "PAY_PER_REQUEST"


def test_waiting_index_keys_and_projection(client):
    desc = create_waiting_room_table(client)
    gsi = _gsi(desc, WAITING_INDEX_NAME)

    assert _key(gsi["KeySchema"], "HASH") == "Waiting_Shard"
    assert _key(gsi["KeySchema"], "RANGE") == "Ordering_Key"

    proj = gsi["Projection"]
    assert proj["ProjectionType"] == "INCLUDE"
    assert set(proj["NonKeyAttributes"]) == {"Fan_Id", "Entry_Timestamp"}

    attr_types = {a["AttributeName"]: a["AttributeType"] for a in desc["AttributeDefinitions"]}
    assert attr_types["Waiting_Shard"] == "S"
    assert attr_types["Ordering_Key"] == "S"


def test_eligibility_index_keys_and_projection(client):
    desc = create_waiting_room_table(client)
    gsi = _gsi(desc, ELIGIBILITY_INDEX_NAME)

    assert _key(gsi["KeySchema"], "HASH") == "Elig_PK"
    assert _key(gsi["KeySchema"], "RANGE") == "Promotion_Time"

    proj = gsi["Projection"]
    assert proj["ProjectionType"] == "INCLUDE"
    assert set(proj["NonKeyAttributes"]) == {
        "Fan_Id",
        "Batch_Id",
        "Write_Shard",
        "Ordering_Key",
    }

    attr_types = {a["AttributeName"]: a["AttributeType"] for a in desc["AttributeDefinitions"]}
    assert attr_types["Elig_PK"] == "S"
    # Promotion_Time is numeric so the expiry sweep can range-scan by time.
    assert attr_types["Promotion_Time"] == "N"


def test_exactly_two_gsis_and_no_lsis(client):
    desc = create_waiting_room_table(client)
    names = {g["IndexName"] for g in desc["GlobalSecondaryIndexes"]}
    assert names == {WAITING_INDEX_NAME, ELIGIBILITY_INDEX_NAME}
    # The design deliberately uses zero LSIs.
    assert not desc.get("LocalSecondaryIndexes")


def test_seed_event_writes_config_and_capacity(client):
    create_waiting_room_table(client)
    cfg = EventConfig(shard_count=8, downstream_capacity=1000, max_batch_size=500)
    seed_event(client, "evt-seed", cfg)

    pk = event_pk("evt-seed")
    config_item = client.get_item(
        TableName=TABLE_NAME, Key={"PK": {"S": pk}, "SK": {"S": CONFIG_SK}}
    )["Item"]
    capacity_item = client.get_item(
        TableName=TABLE_NAME, Key={"PK": {"S": pk}, "SK": {"S": CAPACITY_SK}}
    )["Item"]

    assert config_item["Event_Status"]["S"] == "OPEN"
    assert config_item["Shard_Count"]["N"] == "8"
    assert config_item["Max_Batch_Size"]["N"] == "500"
    assert capacity_item["Downstream_Capacity"]["N"] == "1000"
    assert capacity_item["Eligible_Count"]["N"] == "0"
    assert capacity_item["Active_Count"]["N"] == "0"
