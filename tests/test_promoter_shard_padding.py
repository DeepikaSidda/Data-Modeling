"""Regression test: promoter must match the Admission_Writer's shard padding.

The Admission_Writer stores ``Waiting_Shard`` as a *zero-padded* shard string
(``format_shard``), whose width depends on ``Shard_Count`` (e.g. width 3 at
1000 -> ``EVT#e#SH#042``). The promoter iterates ``range(shard_count)`` to query
the sparse ``WaitingIndex`` per shard; if it builds that key *unpadded*
(``EVT#e#SH#42``) it never matches the stored key and silently promotes nobody.

That mismatch only appears when the padding width is > 1, i.e. ``shard_count``
> 9 - so the rest of the suite (which uses ``shard_count = 6/8``) never caught
it. This test pins ``shard_count = 1000`` (the deployed Lambda default) and
admits real fans through the Admission_Writer, then asserts the promoter
actually promotes the globally-earliest ones.

Validates: Requirements 5.1, 5.6 (promotion selects WAITING entries in order).
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from waiting_room.admission import Admission_Writer
from waiting_room.config import EligibilityStatus, EventConfig, WaitingRoomConfig
from waiting_room.ordering import OrderingKeyAllocator
from waiting_room.promoter import BatchPromoter, entry_sk
from waiting_room.provisioning import (
    TABLE_NAME,
    create_waiting_room_table,
    seed_event,
)
from waiting_room.sharding import assign_shard

SECRET = "shard-padding-regression-secret"
EVENT_ID = "evt-pad"
# The deployed Lambda default. Width = len(str(999)) = 3, so padded ("042")
# differs from unpadded ("42") - exactly the case that exposed the bug.
SHARD_COUNT = 1000


@pytest.fixture()
def env():
    config = WaitingRoomConfig(
        event=EventConfig(
            shard_count=SHARD_COUNT,
            downstream_capacity=100,
            max_batch_size=5,
        )
    )
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_waiting_room_table(client)
        seed_event(client, EVENT_ID, config.event)
        writer = Admission_Writer(
            client=client, secret=SECRET, config=config, allocator=OrderingKeyAllocator()
        )
        yield client, writer


def test_promoter_matches_padded_shard_at_large_shard_count(env):
    client, writer = env

    # Admit several fans; confirm at least one lands on a shard whose padded
    # form differs from its raw int (i.e. shard >= 10), so the test genuinely
    # exercises the padding path.
    fans = [f"fan-{i}" for i in range(8)]
    for fan in fans:
        writer.admit(EVENT_ID, fan)
    shards = [assign_shard(fan, SHARD_COUNT)[0] for fan in fans]
    assert any(s >= 10 for s in shards), "test setup should hit a multi-digit shard"

    promoter = BatchPromoter(client=client)
    result = promoter.promote_cycle(EVENT_ID)

    # Before the fix this was 0 (the promoter queried unpadded keys and found
    # nothing). It must now promote up to max_batch_size of the admitted fans.
    assert result.promoted_count == min(len(fans), 5)

    # And the promoted entries are genuinely ELIGIBLE in the table.
    eligible = 0
    for fan in fans:
        shard_int, shard_str = assign_shard(fan, SHARD_COUNT)
        # We do not know each fan's Ordering_Key here; scan the shard partition.
        resp = client.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="PK = :pk AND begins_with(SK, :e)",
            ExpressionAttributeValues={
                ":pk": {"S": f"EVT#{EVENT_ID}#SH#{shard_str}"},
                ":e": {"S": "ENTRY#"},
            },
        )
        for item in resp.get("Items", []):
            if item["Eligibility_Status"]["S"] == EligibilityStatus.ELIGIBLE.value:
                eligible += 1
    assert eligible == result.promoted_count
