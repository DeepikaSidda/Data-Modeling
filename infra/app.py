#!/usr/bin/env python3
"""CDK application entrypoint for the Virtual Waiting Room.

Instantiates the single :class:`WaitingRoomStack`, which provisions the
``WaitingRoom`` DynamoDB table (with its two GSIs and a stream), the three
Lambda handlers, an HTTP API for admit/status, and a 1-minute scheduled
promotion rule.
"""

import aws_cdk as cdk

from waiting_room_stack import WaitingRoomStack

app = cdk.App()

WaitingRoomStack(
    app,
    "WaitingRoomStack",
    description="Virtual Waiting Room: DynamoDB table, Lambda handlers, HTTP API, scheduled promoter.",
)

app.synth()
