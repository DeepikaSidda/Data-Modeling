"""CDK stack for the Virtual Waiting Room.

Provisions, in one stack:

* A ``WaitingRoom`` DynamoDB table (on-demand) with the two GSIs the design
  requires (``WaitingIndex``, ``EligibilityIndex``) and a stream enabled for a
  future Streams aggregator.
* Three Python 3.12 Lambda functions wiring the handlers in
  ``waiting_room.handlers`` (admit, status, promote). The function code is the
  repository root minus everything that is not the ``waiting_room`` package, so
  no Docker bundling is needed (the handlers depend only on the stdlib and the
  ``boto3`` already present in the Lambda runtime).
* An HTTP API (API Gateway v2) routing ``POST /admit`` and ``GET /status``.
* A 1-minute EventBridge rule invoking the promoter for a demo event.
* A generated Secrets Manager secret holding the Entry_Token signing key,
  passed to the admit/status Lambdas by ARN (fetched at runtime) so the secret
  never lives in a plaintext environment variable.
"""

from __future__ import annotations

import os

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import aws_apigatewayv2 as apigwv2
from aws_cdk import aws_apigatewayv2_integrations as apigwv2_integrations
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

#: Repository root (one level above this ``infra/`` directory). Used as the
#: Lambda asset so the ``waiting_room`` package ships at the zip root.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

#: Paths excluded from the Lambda asset so only ``waiting_room`` (+ pyproject)
#: ships. Everything else is dev/test/build tooling irrelevant at runtime.
_ASSET_EXCLUDES = [
    "infra",
    "tests",
    "submission",
    "scripts",
    ".kiro",
    ".git",
    ".venv",
    "__pycache__",
    "*.md",
    "cdk.out",
    ".pytest_cache",
    ".hypothesis",
    "*.pyc",
    ".mypy_cache",
]

#: Demo event id used by the scheduled promoter (also settable per deployment).
_DEMO_EVENT_ID = "demo-event"

TABLE_NAME = "WaitingRoom"
WAITING_INDEX_NAME = "WaitingIndex"
ELIGIBILITY_INDEX_NAME = "EligibilityIndex"


class WaitingRoomStack(Stack):
    """All AWS resources for a deployable Virtual Waiting Room."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table = self._build_table()
        secret = self._build_secret()

        # --- Lambda functions -------------------------------------------- #
        code = lambda_.Code.from_asset(_REPO_ROOT, exclude=_ASSET_EXCLUDES)

        common_env = {"WAITING_ROOM_TABLE": table.table_name}
        secret_env = {
            **common_env,
            "ENTRY_TOKEN_SECRET_ARN": secret.secret_arn,
        }

        admit_fn = lambda_.Function(
            self,
            "AdmitFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="waiting_room.handlers.admit_handler",
            code=code,
            memory_size=256,
            timeout=Duration.seconds(10),
            environment=secret_env,
        )

        status_fn = lambda_.Function(
            self,
            "StatusFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="waiting_room.handlers.status_handler",
            code=code,
            memory_size=256,
            timeout=Duration.seconds(10),
            environment=secret_env,
        )

        promote_fn = lambda_.Function(
            self,
            "PromoteFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="waiting_room.handlers.promote_handler",
            code=code,
            memory_size=256,
            timeout=Duration.seconds(60),
            environment={
                **common_env,
                "PROMOTE_EVENT_ID": _DEMO_EVENT_ID,
            },
        )

        # --- IAM grants --------------------------------------------------- #
        table.grant_read_write_data(admit_fn)
        table.grant_read_write_data(status_fn)
        table.grant_read_write_data(promote_fn)
        # Admit + status verify/sign tokens, so they read the signing secret.
        secret.grant_read(admit_fn)
        secret.grant_read(status_fn)

        # --- HTTP API ----------------------------------------------------- #
        http_api = apigwv2.HttpApi(self, "WaitingRoomApi")
        http_api.add_routes(
            path="/admit",
            methods=[apigwv2.HttpMethod.POST],
            integration=apigwv2_integrations.HttpLambdaIntegration(
                "AdmitIntegration", handler=admit_fn
            ),
        )
        http_api.add_routes(
            path="/status",
            methods=[apigwv2.HttpMethod.GET],
            integration=apigwv2_integrations.HttpLambdaIntegration(
                "StatusIntegration", handler=status_fn
            ),
        )

        # --- Scheduled promotion (every 1 minute) ------------------------- #
        schedule_rule = events.Rule(
            self,
            "PromoteSchedule",
            schedule=events.Schedule.rate(Duration.minutes(1)),
            description="Runs a Virtual Waiting Room promotion + expiry cycle every minute.",
        )
        schedule_rule.add_target(
            targets.LambdaFunction(
                promote_fn,
                event=events.RuleTargetInput.from_object(
                    {"detail": {"event_id": _DEMO_EVENT_ID}}
                ),
            )
        )

        # --- Outputs ------------------------------------------------------ #
        CfnOutput(self, "TableName", value=table.table_name)
        CfnOutput(self, "ApiUrl", value=http_api.api_endpoint)
        CfnOutput(self, "EntryTokenSecretArn", value=secret.secret_arn)

    # ------------------------------------------------------------------ #
    # Resource builders
    # ------------------------------------------------------------------ #
    def _build_table(self) -> dynamodb.Table:
        """Create the ``WaitingRoom`` table with both GSIs and a stream."""
        table = dynamodb.Table(
            self,
            "WaitingRoomTable",
            table_name=TABLE_NAME,
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="SK", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Sparse WaitingIndex: front-of-line reads in Ordering_Key order.
        table.add_global_secondary_index(
            index_name=WAITING_INDEX_NAME,
            partition_key=dynamodb.Attribute(
                name="Waiting_Shard", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="Ordering_Key", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=["Fan_Id", "Entry_Timestamp"],
        )

        # EligibilityIndex: capacity accounting + expiry sweep.
        table.add_global_secondary_index(
            index_name=ELIGIBILITY_INDEX_NAME,
            partition_key=dynamodb.Attribute(
                name="Elig_PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="Promotion_Time", type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=["Fan_Id", "Batch_Id", "Write_Shard", "Ordering_Key"],
        )

        return table

    def _build_secret(self) -> secretsmanager.Secret:
        """Create a generated Secrets Manager secret for Entry_Token signing.

        The secret string is a random 48-char token generated by CloudFormation
        (never committed to source). Lambdas receive only its ARN and fetch the
        value at runtime, so the plaintext key never lands in an environment
        variable.
        """
        return secretsmanager.Secret(
            self,
            "EntryTokenSecret",
            description="HMAC signing key for Virtual Waiting Room Entry_Tokens.",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                password_length=48,
                exclude_punctuation=True,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )
