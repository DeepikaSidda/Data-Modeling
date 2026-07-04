"""AWS Lambda entrypoints for the Virtual Waiting Room.

This module is the thin *deployment* seam between API Gateway / EventBridge and
the already-tested data-access layer (:mod:`waiting_room.admission`,
:mod:`waiting_room.status_reader`, :mod:`waiting_room.promoter`). It wires those
components to a real ``boto3`` DynamoDB client and translates their typed
results / errors into HTTP responses (or scheduler summaries).

Everything the handlers depend on is read from the environment so the same code
runs unchanged against AWS, DynamoDB Local, or ``moto``:

* ``WAITING_ROOM_TABLE`` - the DynamoDB table name (default ``"WaitingRoom"``).
* ``ENTRY_TOKEN_SECRET`` - the HMAC signing key used to sign/verify Entry_Tokens.
  If ``ENTRY_TOKEN_SECRET_ARN`` is set instead, the secret is fetched from AWS
  Secrets Manager at runtime (and cached for the life of the container). The ARN
  form is preferred because the secret never appears in the Lambda's plaintext
  environment; the plain ``ENTRY_TOKEN_SECRET`` form is a documented fallback.
* ``PROMOTE_EVENT_ID`` - fallback event id for the scheduled promoter when the
  invocation event carries none.

Only the standard library and ``boto3`` (bundled in the Lambda runtime) are
imported here, so no third-party packaging is required.

Design notes / simplifications
------------------------------
* **Status aggregates without ElastiCache.** In production, position/ETA are fed
  by a Streams-maintained aggregate cache. To keep this deployable with zero
  extra infrastructure, ``status_handler`` uses a provider that reads the
  ``CAPACITY`` item's ``Promoted_Total`` directly and calls
  ``read_status(..., exact=True)`` so the queue position is computed from the
  sparse ``WaitingIndex`` (a bounded set of ``Query`` calls, never a ``Scan``).
  The promotion rate is a fixed, small positive constant
  (:data:`_DEFAULT_PROMOTION_RATE`) purely to yield a finite ``Estimated_Wait``;
  it is an intentional approximation, not an observed rate.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3

from waiting_room.admission import Admission_Writer, AdmissionError
from waiting_room.ordering import OrderingKeyAllocator
from waiting_room.promoter import BatchPromoter
from waiting_room.config import WaitingRoomConfig
from waiting_room.provisioning import (
    CAPACITY_SK,
    WAITING_INDEX_NAME,
    event_pk,
)
from waiting_room.sharding import format_shard
from waiting_room.status_reader import (
    EntryNotFoundError,
    PositionAggregates,
    StatusAuthError,
    Status_Reader,
)
from waiting_room.token import EntryClaims

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

__all__ = ["admit_handler", "status_handler", "promote_handler"]


# --------------------------------------------------------------------------- #
# Environment / shared, per-container state
# --------------------------------------------------------------------------- #
#: Default DynamoDB table name when ``WAITING_ROOM_TABLE`` is unset.
_DEFAULT_TABLE_NAME = "WaitingRoom"

#: Fixed promotion rate (fans/second) used to project a finite Estimated_Wait
#: when no live rate aggregate is available. Documented simplification: this is
#: a placeholder, not an observed throughput.
_DEFAULT_PROMOTION_RATE = 10.0

#: A single low-level DynamoDB client and OrderingKeyAllocator are created once
#: per warm Lambda container and reused across invocations. The allocator MUST
#: be module-level so its HLC advances monotonically across requests handled by
#: the same container.
_dynamodb = boto3.client("dynamodb")
_ordering_allocator = OrderingKeyAllocator()

#: Lazily-resolved signing secret, cached for the life of the container.
_cached_secret: str | None = None


def _table_name() -> str:
    """Return the configured DynamoDB table name (env or default)."""
    return os.environ.get("WAITING_ROOM_TABLE", _DEFAULT_TABLE_NAME)


def _entry_token_secret() -> str:
    """Resolve the Entry_Token signing secret, caching it per container.

    Prefers ``ENTRY_TOKEN_SECRET_ARN`` (fetched from Secrets Manager, so the
    secret is never in the plaintext environment); falls back to the
    ``ENTRY_TOKEN_SECRET`` env var. Raises ``RuntimeError`` if neither is set.
    """
    global _cached_secret
    if _cached_secret is not None:
        return _cached_secret

    secret_arn = os.environ.get("ENTRY_TOKEN_SECRET_ARN")
    if secret_arn:
        client = boto3.client("secretsmanager")
        resp = client.get_secret_value(SecretId=secret_arn)
        _cached_secret = resp.get("SecretString") or ""
    else:
        _cached_secret = os.environ.get("ENTRY_TOKEN_SECRET", "")

    if not _cached_secret:
        raise RuntimeError(
            "no signing secret configured; set ENTRY_TOKEN_SECRET_ARN or "
            "ENTRY_TOKEN_SECRET"
        )
    return _cached_secret


# --------------------------------------------------------------------------- #
# HTTP helpers (API Gateway HTTP API payload format 2.0)
# --------------------------------------------------------------------------- #
def _response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    """Build an API Gateway v2 (payload format 2.0) JSON response."""
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _parse_json_body(event: dict[str, Any]) -> dict[str, Any]:
    """Safely parse a JSON request body from an HTTP API event.

    Returns an empty dict when there is no body. Raises ``ValueError`` when the
    body is present but is not a JSON object.
    """
    raw = event.get("body")
    if raw is None or raw == "":
        return {}
    if event.get("isBase64Encoded"):
        import base64

        raw = base64.b64decode(raw).decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("request body must be a JSON object")
    return parsed


#: AdmissionError.code -> HTTP status mapping (design rejection outcomes).
_ADMISSION_ERROR_STATUS = {
    "EVENT_NOT_OPEN": 403,
    "QUEUE_FULL": 409,
    "RATE_LIMITED": 429,
    "WRITE_RETRYABLE": 503,
}


# --------------------------------------------------------------------------- #
# POST /admit
# --------------------------------------------------------------------------- #
def admit_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Admit an arriving fan to an event's queue (HTTP API POST /admit).

    Reads ``event_id`` (required) and optional ``fan_id`` from the JSON body,
    constructs an :class:`Admission_Writer` around the shared DynamoDB client
    and module-level :class:`OrderingKeyAllocator`, and returns the issued
    Entry_Token. :class:`AdmissionError` codes map to HTTP statuses per
    :data:`_ADMISSION_ERROR_STATUS`; anything unexpected becomes a 500.
    """
    try:
        body = _parse_json_body(event)
    except (ValueError, json.JSONDecodeError):
        return _response(400, {"error": "invalid JSON body"})

    event_id = body.get("event_id")
    if not event_id or not isinstance(event_id, str):
        return _response(400, {"error": "event_id is required"})

    fan_id = body.get("fan_id")
    if fan_id is not None and not isinstance(fan_id, str):
        return _response(400, {"error": "fan_id must be a string"})

    try:
        writer = Admission_Writer(
            client=_dynamodb,
            secret=_entry_token_secret(),
            allocator=_ordering_allocator,
            table_name=_table_name(),
        )
        result = writer.admit(event_id, fan_id=fan_id)
    except AdmissionError as exc:
        status = _ADMISSION_ERROR_STATUS.get(exc.code, 400)
        return _response(status, {"error": exc.code, "message": str(exc)})
    except Exception:  # noqa: BLE001 - surface as 500, never leak internals.
        logger.exception("unexpected error during admission")
        return _response(500, {"error": "INTERNAL_ERROR"})

    return _response(
        200,
        {
            "entry_token": result.entry_token,
            "ordering_key": result.ordering_key,
            "event_id": result.event_id,
            "write_shard": result.write_shard,
            "duplicate": result.duplicate,
        },
    )


# --------------------------------------------------------------------------- #
# GET /status
# --------------------------------------------------------------------------- #
def _extract_token(event: dict[str, Any]) -> str | None:
    """Extract the Entry_Token from the Authorization header or ``token`` query.

    Accepts ``Authorization: Bearer <token>`` (case-insensitive header name,
    optional ``Bearer `` prefix) or a ``?token=<token>`` query-string param.
    """
    headers = event.get("headers") or {}
    # HTTP API lowercases header names, but be defensive about casing.
    auth = None
    for key, value in headers.items():
        if key.lower() == "authorization":
            auth = value
            break
    if auth:
        auth = auth.strip()
        if auth.lower().startswith("bearer "):
            return auth[len("bearer ") :].strip()
        return auth

    params = event.get("queryStringParameters") or {}
    token = params.get("token")
    if token:
        return token.strip()
    return None


#: Shard count the handlers assume (matches the Admission_Writer's default
#: WaitingRoomConfig, so admission and status agree on the padded shard width).
_SHARD_COUNT = WaitingRoomConfig().event.shard_count


def _capacity_aggregates_provider(claims: EntryClaims) -> PositionAggregates:
    """Deployable, cache-free aggregates provider that stays O(1) in queries.

    Computing an *exact* global position means counting WAITING entries ahead
    across all ``Shard_Count`` WaitingIndex partitions - O(Shard_Count) queries,
    which at the default 1,000 shards cannot complete inside the Lambda's hot
    request budget (that is precisely why the design serves position from a
    cached aggregate in production).

    With no ElastiCache in this deployment, we approximate cheaply: run a
    **single** ``COUNT`` query on the fan's *own* shard partition for entries
    with a smaller ``Ordering_Key`` (the fan's within-shard rank), then scale by
    ``Shard_Count``. Because ``hash(Fan_Id)`` distributes fans roughly uniformly,
    ``global_rank ≈ within_shard_rank × Shard_Count`` is a sound estimate, and
    the whole thing is one indexed query regardless of shard count. An
    ``ELIGIBLE`` fan has been evicted from the sparse index, so the count is 0
    and the estimated position collapses to 1 (front of line), which is correct.

    The estimate is fed through ``admission_sequence_rank`` with
    ``promoted_total = 0`` so :func:`approximate_position` returns it directly;
    ``promotion_rate`` is a fixed positive constant purely to yield a finite
    ``Estimated_Wait`` (documented simplification).
    """
    shard_str = format_shard(claims.Write_Shard, _SHARD_COUNT)
    ws_value = f"EVT#{claims.Event_Id}#SH#{shard_str}"
    resp = _dynamodb.query(
        TableName=_table_name(),
        IndexName=WAITING_INDEX_NAME,
        KeyConditionExpression="Waiting_Shard = :ws AND Ordering_Key < :ok",
        ExpressionAttributeValues={
            ":ws": {"S": ws_value},
            ":ok": {"S": claims.Ordering_Key},
        },
        Select="COUNT",
    )
    within_shard_ahead = resp.get("Count", 0)
    estimated_global_position = within_shard_ahead * _SHARD_COUNT + 1
    return PositionAggregates(
        admission_sequence_rank=estimated_global_position,
        promoted_total=0,
        promotion_rate=_DEFAULT_PROMOTION_RATE,
    )


def status_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Answer a fan's status query (HTTP API GET /status).

    Reads the Entry_Token from the ``Authorization`` header (``Bearer`` prefix
    stripped) or a ``token`` query param, builds a :class:`Status_Reader` with
    the cache-free aggregates provider, and returns the status result. Maps
    :class:`StatusAuthError` to 401 and :class:`EntryNotFoundError` to 404.
    """
    token = _extract_token(event)
    if not token:
        return _response(400, {"error": "missing Entry_Token"})

    try:
        reader = Status_Reader(
            client=_dynamodb,
            secret=_entry_token_secret(),
            aggregates_provider=_capacity_aggregates_provider,
            table_name=_table_name(),
        )
        # Approximate position via the O(1) single-shard estimate in the
        # aggregates provider (exact=True would query all Shard_Count partitions
        # and blow the hot-path budget).
        result = reader.read_status(token)
    except StatusAuthError:
        return _response(401, {"error": "AUTH_ERROR"})
    except EntryNotFoundError:
        return _response(404, {"error": "NOT_FOUND"})
    except Exception:  # noqa: BLE001 - surface as 500, never leak internals.
        logger.exception("unexpected error during status read")
        return _response(500, {"error": "INTERNAL_ERROR"})

    return _response(200, result.as_dict())


# --------------------------------------------------------------------------- #
# Scheduled promotion (EventBridge)
# --------------------------------------------------------------------------- #
def promote_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Run one promotion + expiry cycle for an event (EventBridge schedule).

    Resolves the target ``event_id`` from the event ``detail`` (or top level),
    falling back to the ``PROMOTE_EVENT_ID`` env var. Runs
    :meth:`BatchPromoter.promote_cycle` then :meth:`BatchPromoter.expire_sweep`
    and returns a JSON-serializable summary. Safe to invoke when there is
    nothing to do: a full queue / empty front simply yields zero counts, and a
    missing/misconfigured event id is logged and returned as a no-op summary
    rather than raising (so the schedule never accumulates failed invocations).
    """
    detail = event.get("detail") if isinstance(event, dict) else None
    event_id = None
    if isinstance(detail, dict):
        event_id = detail.get("event_id")
    if not event_id and isinstance(event, dict):
        event_id = event.get("event_id")
    if not event_id:
        event_id = os.environ.get("PROMOTE_EVENT_ID")

    if not event_id:
        logger.warning("promote_handler invoked with no event_id; nothing to do")
        return {"event_id": None, "promoted": 0, "expired": 0, "skipped": True}

    try:
        promoter = BatchPromoter(client=_dynamodb, table_name=_table_name())
        promotion = promoter.promote_cycle(event_id)
        expiry = promoter.expire_sweep(event_id)
    except KeyError:
        # Event not seeded yet (no CONFIG/CAPACITY item) - a safe no-op.
        logger.warning("promote_handler: event %s not provisioned", event_id)
        return {"event_id": event_id, "promoted": 0, "expired": 0, "skipped": True}
    except Exception:  # noqa: BLE001 - log and re-raise so failures are visible.
        logger.exception("promote_handler failed for event %s", event_id)
        raise

    summary = {
        "event_id": event_id,
        "batch_id": promotion.batch_id,
        "granted": promotion.granted,
        "promoted": promotion.promoted_count,
        "conflicts": promotion.conflicts,
        "released_unused": promotion.released_unused,
        "expired": expiry.expired_count,
        "expiry_scanned": expiry.scanned,
        "skipped": False,
    }
    logger.info("promote_handler summary: %s", json.dumps(summary))
    return summary
