"""Status_Reader data-access layer for the Virtual Waiting Room.

A waiting fan polls "where am I?" - their current ``Queue_Position``,
``Eligibility_Status``, ``Estimated_Wait_Time``, and whether they may proceed
to browse. Millions of fans poll repeatedly during a burst, so this path must
be **cheap, indexed, and cacheable** (Req 8.1-8.8). The :class:`Status_Reader`
wires the pure-logic layer to DynamoDB while honoring four hard rules from the
design's *Low-Latency Status Reads* section:

#. **Token verification first.** The presented ``Entry_Token`` is verified
   (:func:`waiting_room.token.verify`) before anything else. An invalid or
   tampered token is rejected with an auth error and the entry is never read
   or mutated (Req 4.3, 8.5).
#. **Indexed reads, never scans.** The verified claims yield
   ``{Event_Id, Fan_Id, Ordering_Key, Write_Shard}``, from which the exact
   item key ``PK = EVT#<Event_Id>#SH#<shard>`` / ``SK = ENTRY#<Ordering_Key>``
   is derived and fetched with a single ``GetItem`` (Req 8.2). No ``Scan`` is
   ever issued - not on the hot path, and not on the exact fallback (which
   uses bounded ``Query`` calls against the sparse ``WaitingIndex``).
#. **Approximate position from cached aggregates.** Position and ETA come from
   an injected aggregates provider (backed by ElastiCache in production), not
   from per-request counting (Req 8.3, 8.4). :func:`approximate_position`
   turns ``admission_sequence_rank`` and ``Promoted_Total`` into a position;
   :func:`estimated_wait` turns that position and the observed promotion rate
   into an ETA. An **exact** counting path is provided for audit/verification
   but is deliberately kept off the hot path.
#. **Cacheable responses.** Each response carries a ``Cache-Control`` directive
   (:func:`cache_directive`) whose ``max-age`` bounds staleness to the
   configured maximum, so repeat polls are absorbed at the edge (Req 8.8).

Everything DynamoDB-facing is **injected** (the ``boto3`` client, the table
name, the signing secret, the clock, and the aggregates provider) so the
reader is exercised end-to-end against ``moto``/DynamoDB Local with no global
state.

Requirements: 8.1, 8.2, 8.3, 8.5, 8.6, 8.7, 8.8.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol

from boto3.dynamodb.types import TypeDeserializer

from waiting_room.config import EligibilityStatus, WaitingRoomConfig
from waiting_room.position import approximate_position
from waiting_room.provisioning import TABLE_NAME
from waiting_room.sharding import format_shard
from waiting_room.status_logic import (
    BrowseReason,
    cache_directive,
    estimated_wait,
    evaluate_browse,
)
from waiting_room.token import EntryClaims, InvalidTokenError, verify

__all__ = [
    "StatusAuthError",
    "EntryNotFoundError",
    "PositionAggregates",
    "AggregatesProvider",
    "StatusResult",
    "Status_Reader",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class StatusAuthError(Exception):
    """Raised when a status query presents an invalid/unverifiable token.

    Carries a stable ``code`` (``"AUTH_ERROR"``) matching the design's
    authentication-error outcome. Deliberately generic about *why* verification
    failed so it cannot be used as a forgery oracle (Req 4.3, 8.5).
    """

    code: str = "AUTH_ERROR"


class EntryNotFoundError(Exception):
    """Raised when a verified token's entry key has no item in the table.

    A token can verify (its signature is intact) yet reference an entry that
    does not exist - e.g. it was never written, or was deleted. This is
    distinct from :class:`StatusAuthError`: the credential is authentic, the
    data simply is not there.
    """

    code: str = "NOT_FOUND"


# --------------------------------------------------------------------------- #
# Aggregates contract (hot-path position/ETA inputs)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class PositionAggregates:
    """Cached aggregates that feed the hot-path position and ETA computation.

    In production these are served from ElastiCache and refreshed by the
    Streams aggregator (task 16); here they are supplied by an injected
    :class:`AggregatesProvider` so the reader stays free of any counting on the
    request path (Req 8.3, 8.4).

    Attributes:
        admission_sequence_rank: The fan's 1-based ordinal at admission (how
            many fans, including this one, had been admitted).
        promoted_total: How many entries have been promoted out of ``WAITING``
            so far for the event. Because promotion is strictly in
            ``Ordering_Key`` order, this is exactly how many have left the
            front.
        promotion_rate: The observed promotion rate ``rho`` in fans/second used
            to project ``Estimated_Wait_Time``. Must be ``> 0`` for a finite
            estimate; a non-positive rate yields an "unknown" (infinite) ETA.
        downstream_available: Whether downstream browsing is currently
            available (drives browse gating, Req 8.6/8.7). Defaults to ``True``.
    """

    admission_sequence_rank: int
    promoted_total: int
    promotion_rate: float
    downstream_available: bool = True


class AggregatesProvider(Protocol):
    """Callable that returns :class:`PositionAggregates` for a fan's claims.

    Implementations look the aggregates up by ``Event_Id`` (and, where
    per-fan, by the claims). Kept as a ``Protocol`` so any callable or object
    with a matching ``__call__`` can be injected - a plain function, a lambda
    wrapping ElastiCache, or a test double.
    """

    def __call__(self, claims: EntryClaims) -> PositionAggregates:  # pragma: no cover - protocol
        ...


# --------------------------------------------------------------------------- #
# Result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class StatusResult:
    """The status response returned to a polling fan.

    ``reason`` is populated only when ``may_browse`` is ``False`` (mirroring
    :class:`waiting_room.status_logic.BrowseDecision`). ``cache_control`` is the
    ``Cache-Control`` directive string bounding staleness (Req 8.8).
    """

    position: int
    eligibility_status: EligibilityStatus
    estimated_wait: float
    may_browse: bool
    cache_control: str
    reason: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        """Return the result as a plain, JSON-serializable dict."""
        return {
            "position": self.position,
            "eligibility_status": self.eligibility_status.value,
            "estimated_wait": self.estimated_wait,
            "may_browse": self.may_browse,
            "reason": self.reason,
            "cache_control": self.cache_control,
        }


# --------------------------------------------------------------------------- #
# Status_Reader
# --------------------------------------------------------------------------- #
_DESERIALIZER = TypeDeserializer()


class Status_Reader:
    """Answers fan status queries with a single indexed read (never a scan).

    Args:
        client: A low-level ``boto3`` DynamoDB client (real, DynamoDB Local, or
            ``moto``). Only ``get_item`` is used on the hot path; ``query`` is
            used solely by the off-hot-path exact fallback.
        secret: The server-held signing key used to verify Entry_Tokens.
        aggregates_provider: An :class:`AggregatesProvider` supplying cached
            position/ETA aggregates for a fan's claims.
        config: The :class:`WaitingRoomConfig` whose ``shard_count`` reconstructs
            the padded shard in the item key, ``eligibility_window_secs`` gates
            browsing, and ``staleness_bound_secs`` bounds the cache directive.
        table_name: The DynamoDB table name (defaults to ``WaitingRoom``).
        clock: A zero-arg callable returning the current time in **epoch
            seconds** (defaults to :func:`time.time`). Stored ``Promotion_Time``
            values (epoch milliseconds, per the frozen model) are converted to
            seconds before browse gating so both share one time base.
    """

    def __init__(
        self,
        *,
        client: Any,
        secret: str | bytes,
        aggregates_provider: AggregatesProvider,
        config: WaitingRoomConfig | None = None,
        table_name: str = TABLE_NAME,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._secret = secret
        self._aggregates_provider = aggregates_provider
        self._config = config or WaitingRoomConfig()
        self._table_name = table_name
        self._clock = clock

    # ------------------------------------------------------------------ #
    # Key derivation
    # ------------------------------------------------------------------ #
    def _entry_key(self, claims: EntryClaims) -> dict[str, dict[str, str]]:
        """Derive the exact base-table key for the entry from verified claims.

        Reconstructs ``PK = EVT#<Event_Id>#SH#<shard>`` /
        ``SK = ENTRY#<Ordering_Key>`` (Req 8.2). The shard is zero-padded with
        :func:`format_shard` using the configured ``shard_count`` so it matches
        the padded form the Admission_Writer stored.
        """
        shard_str = format_shard(claims.Write_Shard, self._config.event.shard_count)
        pk = f"EVT#{claims.Event_Id}#SH#{shard_str}"
        sk = f"ENTRY#{claims.Ordering_Key}"
        return {"PK": {"S": pk}, "SK": {"S": sk}}

    def _waiting_shard_pk(self, event_id: str, shard: int) -> str:
        """Return the ``WaitingIndex`` partition value for a shard.

        Matches the base-table ``PK`` form (``EVT#<Event_Id>#SH#<shard>``), which
        the frozen model uses verbatim for the ``Waiting_Shard`` GSI key.
        """
        shard_str = format_shard(shard, self._config.event.shard_count)
        return f"EVT#{event_id}#SH#{shard_str}"

    # ------------------------------------------------------------------ #
    # Token verification
    # ------------------------------------------------------------------ #
    def _verify(self, token: str) -> EntryClaims:
        """Verify the token, translating any failure into a :class:`StatusAuthError`."""
        try:
            return verify(token, self._secret)
        except InvalidTokenError as exc:
            # Do not leak the underlying reason; a generic auth error only.
            raise StatusAuthError("authentication failed") from exc

    # ------------------------------------------------------------------ #
    # Single-item read
    # ------------------------------------------------------------------ #
    def _get_entry(self, claims: EntryClaims) -> dict[str, Any]:
        """Fetch the entry with a single ``GetItem`` and deserialize it.

        Uses a strongly-consistent read so a fan sees their own just-written
        entry. Raises :class:`EntryNotFoundError` when no item exists for the
        derived key.
        """
        response = self._client.get_item(
            TableName=self._table_name,
            Key=self._entry_key(claims),
            ConsistentRead=True,
        )
        raw = response.get("Item")
        if not raw:
            raise EntryNotFoundError("entry not found for verified token")
        return {name: _DESERIALIZER.deserialize(value) for name, value in raw.items()}

    # ------------------------------------------------------------------ #
    # Position
    # ------------------------------------------------------------------ #
    def _approximate_position(self, claims: EntryClaims, aggregates: PositionAggregates) -> int:
        """Hot-path position from cached aggregates (Req 8.3)."""
        return approximate_position(
            aggregates.admission_sequence_rank, aggregates.promoted_total
        )

    def _exact_position(self, claims: EntryClaims) -> int:
        """Exact ``Queue_Position`` via bounded ``Query`` calls (off hot path).

        Counts, across every shard partition of the sparse ``WaitingIndex``,
        the ``WAITING`` entries whose ``Ordering_Key`` is strictly less than the
        querying fan's, then returns ``1 + that count`` (Req 8.3). The index is
        sparse (only ``WAITING`` entries carry ``Waiting_Shard``), so this
        naturally excludes promoted entries and the querying entry itself.

        This uses ``Query`` with ``Select=COUNT`` per shard and paginates via
        ``LastEvaluatedKey`` - **never** a ``Scan``. It is intended for
        audit/verification, not the millions-of-polls hot path.
        """
        ahead = 0
        shard_count = self._config.event.shard_count
        for shard in range(shard_count):
            exclusive_start_key: dict[str, Any] | None = None
            while True:
                kwargs: dict[str, Any] = {
                    "TableName": self._table_name,
                    "IndexName": "WaitingIndex",
                    "KeyConditionExpression": "Waiting_Shard = :ws AND Ordering_Key < :ok",
                    "ExpressionAttributeValues": {
                        ":ws": {"S": self._waiting_shard_pk(claims.Event_Id, shard)},
                        ":ok": {"S": claims.Ordering_Key},
                    },
                    "Select": "COUNT",
                }
                if exclusive_start_key is not None:
                    kwargs["ExclusiveStartKey"] = exclusive_start_key
                page = self._client.query(**kwargs)
                ahead += page.get("Count", 0)
                exclusive_start_key = page.get("LastEvaluatedKey")
                if not exclusive_start_key:
                    break
        return ahead + 1

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def read_status(
        self,
        token: str,
        *,
        downstream_available: bool | None = None,
        exact: bool = False,
    ) -> StatusResult:
        """Answer a fan's status query for the given Entry_Token.

        Flow (Req 8.1-8.8):

        #. Verify the token; reject invalid/tampered tokens with
           :class:`StatusAuthError` before any read (Req 8.5).
        #. Derive the exact key and fetch the entry with one ``GetItem``
           (Req 8.2).
        #. Compute position from cached aggregates (Req 8.3), or exactly via
           bounded queries when ``exact=True``.
        #. Derive ``Estimated_Wait_Time`` from position and the observed
           promotion rate (Req 8.4).
        #. Apply browse gating (Req 8.6/8.7) and attach the cache directive
           (Req 8.8).

        Args:
            token: The signed Entry_Token presented by the fan.
            downstream_available: Optional override for downstream availability;
                when ``None``, the value from the aggregates provider is used.
            exact: When ``True``, compute the exact position via the (off
                hot-path) indexed count instead of the cached approximation.

        Returns:
            A :class:`StatusResult`.

        Raises:
            StatusAuthError: If the token fails verification (Req 8.5).
            EntryNotFoundError: If no entry exists for the verified key.
        """
        claims = self._verify(token)
        item = self._get_entry(claims)

        status = EligibilityStatus(item["Eligibility_Status"])
        aggregates = self._aggregates_provider(claims)

        # Position: cached approximation on the hot path; exact only on request.
        if exact:
            position = self._exact_position(claims)
        else:
            position = self._approximate_position(claims, aggregates)

        # Estimated wait: position / rho. A non-positive observed rate has no
        # finite projection, so report an "unknown" (infinite) wait rather than
        # raising on the hot path.
        rho = aggregates.promotion_rate
        est = estimated_wait(position, rho) if rho > 0 else float("inf")

        # Browse gating. Promotion_Time is stored as epoch milliseconds (frozen
        # model); the clock is epoch seconds, so convert to a common base.
        available = (
            aggregates.downstream_available
            if downstream_available is None
            else downstream_available
        )
        promotion_time_secs: float | None = None
        raw_promotion_time = item.get("Promotion_Time")
        if raw_promotion_time is not None:
            promotion_time_secs = float(raw_promotion_time) / 1000.0

        decision = evaluate_browse(
            status=status,
            promotion_time=promotion_time_secs,
            now=self._clock(),
            eligibility_window_secs=self._config.event.eligibility_window_secs,
            downstream_available=available,
        )

        reason: str | None = (
            decision.reason.value if isinstance(decision.reason, BrowseReason) else None
        )

        return StatusResult(
            position=position,
            eligibility_status=status,
            estimated_wait=est,
            may_browse=decision.may_browse,
            cache_control=cache_directive(self._config),
            reason=reason,
        )
