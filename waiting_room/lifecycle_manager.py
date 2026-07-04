"""Lifecycle manager - DynamoDB data-access layer for eligibility transitions.

This module layers *persistence* on top of the pure
:mod:`waiting_room.lifecycle` state machine. Where ``lifecycle`` answers the
question *"is ``(from, to)`` a permitted edge?"*, this manager actually applies
a permitted transition to the Queue_Entry item in the ``WaitingRoom`` table
using a **conditional ``UpdateItem``** predicated on the expected current
status, so concurrent transitions cannot corrupt state (Req 10.3).

It is a small library shared by the ``Batch_Promoter`` and downstream
completion/expiry callbacks (see design.md - "Lifecycle manager"). It performs
three jobs, mapping directly to the design's *Eligibility Lifecycle Integrity*
section:

1. **Reject illegal transitions before any write.** The requested
   ``(from_status, to_status)`` is validated against
   :data:`waiting_room.lifecycle.ALLOWED_TRANSITIONS` *first*; a disallowed
   transition raises :class:`~waiting_room.lifecycle.IllegalTransitionError`
   and no ``UpdateItem`` is ever issued, so the item is left unchanged
   (Req 10.2).

2. **Roll back and re-read on a conditional-write failure.** The write carries
   ``ConditionExpression: Eligibility_Status = :expected_from``. If the
   expected status did not match (``ConditionalCheckFailedException`` - another
   worker already moved the entry, or the item is absent), the manager invokes
   the caller-supplied ``rollback`` callback (e.g. ``release()`` a reserved
   capacity slot so no capacity leaks), re-reads the authoritative current
   status, and returns it so the caller can decide whether to retry (Req 10.5).

3. **Validate the persisted value and flag corruption for reconciliation.**
   After a successful transition the manager inspects the persisted
   ``Eligibility_Status`` (returned via ``ALL_NEW``). If it is somehow not one
   of the permitted :class:`~waiting_room.config.EligibilityStatus` values, the
   entry is flagged with ``needs_reconciliation`` and evicted from the sparse
   ``WaitingIndex`` (``REMOVE Waiting_Shard``) so it can never be selected for
   promotion until reconciled (Req 10.6). :meth:`LifecycleManager.reconcile_if_corrupt`
   exposes the same guard for entries discovered out-of-band.

The manager accepts an injected boto3 DynamoDB **client** and table name so it
can be exercised against ``moto`` / DynamoDB Local as easily as against AWS.

Requirements: 10.3, 10.5, 10.6.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

from botocore.exceptions import ClientError

from waiting_room import lifecycle
from waiting_room.config import EligibilityStatus
from waiting_room.provisioning import TABLE_NAME

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mypy_boto3_dynamodb.client import DynamoDBClient

__all__ = [
    "STATUS_ATTR",
    "WAITING_SHARD_ATTR",
    "RECONCILE_ATTR",
    "TransitionOutcome",
    "TransitionResult",
    "ReconciliationResult",
    "LifecycleManager",
]


# --------------------------------------------------------------------------- #
# Attribute names (must match the design's Queue-entry item schema)
# --------------------------------------------------------------------------- #
#: The lifecycle status attribute the conditional transition is predicated on.
STATUS_ATTR: str = "Eligibility_Status"
#: Sparse ``WaitingIndex`` partition key; present only while ``WAITING``.
#: Removing it evicts the entry from promotion selection.
WAITING_SHARD_ATTR: str = "Waiting_Shard"
#: Boolean flag marking an entry as needing manual/automated reconciliation.
RECONCILE_ATTR: str = "needs_reconciliation"

#: Set of the string values a persisted ``Eligibility_Status`` may legitimately
#: hold. Anything outside this set is treated as corrupt.
_VALID_STATUS_VALUES: frozenset[str] = frozenset(s.value for s in EligibilityStatus)


def _parse_status(raw: str | None) -> EligibilityStatus | None:
    """Return the :class:`EligibilityStatus` for ``raw`` or ``None`` if invalid.

    ``None`` covers both a missing attribute and a corrupt/unknown value, so
    callers can distinguish "well-formed status" from "needs reconciliation".
    """
    if raw is None:
        return None
    try:
        return EligibilityStatus(raw)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Result types
# --------------------------------------------------------------------------- #
class TransitionOutcome(str, Enum):
    """The outcome of an attempted lifecycle transition."""

    #: The conditional write succeeded and the persisted status is valid.
    COMMITTED = "COMMITTED"
    #: The expected-from condition did not match; side effects were rolled back
    #: and the authoritative current status was re-read (Req 10.5).
    CONFLICT = "CONFLICT"
    #: The write succeeded but the persisted value was invalid; the entry was
    #: flagged for reconciliation and excluded from promotion (Req 10.6).
    RECONCILED = "RECONCILED"


@dataclass(frozen=True, slots=True)
class TransitionResult:
    """Structured result of :meth:`LifecycleManager.apply_transition`."""

    #: What happened to the transition attempt.
    outcome: TransitionOutcome
    #: The status the entry now holds according to the authoritative store:
    #: the target on ``COMMITTED``, the re-read current status on ``CONFLICT``
    #: (``None`` if the item is absent or its status is unparseable), or the
    #: corrupt value's parse (``None``) on ``RECONCILED``.
    current_status: EligibilityStatus | None = None
    #: Raw persisted status string, useful for logging a corrupt value.
    raw_status: str | None = None
    #: Whether the caller-supplied rollback callback was invoked.
    rolled_back: bool = False

    @property
    def committed(self) -> bool:
        """Whether the transition was durably applied."""
        return self.outcome is TransitionOutcome.COMMITTED

    @property
    def needs_reconciliation(self) -> bool:
        """Whether the entry was flagged for reconciliation."""
        return self.outcome is TransitionOutcome.RECONCILED


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Structured result of :meth:`LifecycleManager.reconcile_if_corrupt`."""

    #: ``True`` if the persisted status was invalid and the entry was flagged.
    flagged: bool
    #: The parsed status, or ``None`` when missing/corrupt.
    status: EligibilityStatus | None = None
    #: The raw persisted status string (``None`` when the item/attr is absent).
    raw_status: str | None = None


# --------------------------------------------------------------------------- #
# Lifecycle manager
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class LifecycleManager:
    """Applies conditional eligibility transitions to Queue_Entry items.

    Parameters
    ----------
    client:
        An injected low-level boto3 DynamoDB client (real, DynamoDB Local, or
        ``moto``). Injected rather than constructed so the manager is trivially
        testable.
    table_name:
        The table holding Queue_Entry items. Defaults to
        :data:`waiting_room.provisioning.TABLE_NAME`.
    """

    client: "DynamoDBClient"
    table_name: str = TABLE_NAME

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def apply_transition(
        self,
        pk: str,
        sk: str,
        from_status: EligibilityStatus,
        to_status: EligibilityStatus,
        *,
        rollback: Callable[[], None] | None = None,
        extra_set: Mapping[str, dict[str, Any]] | None = None,
        extra_remove: Sequence[str] | None = None,
    ) -> TransitionResult:
        """Apply ``from_status -> to_status`` to the entry at ``(pk, sk)``.

        The transition is validated against the allowed set **before** any
        write; a disallowed transition raises
        :class:`~waiting_room.lifecycle.IllegalTransitionError` and leaves the
        item untouched (Req 10.2). A permitted transition is applied with a
        conditional ``UpdateItem`` predicated on
        ``Eligibility_Status = :expected_from`` (Req 10.3).

        On a conditional-check failure the ``rollback`` callback (if given) is
        invoked to undo reserved side effects (e.g. release a capacity slot),
        the authoritative current status is re-read, and a
        :class:`TransitionResult` with :attr:`TransitionOutcome.CONFLICT` is
        returned so the caller can retry against fresh state (Req 10.5).

        On success the persisted status is validated; a corrupt value flags the
        entry for reconciliation and excludes it from promotion
        (:attr:`TransitionOutcome.RECONCILED`, Req 10.6).

        Parameters
        ----------
        pk, sk:
            The Queue_Entry item key
            (``EVT#<Event_Id>#SH#<shard>`` / ``ENTRY#<Ordering_Key>``).
        from_status:
            The status the entry is expected to currently hold.
        to_status:
            The target status.
        rollback:
            Optional zero-arg callback invoked when the conditional write fails,
            used to release side effects reserved in anticipation of the
            transition (e.g. a capacity reservation).
        extra_set:
            Optional additional attributes to ``SET`` atomically with the status
            change (e.g. ``{"Batch_Id": {"S": ...}, "Promotion_Time": {"N": ...}}``),
            each value in DynamoDB attribute-value form.
        extra_remove:
            Optional additional attributes to ``REMOVE`` atomically with the
            status change (e.g. ``["Waiting_Shard"]`` to evict from
            ``WaitingIndex`` on promotion).
        """
        # (1) Reject illegal transitions BEFORE any write (Req 10.2). This
        # raises IllegalTransitionError for a disallowed edge, so the item is
        # never touched.
        lifecycle.transition(from_status, to_status)

        names: dict[str, str] = {"#st": STATUS_ATTR}
        values: dict[str, dict[str, Any]] = {
            ":to": {"S": to_status.value},
            ":from": {"S": from_status.value},
        }
        set_clauses = ["#st = :to"]
        remove_clauses: list[str] = []

        # Merge caller-provided attribute mutations using generated, collision-
        # free aliases so arbitrary attribute names (including reserved words)
        # are always safe.
        for idx, (attr, value) in enumerate(dict(extra_set or {}).items()):
            alias = f"#s{idx}"
            val_ref = f":s{idx}"
            names[alias] = attr
            values[val_ref] = value
            set_clauses.append(f"{alias} = {val_ref}")

        for idx, attr in enumerate(extra_remove or ()):
            alias = f"#r{idx}"
            names[alias] = attr
            remove_clauses.append(alias)

        update_expression = "SET " + ", ".join(set_clauses)
        if remove_clauses:
            update_expression += " REMOVE " + ", ".join(remove_clauses)

        # (2) Conditional UpdateItem predicated on the expected current status.
        try:
            response = self.client.update_item(
                TableName=self.table_name,
                Key={"PK": {"S": pk}, "SK": {"S": sk}},
                UpdateExpression=update_expression,
                ConditionExpression="#st = :from",
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
                ReturnValues="ALL_NEW",
            )
        except ClientError as exc:
            if _is_conditional_failure(exc):
                # Expected status did not match: roll back reserved side effects
                # and re-read authoritative status so the caller can retry
                # (Req 10.5).
                rolled_back = False
                if rollback is not None:
                    rollback()
                    rolled_back = True
                current = self._read_status(pk, sk)
                return TransitionResult(
                    outcome=TransitionOutcome.CONFLICT,
                    current_status=current,
                    raw_status=current.value if current is not None else None,
                    rolled_back=rolled_back,
                )
            raise

        # (3) Validate the persisted status; flag corruption for reconciliation
        # and exclude the entry from promotion (Req 10.6).
        raw_status = _attr_str(response.get("Attributes", {}), STATUS_ATTR)
        parsed = _parse_status(raw_status)
        if parsed is None:
            self._flag_for_reconciliation(pk, sk)
            return TransitionResult(
                outcome=TransitionOutcome.RECONCILED,
                current_status=None,
                raw_status=raw_status,
            )

        return TransitionResult(
            outcome=TransitionOutcome.COMMITTED,
            current_status=parsed,
            raw_status=raw_status,
        )

    def reconcile_if_corrupt(self, pk: str, sk: str) -> ReconciliationResult:
        """Validate an entry's persisted status, flagging it if corrupt.

        Reads the entry at ``(pk, sk)`` and checks that ``Eligibility_Status``
        holds one of the permitted lifecycle values. If it does not (a corrupt
        or unknown value), the entry is flagged with ``needs_reconciliation``
        and evicted from the sparse ``WaitingIndex`` so it is excluded from
        promotion until reconciled (Req 10.6).

        A missing item or missing status attribute is reported as
        ``flagged=False`` with ``status=None`` - there is nothing to reconcile
        against a non-existent entry.
        """
        raw_status = self._read_raw_status(pk, sk)
        if raw_status is None:
            return ReconciliationResult(flagged=False, status=None, raw_status=None)

        parsed = _parse_status(raw_status)
        if parsed is None:
            self._flag_for_reconciliation(pk, sk)
            return ReconciliationResult(
                flagged=True, status=None, raw_status=raw_status
            )
        return ReconciliationResult(flagged=False, status=parsed, raw_status=raw_status)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _read_raw_status(self, pk: str, sk: str) -> str | None:
        """Return the raw persisted status string, or ``None`` if absent."""
        result = self.client.get_item(
            TableName=self.table_name,
            Key={"PK": {"S": pk}, "SK": {"S": sk}},
            ConsistentRead=True,
            ProjectionExpression="#st",
            ExpressionAttributeNames={"#st": STATUS_ATTR},
        )
        item = result.get("Item")
        if not item:
            return None
        return _attr_str(item, STATUS_ATTR)

    def _read_status(self, pk: str, sk: str) -> EligibilityStatus | None:
        """Re-read and parse the authoritative current status."""
        return _parse_status(self._read_raw_status(pk, sk))

    def _flag_for_reconciliation(self, pk: str, sk: str) -> None:
        """Mark the entry for reconciliation and exclude it from promotion.

        Sets ``needs_reconciliation = true`` and removes ``Waiting_Shard`` so
        the entry can never be selected via the sparse ``WaitingIndex`` until a
        reconciliation process clears the flag (Req 10.6).
        """
        self.client.update_item(
            TableName=self.table_name,
            Key={"PK": {"S": pk}, "SK": {"S": sk}},
            UpdateExpression="SET #nr = :true REMOVE #ws",
            ExpressionAttributeNames={
                "#nr": RECONCILE_ATTR,
                "#ws": WAITING_SHARD_ATTR,
            },
            ExpressionAttributeValues={":true": {"BOOL": True}},
        )


# --------------------------------------------------------------------------- #
# Module-level helpers
# --------------------------------------------------------------------------- #
def _is_conditional_failure(exc: ClientError) -> bool:
    """Return whether ``exc`` is a DynamoDB ConditionalCheckFailedException."""
    return (
        exc.response.get("Error", {}).get("Code")
        == "ConditionalCheckFailedException"
    )


def _attr_str(item: Mapping[str, Any], attr: str) -> str | None:
    """Extract a string attribute value from a low-level item mapping."""
    value = item.get(attr)
    if not value:
        return None
    return value.get("S")
