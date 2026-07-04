"""Entry_Token sign/verify codec for the Virtual Waiting Room.

An ``Entry_Token`` is a signed, verifiable credential returned to a fan on
successful admission. It encodes the claim set
``{Fan_Id, Event_Id, Ordering_Key, Write_Shard}`` so the status-read path can
recover the exact item key with a single ``GetItem`` (no scan) *and* prove the
claims were server-issued and untampered before honoring the encoded position
(Req 1.4, 4.2, 4.3, 8.5).

## Why HMAC-SHA256 (and not a JWT library)

The token only needs **symmetric integrity/authenticity** with a server-held
secret - the same service signs and verifies. HMAC-SHA256 over a canonical
serialization gives exactly that using only the standard library
(:mod:`hmac`, :mod:`hashlib`, :mod:`base64`), so no external JWT dependency is
pulled in. Verification recomputes the MAC over the payload and compares it to
the presented MAC with :func:`hmac.compare_digest`, a constant-time comparison
that does not leak, via timing, how much of a forged signature was correct.

## Token format

``token = base64url(payload_json) + "." + base64url(mac)``

* ``payload_json`` is a **canonical** JSON serialization of the claims:
  sorted keys, no insignificant whitespace, UTF-8. Canonicalization matters -
  signer and verifier must serialize identically or every token would appear
  tampered. Verification recovers the claims by parsing this exact payload,
  not by re-deriving it, so any single-byte mutation of either segment (or the
  separator) changes the recomputed MAC or breaks decoding and is rejected.
* ``mac`` is the raw 32-byte HMAC-SHA256 digest over the encoded payload
  segment (the base64url text), keyed by the secret.

Both segments use URL-safe base64 **without padding**, so a token is safe in
URLs, headers, and query strings.

This module is **pure logic**: no I/O, no ``boto3``, no global state.
Deterministic given ``(claims, secret)`` - signing the same claims with the
same secret always yields the same token.

Requirements: 1.4, 4.2, 4.3, 8.5.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, Mapping

__all__ = [
    "CLAIM_FIELDS",
    "TOKEN_SEPARATOR",
    "InvalidTokenError",
    "EntryClaims",
    "sign",
    "verify",
]

#: The exact set of claim fields carried by an Entry_Token, in canonical order.
CLAIM_FIELDS: tuple[str, ...] = ("Fan_Id", "Event_Id", "Ordering_Key", "Write_Shard")

#: Separator between the encoded payload and the encoded MAC.
TOKEN_SEPARATOR: str = "."

#: HMAC digest algorithm.
_DIGESTMOD = hashlib.sha256


class InvalidTokenError(Exception):
    """Raised when a token fails signature verification or is malformed.

    The message is intentionally generic about *why* verification failed so a
    caller cannot use it as an oracle to distinguish a bad signature from a
    malformed payload while probing forgeries.
    """


@dataclass(frozen=True, slots=True)
class EntryClaims:
    """The verified claim set recovered from an Entry_Token.

    Mirrors the fields in :data:`CLAIM_FIELDS`. ``Write_Shard`` is an ``int``
    shard index; the remaining fields are strings.
    """

    Fan_Id: str
    Event_Id: str
    Ordering_Key: str
    Write_Shard: int

    def as_dict(self) -> dict[str, Any]:
        """Return the claims as a plain dict keyed by :data:`CLAIM_FIELDS`."""
        return {
            "Fan_Id": self.Fan_Id,
            "Event_Id": self.Event_Id,
            "Ordering_Key": self.Ordering_Key,
            "Write_Shard": self.Write_Shard,
        }


# --------------------------------------------------------------------------- #
# base64url helpers (no padding)
# --------------------------------------------------------------------------- #
def _b64url_encode(raw: bytes) -> str:
    """Encode bytes as URL-safe base64 text without ``=`` padding."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    """Decode URL-safe base64 text that may be missing its ``=`` padding.

    Enforces **canonical** encoding: standard base64 decoding ignores the
    unused trailing bits of the final character, so for a value whose length is
    not a multiple of 3 bytes several distinct final characters decode to the
    same bytes (e.g. the 32-byte HMAC's last char ``A`` and ``B`` both decode
    identically). That aliasing would let a one-character mutation of a token
    still verify. To close it, we re-encode the decoded bytes and require the
    result to match the input exactly, rejecting any non-canonical form.

    :raises ValueError: if ``text`` is not valid or non-canonical base64.
    """
    # Restore the padding stripped by :func:`_b64url_encode` before decoding.
    padding = (-len(text)) % 4
    raw = base64.urlsafe_b64decode(text + ("=" * padding))
    if _b64url_encode(raw) != text:
        raise ValueError("non-canonical base64url encoding")
    return raw


# --------------------------------------------------------------------------- #
# Canonical claim serialization
# --------------------------------------------------------------------------- #
def _coerce_secret(secret: str | bytes) -> bytes:
    """Return the secret as bytes (UTF-8 encoding a ``str``)."""
    if isinstance(secret, str):
        return secret.encode("utf-8")
    if isinstance(secret, (bytes, bytearray)):
        return bytes(secret)
    raise TypeError(f"secret must be str or bytes, got {type(secret).__name__}")


def _canonical_claims(claims: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize the claim set into its canonical dict form.

    Enforces that exactly :data:`CLAIM_FIELDS` are present, coerces
    ``Write_Shard`` to a non-negative ``int``, and requires the string claims
    to be non-empty strings.

    :raises ValueError: if a required claim is missing/extra or malformed.
    :raises TypeError: if a claim has the wrong type.
    """
    missing = [f for f in CLAIM_FIELDS if f not in claims]
    if missing:
        raise ValueError(f"missing required claim(s): {', '.join(missing)}")
    extra = [k for k in claims if k not in CLAIM_FIELDS]
    if extra:
        raise ValueError(f"unexpected claim(s): {', '.join(sorted(extra))}")

    canonical: dict[str, Any] = {}
    for field in ("Fan_Id", "Event_Id", "Ordering_Key"):
        value = claims[field]
        if not isinstance(value, str):
            raise TypeError(f"claim {field} must be a str, got {type(value).__name__}")
        if value == "":
            raise ValueError(f"claim {field} must be a non-empty string")
        canonical[field] = value

    shard = claims["Write_Shard"]
    # bool is a subclass of int; reject it explicitly to avoid True/False shards.
    if isinstance(shard, bool) or not isinstance(shard, int):
        raise TypeError(
            f"claim Write_Shard must be an int, got {type(shard).__name__}"
        )
    if shard < 0:
        raise ValueError("claim Write_Shard must be non-negative")
    canonical["Write_Shard"] = shard

    return canonical


def _serialize_payload(canonical: Mapping[str, Any]) -> bytes:
    """Serialize canonical claims to deterministic JSON bytes.

    Sorted keys and compact separators ensure the signer and verifier produce
    byte-identical payloads for equal claim sets.
    """
    return json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _mac(payload_segment: str, secret: bytes) -> bytes:
    """Compute the HMAC-SHA256 digest over the encoded payload segment."""
    return hmac.new(secret, payload_segment.encode("ascii"), _DIGESTMOD).digest()


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def sign(claims: Mapping[str, Any], secret: str | bytes) -> str:
    """Sign ``claims`` into a verifiable Entry_Token string.

    Args:
        claims: A mapping carrying exactly :data:`CLAIM_FIELDS`
            (``Fan_Id``, ``Event_Id``, ``Ordering_Key`` as non-empty strings and
            ``Write_Shard`` as a non-negative ``int``).
        secret: The server-held signing key (``str`` or ``bytes``).

    Returns:
        A ``base64url(payload).base64url(mac)`` token string. Deterministic for
        a given ``(claims, secret)``.

    Raises:
        TypeError: If a claim or the secret has the wrong type.
        ValueError: If a required claim is missing, extra, or malformed, or if
            the secret is empty.
    """
    key = _coerce_secret(secret)
    if not key:
        raise ValueError("secret must be non-empty")

    canonical = _canonical_claims(claims)
    payload_segment = _b64url_encode(_serialize_payload(canonical))
    mac_segment = _b64url_encode(_mac(payload_segment, key))
    return f"{payload_segment}{TOKEN_SEPARATOR}{mac_segment}"


def verify(token: str, secret: str | bytes) -> EntryClaims:
    """Verify an Entry_Token and recover its claims.

    Recomputes the HMAC over the presented payload segment and compares it to
    the presented MAC in constant time (:func:`hmac.compare_digest`). Any
    signature mismatch, structural malformation, or claim-shape violation is
    rejected with :class:`InvalidTokenError` (Req 4.3, 8.5).

    Args:
        token: The token string produced by :func:`sign`.
        secret: The server-held signing key (``str`` or ``bytes``).

    Returns:
        The recovered :class:`EntryClaims`.

    Raises:
        InvalidTokenError: If the token is malformed or its signature does not
            match the recomputed MAC under ``secret``.
    """
    key = _coerce_secret(secret)
    if not key:
        # An empty secret can never have produced a valid token via ``sign``.
        raise InvalidTokenError("invalid token")

    if not isinstance(token, str):
        raise InvalidTokenError("invalid token")

    payload_segment, sep, mac_segment = token.partition(TOKEN_SEPARATOR)
    if sep == "" or payload_segment == "" or mac_segment == "":
        raise InvalidTokenError("invalid token")

    # Constant-time comparison of the presented MAC against the recomputed one.
    expected_mac = _mac(payload_segment, key)
    try:
        presented_mac = _b64url_decode(mac_segment)
    except (ValueError, TypeError) as exc:
        raise InvalidTokenError("invalid token") from exc

    if not hmac.compare_digest(expected_mac, presented_mac):
        raise InvalidTokenError("invalid token")

    # Signature is valid: decode and validate the payload's claim shape.
    try:
        payload_bytes = _b64url_decode(payload_segment)
        parsed = json.loads(payload_bytes.decode("utf-8"))
    except (ValueError, TypeError, UnicodeDecodeError) as exc:
        raise InvalidTokenError("invalid token") from exc

    if not isinstance(parsed, dict):
        raise InvalidTokenError("invalid token")

    try:
        canonical = _canonical_claims(parsed)
    except (ValueError, TypeError) as exc:
        raise InvalidTokenError("invalid token") from exc

    return EntryClaims(
        Fan_Id=canonical["Fan_Id"],
        Event_Id=canonical["Event_Id"],
        Ordering_Key=canonical["Ordering_Key"],
        Write_Shard=canonical["Write_Shard"],
    )
