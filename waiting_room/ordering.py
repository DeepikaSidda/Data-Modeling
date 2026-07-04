"""Ordering_Key allocation and comparison for the Virtual Waiting Room.

This module is **pure logic** - it performs no DynamoDB access and no I/O
beyond reading an injectable clock and an injectable randomness source. That
makes it fully deterministic and testable: tests supply their own clock (to
inject regressions and same-millisecond clusters) and their own tie-breaker
source (to force collisions or fix values).

## Ordering_Key format

``Ordering_Key = "<seq>#<tiebreak>"`` where:

* ``seq`` is a **Hybrid Logical Clock (HLC)** value: a 48-bit physical
  millisecond component concatenated with a 16-bit per-node logical counter.
  Both components are rendered as fixed-width, zero-padded **decimal** strings
  (:data:`PHYSICAL_WIDTH` and :data:`LOGICAL_WIDTH` digits respectively), so
  lexicographic string comparison of ``seq`` equals numeric/chronological
  comparison of ``(physical_ms, logical_counter)`` (Req 3.1).
* ``tiebreak`` is a server-side CSPRNG token (64-bit, rendered as
  :data:`TIEBREAK_HEX_WIDTH` fixed-width hex chars). It resolves
  near-simultaneous arrivals fairly and unpredictably, and a fan cannot
  influence it, so repeated or crafted requests cannot bias position
  (Req 3.3, 3.4, 4.5).

## Monotonicity guarantee

:class:`OrderingKeyAllocator` is a **stateful per-node allocator**. Its HLC
advances monotonically non-decreasing on every call, even when the wall clock
regresses or skews beyond a configured bound: when the physical reading does
not advance, the logical counter absorbs the difference, and on logical
overflow the physical component is carried forward by one millisecond. Thus
the emitted ``seq`` is strictly increasing per node and never inverts admission
order (Req 3.1, 3.6).

## Total, deterministic order

:func:`compare_ordering_keys` compares ``seq`` first, then ``tiebreak``.
Because every ``tiebreak`` is a unique random token, no two distinct entries
compare equal, so the comparator is a strict total order (irreflexive,
antisymmetric, transitive, total) and recomputing an ordering over the same
set always yields identical results (Req 3.3, 3.7).

Requirements: 3.1, 3.3, 3.4, 3.6, 3.7.
"""

from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass
from typing import Callable

__all__ = [
    "PHYSICAL_BITS",
    "LOGICAL_BITS",
    "MAX_PHYSICAL_MS",
    "MAX_LOGICAL",
    "PHYSICAL_WIDTH",
    "LOGICAL_WIDTH",
    "SEQ_WIDTH",
    "TIEBREAK_BYTES",
    "TIEBREAK_HEX_WIDTH",
    "ORDERING_KEY_SEPARATOR",
    "default_clock_ms",
    "default_tiebreak",
    "render_seq",
    "OrderingKeyParts",
    "OrderingKeyAllocator",
    "parse_ordering_key",
    "compare_ordering_keys",
    "ordering_key_sort_key",
]


# --------------------------------------------------------------------------- #
# Fixed-width rendering constants
# --------------------------------------------------------------------------- #
#: Bit width of the physical (millisecond) HLC component.
PHYSICAL_BITS: int = 48
#: Bit width of the logical (counter) HLC component.
LOGICAL_BITS: int = 16

#: Largest representable physical millisecond value (48-bit).
MAX_PHYSICAL_MS: int = (1 << PHYSICAL_BITS) - 1
#: Largest representable logical counter value (16-bit).
MAX_LOGICAL: int = (1 << LOGICAL_BITS) - 1

#: Decimal digits needed to render :data:`MAX_PHYSICAL_MS` (== 15).
PHYSICAL_WIDTH: int = len(str(MAX_PHYSICAL_MS))
#: Decimal digits needed to render :data:`MAX_LOGICAL` (== 5).
LOGICAL_WIDTH: int = len(str(MAX_LOGICAL))
#: Total width of a rendered ``seq`` string (physical || logical).
SEQ_WIDTH: int = PHYSICAL_WIDTH + LOGICAL_WIDTH

#: Bytes of CSPRNG entropy in the tie-breaker (64-bit).
TIEBREAK_BYTES: int = 8
#: Hex-character width of the rendered tie-breaker (fixed width so that
#: whole-key string comparison stays order-preserving).
TIEBREAK_HEX_WIDTH: int = TIEBREAK_BYTES * 2

#: Separator between the ``seq`` and ``tiebreak`` components.
ORDERING_KEY_SEPARATOR: str = "#"


# --------------------------------------------------------------------------- #
# Default injectable sources
# --------------------------------------------------------------------------- #
def default_clock_ms() -> int:
    """Return the current authoritative server time in whole milliseconds.

    This is the default physical-clock source for :class:`OrderingKeyAllocator`.
    Tests inject their own callable to simulate regressions and skew.
    """
    return time.time_ns() // 1_000_000


def default_tiebreak() -> str:
    """Return a fresh 64-bit CSPRNG tie-breaker as fixed-width hex.

    Uses :mod:`secrets` (a cryptographically secure source) so the value is
    unpredictable and cannot be influenced by a fan (Req 3.4, 4.5).
    """
    return secrets.token_bytes(TIEBREAK_BYTES).hex().zfill(TIEBREAK_HEX_WIDTH)


# --------------------------------------------------------------------------- #
# Rendering / parsing helpers
# --------------------------------------------------------------------------- #
def render_seq(physical_ms: int, logical: int) -> str:
    """Render an HLC ``(physical_ms, logical)`` pair as a sortable ``seq``.

    The two components are zero-padded to fixed widths and concatenated, so
    lexicographic comparison of the returned string equals numeric comparison
    of the pair (Req 3.1).

    :raises ValueError: if either component is out of its representable range.
    """
    if not 0 <= physical_ms <= MAX_PHYSICAL_MS:
        raise ValueError(
            f"physical_ms {physical_ms} out of 48-bit range [0, {MAX_PHYSICAL_MS}]"
        )
    if not 0 <= logical <= MAX_LOGICAL:
        raise ValueError(
            f"logical {logical} out of 16-bit range [0, {MAX_LOGICAL}]"
        )
    return f"{physical_ms:0{PHYSICAL_WIDTH}d}{logical:0{LOGICAL_WIDTH}d}"


@dataclass(frozen=True, slots=True)
class OrderingKeyParts:
    """The decoded components of an ``Ordering_Key``."""

    seq: str
    tiebreak: str
    physical_ms: int
    logical: int


def parse_ordering_key(ordering_key: str) -> OrderingKeyParts:
    """Decode an ``Ordering_Key`` string into its components.

    :raises ValueError: if the string is not a well-formed ``<seq>#<tiebreak>``
        with a fixed-width numeric ``seq``.
    """
    seq, sep, tiebreak = ordering_key.partition(ORDERING_KEY_SEPARATOR)
    if sep == "":
        raise ValueError(f"missing '{ORDERING_KEY_SEPARATOR}' separator in {ordering_key!r}")
    if len(seq) != SEQ_WIDTH or not seq.isdigit():
        raise ValueError(f"malformed seq component in {ordering_key!r}")
    if tiebreak == "":
        raise ValueError(f"empty tiebreak component in {ordering_key!r}")
    physical_ms = int(seq[:PHYSICAL_WIDTH])
    logical = int(seq[PHYSICAL_WIDTH:])
    return OrderingKeyParts(
        seq=seq, tiebreak=tiebreak, physical_ms=physical_ms, logical=logical
    )


# --------------------------------------------------------------------------- #
# Stateful HLC allocator
# --------------------------------------------------------------------------- #
class OrderingKeyAllocator:
    """Per-node, stateful Hybrid Logical Clock ``Ordering_Key`` allocator.

    Each call to :meth:`next_ordering_key` returns a fresh, strictly increasing
    ``<seq>#<tiebreak>`` string. The HLC state (last physical component and
    logical counter) is advanced under a lock so the allocator is safe to share
    across threads on a single admission node.

    The clock and tie-breaker sources are injectable for testability:

    * ``clock_ms`` returns the authoritative server time in whole milliseconds.
      It may regress or skew arbitrarily; the allocator still guarantees a
      monotonically non-decreasing ``seq`` (Req 3.6).
    * ``tiebreak_source`` returns a server-side random token string.

    Requirements: 3.1, 3.4, 3.6.
    """

    __slots__ = ("_clock_ms", "_tiebreak_source", "_lock", "_last_physical", "_last_logical")

    def __init__(
        self,
        clock_ms: Callable[[], int] = default_clock_ms,
        tiebreak_source: Callable[[], str] = default_tiebreak,
    ) -> None:
        self._clock_ms = clock_ms
        self._tiebreak_source = tiebreak_source
        self._lock = threading.Lock()
        # HLC state. Start "before" any real timestamp so the very first call
        # adopts the wall clock rather than an artificial counter.
        self._last_physical: int = 0
        self._last_logical: int = 0

    def _advance(self) -> tuple[int, int]:
        """Advance the HLC once and return the new ``(physical, logical)``.

        Canonical single-node HLC step, hardened against a regressing wall
        clock and logical-counter overflow:

        * ``new_physical = max(last_physical, clock())`` - never moves backward.
        * If the physical component did not advance (same ms, or the wall
          clock regressed), bump the logical counter to preserve order.
        * If it advanced, reset the logical counter to 0.
        * On logical overflow, carry one millisecond into the physical
          component and reset the counter.

        The result is strictly greater than the previous state, so the emitted
        ``seq`` is monotonically increasing per node (Req 3.1, 3.6).
        """
        raw = int(self._clock_ms())
        # Clamp the physical reading into the representable 48-bit range so a
        # misbehaving clock can never produce an unrenderable value.
        if raw < 0:
            raw = 0
        elif raw > MAX_PHYSICAL_MS:
            raw = MAX_PHYSICAL_MS

        new_physical = self._last_physical if self._last_physical > raw else raw

        if new_physical == self._last_physical:
            # Clock did not advance (equal ms or regression): absorb into logical.
            new_logical = self._last_logical + 1
            if new_logical > MAX_LOGICAL:
                # Logical overflow within a single ms: carry into physical.
                new_physical += 1
                new_logical = 0
                if new_physical > MAX_PHYSICAL_MS:
                    raise OverflowError("HLC physical component exhausted 48-bit range")
        else:
            # Physical advanced: fresh millisecond, reset the counter.
            new_logical = 0

        self._last_physical = new_physical
        self._last_logical = new_logical
        return new_physical, new_logical

    def next_seq(self) -> str:
        """Advance the HLC and return only the rendered ``seq`` component."""
        with self._lock:
            physical, logical = self._advance()
        return render_seq(physical, logical)

    def next_ordering_key(self) -> str:
        """Advance the HLC and return a fresh ``<seq>#<tiebreak>`` key.

        The ``seq`` is drawn under the lock so concurrent callers on the same
        node still receive strictly increasing sequences; the tie-breaker is
        drawn from the injected CSPRNG source.
        """
        with self._lock:
            physical, logical = self._advance()
        seq = render_seq(physical, logical)
        tiebreak = self._tiebreak_source()
        return f"{seq}{ORDERING_KEY_SEPARATOR}{tiebreak}"


# --------------------------------------------------------------------------- #
# Pure comparator (strict total order)
# --------------------------------------------------------------------------- #
def compare_ordering_keys(a: str, b: str) -> int:
    """Compare two ``Ordering_Key`` strings, returning -1, 0, or 1.

    Ordering is defined by ``seq`` first, then ``tiebreak`` (Req 3.3, 3.7).
    Because both components are fixed-width, this is equivalent to a plain
    lexicographic comparison of the whole key; the components are compared
    explicitly here to make the total-order contract unambiguous.

    Returns ``0`` only when both components are identical (i.e. the same key);
    distinct entries always compare unequal because their tie-breakers differ.
    """
    pa = parse_ordering_key(a)
    pb = parse_ordering_key(b)
    if pa.seq != pb.seq:
        return -1 if pa.seq < pb.seq else 1
    if pa.tiebreak != pb.tiebreak:
        return -1 if pa.tiebreak < pb.tiebreak else 1
    return 0


def ordering_key_sort_key(ordering_key: str) -> tuple[str, str]:
    """Return a ``(seq, tiebreak)`` tuple usable as a ``sorted(key=...)`` key.

    Sorting a collection of ``Ordering_Key`` strings by this key yields the
    same strict total order as :func:`compare_ordering_keys`.
    """
    parts = parse_ordering_key(ordering_key)
    return (parts.seq, parts.tiebreak)
