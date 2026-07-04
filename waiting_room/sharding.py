"""Write-shard assignment for the Virtual Waiting Room.

A single ``Event_Id`` partition caps at ~1,000 WCU/s, which is orders of
magnitude short of a 10M-arrival burst. The design therefore keys queue
entries by ``EVT#<Event_Id>#SH#<shard>``, scattering the burst across
``Shard_Count`` partitions. This module computes that shard::

    Write_Shard = hash(Fan_Id) mod Shard_Count

The assignment must be:

* **Deterministic per Fan_Id and stable across processes.** The shard is
  recomputed from the token on the status-read path (there is no shard
  directory to consult), so two different Python processes - the admission
  writer and the status reader - must derive the identical shard for the same
  ``Fan_Id``. Python's built-in :func:`hash` is salted per-process
  (``PYTHONHASHSEED``) and is therefore unusable here. We hash with
  :mod:`hashlib` (SHA-256) so results are reproducible across runs, processes,
  and machines.
* **Independent of any client input.** The shard is a pure function of the
  server-issued ``Fan_Id`` and the configured ``Shard_Count`` - nothing the
  fan supplies can influence it (Req 4.1).
* **Evenly distributed.** A cryptographic digest spreads distinct ``Fan_Id``s
  approximately uniformly across shards, so no single partition (base table or
  the sparse ``WaitingIndex`` GSI) receives a disproportionate share of the
  write burst (Req 2.2, 9.4).

This module is pure logic: no I/O, no ``boto3``, no global mutable state.

Requirements: 2.2, 4.1, 9.4.
"""

from __future__ import annotations

import hashlib

__all__ = [
    "compute_write_shard",
    "format_shard",
    "shard_width",
    "assign_shard",
]


def _stable_hash(fan_id: str) -> int:
    """Return a stable, process-independent non-negative hash of ``fan_id``.

    Uses SHA-256 over the UTF-8 encoding of the identifier. Unlike the
    built-in :func:`hash`, this is identical across processes and runs, which
    is required because the status-read path recomputes the shard from the
    token rather than storing a shard directory.
    """
    digest = hashlib.sha256(fan_id.encode("utf-8")).digest()
    return int.from_bytes(digest, byteorder="big")


def compute_write_shard(fan_id: str, shard_count: int) -> int:
    """Compute ``Write_Shard = hash(Fan_Id) mod Shard_Count``.

    Args:
        fan_id: The server-issued fan identifier. Must be a non-empty string.
        shard_count: The configured number of write shards. Must be positive.

    Returns:
        An ``int`` shard index in ``[0, shard_count)``. Deterministic per
        ``fan_id`` and stable across processes.

    Raises:
        TypeError: If ``fan_id`` is not a ``str``.
        ValueError: If ``fan_id`` is empty or ``shard_count`` is not positive.
    """
    if not isinstance(fan_id, str):
        raise TypeError(f"fan_id must be a str, got {type(fan_id).__name__}")
    if not fan_id:
        raise ValueError("fan_id must be a non-empty string")
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")

    return _stable_hash(fan_id) % shard_count


def shard_width(shard_count: int) -> int:
    """Return the zero-padding width for shard strings at ``shard_count``.

    The width is the number of decimal digits in the largest shard index
    (``shard_count - 1``), so shards sort lexicographically in numeric order
    and every key has a uniform length. For ``shard_count = 1000`` the width
    is ``3`` (indices ``000``..``999``); for ``4000`` it is ``4``.

    Args:
        shard_count: The configured number of write shards. Must be positive.

    Returns:
        The zero-padding width as a positive ``int``.

    Raises:
        ValueError: If ``shard_count`` is not positive.
    """
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    return len(str(shard_count - 1))


def format_shard(shard: int, shard_count: int) -> str:
    """Format ``shard`` as a zero-padded string for use in keys (e.g. ``"042"``).

    The width is derived from ``shard_count`` (see :func:`shard_width`) so all
    shard strings for an event are the same length and sort lexicographically
    in numeric order - matching the ``EVT#<Event_Id>#SH#<shard>`` key form in
    the design.

    Args:
        shard: A shard index in ``[0, shard_count)``.
        shard_count: The configured number of write shards. Must be positive.

    Returns:
        The zero-padded shard string.

    Raises:
        ValueError: If ``shard_count`` is not positive or ``shard`` is outside
            ``[0, shard_count)``.
    """
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    if not 0 <= shard < shard_count:
        raise ValueError(
            f"shard {shard} is outside the valid range [0, {shard_count})"
        )
    return f"{shard:0{shard_width(shard_count)}d}"


def assign_shard(fan_id: str, shard_count: int) -> tuple[int, str]:
    """Compute the shard for ``fan_id`` and return both int and padded string.

    Convenience helper combining :func:`compute_write_shard` and
    :func:`format_shard` in a single call, since callers writing keys typically
    need both the numeric shard and its key-ready string form.

    Args:
        fan_id: The server-issued fan identifier.
        shard_count: The configured number of write shards.

    Returns:
        A ``(shard_int, shard_str)`` tuple.
    """
    shard = compute_write_shard(fan_id, shard_count)
    return shard, format_shard(shard, shard_count)
