"""Unit tests for the event-configuration schema (Requirements 2.1, 2.2)."""

import pytest

from waiting_room.config import (
    DEFAULT_ACTIVE_TARGET,
    DEFAULT_DOWNSTREAM_CAPACITY,
    DEFAULT_ELIGIBILITY_WINDOW_SECS,
    DEFAULT_MAX_BATCH_SIZE,
    DEFAULT_MAX_QUEUE_SIZE,
    DEFAULT_SHARD_COUNT,
    BackoffConfig,
    EligibilityStatus,
    EventConfig,
    EventStatus,
    WaitingRoomConfig,
)


def test_event_config_defaults_match_design():
    cfg = EventConfig()
    assert cfg.event_status is EventStatus.OPEN
    assert cfg.shard_count == DEFAULT_SHARD_COUNT == 1000
    assert cfg.max_queue_size == DEFAULT_MAX_QUEUE_SIZE == 10_000_000
    assert cfg.eligibility_window_secs == DEFAULT_ELIGIBILITY_WINDOW_SECS == 120
    assert cfg.max_batch_size == DEFAULT_MAX_BATCH_SIZE == 500
    assert cfg.active_target == DEFAULT_ACTIVE_TARGET == 1000
    assert cfg.downstream_capacity == DEFAULT_DOWNSTREAM_CAPACITY == 1000
    assert cfg.is_open is True


def test_waiting_room_config_defaults_instantiate():
    cfg = WaitingRoomConfig()
    assert isinstance(cfg.event, EventConfig)
    assert isinstance(cfg.backoff, BackoffConfig)
    assert cfg.staleness_bound_secs == 5


def test_backoff_config_defaults():
    b = BackoffConfig()
    assert b.base_delay_secs > 0
    assert b.max_retries >= 0
    assert 0.0 <= b.jitter_fraction <= 1.0
    assert b.max_delay_secs > 0


def test_closed_event_is_not_open():
    cfg = EventConfig(event_status=EventStatus.CLOSED)
    assert cfg.is_open is False


@pytest.mark.parametrize(
    "kwargs",
    [
        {"shard_count": 0},
        {"max_queue_size": 0},
        {"eligibility_window_secs": 0},
        {"max_batch_size": 0},
        {"active_target": -1},
        {"downstream_capacity": 0},
    ],
)
def test_event_config_rejects_invalid_values(kwargs):
    with pytest.raises(ValueError):
        EventConfig(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base_delay_secs": 0},
        {"max_retries": -1},
        {"jitter_fraction": 1.5},
        {"jitter_fraction": -0.1},
        {"max_delay_secs": 0},
    ],
)
def test_backoff_config_rejects_invalid_values(kwargs):
    with pytest.raises(ValueError):
        BackoffConfig(**kwargs)


def test_waiting_room_config_rejects_invalid_staleness():
    with pytest.raises(ValueError):
        WaitingRoomConfig(staleness_bound_secs=0)


def test_eligibility_status_values():
    assert {s.value for s in EligibilityStatus} == {
        "WAITING",
        "ELIGIBLE",
        "ACTIVE",
        "EXPIRED",
        "COMPLETED",
    }
