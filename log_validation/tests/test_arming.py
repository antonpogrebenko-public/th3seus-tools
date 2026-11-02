"""Tests for the disarmed-sample mask read from the arming topics."""

import numpy as np
import pytest

from log_validation import arming
from log_validation.tests.fakes import FakeULog

T = np.array([0.0, 5.0, 10.0, 15.0, 25.0, 31.0])


def _armed(ts_s, values, dtype=float):
    return {
        "actuator_armed": {
            "timestamp": np.asarray(ts_s, float) * 1e6,
            "armed": np.asarray(values, dtype=dtype),
        }
    }


def _status(ts_s, states):
    return {
        "vehicle_status": {
            "timestamp": np.asarray(ts_s, float) * 1e6,
            "arming_state": np.asarray(states, float),
        }
    }


def test_step_hold_at_or_before():
    m = arming.disarmed_mask(FakeULog(_armed([0, 10, 20], [0, 1, 0])), T)
    assert m.tolist() == [True, True, False, False, False, True]


def test_samples_before_the_first_message_are_not_disarmed():
    m = arming.disarmed_mask(FakeULog(_armed([6], [0])), T)
    assert m.tolist() == [False, False, True, True, True, True]


@pytest.mark.parametrize("dtype", [bool, np.uint8, float])
def test_armed_dtypes(dtype):
    m = arming.disarmed_mask(FakeULog(_armed([0, 10], [0, 1], dtype)), T)
    assert m.tolist() == [True, True, False, False, False, False]


def test_actuator_armed_wins_over_vehicle_status():
    m = arming.disarmed_mask(FakeULog({**_armed([0], [0]), **_status([0], [2])}), T)
    assert m.all()


def test_vehicle_status_does_not_override_an_armed_actuator_armed():
    m = arming.disarmed_mask(FakeULog({**_armed([0], [1]), **_status([0], [1])}), T)
    assert not m.any()


@pytest.mark.parametrize(
    "state,disarmed",
    [(0, True), (1, True), (2, False), (3, False), (4, False), (5, False)],
)
def test_vehicle_status_states(state, disarmed):
    m = arming.disarmed_mask(FakeULog(_status([0], [state])), T)
    assert m.tolist() == [disarmed] * 6


def test_actuator_armed_without_its_field_falls_back_to_vehicle_status():
    log = FakeULog({"actuator_armed": {"timestamp": np.array([0.0])}, **_status([0], [1])})
    assert arming.disarmed_mask(log, T).all()


def test_neither_topic_gives_none():
    assert arming.disarmed_mask(FakeULog({}), T) is None


SETTLE_QUERIES = np.array([15.0, 19.99, 20.0, 25.0])
SETTLE_EXPECTED = [False, False, True, True]


def test_spin_down_after_a_disarm_is_not_disarmed_actuator_armed():
    m = arming.disarmed_mask(FakeULog(_armed([0, 10], [1, 0])), SETTLE_QUERIES)
    assert m.tolist() == SETTLE_EXPECTED


def test_spin_down_after_a_disarm_is_not_disarmed_vehicle_status():
    m = arming.disarmed_mask(FakeULog(_status([0, 10], [2, 1])), SETTLE_QUERIES)
    assert m.tolist() == SETTLE_EXPECTED


def test_series_that_starts_disarmed_loses_nothing():
    m = arming.disarmed_mask(FakeULog(_armed([0], [0])), np.array([5.0]))
    assert m.tolist() == [True]


def test_each_disarm_starts_its_own_settle_time():
    m = arming.disarmed_mask(
        FakeULog(_armed([0, 10, 40, 60], [1, 0, 1, 0])), np.array([25.0, 65.0, 75.0])
    )
    assert m.tolist() == [True, False, True]


def test_repeated_messages_of_the_same_state_do_not_restart_the_settle_time():
    m = arming.disarmed_mask(
        FakeULog(_armed([0, 2, 4, 10, 12, 14], [0, 0, 1, 1, 0, 0])), np.array([3.0, 23.0])
    )
    assert m.tolist() == [True, True]
