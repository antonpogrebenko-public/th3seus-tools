"""Tests for the stationary window search restricted to eligible samples."""

import numpy as np
import pytest

from log_validation.stationary import (
    DEFAULT_GYRO_THRESHOLD,
    _smoothed_magnitude,
    find_stationary_window,
)

RATE_HZ = 50.0
NOISE_STD = 0.002  # rad/s per axis


def _quiet(seconds):
    """50 Hz timestamps from 0 and seeded white gyro noise (Nx3)."""
    n = int(seconds * RATE_HZ)
    t = np.arange(n) / RATE_HZ
    g = np.random.default_rng(0).normal(0.0, NOISE_STD, (n, 3))
    return t, g


def test_window_stays_inside_the_eligible_samples():
    t, g = _quiet(100.0)
    w = find_stationary_window(t, g, eligible=(t >= 10) & (t < 20))
    assert 10.0 <= w.t0_s and w.t1_s < 20.0 and w.duration_s > 9.9


def test_longest_eligible_run_wins():
    t, g = _quiet(100.0)
    w = find_stationary_window(t, g, eligible=(t < 10) | (t >= 60))
    assert w.t0_s >= 60.0


def test_eligible_run_below_the_minimum_gives_none():
    t, g = _quiet(100.0)
    assert find_stationary_window(t, g, eligible=(t >= 10) & (t < 12)) is None


def test_without_eligible_the_whole_quiet_log_is_the_window():
    t, g = _quiet(100.0)
    assert find_stationary_window(t, g).duration_s > 99.9


@pytest.mark.parametrize(
    "make_eligible",
    [
        lambda n: np.ones(n - 1, bool),
        lambda n: np.ones(1, bool),
        lambda n: np.array(True),
    ],
    ids=["one-short", "single-element", "zero-dimensional"],
)
def test_eligible_of_another_length_is_refused(make_eligible):
    t, g = _quiet(10.0)
    with pytest.raises(ValueError, match="eligible has shape"):
        find_stationary_window(t, g, eligible=make_eligible(t.size))


def _constant_rate(seconds, magnitude):
    """50 Hz timestamps and a constant gyro rate of the given magnitude on one axis."""
    n = int(seconds * RATE_HZ)
    g = np.zeros((n, 3))
    g[:, 0] = magnitude
    return np.arange(n) / RATE_HZ, g


def test_constant_rate_just_below_the_threshold_is_quiet():
    t, g = _constant_rate(10.0, DEFAULT_GYRO_THRESHOLD * 0.97)
    assert find_stationary_window(t, g).duration_s >= 9.9


def test_constant_rate_just_above_the_threshold_is_not_quiet():
    t, g = _constant_rate(10.0, DEFAULT_GYRO_THRESHOLD * 1.03)
    assert find_stationary_window(t, g) is None


def test_noise_that_averages_below_the_threshold_is_quiet():
    n = int(20.0 * RATE_HZ)
    g = np.random.default_rng(1).normal(0.0, 0.02, (n, 3))
    assert np.mean(np.linalg.norm(g, axis=1) > DEFAULT_GYRO_THRESHOLD) > 0.3
    w = find_stationary_window(np.arange(n) / RATE_HZ, g)
    assert w is not None and w.duration_s >= 19.0


def _still_khz(n):
    """1 kHz timestamps from 0 and a motionless gyro (Nx3)."""
    return np.arange(n) / 1000.0, np.zeros((n, 3))


@pytest.mark.parametrize("n", [0, 1, 2, 50, 199, 200, 400])
def test_one_averaged_value_per_sample(n):
    t, g = _still_khz(n)
    assert _smoothed_magnitude(t, g, 0.2).shape == (n,)


@pytest.mark.parametrize("n", [200, 400])
def test_average_of_a_stream_at_least_as_long_as_the_window_is_unchanged(n):
    t = np.arange(n) / 1000.0
    g = np.random.default_rng(3).normal(0.0, 0.01, (n, 3))
    kernel = np.ones(200) / 200
    expected = np.linalg.norm(
        np.column_stack([np.convolve(g[:, a], kernel, mode="same") for a in range(3)]), axis=1)
    np.testing.assert_array_equal(_smoothed_magnitude(t, g, 0.2), expected)


@pytest.mark.parametrize("n", [50, 199])
@pytest.mark.parametrize("with_eligible", [False, True], ids=["no-eligible", "eligible"])
def test_stream_shorter_than_the_window_gives_no_window(n, with_eligible):
    t, g = _still_khz(n)
    eligible = np.ones(n, bool) if with_eligible else None
    assert find_stationary_window(t, g, eligible=eligible) is None
