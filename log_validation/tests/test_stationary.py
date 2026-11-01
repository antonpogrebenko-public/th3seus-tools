"""Tests for the stationary window search restricted to eligible samples."""

import numpy as np
import pytest

from log_validation.stationary import find_stationary_window

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


def test_eligible_of_another_length_is_refused():
    t, g = _quiet(10.0)
    with pytest.raises(ValueError):
        find_stationary_window(t, g, eligible=np.ones(t.size - 1, bool))
