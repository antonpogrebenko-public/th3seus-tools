"""Tests for sysid: resampling, the vectorized low-pass, the plant fit and filter delays."""

from __future__ import annotations

import math

import numpy as np
import pytest

from log_validation.sysid import (
    PlantFit,
    butterworth2_group_delay_s,
    first_order_group_delay_s,
    first_order_lowpass,
    fit_first_order_plant,
    fit_first_order_plant_runs,
    uniform_resample,
)
from log_validation.tests.response_fakes import reference_lowpass, simulate_plant, sum_of_sines

DELAYS = [i * 1e-3 for i in range(41)]
TAUS = [i * 1e-3 for i in range(61)]


def _plant(rate_hz: float = 800.0, seconds: float = 60.0, seed: int = 11):
    rng = np.random.default_rng(seed)
    dt = 1.0 / rate_hz
    t = np.arange(int(seconds * rate_hz)) * dt
    u = sum_of_sines(t, 0.1, rng)
    y = simulate_plant(u, dt, gain=40.0, delay_s=0.010, tau_s=0.020, noise_std=0.5, rng=rng)
    return u, y, dt


def test_known_plant():
    u, y, dt = _plant()
    fit = fit_first_order_plant(u, y, dt, DELAYS, TAUS)
    assert abs(fit.gain - 40) / 40 < 0.10 and abs(fit.lag_s - 0.030) < 0.005
    assert 0.0 <= fit.r2 <= 1.0 and fit.r2 > 0.9


def test_known_plant_at_400_hz():
    # At 2.5 ms per sample most 1 ms grid delays fall between samples.
    u, y, dt = _plant(rate_hz=400.0, seconds=60.0)
    fit = fit_first_order_plant(u, y, dt, DELAYS, TAUS)
    assert abs(fit.gain - 40) / 40 < 0.10 and abs(fit.lag_s - 0.030) < 0.005


def test_offset_in_the_setpoint_does_not_bias_the_gain():
    u, y, dt = _plant()
    fit = fit_first_order_plant(u + 0.05, y, dt, DELAYS, TAUS)
    assert abs(fit.gain - 40) / 40 < 0.10


def test_runs_are_fitted_together_without_crossing_their_boundaries():
    u, y, dt = _plant()
    half = len(u) // 2
    runs = [(u[:half], y[:half]), (u[half + 1000:], y[half + 1000:])]
    fit = fit_first_order_plant_runs(runs, dt, DELAYS, TAUS)
    assert abs(fit.gain - 40) / 40 < 0.10 and abs(fit.lag_s - 0.030) < 0.005


def test_plant_fit_is_frozen_and_lag_is_delay_plus_tau():
    fit = PlantFit(gain=1.0, delay_s=0.01, tau_s=0.02, r2=0.9)
    assert fit.lag_s == pytest.approx(0.03)
    with pytest.raises(AttributeError):
        fit.gain = 2.0  # type: ignore[misc]


def test_fit_refuses_an_input_without_variation():
    u = np.full(4000, 0.2)
    with pytest.raises(ValueError, match="variation"):
        fit_first_order_plant(u, np.random.default_rng(0).normal(size=4000), 1 / 800, DELAYS, TAUS)


def test_fit_refuses_too_few_samples():
    with pytest.raises(ValueError, match="samples"):
        fit_first_order_plant(np.arange(10.0), np.arange(10.0), 1 / 800, DELAYS, TAUS)


def test_fit_does_not_change_its_inputs():
    u, y, dt = _plant(seconds=5.0)
    u0, y0 = u.copy(), y.copy()
    fit_first_order_plant(u, y, dt, DELAYS, TAUS)
    assert np.array_equal(u, u0) and np.array_equal(y, y0)


@pytest.mark.parametrize("tau_s", [0.001, 0.0125, 0.06, 0.5])
def test_lowpass_matches_the_recursion(tau_s):
    # Long enough to cross many internal blocks for every time constant.
    u = np.random.default_rng(5).normal(size=20000) + 3.0
    got = first_order_lowpass(u, 1 / 800, tau_s)
    np.testing.assert_allclose(got, reference_lowpass(u, 1 / 800, tau_s), rtol=1e-9, atol=1e-9)


def test_lowpass_with_zero_tau_returns_the_input():
    u = np.array([1.0, -2.0, 3.0])
    out = first_order_lowpass(u, 0.001, 0.0)
    assert np.array_equal(out, u) and out is not u


def test_uniform_resample_drops_repeated_and_backward_stamps():
    t = np.array([0.0, 0.001, 0.001, 0.0005, 0.002, 0.003])
    x = np.array([0.0, 1.0, 50.0, 70.0, 2.0, 3.0])
    grid, values = uniform_resample(t, x, 0.0005)
    np.testing.assert_allclose(grid, [0.0, 0.0005, 0.001, 0.0015, 0.002, 0.0025, 0.003])
    np.testing.assert_allclose(values, [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0])


def test_uniform_resample_of_nothing_is_empty():
    grid, values = uniform_resample(np.array([]), np.array([]), 0.001)
    assert grid.size == 0 and values.size == 0


def test_group_delay_of_40_hz_cutoff():
    assert abs(butterworth2_group_delay_s(40.0) - 0.005627) < 1e-6


def test_first_order_group_delay_is_the_time_constant():
    assert first_order_group_delay_s(30.0) == pytest.approx(1 / (2 * math.pi * 30.0))


@pytest.mark.parametrize("cutoff", [0.0, -1.0])
def test_disabled_filters_have_no_delay(cutoff):
    assert butterworth2_group_delay_s(cutoff) == 0.0
    assert first_order_group_delay_s(cutoff) == 0.0
