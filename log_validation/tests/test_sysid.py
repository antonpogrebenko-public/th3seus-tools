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
    hover_command,
    inflight_noise_std,
    resample_on,
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


def _lagged(x: np.ndarray, shifts=(0, 4, 8, 16, 32, 64)) -> np.ndarray:
    """x at whole-sample lags, rows before the longest lag dropped."""
    longest = max(shifts)
    return np.column_stack([x[longest - s: len(x) - s] for s in shifts])


def test_least_squares_reports_a_full_first_stage():
    u, y, dt = _plant(seconds=10.0)
    fit = fit_first_order_plant(u, y, dt, DELAYS, TAUS)
    assert fit.first_stage_r2 == 1.0 and fit.reduced_form_r2 == 1.0


def test_two_stage_fit_recovers_an_open_loop_plant():
    u, y, dt = _plant()
    fit = fit_first_order_plant(u[64:], y[64:], dt, DELAYS, TAUS, instruments=_lagged(u))
    assert abs(fit.gain - 40) / 40 < 0.10 and abs(fit.lag_s - 0.030) < 0.005
    assert 0.9 < fit.r2 <= 1.0 and 0.5 < fit.first_stage_r2 <= 1.0


def test_two_stage_fit_with_runs_of_instruments():
    u, y, dt = _plant()
    z = _lagged(u)
    u, y = u[64:], y[64:]
    half = len(u) // 2
    runs = [(u[:half], y[:half], z[:half]), (u[half:], y[half:], z[half:])]
    fit = fit_first_order_plant_runs(runs, dt, DELAYS, TAUS)
    assert abs(fit.gain - 40) / 40 < 0.10 and abs(fit.lag_s - 0.030) < 0.005


def test_fit_r2_is_the_share_of_y_the_two_stage_model_explains():
    u, y, dt = _plant(seconds=20.0)
    fit = fit_first_order_plant(u[64:], y[64:], dt, DELAYS, TAUS, instruments=_lagged(u))
    ls = fit_first_order_plant(u[64:], y[64:], dt, DELAYS, TAUS)
    assert fit.r2 <= fit.reduced_form_r2 <= 1.0
    assert fit.r2 == pytest.approx(ls.r2, abs=0.02)  # open loop: about the LS share


def test_fit_r2_of_a_response_unrelated_to_the_instruments_is_small():
    u, _, dt = _plant(seconds=20.0)
    noise = np.random.default_rng(4).normal(0.0, 4.0, len(u))
    fit = fit_first_order_plant(u[64:], noise[64:], dt, DELAYS, TAUS, instruments=_lagged(u))
    assert fit.r2 < 0.01 and fit.reduced_form_r2 < 0.01


def test_reduced_form_r2_is_the_same_at_every_grid_point():
    # The fit keeps the highest fit_r2; that is the highest R^2 projected on the
    # instruments only because y'Py / y'y does not depend on the grid point. Every
    # grid below ends at 40 ms, so all score the same response samples.
    u, y, dt = _plant(seconds=10.0)
    z = _lagged(u)
    full = fit_first_order_plant(u[64:], y[64:], dt, DELAYS, TAUS, instruments=z)
    for d, tau in ((0.0, 0.0), (0.010, 0.020), (0.003, 0.051)):
        one = fit_first_order_plant(u[64:], y[64:], dt, [d, 0.040], [tau], instruments=z)
        assert one.reduced_form_r2 == pytest.approx(full.reduced_form_r2, abs=1e-12)
        assert one.r2 <= full.r2 + 1e-12


def test_fit_refuses_constant_instruments():
    u, y, dt = _plant(seconds=5.0)
    with pytest.raises(ValueError, match="instruments"):
        fit_first_order_plant(u, y, dt, DELAYS, TAUS, instruments=np.full((len(u), 6), 0.1))


def test_fit_refuses_instruments_with_a_single_direction():
    # Two-stage fitting here is over-identified: one direction is not enough.
    u, y, dt = _plant(seconds=5.0)
    z = np.column_stack([u, 2.0 * u, u + 1.0])
    with pytest.raises(ValueError, match="2 independent"):
        fit_first_order_plant(u, y, dt, DELAYS, TAUS, instruments=z)


def test_instruments_without_signal_have_their_own_error():
    # Instruments orthogonal to the input at the only grid point (no delay, no lag).
    u, y, dt = _plant(seconds=5.0)
    scored = u[1:] - u.mean()  # the input a one-point grid (no delay, no lag) scores
    basis = [np.ones(scored.size) / np.sqrt(scored.size)]
    centred = scored - np.dot(scored, basis[0]) * basis[0]
    basis.append(centred / np.sqrt(np.dot(centred, centred)))
    columns = []
    for column in np.random.default_rng(2).normal(size=(3, scored.size)):
        for b in basis:
            column = column - np.dot(column, b) * b
        columns.append(np.concatenate(([0.0], column)))
    z = np.column_stack(columns)
    with pytest.raises(ValueError, match="no signal") as caught:
        fit_first_order_plant(u, y, dt, [0.0], [0.0], instruments=z)
    assert "variation" not in str(caught.value)


def test_fit_refuses_instruments_of_another_length():
    u, y, dt = _plant(seconds=5.0)
    with pytest.raises(ValueError, match="length"):
        fit_first_order_plant(u, y, dt, DELAYS, TAUS, instruments=np.ones((len(u) - 1, 3)))


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


def test_resample_on_does_not_bridge_long_gaps():
    t = np.array([0.0, 0.001, 0.002, 0.003, 0.010, 0.011])
    x = np.arange(6.0)
    grid = np.array([0.0015, 0.0025, 0.005, 0.009, 0.0105])
    out = resample_on(t, x, grid, max_gap_s=0.003)
    np.testing.assert_allclose(out[[0, 1, 4]], [1.5, 2.5, 4.5])
    assert np.isnan(out[2]) and np.isnan(out[3])
    assert np.all(np.isfinite(resample_on(t, x, grid)))  # no limit: bridged as before


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


# ---------------------------------------------------------------------------
# Hover command and in-flight noise
# ---------------------------------------------------------------------------


def test_quadratic_thrust_model():
    assert hover_command(0.25, 1.0) == pytest.approx(0.5)


def test_linear_thrust_model():
    assert hover_command(0.4, 0.0) == pytest.approx(0.4)


@pytest.mark.parametrize("f", [0.3, 0.7])
def test_hover_command_inverts_the_thrust_model(f):
    u = hover_command(0.35, f)
    assert (1 - f) * u + f * u * u == pytest.approx(0.35)


@pytest.mark.parametrize("f", [-0.1, 1.5, float("nan")])
def test_thr_mdl_fac_outside_unit_interval_is_refused(f):
    with pytest.raises(ValueError, match="THR_MDL_FAC"):
        hover_command(0.4, f)


def test_negative_or_non_finite_thrust_is_refused():
    for thrust in (-0.1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="thrust"):
            hover_command(thrust, 0.3)


def test_inflight_noise_of_a_slow_sine_plus_white_noise():
    rng = np.random.default_rng(5)
    dt = 1.0 / 800.0
    t = np.arange(8000) * dt
    x = 0.2 * np.sin(2 * np.pi * 1.0 * t) + rng.normal(0.0, 0.05, t.size)
    assert inflight_noise_std(x, dt, 20.0) == pytest.approx(0.05, rel=0.05)


def test_inflight_noise_of_a_constant_is_zero():
    assert inflight_noise_std(np.full(400, 3.0), 1 / 800, 20.0) == pytest.approx(0.0, abs=1e-12)


def test_inflight_noise_does_not_change_its_input():
    x = np.random.default_rng(1).normal(size=500)
    before = x.copy()
    inflight_noise_std(x, 1 / 800, 20.0)
    assert np.array_equal(x, before)


@pytest.mark.parametrize("x, dt, cutoff", [
    (np.zeros(10), 1 / 800, 20.0),  # fewer samples than the window
    (np.zeros(400), 1 / 800, 0.0),
    (np.zeros(400), 0.0, 20.0),
    (np.zeros(400), 1 / 800, 700.0),  # window below 2 samples
])
def test_inflight_noise_refuses_unusable_input(x, dt, cutoff):
    with pytest.raises(ValueError):
        inflight_noise_std(x, dt, cutoff)
