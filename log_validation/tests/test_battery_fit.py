"""Tests for battery_fit.py using synthetic signals with known R and OCV.

No real logs required.  We construct minimal pyulog-shaped objects and inject
known voltage/current/soc traces, then assert the fit recovers the injected
parameters within a tolerance.

Design: TDD — each test defines expected output first, then calls the
production function and asserts the expectation.
"""

from __future__ import annotations


import numpy as np
import pytest

from log_validation.battery_fit import (
    OCV_N_BINS,
    OcvCurve,
    _build_ocv_curve,
    _extract_ocv_observations,
    _extract_r_observations,
    _isotonic_increasing,
    _predict_terminal_voltage,
)


# ---------------------------------------------------------------------------
# _isotonic_increasing
# ---------------------------------------------------------------------------


class TestIsotonicIncreasing:
    def test_already_monotone_unchanged(self):
        vals = [1.0, 2.0, 3.0, 4.0]
        result = _isotonic_increasing(vals)
        assert result == pytest.approx(vals)

    def test_single_violation_pooled(self):
        # [1, 3, 2, 4] → idx 1 and 2 violate; pool to avg 2.5
        vals = [1.0, 3.0, 2.0, 4.0]
        result = _isotonic_increasing(vals)
        assert all(result[i] <= result[i + 1] for i in range(len(result) - 1))

    def test_all_equal_stays_equal(self):
        vals = [3.0, 3.0, 3.0]
        result = _isotonic_increasing(vals)
        assert all(v == pytest.approx(3.0) for v in result)

    def test_decreasing_flattened(self):
        vals = [4.0, 3.0, 2.0, 1.0]
        result = _isotonic_increasing(vals)
        assert all(result[i] <= result[i + 1] for i in range(len(result) - 1))

    def test_input_not_mutated(self):
        vals = [3.0, 1.0, 2.0]
        original = list(vals)
        _isotonic_increasing(vals)
        assert vals == original


# ---------------------------------------------------------------------------
# _extract_r_observations
# ---------------------------------------------------------------------------


class TestExtractRObservations:
    def _make_step_trace(
        self,
        cell_count: int,
        r_mohm_per_cell: float,
        n_steps: int = 10,
        step_di: float = 20.0,
    ) -> tuple:
        """Synthesise a voltage/current/time trace with known R.

        Current makes ``n_steps`` step increases of ``step_di`` A.
        Voltage responds via V = V_OCV - I * R_total.
        """
        r_total = cell_count * r_mohm_per_cell / 1000.0
        v_ocv_per_cell = 3.8  # fixed OCV for this test
        t = np.arange(0, n_steps * 2.0, 1.0)  # 1 Hz, steps every 2 s
        i_trace = np.zeros(len(t))
        for j in range(1, n_steps):
            i_trace[j * 2 :] += step_di

        v_trace = v_ocv_per_cell * cell_count - i_trace * r_total
        return v_trace, i_trace, t

    def test_known_r_recovered_3s(self):
        # Inject R = 10 mΩ/cell into a 3S trace; expect recovery within ±2 mΩ
        cc = 3
        r_true = 10.0
        v, i, t = self._make_step_trace(cc, r_true, n_steps=30, step_di=15.0)
        obs = _extract_r_observations(v, i, t, cc)
        assert len(obs) > 0, "No R observations extracted from synthetic trace"
        assert np.median(obs) == pytest.approx(r_true, abs=2.0)

    def test_known_r_recovered_4s(self):
        cc = 4
        r_true = 8.0
        v, i, t = self._make_step_trace(cc, r_true, n_steps=30, step_di=20.0)
        obs = _extract_r_observations(v, i, t, cc)
        assert len(obs) > 0
        assert np.median(obs) == pytest.approx(r_true, abs=2.0)

    def test_no_steps_returns_empty(self):
        # Constant current → no step events → empty list
        t = np.linspace(0, 60, 100)
        i = np.full(100, 5.0)  # constant 5 A
        v = np.full(100, 14.8) - i * 0.04  # constant
        obs = _extract_r_observations(v, i, t, 4)
        assert obs == []

    def test_filters_unphysical_r(self):
        # Inject noise-only voltage change paired with large dI → R would be < 0
        t = np.array([0.0, 1.0, 2.0])
        i = np.array([0.0, 30.0, 0.0])
        # Voltage goes *up* when current increases — unphysical → should be filtered
        v = np.array([16.0, 16.5, 16.0])
        obs = _extract_r_observations(v, i, t, 4)
        # R = -(+0.5) / 30 < 0 → should not appear
        assert all(r > 0 for r in obs)

    def test_too_short_returns_empty(self):
        obs = _extract_r_observations(
            np.array([16.0]), np.array([5.0]), np.array([0.0]), 4
        )
        assert obs == []


# ---------------------------------------------------------------------------
# _extract_ocv_observations
# ---------------------------------------------------------------------------


class TestExtractOcvObservations:
    def test_low_current_samples_extracted(self):
        # 100 samples; first 50 at low current, next 50 at high current
        n = 100
        soc = np.linspace(1.0, 0.5, n)
        i = np.concatenate([np.full(50, 1.0), np.full(50, 30.0)])
        v_per_cell = 3.8  # flat OCV
        cell_count = 4
        v = np.full(n, v_per_cell * cell_count)
        obs = _extract_ocv_observations(v, i, soc, cell_count)
        # Only the 50 low-current samples should be included
        assert len(obs) == 50
        for soc_obs, v_obs in obs:
            assert v_obs == pytest.approx(v_per_cell, abs=1e-6)

    def test_high_current_excluded(self):
        n = 20
        soc = np.full(n, 0.5)
        i = np.full(n, 50.0)  # always high current
        v = np.full(n, 15.0)
        obs = _extract_ocv_observations(v, i, soc, 4)
        assert obs == []

    def test_unphysical_voltage_filtered(self):
        n = 20
        soc = np.full(n, 0.5)
        i = np.full(n, 0.0)
        # Half above ceiling, half below floor
        v = np.array(
            [17.2] * 10 + [11.5] * 10
        )  # 4.3 V/cell and 2.875 V/cell — both invalid
        obs = _extract_ocv_observations(v, i, soc, 4)
        assert obs == []


# ---------------------------------------------------------------------------
# _build_ocv_curve
# ---------------------------------------------------------------------------


class TestBuildOcvCurve:
    def _make_linear_observations(
        self, v0: float, v1: float, n: int = 500
    ) -> list[tuple[float, float]]:
        """Generate synthetic (soc, v_per_cell) pairs for a linear OCV."""
        soc = np.random.default_rng(42).uniform(0.0, 1.0, n)
        v = v0 + soc * (v1 - v0)
        return list(zip(soc.tolist(), v.tolist()))

    def test_curve_is_monotone(self):
        obs = self._make_linear_observations(3.3, 4.2)
        curve = _build_ocv_curve(obs)
        knots = curve.v_per_cell_knots
        assert all(knots[i] <= knots[i + 1] for i in range(len(knots) - 1))

    def test_curve_has_correct_n_bins(self):
        obs = self._make_linear_observations(3.3, 4.2)
        curve = _build_ocv_curve(obs)
        assert len(curve.soc_knots) == OCV_N_BINS
        assert len(curve.v_per_cell_knots) == OCV_N_BINS

    def test_linear_fit_accuracy(self):
        # A perfectly linear OCV should be recovered closely (< 0.05 V/cell error)
        obs = self._make_linear_observations(3.3, 4.2, n=2000)
        curve = _build_ocv_curve(obs)
        for soc, expected_v in zip([0.0, 0.5, 1.0], [3.3, 3.75, 4.2]):
            got = curve.interpolate(soc)
            assert abs(got - expected_v) < 0.05, (
                f"At soc={soc}: expected {expected_v:.2f}, got {got:.2f}"
            )

    def test_observation_count_stored(self):
        obs = self._make_linear_observations(3.3, 4.2, n=100)
        curve = _build_ocv_curve(obs)
        assert curve.n_total_observations == 100

    def test_too_few_observations_raises(self):
        # Single point cannot build a curve
        with pytest.raises(ValueError, match="Too few populated SoC bins"):
            _build_ocv_curve([(0.5, 3.8)])

    def test_empty_observations_raises(self):
        with pytest.raises(ValueError, match="No OCV observations"):
            _build_ocv_curve([])


# ---------------------------------------------------------------------------
# _predict_terminal_voltage
# ---------------------------------------------------------------------------


class TestPredictTerminalVoltage:
    def _make_simple_curve(self, v_constant: float = 3.8) -> OcvCurve:
        """A flat OCV curve at v_constant for simplicity."""
        soc_knots = [i / (OCV_N_BINS - 1) for i in range(OCV_N_BINS)]
        v_knots = [v_constant] * OCV_N_BINS
        return OcvCurve(
            soc_knots=soc_knots,
            v_per_cell_knots=v_knots,
            n_total_observations=100,
        )

    def test_zero_current_equals_ocv(self):
        curve = self._make_simple_curve(3.8)
        soc = np.array([0.5] * 10)
        i = np.zeros(10)
        v_pred = _predict_terminal_voltage(
            soc, i, cell_count=4, ocv=curve, r_mohm_per_cell=10.0
        )
        # No current → terminal = OCV
        assert np.allclose(v_pred, 3.8 * 4, atol=1e-6)

    def test_known_sag(self):
        # R = 10 mΩ/cell, 4S, current = 20 A → sag = 0.010 * 4 * 20 = 0.8 V
        curve = self._make_simple_curve(3.8)
        soc = np.array([0.5])
        i = np.array([20.0])
        v_pred = _predict_terminal_voltage(
            soc, i, cell_count=4, ocv=curve, r_mohm_per_cell=10.0
        )
        expected = 3.8 * 4 - 20.0 * (4 * 10.0 / 1000.0)
        assert v_pred[0] == pytest.approx(expected, abs=1e-6)

    def test_vectorised_shape_preserved(self):
        curve = self._make_simple_curve(3.8)
        n = 50
        soc = np.linspace(0.2, 0.9, n)
        i = np.linspace(5.0, 40.0, n)
        v_pred = _predict_terminal_voltage(
            soc, i, cell_count=3, ocv=curve, r_mohm_per_cell=9.0
        )
        assert v_pred.shape == (n,)


# ---------------------------------------------------------------------------
# OcvCurve.interpolate edge cases
# ---------------------------------------------------------------------------


class TestOcvCurveInterpolate:
    def test_interpolate_clamps_below_zero(self):
        soc_knots = [0.025, 0.5, 0.975]
        v_knots = [3.3, 3.75, 4.2]
        curve = OcvCurve(
            soc_knots=soc_knots, v_per_cell_knots=v_knots, n_total_observations=10
        )
        # numpy interp extrapolates by clamping to boundary values
        assert curve.interpolate(-0.1) == pytest.approx(3.3, abs=0.01)
        assert curve.interpolate(1.1) == pytest.approx(4.2, abs=0.01)

    def test_interpolate_midpoint(self):
        soc_knots = [0.0, 0.5, 1.0]
        v_knots = [3.3, 3.75, 4.2]
        curve = OcvCurve(
            soc_knots=soc_knots, v_per_cell_knots=v_knots, n_total_observations=10
        )
        assert curve.interpolate(0.25) == pytest.approx(3.525, abs=0.01)
