"""Tests for battery_fit.py using synthetic signals with known R and OCV.

No real logs required.  We construct minimal pyulog-shaped objects and inject
known voltage/current/soc traces, then assert the fit recovers the injected
parameters within a tolerance.

Design: TDD — each test defines expected output first, then calls the
production function and asserts the expectation.
"""

from __future__ import annotations

import math
import uuid
import warnings
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from log_validation.battery_fit import (
    DEFAULT_HOLDOUT_FRAC,
    DEFAULT_SPLIT_SEED,
    OCV_N_BINS,
    OcvCurve,
    _build_ocv_curve,
    _extract_ocv_observations,
    _extract_r_observations,
    _isotonic_increasing,
    _predict_terminal_voltage,
    analyze_logs,
    extract_log,
    fit_extracted,
    result_to_dict,
    split_logs,
)
from log_validation.tests.battery_fakes import (
    CORPUS,
    FAKE_SAMPLES,
    R_3S_MOHM,
    R_4S_MOHM,
    battery_status,
    fake_from,
    load_by_stem,
    make_data,
    make_log_dir,
)


# ---------------------------------------------------------------------------
# _isotonic_increasing
# ---------------------------------------------------------------------------


def _ocv_like_values(seed: int, n: int = 50) -> list[float]:
    """Sorted V/cell values plus noise, like OCV bin medians with some violations."""
    rng = np.random.default_rng(seed)
    return (np.sort(rng.uniform(3.3, 4.2, n)) + rng.normal(0.0, 0.03, n)).tolist()


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

    def test_few_ulp_decreasing_block_terminates(self):
        # OCV bin values from a real 24-log sample: a pooled block left a few
        # ULPs apart in decreasing order, on which pairwise averaging never ends.
        vals = [3.7428839320228207, 3.74288393202282, 3.7428839320228198,
                3.742883932022819, 3.7428839320228184, 3.742883932022818,
                3.742883932022818, 3.7758580446243286, 3.79372501373291]
        assert not _previous_isotonic(vals, max_sweeps=10_000)[1]
        result = _isotonic_increasing(vals)
        assert all(a <= b for a, b in zip(result, result[1:]))
        assert result[0] == pytest.approx(3.742883932022819, abs=1e-12)

    @pytest.mark.parametrize("vals", [
        [3.0, 2.0, 1.0],
        [0.0, 4.0, 1.0, 1.0],
        [2.0, 1.0, 1.4],
        _ocv_like_values(seed=4),  # 17 violations; seed 4 is one the old loop finishes
    ])
    def test_equals_the_previous_algorithm_where_it_converges(self, vals):
        expected, converged = _previous_isotonic(vals, max_sweeps=100_000)
        assert converged
        assert _isotonic_increasing(vals) == pytest.approx(expected, abs=1e-9)


def _previous_isotonic(values: list[float], max_sweeps: int) -> tuple[list[float], bool]:
    """The pre-change pairwise-averaging loop, copied, with a sweep cap: (result, converged)."""
    out = list(values)
    n = len(out)
    changed = True
    sweeps = 0
    while changed and sweeps < max_sweeps:
        changed = False
        sweeps += 1
        i = 0
        while i < n - 1:
            if out[i] > out[i + 1]:
                avg = (out[i] + out[i + 1]) / 2.0
                out[i] = avg
                out[i + 1] = avg
                changed = True
            i += 1
    return out, not changed


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

    def test_non_finite_current_is_skipped_without_a_warning(self):
        # Real logs carry +inf and NaN current samples; the pairs touching them
        # are steps (9,10), (11,12) and (19,20) of this trace.
        v, i, t = self._make_step_trace(4, 9.0, n_steps=30, step_di=20.0)
        clean = _extract_r_observations(v, i, t, 4)
        bad_i = i.copy()
        bad_i[[10, 11]] = np.inf
        bad_i[20] = np.nan
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            obs = _extract_r_observations(v, bad_i, t, 4)
        assert obs == pytest.approx([9.0] * (len(clean) - 3))


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


def _previous_split(ids, holdout_frac: float, seed: int) -> tuple[list[str], list[str]]:
    """The split of the pre-change analyze_logs, copied as it was (over file paths)."""
    paths = sorted(Path("logs") / f"{i}.ulg" for i in ids)
    n_holdout = max(1, int(len(paths) * holdout_frac))
    rng = np.random.default_rng(seed)
    order = rng.permutation(len(paths))
    holdout_idx = set(order[:n_holdout].tolist())
    fit_paths = [paths[j] for j in range(len(paths)) if j not in holdout_idx]
    holdout_paths = [paths[j] for j in range(len(paths)) if j in holdout_idx]
    return [p.stem for p in fit_paths], [p.stem for p in holdout_paths]


# ---------------------------------------------------------------------------
# split_logs
# ---------------------------------------------------------------------------


class TestSplitLogs:
    def test_one_vehicle_one_side(self):
        ids = [f"log{k:02d}" for k in range(20)]
        vehicle_of = {i: "v1" if k < 4 else f"v{2 + k % 7}" for k, i in enumerate(ids)}
        v1_logs = {i for i in ids if vehicle_of[i] == "v1"}
        assert len(set(vehicle_of.values())) == 8
        for seed in range(21):
            fit, holdout = split_logs(ids, vehicle_of, 0.2, seed)
            assert sorted(fit + holdout) == ids
            assert v1_logs <= set(fit) or v1_logs <= set(holdout)
            assert not {vehicle_of[i] for i in fit} & {vehicle_of[i] for i in holdout}

    def test_same_inputs_same_split(self):
        ids = [f"log{k:02d}" for k in range(20)]
        m = {i: f"v{k % 8}" for k, i in enumerate(ids)}
        assert split_logs(ids, m, 0.2, 7) == split_logs(ids, m, 0.2, 7)
        assert split_logs(ids, m, 0.2, 7) == split_logs(ids[::-1], m, 0.2, 7)

    @pytest.mark.parametrize("n", [1, 5, 37, 1000])
    @pytest.mark.parametrize("seed", [0, 7, DEFAULT_SPLIT_SEED])
    def test_split_without_manifest_matches_the_previous_algorithm(self, n, seed):
        ids = [str(uuid.UUID(int=(k * 2654435761) % 2**128)) for k in range(n)]
        expected = _previous_split(ids, DEFAULT_HOLDOUT_FRAC, seed)
        assert split_logs(ids[::-1], {}, DEFAULT_HOLDOUT_FRAC, seed) == expected

    def test_log_without_a_vehicle_is_refused(self):
        with pytest.raises(ValueError, match="no vehicle"):
            split_logs(["a", "b"], {"a": "v1"}, 0.2, 0)

    def test_holdout_fraction_outside_zero_one_is_refused(self):
        with pytest.raises(ValueError, match="holdout_frac"):
            split_logs(["a", "b"], {}, 1.0, 0)


# ---------------------------------------------------------------------------
# Fit step
# ---------------------------------------------------------------------------


class TestFitExtracted:
    def test_spread_row(self):
        r_of = {"a1": 8.0, "a2": 8.5, "b1": 9.0, "c1": 10.0, "d1": 11.0, "e1": 12.0}
        vehicle_of = {"a1": "v1", "a2": "v1", "b1": "v2", "c1": "v3", "d1": "v4",
                      "e1": "v5", "h1": "vh"}
        datas = [make_data(i, 4, r) for i, r in r_of.items()] + [make_data("h1", 3, R_3S_MOHM)]
        seed = next(s for s in range(100)
                    if split_logs(list(vehicle_of), vehicle_of, 0.2, s)[1] == ["h1"])
        result = fit_extracted(datas, vehicle_of, 0.2, seed)
        row = next(f for f in result.r_fits if f.cell_count == 4)
        assert row.n_vehicles == 5 and row.n_observations > 0
        assert row.p10_mohm_per_cell <= row.median_mohm_per_cell <= row.p90_mohm_per_cell
        assert row.p10_mohm_per_cell < row.p90_mohm_per_cell
        assert [r["log_id"] for r in result.per_log_r] == sorted(r_of)
        assert {r["vehicle_id"] for r in result.per_log_r} == {"v1", "v2", "v3", "v4", "v5"}
        assert all(r["cell_count"] == 4 for r in result.per_log_r)

    def test_log_without_a_vehicle_counts_as_its_own(self):
        result = fit_extracted([make_data(f"a{k}") for k in range(6)], {}, 0.2, 0)
        assert result.r_fits[0].n_vehicles == len(result.fit_log_ids) == 5

    def test_fit_from_extracted_equals_one_shot(self, tmp_path):
        log_dir = make_log_dir(tmp_path, CORPUS)
        one_shot = analyze_logs(log_dir, loader=load_by_stem)
        extracted = {p.stem: extract_log(p, load_by_stem) for p in sorted(log_dir.glob("*.ulg"))}
        datas = [d for _, d in extracted.values() if d is not None]
        no_data = {i: s for i, (s, d) in extracted.items() if d is None}
        assert result_to_dict(fit_extracted(datas, {}, no_data=no_data)) == result_to_dict(one_shot)

    def test_sim_and_unusable_logs_keep_their_place_in_the_split(self, tmp_path):
        result = analyze_logs(make_log_dir(tmp_path, CORPUS), loader=load_by_stem)
        fit, holdout = split_logs(CORPUS, {}, DEFAULT_HOLDOUT_FRAC, DEFAULT_SPLIT_SEED)
        assert result.holdout_log_ids == [i for i in holdout if not i.startswith(("sim", "bad"))]
        assert set(result.fit_log_ids) <= set(fit)
        assert (result.real_log_count, result.sim_log_count, result.skipped_log_count) == (18, 2, 3)

    def test_load_errors_and_dead_workers_are_skipped_but_not_real(self):
        datas = [make_data(f"a{k}") for k in range(8)]
        no_data = {"e1": "load_error", "w1": "worker_died", "u1": "unusable", "s1": "sim"}
        ids = [d.log_id for d in datas] + list(no_data)
        seed = next(s for s in range(100)
                    if {"e1", "w1", "u1"} <= set(split_logs(ids, {}, 0.34, s)[1]))
        result = fit_extracted(datas, {}, 0.34, seed, no_data=no_data)
        assert (result.real_log_count, result.sim_log_count, result.skipped_log_count) == (9, 1, 3)
        assert "u1" in result.holdout_log_ids
        assert not {"e1", "w1", "s1"} & set(result.holdout_log_ids)
        assert result.holdout_log_count == len(result.holdout_log_ids)

    def test_fit_log_ids_hold_only_contributing_logs(self):
        quiet = {**battery_status(4, R_4S_MOHM, 5), "current_a": np.full(FAKE_SAMPLES, 4.0)}
        _, quiet_data = extract_log(Path("q1.ulg"), loader=fake_from(quiet))
        datas = [make_data(f"a{k}") for k in range(6)] + [quiet_data]
        ids = [d.log_id for d in datas]
        seed = next(s for s in range(100) if "q1" in split_logs(ids, {}, 0.2, s)[0])
        result = fit_extracted(datas, {}, 0.2, seed)
        assert "q1" not in result.fit_log_ids
        assert sorted(result.fit_log_ids + result.holdout_log_ids + ["q1"]) == sorted(ids)

    def test_same_inputs_same_tables_and_errors(self):
        datas = [make_data(f"a{k}") for k in range(6)]
        m = {d.log_id: f"v{k % 3}" for k, d in enumerate(datas)}
        first = result_to_dict(fit_extracted(datas, m, 0.34, 3))
        assert first == result_to_dict(fit_extracted(datas[::-1], m, 0.34, 3))

    def test_repeated_log_id_is_refused(self):
        with pytest.raises(ValueError, match="more than once"):
            fit_extracted([make_data("a1"), make_data("a1")], {})

    @pytest.mark.parametrize("status", ["ok", "no_manifest_row", "capped"])
    def test_only_the_named_no_data_statuses_are_accepted(self, status):
        from log_validation.battery_extract import NO_DATA_STATUSES

        assert NO_DATA_STATUSES == {"sim", "unusable", "load_error", "worker_died"}
        with pytest.raises(ValueError, match="unknown extract status"):
            fit_extracted([make_data(f"a{k}") for k in range(6)], {}, no_data={"x": status})

    @staticmethod
    def _with_holdout_voltage(datas, log_id, change):
        """Copies of datas with one log's voltage changed by `change` (the inputs stay as they are)."""
        out = []
        for d in datas:
            if d.log_id == log_id:
                v = d.v.copy()
                change(v)
                d = replace(d, v=v)
            out.append(d)
        return out

    def test_one_nan_voltage_in_a_holdout_log_gives_a_finite_mae(self):
        datas = [make_data(f"a{k}") for k in range(10)]
        clean = fit_extracted(datas, {})
        target = clean.holdout_log_ids[0]

        def one_nan(v):
            v[5] = np.nan

        result = fit_extracted(self._with_holdout_voltage(datas, target, one_nan), {})
        assert math.isfinite(result.holdout_median_mae_v_per_cell)
        assert math.isfinite(result.holdout_p75_mae_v_per_cell)
        assert result.holdout_scored_count == clean.holdout_scored_count

    def test_holdout_log_without_a_finite_voltage_is_not_scored(self):
        datas = [make_data(f"a{k}") for k in range(10)]
        clean = fit_extracted(datas, {})
        target = clean.holdout_log_ids[0]

        def all_nan(v):
            v[:] = np.nan

        result = fit_extracted(self._with_holdout_voltage(datas, target, all_nan), {})
        assert result.holdout_scored_count == clean.holdout_scored_count - 1
        assert math.isfinite(result.holdout_median_mae_v_per_cell)
