"""Tests for battery_fit.py using synthetic signals with known R and OCV.

No real logs required.  We construct minimal pyulog-shaped objects and inject
known voltage/current/soc traces, then assert the fit recovers the injected
parameters within a tolerance.

Design: TDD — each test defines expected output first, then calls the
production function and asserts the expectation.
"""

from __future__ import annotations

import json
import os
import uuid
import zlib
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from log_validation import battery_extract
from log_validation.battery_fit import (
    DEFAULT_HOLDOUT_FRAC,
    DEFAULT_SPLIT_SEED,
    OCV_N_BINS,
    LogBatteryData,
    OcvCurve,
    _build_ocv_curve,
    _extract_ocv_observations,
    _extract_r_observations,
    _isotonic_increasing,
    _predict_terminal_voltage,
    analyze_logs,
    extract_dir,
    extract_log,
    fit_extracted,
    fit_obs_dir,
    load_extracted,
    main,
    result_to_dict,
    save_extracted,
    split_logs,
)
from log_validation.tests.fakes import FakeULog, ts_us


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


# ---------------------------------------------------------------------------
# Fake battery logs for the extract and fit steps
# ---------------------------------------------------------------------------

REAL_INFO = {"ver_hw": "PX4_FMU_V5"}
SITL_INFO = {"ver_hw": "PX4_SITL"}
R_4S_MOHM = 9.0
R_3S_MOHM = 11.0
FAKE_RATE_HZ = 5.0
FAKE_SAMPLES = 400

# 16 usable real logs (4S and 3S), 2 simulation logs, and 3 real logs without
# usable battery data (load error, no battery topic, current below 2 A).
CORPUS = (
    [f"a{k:02d}" for k in range(10)]
    + [f"c3-{k:02d}" for k in range(6)]
    + ["sim01", "sim02", "bad01", "nobat01", "flat01"]
)


def _battery(cells: int, r_mohm: float, seed: int) -> dict[str, np.ndarray]:
    """battery_status of a pack with a linear OCV and r_mohm of resistance per cell."""
    rng = np.random.default_rng(seed)
    soc = np.linspace(1.0, 0.3, FAKE_SAMPLES)
    levels = rng.choice([1.0, 12.0, 25.0, 40.0], size=FAKE_SAMPLES // 10 + 1)
    current = np.repeat(levels, 10)[:FAKE_SAMPLES]
    volts = cells * (3.5 + 0.7 * soc) - current * cells * r_mohm / 1000.0
    return {
        "timestamp": ts_us(FAKE_SAMPLES, FAKE_RATE_HZ),
        "voltage_v": volts + rng.normal(0.0, 0.005, FAKE_SAMPLES),
        "current_a": current,
        "cell_count": np.full(FAKE_SAMPLES, cells),
        "remaining": soc,
    }


def load_by_stem(path: Path) -> FakeULog:
    """Top-level so a spawned worker can import it; the stem's prefix picks the kind of log."""
    stem = path.stem
    if stem.startswith("bad"):
        raise OSError("corrupt log")
    if stem.startswith("die"):
        os._exit(1)
    if stem.startswith("nobat"):
        return FakeULog({"vehicle_status": {"timestamp": ts_us(10, 1.0)}}, info=REAL_INFO)
    cells, r_mohm = (3, R_3S_MOHM) if stem.startswith("c3") else (4, R_4S_MOHM)
    data = _battery(cells, r_mohm, zlib.crc32(stem.encode()))
    if stem.startswith("flat"):
        data = {**data, "current_a": np.full(FAKE_SAMPLES, 1.0)}
    info = SITL_INFO if stem.startswith("sim") else REAL_INFO
    return FakeULog({"battery_status": data}, info=info)


def _fake_from(battery: dict[str, np.ndarray]):
    return lambda _path: FakeULog({"battery_status": battery}, info=REAL_INFO)


def _data(log_id: str, cells: int = 4, r_mohm: float = R_4S_MOHM) -> LogBatteryData:
    battery = _battery(cells, r_mohm, zlib.crc32(log_id.encode()))
    status, data = extract_log(Path(f"{log_id}.ulg"), loader=_fake_from(battery))
    assert status == "ok" and data is not None
    return data


def _log_dir(tmp_path: Path, stems) -> Path:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    for s in stems:
        (log_dir / f"{s}.ulg").write_bytes(b"")
    return log_dir


def _manifest(tmp_path: Path, rows: dict[str, str]) -> Path:
    path = tmp_path / "manifest.csv"
    path.write_text("log_id,vehicle_id\n" + "".join(f"{k},{v}\n" for k, v in rows.items()))
    return path


def _names(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir())


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
# Extract step
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_round_trip(self, tmp_path):
        data = _data("a01")
        path = save_extracted(data, tmp_path)
        loaded = load_extracted(path)
        assert path == tmp_path / "a01.npz"
        assert (loaded.log_id, loaded.cell_count) == ("a01", 4)
        for name in ("t_s", "v", "i", "remaining"):
            np.testing.assert_array_equal(getattr(loaded, name), getattr(data, name))
        assert loaded.discharged_mah is None

    def test_round_trip_keeps_discharged_without_remaining(self, tmp_path):
        disch = np.linspace(0.0, 900.0, FAKE_SAMPLES)
        data = replace(_data("a01"), remaining=None, discharged_mah=disch)
        loaded = load_extracted(save_extracted(data, tmp_path))
        assert loaded.remaining is None
        np.testing.assert_array_equal(loaded.discharged_mah, disch)

    @pytest.mark.parametrize(
        "stem, status",
        [("a01", "ok"), ("sim01", "sim"), ("bad01", "unusable"),
         ("nobat01", "unusable"), ("flat01", "unusable")],
    )
    def test_extract_log_status(self, stem, status):
        got, data = extract_log(Path(f"{stem}.ulg"), loader=load_by_stem)
        assert got == status
        assert (data is not None) == (status == "ok")

    def test_cell_count_out_of_range_is_unusable(self):
        battery = {**_battery(4, R_4S_MOHM, 1), "cell_count": np.full(FAKE_SAMPLES, 15)}
        assert extract_log(Path("x.ulg"), loader=_fake_from(battery)) == ("unusable", None)

    def test_default_loader_reads_only_the_battery_topic(self, monkeypatch):
        calls = []

        def fake_ulog(path, message_name_filter_list=None):
            calls.append(message_name_filter_list)
            return load_by_stem(Path(path))

        monkeypatch.setattr(battery_extract, "ULog", fake_ulog)
        status, _ = extract_log(Path("a01.ulg"))
        assert status == "ok" and calls == [["battery_status"]]

    def test_cap_applies_to_extraction(self, tmp_path):
        ids = [f"a{k:02d}" for k in range(8)]
        manifest = _manifest(tmp_path, {i: "v1" for i in ids})
        out = tmp_path / "obs"
        counts = extract_dir(_log_dir(tmp_path, ids), out, manifest, max_per_vehicle=3,
                             loader=load_by_stem)
        assert _names(out) == ["a00.npz", "a01.npz", "a02.npz"]
        assert counts["ok"] == 3 and counts["capped_out"] == 5

    def test_every_selected_log_leaves_one_file(self, tmp_path):
        out = tmp_path / "obs"
        counts = extract_dir(_log_dir(tmp_path, ["a01", "sim01", "bad01", "flat01"]), out,
                             loader=load_by_stem)
        assert _names(out) == ["a01.npz", "bad01.unusable", "flat01.unusable", "sim01.sim"]
        assert (counts["ok"], counts["sim"], counts["unusable"]) == (1, 1, 2)

    def test_jobs_do_not_change_the_extraction(self, tmp_path):
        log_dir = _log_dir(tmp_path, ["a01", "sim01", "c3-01", "bad01"])
        one, two = tmp_path / "one", tmp_path / "two"
        assert extract_dir(log_dir, one, jobs=1, loader=load_by_stem) == extract_dir(
            log_dir, two, jobs=2, loader=load_by_stem
        )
        assert _names(one) == _names(two)
        for p in one.glob("*.npz"):
            np.testing.assert_array_equal(load_extracted(p).v, load_extracted(two / p.name).v)

    def test_dead_worker_does_not_stop_the_extraction(self, tmp_path):
        out = tmp_path / "obs"
        counts = extract_dir(_log_dir(tmp_path, ["a01", "die01", "c3-01"]), out, jobs=2,
                             loader=load_by_stem)
        assert (counts["ok"], counts["worker_died"]) == (2, 1)
        assert _names(out) == ["a01.npz", "c3-01.npz", "die01.worker_died"]

    def test_reextraction_replaces_a_stale_status(self, tmp_path):
        out = tmp_path / "obs"
        out.mkdir()
        (out / "a01.unusable").touch()
        extract_dir(_log_dir(tmp_path, ["a01"]), out, loader=load_by_stem)
        assert _names(out) == ["a01.npz"]


# ---------------------------------------------------------------------------
# Fit step
# ---------------------------------------------------------------------------


class TestFitExtracted:
    def test_spread_row(self):
        r_of = {"a1": 8.0, "a2": 8.5, "b1": 9.0, "c1": 10.0, "d1": 11.0, "e1": 12.0}
        vehicle_of = {"a1": "v1", "a2": "v1", "b1": "v2", "c1": "v3", "d1": "v4",
                      "e1": "v5", "h1": "vh"}
        datas = [_data(i, 4, r) for i, r in r_of.items()] + [_data("h1", 3, R_3S_MOHM)]
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
        result = fit_extracted([_data(f"a{k}") for k in range(6)], {}, 0.2, 0)
        assert result.r_fits[0].n_vehicles == len(result.fit_log_ids) == 5

    def test_fit_from_extracted_equals_one_shot(self, tmp_path):
        log_dir = _log_dir(tmp_path, CORPUS)
        one_shot = analyze_logs(log_dir, loader=load_by_stem)
        extracted = {p.stem: extract_log(p, load_by_stem) for p in sorted(log_dir.glob("*.ulg"))}
        datas = [d for _, d in extracted.values() if d is not None]
        no_data = {i: s for i, (s, d) in extracted.items() if d is None}
        assert result_to_dict(fit_extracted(datas, {}, no_data=no_data)) == result_to_dict(one_shot)

    def test_sim_and_unusable_logs_keep_their_place_in_the_split(self, tmp_path):
        result = analyze_logs(_log_dir(tmp_path, CORPUS), loader=load_by_stem)
        fit, holdout = split_logs(CORPUS, {}, DEFAULT_HOLDOUT_FRAC, DEFAULT_SPLIT_SEED)
        assert result.holdout_log_ids == [i for i in holdout if not i.startswith("sim")]
        assert set(result.fit_log_ids) <= set(fit)
        assert (result.real_log_count, result.sim_log_count, result.skipped_log_count) == (19, 2, 3)

    def test_fit_log_ids_hold_only_contributing_logs(self):
        quiet = {**_battery(4, R_4S_MOHM, 5), "current_a": np.full(FAKE_SAMPLES, 4.0)}
        _, quiet_data = extract_log(Path("q1.ulg"), loader=_fake_from(quiet))
        datas = [_data(f"a{k}") for k in range(6)] + [quiet_data]
        ids = [d.log_id for d in datas]
        seed = next(s for s in range(100) if "q1" in split_logs(ids, {}, 0.2, s)[0])
        result = fit_extracted(datas, {}, 0.2, seed)
        assert "q1" not in result.fit_log_ids
        assert sorted(result.fit_log_ids + result.holdout_log_ids + ["q1"]) == sorted(ids)

    def test_same_inputs_same_tables_and_errors(self):
        datas = [_data(f"a{k}") for k in range(6)]
        m = {d.log_id: f"v{k % 3}" for k, d in enumerate(datas)}
        first = result_to_dict(fit_extracted(datas, m, 0.34, 3))
        assert first == result_to_dict(fit_extracted(datas[::-1], m, 0.34, 3))

    def test_repeated_log_id_is_refused(self):
        with pytest.raises(ValueError, match="more than once"):
            fit_extracted([_data("a1"), _data("a1")], {})


class TestFitObsDir:
    def test_manifest_one_shot_equals_extract_then_fit(self, tmp_path):
        rows = {i: f"v{k % 6}" for k, i in enumerate(CORPUS) if i != "a09"}
        manifest = _manifest(tmp_path, rows)
        log_dir = _log_dir(tmp_path, CORPUS)
        one_shot = analyze_logs(log_dir, manifest_path=manifest, loader=load_by_stem)
        obs = tmp_path / "obs"
        extract_dir(log_dir, obs, loader=load_by_stem)  # every log, no manifest
        assert result_to_dict(fit_obs_dir(obs, manifest)) == result_to_dict(one_shot)
        assert one_shot.logs_without_manifest_row == 1
        assert "a09" not in one_shot.fit_log_ids + one_shot.holdout_log_ids
        fit_vehicles = {rows[i] for i in one_shot.fit_log_ids}
        assert not fit_vehicles & {rows[i] for i in one_shot.holdout_log_ids}

    def test_cap_applies_to_the_fit(self, tmp_path):
        ids = [f"a{k:02d}" for k in range(8)] + ["c3-01", "c3-02"]
        manifest = _manifest(tmp_path, {**{i: "v1" for i in ids[:8]}, "c3-01": "v2", "c3-02": "v3"})
        obs = tmp_path / "obs"
        extract_dir(_log_dir(tmp_path, ids), obs, loader=load_by_stem)  # no cap here
        result = fit_obs_dir(obs, manifest, max_per_vehicle=3)
        counted = result.fit_log_ids + result.holdout_log_ids
        assert sorted(i for i in counted if i.startswith("a")) == ["a00", "a01", "a02"]
        assert result.real_log_count == 5

    def test_log_id_with_two_files_is_refused(self, tmp_path):
        obs = tmp_path / "obs"
        save_extracted(_data("a01"), obs)
        (obs / "a01.sim").touch()
        with pytest.raises(ValueError, match="a01"):
            fit_obs_dir(obs)


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


class TestCli:
    def test_extract_then_fit_matches_log_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            battery_extract, "ULog", lambda path, message_name_filter_list=None: load_by_stem(Path(path))
        )
        log_dir = _log_dir(tmp_path, CORPUS)
        obs, out = tmp_path / "obs", tmp_path / "out"
        assert main(["--log-dir", str(log_dir), "--extract-to", str(obs)]) == 0
        assert len(list(obs.glob("*.npz"))) == 16
        main(["--obs-dir", str(obs), "--out-json", str(out / "obs.json"),
              "--out-md", str(out / "obs.md")])
        main(["--log-dir", str(log_dir), "--out-json", str(out / "dir.json"),
              "--out-md", str(out / "dir.md")])
        text = (out / "dir.json").read_text()
        assert (out / "obs.json").read_text() == text
        assert (out / "obs.md").read_text() == (out / "dir.md").read_text()
        assert str(tmp_path) not in text and str(tmp_path) not in (out / "dir.md").read_text()
        doc = json.loads(text)
        assert {"per_log_r", "fit_log_ids", "holdout_log_ids", "logs_without_manifest_row"} <= set(doc)
        assert {"p10_mohm_per_cell", "p90_mohm_per_cell", "n_vehicles"} <= set(doc["r_fits"][0])
        md = (out / "dir.md").read_text()
        assert "p10" in md and doc["fit_log_ids"][0] in md and doc["holdout_log_ids"][0] in md
