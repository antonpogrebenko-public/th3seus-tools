"""Tests for the response pass: effectiveness and lag per axis, refusals, batch output."""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from log_validation.response import (
    AXES,
    FIT_DELAYS_S,
    FIT_TAUS_S,
    INSTRUMENT_LAGS_S,
    ResponseGates,
    analyze_response,
    instrument_matrix,
    main,
    response_batch,
)
from log_validation.sysid import (
    butterworth2_group_delay_s,
    first_order_group_delay_s,
    fit_first_order_plant,
)
from log_validation.tests.response_fakes import (
    AIRFRAME,
    DELAY_S,
    DGYRO_CUTOFF_HZ,
    GAIN,
    GYRO_CUTOFF_HZ,
    TAU_S,
    LoopSpec,
    QuadSpec,
    quad_datasets,
    quad_log,
    simulate_rate_loop,
)

GATES = ResponseGates()
CLOSED_LOOP_SEED = 21
FAST = QuadSpec(duration_s=20.0, takeoff_s=3.0, landing_s=18.0, rate_hz=400.0)


def _analyze(spec: QuadSpec = QuadSpec(), datasets: dict | None = None):
    return analyze_response(quad_log(spec, datasets), "log1", "veh1", GATES)


def test_known_plant_in_a_log():
    result = _analyze()
    m = result.measurements
    filter_delay = (butterworth2_group_delay_s(GYRO_CUTOFF_HZ)
                    + first_order_group_delay_s(DGYRO_CUTOFF_HZ))
    assert m["filter_delay_s"] == pytest.approx(filter_delay)
    assert m["angular_accel_from_rate"] == 0.0
    for axis in AXES:
        assert abs(m[f"effectiveness.{axis}"] - GAIN) / GAIN < 0.10
        assert abs(m[f"lag_s.{axis}"] - 0.030) < 0.005
        assert m[f"lag_minus_filter_s.{axis}"] == pytest.approx(m[f"lag_s.{axis}"] - filter_delay)
        assert 0.5 < m[f"fit_r2.{axis}"] <= m[f"reduced_form_r2.{axis}"] <= 1.0
    assert result.errors == []


def test_used_fraction_is_the_trimmed_share_of_in_air_time():
    m = _analyze().measurements
    # 30 s in air, 2 s trimmed at each end: 26 / 30.
    for axis in AXES:
        assert m[f"used_fraction.{axis}"] == pytest.approx(26 / 30, abs=0.01)


def test_used_fraction_counts_only_the_samples_the_fit_scores():
    # One missing roll sample every 0.5 s cuts 11 s into 22 runs; the fit skips the
    # first 17 samples (42.5 ms) of each run, which used_fraction must not count.
    datasets = quad_datasets(FAST)
    rate = dict(datasets["vehicle_angular_velocity"])
    accel = rate["xyz_derivative[0]"].copy()
    accel[::200] = np.nan
    rate["xyz_derivative[0]"] = accel
    datasets["vehicle_angular_velocity"] = rate
    m = _analyze(FAST, datasets).measurements
    assert m["used_fraction.roll"] == pytest.approx((11 - 22 * 0.045) / 15, abs=0.01)
    assert m["used_fraction.pitch"] == pytest.approx((11 - 0.0425) / 15, abs=0.005)


def test_low_excitation_axis():
    result = _analyze(QuadSpec(constant_axes=(0,)))
    m = result.measurements
    assert "effectiveness.roll" not in m and "lag_s.roll" not in m
    assert "lag_minus_filter_s.roll" not in m
    errors = [e for e in result.errors if "excitation" in e]
    assert len(errors) == 1 and "roll" in errors[0] and "0.02" in errors[0]
    assert "effectiveness.pitch" in m and "effectiveness.yaw" in m


def test_torque_std_is_reported_on_both_sides_of_the_excitation_gate():
    # Task 13 sets the excitation gate from where the gain stops depending on this value,
    # so a gated axis must report it too.
    m = _analyze(QuadSpec(constant_axes=(0,))).measurements
    assert 0.0 <= m["torque_std.roll"] < GATES.min_torque_std
    assert m["torque_std.pitch"] >= GATES.min_torque_std
    assert m["torque_std.yaw"] >= GATES.min_torque_std


@pytest.mark.parametrize("seed", range(1, 13))
def test_low_fit_quality_axis_gets_no_estimate(seed):
    result = _analyze(QuadSpec(noise_only=True, seed=seed))
    assert not any(k.startswith("effectiveness.") for k in result.measurements)
    for axis in AXES:
        assert any(axis in e and "fit quality" in e for e in result.errors)


def _no_estimate(measurements: dict) -> bool:
    return not any(k.startswith(("effectiveness.", "lag_s.", "lag_minus_filter_s."))
                   for k in measurements)


def test_negative_gain_is_an_implausible_gain():
    result = _analyze(replace(FAST, gain=-GAIN))
    assert _no_estimate(result.measurements)
    for axis in AXES:
        assert result.measurements[f"fit_r2.{axis}"] > 0.5  # a good fit, refused for its sign
        assert any(e.startswith(f"{axis}: implausible gain (-") for e in result.errors)


def test_lag_below_the_filter_delay_gets_no_estimate():
    # No plant lag at all: the fitted lag (about 0) is below the 10.9 ms filter delay.
    result = _analyze(replace(FAST, delay_s=0.0, tau_s=0.0))
    assert _no_estimate(result.measurements)
    for axis in AXES:
        assert any(e.startswith(f"{axis}: lag below filter delay") and "10.9 ms" in e
                   for e in result.errors)


def test_fixed_wing_log():
    result = _analyze(QuadSpec(params={"MAV_TYPE": 1}))
    assert result.measurements == {}
    assert any("MAV_TYPE=1" in e for e in result.errors)


def test_rotor_count_decides_when_mav_type_is_absent():
    quad = _analyze(replace(FAST, params={"MAV_TYPE": None, "CA_ROTOR_COUNT": 4}))
    hexa = _analyze(replace(FAST, params={"MAV_TYPE": None, "CA_ROTOR_COUNT": 6}))
    assert "effectiveness.roll" in quad.measurements
    assert hexa.measurements == {} and any("not a quadrotor" in e for e in hexa.errors)


def test_sitl_log():
    result = _analyze(QuadSpec(board="PX4_SITL"))
    assert result.measurements == {}
    assert any("simulation" in e for e in result.errors)


def test_log_without_torque_setpoint():
    result = _analyze(replace(FAST, with_torque=False))
    assert result.measurements == {}
    assert result.errors == ["no vehicle_torque_setpoint (firmware before control allocation)"]


def test_missing_derivative_falls_back_to_the_rate_derivative():
    m = _analyze(QuadSpec(with_derivative=False)).measurements
    assert m["angular_accel_from_rate"] == 1.0
    assert m["filter_delay_s"] == pytest.approx(butterworth2_group_delay_s(GYRO_CUTOFF_HZ))
    for axis in AXES:
        assert abs(m[f"effectiveness.{axis}"] - GAIN) / GAIN < 0.10
        assert abs(m[f"lag_s.{axis}"] - 0.030) < 0.005


def test_missing_cutoff_parameter_gives_no_lag():
    result = _analyze(replace(FAST, params={"IMU_GYRO_CUTOFF": None}))
    assert not any(k.startswith(("lag_s.", "effectiveness.")) for k in result.measurements)
    assert any("IMU_GYRO_CUTOFF" in e for e in result.errors)


def test_repeated_and_backward_timestamps():
    datasets = quad_datasets(FAST)
    for topic, stamp in (("vehicle_torque_setpoint", "timestamp"),
                         ("vehicle_angular_velocity", "timestamp_sample"),
                         ("vehicle_rates_setpoint", "timestamp")):
        t = datasets[topic][stamp].copy()
        t[1000] = t[999]  # repeated
        t[2000] = t[1990]  # backward
        t[3000:3003] = t[2990]  # a run of backward stamps
        datasets[topic] = {**datasets[topic], stamp: t}
    result = _analyze(FAST, datasets)
    assert "effectiveness.roll" in result.measurements
    assert abs(result.measurements["effectiveness.roll"] - GAIN) / GAIN < 0.10


def test_setpoint_logged_below_the_rate_loop_rate_gets_no_estimate():
    datasets = quad_datasets(FAST)
    torque = datasets["vehicle_torque_setpoint"]
    datasets["vehicle_torque_setpoint"] = {k: v[::8] for k, v in torque.items()}  # 50 Hz
    result = _analyze(FAST, datasets)
    assert _only_log_level(result.measurements)
    assert result.errors == ["not logged at the rate controller's rate (vehicle_torque_setpoint "
                             "at 50 Hz, vehicle_angular_velocity at 400 Hz, "
                             "vehicle_rates_setpoint at 400 Hz; need 190 Hz)"]


def test_rate_setpoint_logged_slightly_below_200_hz_is_accepted():
    # PX4 publishes the rate setpoint at about 200 Hz; logged medians of 198 Hz are common.
    result = _analyze(replace(FAST, rates_sp_hz=192.0))
    assert "effectiveness.roll" in result.measurements, result.errors


def test_rate_setpoint_logged_too_slowly_gets_no_fit():
    result = _analyze(replace(FAST, rates_sp_hz=150.0))
    assert _only_log_level(result.measurements)
    assert result.errors == ["not logged at the rate controller's rate (vehicle_torque_setpoint "
                             "at 400 Hz, vehicle_angular_velocity at 400 Hz, "
                             "vehicle_rates_setpoint at 150 Hz; need 190 Hz)"]


def _only_log_level(measurements: dict) -> bool:
    """No per-axis fit value, but the log-level ones a blocked fit still reports."""
    fit_keys = ("effectiveness.", "lag_s.", "fit_r2.", "first_stage_r2.", "reduced_form_r2.",
                "lag_minus_filter_s.", "used_fraction.", "torque_std.")
    return (not any(k.startswith(fit_keys) for k in measurements)
            and {"filter_delay_s", "angular_accel_from_rate"} <= set(measurements))


def test_log_without_rate_setpoint():
    # Only the effectiveness fit needs the rate setpoint; the rest is still reported,
    # in the same shape as a rate setpoint logged too slowly.
    result = _analyze(replace(FAST, instrument="none", params={"MC_BAT_SCALE_EN": 1}))
    assert _only_log_level(result.measurements) and result.measurements["bat_scale_en"] == 1.0
    assert result.errors == ["no vehicle_rates_setpoint (the instrument of the closed-loop fit)"]
    slow = _analyze(replace(FAST, rates_sp_hz=150.0, params={"MC_BAT_SCALE_EN": 1}))
    assert set(slow.measurements) == set(result.measurements)


def test_unrelated_rate_setpoint_is_a_weak_instrument():
    result = _analyze(replace(FAST, instrument="unrelated"))
    assert _no_estimate(result.measurements)
    for axis in AXES:
        assert result.measurements[f"first_stage_r2.{axis}"] < GATES.min_first_stage_r2
        assert any(e.startswith(f"{axis}: weak instrument (first-stage R^2 ")
                   for e in result.errors)


def test_closed_loop_two_stage_fit_beats_least_squares():
    run = simulate_rate_loop(LoopSpec(seconds=120.0, seed=CLOSED_LOOP_SEED))
    t = np.arange(len(run.torque)) * run.dt_s
    keep = t >= max(INSTRUMENT_LAGS_S)
    z = instrument_matrix(t, run.rate_sp, t[keep])
    u, y = run.torque[keep], run.accel[keep]
    tsls = fit_first_order_plant(u, y, run.dt_s, FIT_DELAYS_S, FIT_TAUS_S, instruments=z)
    ls = fit_first_order_plant(u, y, run.dt_s, FIT_DELAYS_S, FIT_TAUS_S)
    # u to y: the plant's 30 ms plus the derivative filter and half a sample.
    path_lag = DELAY_S + TAU_S + LoopSpec().dgyro_tau_s + run.dt_s / 2
    assert abs(tsls.gain - GAIN) / GAIN < 0.15 and abs(tsls.lag_s - path_lag) < 0.005
    assert abs(ls.gain - GAIN) > abs(tsls.gain - GAIN)


@pytest.mark.parametrize("value, expected", [(1, 1.0), (0, 0.0)])
def test_battery_scaling_flag_is_reported(value, expected):
    result = _analyze(replace(FAST, params={"MC_BAT_SCALE_EN": value}))
    assert result.measurements["bat_scale_en"] == expected


def test_battery_scaling_flag_is_absent_without_the_parameter():
    assert "bat_scale_en" not in _analyze(FAST).measurements


def test_setpoint_not_logged_in_the_air_gets_no_estimate():
    datasets = quad_datasets(FAST)
    torque = datasets["vehicle_torque_setpoint"]
    before_takeoff = torque["timestamp"] < 2e6
    datasets["vehicle_torque_setpoint"] = {k: v[before_takeoff] for k, v in torque.items()}
    result = _analyze(FAST, datasets)
    assert _only_log_level(result.measurements)
    assert result.errors == ["no vehicle_torque_setpoint samples in the used in-air time"]


@pytest.mark.parametrize("topic, stamp", [
    ("vehicle_torque_setpoint", "timestamp"),
    ("vehicle_angular_velocity", "timestamp_sample"),
    ("vehicle_rates_setpoint", "timestamp"),
])
def test_in_air_time_without_samples_is_not_used(topic, stamp):
    # A logging gap: 3 s of one topic's samples are missing, not set to NaN.
    datasets = quad_datasets(FAST)
    data = datasets[topic]
    keep = ~((data[stamp] >= 6e6) & (data[stamp] < 9e6))
    datasets[topic] = {k: v[keep] for k, v in data.items()}
    m = _analyze(FAST, datasets).measurements
    # 15 s in air, 11 s after trimming, 3 s of it without data.
    for axis in AXES:
        assert m[f"used_fraction.{axis}"] == pytest.approx(8 / 15, abs=0.015)


def test_non_finite_samples_are_not_used():
    datasets = quad_datasets(FAST)
    rate = dict(datasets["vehicle_angular_velocity"])
    accel = rate["xyz_derivative[0]"].copy()
    accel[(rate["timestamp_sample"] >= 6e6) & (rate["timestamp_sample"] < 9e6)] = np.nan
    rate["xyz_derivative[0]"] = accel
    datasets["vehicle_angular_velocity"] = rate
    m = _analyze(FAST, datasets).measurements
    assert m["used_fraction.roll"] == pytest.approx(8 / 15, abs=0.01)
    assert m["used_fraction.pitch"] == pytest.approx(11 / 15, abs=0.01)


def test_partial_derivative_fields_fall_back_to_the_rate_derivative():
    datasets = quad_datasets(FAST)
    rate = {k: v for k, v in datasets["vehicle_angular_velocity"].items()
            if k not in ("xyz_derivative[1]", "xyz_derivative[2]")}
    datasets["vehicle_angular_velocity"] = rate
    result = _analyze(FAST, datasets)
    assert result.measurements["angular_accel_from_rate"] == 1.0
    assert "effectiveness.roll" in result.measurements, result.errors


def test_analysis_failure_reports_the_type_only(capsys):
    real = quad_log(FAST)

    class Broken:
        initial_parameters = real.initial_parameters
        msg_info_dict = real.msg_info_dict
        data_list = real.data_list

        def get_dataset(self, name, multi_instance=0):
            if name == "vehicle_angular_velocity":
                raise ValueError("cannot read /home/someone/flight.ulg")
            return real.get_dataset(name, multi_instance)

    result = analyze_response(Broken(), "log9", None, GATES)
    assert result.errors[0] == "effectiveness failed: ValueError"
    assert all(e.endswith("failed: ValueError") for e in result.errors)
    err = capsys.readouterr().err
    assert "log9" in err and "/home/someone/flight.ulg" in err


def test_a_failing_header_read_is_one_error_entry(monkeypatch):
    import log_validation.response as response

    def broken(_ulog):
        raise RuntimeError("header")

    monkeypatch.setattr(response, "_read_header", broken)
    result = analyze_response(quad_log(FAST), "log9", "veh9", GATES)
    assert result.errors == ["analysis failed: RuntimeError"] and result.measurements == {}
    assert result.log_id == "log9" and result.vehicle_id == "veh9"


def test_analysis_does_not_change_the_log():
    datasets = quad_datasets(FAST)
    before = {t: {k: v.copy() for k, v in d.items()} for t, d in datasets.items()}
    _analyze(FAST, datasets)
    for topic, fields in before.items():
        for name, values in fields.items():
            np.testing.assert_array_equal(datasets[topic][name], values)


# ---------------------------------------------------------------------------
# Batch output
# ---------------------------------------------------------------------------


def load_by_stem(path: Path):
    """Top-level so a spawned worker can import it."""
    if path.stem == "bad":
        raise OSError(f"cannot parse {path}")
    if path.stem == "die":
        os._exit(1)
    return quad_log(replace(FAST, seed=sum(map(ord, path.stem))))


def _touch(tmp_path: Path, stems) -> Path:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    for s in stems:
        (log_dir / f"{s}.ulg").write_bytes(b"")
    return log_dir


def _read(out: Path) -> list[dict]:
    return json.loads(out.read_text())


def test_one_entry_per_log(tmp_path):
    ids = ["101", "102", "103", "104", "105"]
    log_dir = _touch(tmp_path, ids)
    manifest = tmp_path / "m.csv"
    manifest.write_text("log_id,vehicle_id\n" + "".join(f"{i},v{i}\n" for i in ids))
    out = tmp_path / "out" / "response.json"
    response_batch(log_dir, out, manifest_path=manifest, loader=load_by_stem)
    entries = _read(out)
    assert [e["log_id"] for e in entries] == ids
    assert [e["vehicle_id"] for e in entries] == [f"v{i}" for i in ids]
    assert all(e["airframe_id"] == AIRFRAME for e in entries)
    assert all("effectiveness.roll" in e["measurements"] for e in entries)
    assert all({"hardware", "errors"} <= set(e) for e in entries)


def test_cap_keeps_the_smallest_log_ids_of_a_vehicle(tmp_path):
    ids = ["10", "11", "12", "13"]
    log_dir = _touch(tmp_path, ids)
    manifest = tmp_path / "m.csv"
    manifest.write_text("log_id,vehicle_id\n" + "".join(f"{i},v1\n" for i in ids))
    results = response_batch(log_dir, None, manifest_path=manifest, loader=load_by_stem)
    assert [r.log_id for r in results] == ["10", "11", "12"]


def test_selection_counts_go_to_stderr(tmp_path, capsys):
    log_dir = _touch(tmp_path, ["10", "11", "u1"])
    manifest = tmp_path / "m.csv"
    manifest.write_text("log_id,vehicle_id\n10,v1\n11,v1\ngone,v2\n")
    results = response_batch(log_dir, None, manifest_path=manifest, max_per_vehicle=1,
                             loader=load_by_stem)
    err = capsys.readouterr().err
    assert [r.log_id for r in results] == ["10"]
    assert "manifest_rows_without_log: 1" in err
    assert "logs_without_manifest_row: 1" in err and "capped_out: 1" in err


def test_bad_and_dying_logs_do_not_stop_the_run(tmp_path):
    log_dir = _touch(tmp_path, ["a1", "bad", "die"])
    out = tmp_path / "r.json"
    response_batch(log_dir, out, jobs=2, loader=load_by_stem)
    by_id = {e["log_id"]: e for e in _read(out)}
    assert by_id["die"]["errors"] == ["worker process died"]
    assert by_id["bad"]["measurements"] == {} and "load failed" in by_id["bad"]["errors"][0]
    assert str(log_dir) not in out.read_text()
    assert "effectiveness.roll" in by_id["a1"]["measurements"]


def test_jobs_do_not_change_the_output(tmp_path):
    log_dir = _touch(tmp_path, ["c3", "a1", "b2"])
    one, two = tmp_path / "one.json", tmp_path / "two.json"
    response_batch(log_dir, one, jobs=1, loader=load_by_stem)
    response_batch(log_dir, two, jobs=2, loader=load_by_stem)
    assert one.read_bytes() == two.read_bytes()
    assert [e["log_id"] for e in _read(one)] == ["a1", "b2", "c3"]


def test_gates_reach_the_analysis(tmp_path):
    log_dir = _touch(tmp_path, ["a1"])
    strict = ResponseGates(min_torque_std=1.0)
    results = response_batch(log_dir, None, gates=strict, loader=load_by_stem)
    assert not any(k.startswith("effectiveness.") for k in results[0].measurements)


def test_cli_has_one_option_per_gate(tmp_path, capsys):
    log_dir = _touch(tmp_path, [])
    out = tmp_path / "r.json"
    code = main([str(log_dir), "-o", str(out), "--min-torque-std", "0.05", "--min-r2", "0.6",
                 "--noise-cutoff-hz", "15", "--min-motor-cmd-std", "0.03",
                 "--min-first-stage-r2", "0.1"])
    assert code == 0 and _read(out) == []


def test_cli_refuses_a_bad_manifest(tmp_path, capsys):
    log_dir = _touch(tmp_path, ["a1"])
    manifest = tmp_path / "m.csv"
    manifest.write_text("log_id\n1\n")
    code = main([str(log_dir), "-o", str(tmp_path / "r.json"), "--manifest", str(manifest)])
    assert code == 2 and capsys.readouterr().err.startswith("Error: ")


def test_noise_cutoff_help_describes_the_moving_average(capsys):
    with pytest.raises(SystemExit):
        main(["--help"])
    text = " ".join(capsys.readouterr().out.split())
    assert "moving-average window of 1/f seconds" in text
    assert "first null at f" in text and "-3 dB near 0.44 f" in text


def test_filtered_load_keeps_the_angular_acceleration(monkeypatch):
    """flight_segments classifies windows with vehicle_angular_acceleration, so the
    filtered load must keep it or a flight is classified differently from a full load."""
    from log_validation import response

    calls = []
    monkeypatch.setattr(response, "ULog",
                        lambda path, message_name_filter_list=None: calls.append(
                            message_name_filter_list))
    response.load_response_topics(Path("x.ulg"))
    assert "vehicle_angular_acceleration" in calls[0]
