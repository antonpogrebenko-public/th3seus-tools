"""Tests for the response pass's per-log measurements: hover command, motor lag,
in-flight gyro noise, loop rates, parameters and cell count."""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from log_validation.response import MAX_GAP_INTERVALS, ResponseGates, analyze_response
from log_validation.flight_segments import FlightSegment, SegmentKind
from log_validation.response_measures import (
    MIN_ESC_RPM_RATE_HZ,
    TRIM_S,
    hover_group,
    hover_windows,
    inflight_noise_measurements,
)
from log_validation.sysid import hover_command
from log_validation.tests.response_fakes import (
    HOVER_ESTIMATE,
    HOVER_THRUST,
    MOTOR_LAG_S,
    MPC_THR_HOVER,
    THR_MDL_FAC,
    QuadSpec,
    quad_datasets,
    quad_log,
    quad_params,
)

GATES = ResponseGates()
FAST = QuadSpec(duration_s=20.0, takeoff_s=3.0, landing_s=18.0, rate_hz=400.0)


def _analyze(spec: QuadSpec = FAST, datasets: dict | None = None, gates: ResponseGates = GATES):
    return analyze_response(quad_log(spec, datasets), "log1", "veh1", gates)


def _errors_with(result, text: str) -> list[str]:
    return [e for e in result.errors if text in e]


def _without_hover(datasets: dict) -> dict:
    """Local position of a climb and then a translation: no hover segment."""
    out = dict(datasets)
    pos = dict(datasets["vehicle_local_position"])
    t = pos["timestamp"] * 1e-6
    pos["vz"] = np.where(t < 10.0, -2.0, 0.0)
    pos["vx"] = np.where(t < 10.0, 0.0, 4.0)
    out["vehicle_local_position"] = pos
    return out


# ---------------------------------------------------------------------------
# Hover
# ---------------------------------------------------------------------------


def test_hover_keys():
    m = _analyze().measurements
    assert m["hover.thrust"] == pytest.approx(HOVER_THRUST)
    assert m["hover.command"] == pytest.approx(hover_command(HOVER_THRUST, THR_MDL_FAC))
    assert m["hover.thr_mdl_fac"] == THR_MDL_FAC
    assert m["hover.mpc_thr_hover"] == MPC_THR_HOVER
    assert m["hover.estimate"] == pytest.approx(HOVER_ESTIMATE)


@pytest.mark.parametrize("value", [None, 1.5, -0.2])
def test_thr_mdl_fac_missing_or_out_of_range(value):
    m_result = _analyze(replace(FAST, params={"THR_MDL_FAC": value}))
    assert "hover.command" not in m_result.measurements
    assert m_result.measurements["hover.thrust"] == pytest.approx(HOVER_THRUST)
    assert len(_errors_with(m_result, "THR_MDL_FAC")) == 1


def test_hover_without_the_optional_parameters_and_estimate():
    result = _analyze(replace(FAST, params={"MPC_THR_HOVER": None}, hover_estimate=None))
    assert "hover.mpc_thr_hover" not in result.measurements
    assert "hover.estimate" not in result.measurements
    assert "hover.command" in result.measurements
    assert result.errors == []


def test_estimate_uses_only_valid_samples():
    datasets = quad_datasets(FAST)
    est = dict(datasets["hover_thrust_estimate"])
    t = est["timestamp"] * 1e-6
    est["valid"] = np.where(t < 10.0, 0.0, 1.0)
    est["hover_thrust"] = np.where(t < 10.0, 0.9, 0.4)
    datasets["hover_thrust_estimate"] = est
    assert _analyze(FAST, datasets).measurements["hover.estimate"] == pytest.approx(0.4)


def test_estimate_without_a_valid_field_uses_every_sample():
    datasets = quad_datasets(FAST)
    est = {k: v for k, v in datasets["hover_thrust_estimate"].items() if k != "valid"}
    datasets["hover_thrust_estimate"] = est
    assert _analyze(FAST, datasets).measurements["hover.estimate"] == pytest.approx(HOVER_ESTIMATE)


def test_hover_thrust_is_the_median_over_hover_segments_only():
    datasets = quad_datasets(FAST)
    thrust = dict(datasets["vehicle_thrust_setpoint"])
    t = thrust["timestamp"] * 1e-6
    thrust["xyz[2]"] = np.where((t < 3.0) | (t > 18.0), -0.9, -0.3)  # ground and landing
    datasets["vehicle_thrust_setpoint"] = thrust
    assert _analyze(FAST, datasets).measurements["hover.thrust"] == pytest.approx(0.3)


def test_no_hover_segment():
    result = _analyze(FAST, _without_hover(quad_datasets(FAST)))
    assert not any(k.startswith(("hover.", "inflight_gyro")) for k in result.measurements)
    assert len(_errors_with(result, "hover")) == 1  # once for the group, not once per key


def test_hover_windows_are_trimmed_like_the_in_air_windows():
    import log_validation.response as response

    assert response.TRIM_S is TRIM_S and TRIM_S == 2.0
    segments = [FlightSegment(10.0, 20.0, SegmentKind.HOVER, 100),
                FlightSegment(30.0, 34.0, SegmentKind.HOVER, 40),  # not longer than 2 * TRIM_S
                FlightSegment(40.0, 44.5, SegmentKind.HOVER, 45),
                FlightSegment(50.0, 60.0, SegmentKind.CLIMB, 100)]
    assert hover_windows(segments) == [(12.0, 18.0), (42.0, 42.5)]


def test_hover_segment_too_short_to_trim_gives_the_no_hover_error():
    segments = [FlightSegment(5.0, 9.0, SegmentKind.HOVER, 40)]
    measurements, errors = hover_group(quad_log(FAST), quad_params(), segments, 20.0, 3.5)
    assert measurements == {}
    assert len(errors) == 1 and "no hover segment longer than 4 s" in errors[0]


def test_missing_thrust_topic_keeps_the_noise():
    result = _analyze(replace(FAST, hover_thrust=None))
    assert "hover.thrust" not in result.measurements
    assert "inflight_gyro_noise.roll" in result.measurements
    assert _errors_with(result, "vehicle_thrust_setpoint")


# ---------------------------------------------------------------------------
# Motor lag
# ---------------------------------------------------------------------------


def test_known_motor():
    m = _analyze().measurements
    assert abs(m["motor_lag_s"] - MOTOR_LAG_S) < 0.003
    assert 0.9 < m["motor_lag_r2"] <= 1.0


def test_known_motor_at_the_full_rate():
    m = _analyze(QuadSpec(duration_s=20.0, takeoff_s=3.0, landing_s=18.0,
                          motor_lag_s=0.05, esc_hz=400.0)).measurements
    assert abs(m["motor_lag_s"] - 0.05) < 0.003


def test_no_esc_rpm():
    result = _analyze(replace(FAST, motor_lag_s=None))
    assert "motor_lag_s" not in result.measurements and "motor_lag_r2" not in result.measurements
    assert len(_errors_with(result, "ESC RPM")) == 1


def test_all_rpm_zero():
    datasets = quad_datasets(FAST)
    esc = {k: (np.zeros_like(v) if k.endswith("esc_rpm") else v)
           for k, v in datasets["esc_status"].items()}
    datasets["esc_status"] = esc
    result = _analyze(FAST, datasets)
    assert "motor_lag_s" not in result.measurements
    assert _errors_with(result, "ESC RPM")


def test_rpm_logged_too_slowly_names_the_measured_rate():
    result = _analyze(replace(FAST, esc_hz=20.0))
    assert "motor_lag_s" not in result.measurements
    (error,) = _errors_with(result, "ESC RPM")
    assert "20 Hz" in error and f"{MIN_ESC_RPM_RATE_HZ:.0f}" in error


def test_steady_motor_command_gets_no_lag():
    datasets = quad_datasets(FAST)
    motors = {k: (np.full_like(v, 0.5) if k.startswith("control") else v)
              for k, v in datasets["actuator_motors"].items()}
    datasets["actuator_motors"] = motors
    result = _analyze(FAST, datasets)
    assert "motor_lag_s" not in result.measurements
    assert any("ESC RPM" in e and "command" in e for e in result.errors)


def test_one_dead_esc_does_not_block_the_others():
    datasets = quad_datasets(FAST)
    esc = dict(datasets["esc_status"])
    esc["esc[3].esc_rpm"] = np.zeros_like(esc["esc[3].esc_rpm"])
    datasets["esc_status"] = esc
    result = _analyze(FAST, datasets)
    assert abs(result.measurements["motor_lag_s"] - MOTOR_LAG_S) < 0.003
    assert result.errors == []


def test_rpm_logging_gap_is_not_bridged():
    datasets = quad_datasets(FAST)
    esc = datasets["esc_status"]
    keep = ~((esc["timestamp"] >= 6e6) & (esc["timestamp"] < 9e6))
    datasets["esc_status"] = {k: v[keep] for k, v in esc.items()}
    m = _analyze(FAST, datasets).measurements
    assert abs(m["motor_lag_s"] - MOTOR_LAG_S) < 0.004


# ---------------------------------------------------------------------------
# In-flight gyro noise
# ---------------------------------------------------------------------------


def test_known_inflight_noise():
    spec = QuadSpec(duration_s=20.0, takeoff_s=3.0, landing_s=18.0, rate_hz=800.0)
    datasets = quad_datasets(spec)
    rate = dict(datasets["vehicle_angular_velocity"])
    t = rate["timestamp_sample"] * 1e-6
    rng = np.random.default_rng(8)
    for k in range(3):
        rate[f"xyz[{k}]"] = 0.2 * np.sin(2 * np.pi * t) + rng.normal(0.0, 0.05, t.size)
    datasets["vehicle_angular_velocity"] = rate
    m = _analyze(spec, datasets).measurements
    for axis in ("roll", "pitch", "yaw"):
        assert m[f"inflight_gyro_noise.{axis}"] == pytest.approx(0.05, rel=0.10)
    assert m["inflight_gyro_rate_hz"] == pytest.approx(800.0, rel=0.01)


def test_inflight_noise_follows_the_cutoff_gate():
    datasets = quad_datasets(FAST)
    low = _analyze(FAST, datasets, replace(GATES, noise_cutoff_hz=2.0)).measurements
    high = _analyze(FAST, datasets, replace(GATES, noise_cutoff_hz=20.0)).measurements
    assert low["inflight_gyro_noise.roll"] != high["inflight_gyro_noise.roll"]


# ---------------------------------------------------------------------------
# Rates, parameters, cell count
# ---------------------------------------------------------------------------


def test_measured_rate():
    spec = QuadSpec(duration_s=20.0, takeoff_s=3.0, landing_s=18.0, rate_hz=800.0)
    m = _analyze(spec).measurements
    assert m["rate.torque_setpoint_hz"] == pytest.approx(800.0, rel=0.01)
    assert m["rate.angular_velocity_hz"] == pytest.approx(800.0, rel=0.01)


def test_rate_of_a_slow_torque_topic_is_still_reported():
    datasets = quad_datasets(FAST)
    torque = datasets["vehicle_torque_setpoint"]
    datasets["vehicle_torque_setpoint"] = {k: v[::8] for k, v in torque.items()}
    m = _analyze(FAST, datasets).measurements
    assert m["rate.torque_setpoint_hz"] == pytest.approx(50.0, rel=0.01)


def test_gyro_parameters_only_when_present():
    full = _analyze(replace(FAST, params={"IMU_GYRO_CUTOFF": 30.0, "IMU_DGYRO_CUTOFF": 20.0,
                                           "IMU_GYRO_RATEMAX": 1000})).measurements
    assert full["param.IMU_GYRO_CUTOFF"] == 30.0
    assert full["param.IMU_DGYRO_CUTOFF"] == 20.0
    assert full["param.IMU_GYRO_RATEMAX"] == 1000.0
    bare = _analyze(replace(FAST, params={"IMU_GYRO_RATEMAX": None,
                                          "IMU_DGYRO_CUTOFF": 20.0})).measurements
    assert "param.IMU_GYRO_RATEMAX" not in bare and "param.IMU_DGYRO_CUTOFF" in bare


def test_thr_mdl_fac_recorded_without_a_hover_segment():
    result = _analyze(FAST, _without_hover(quad_datasets(FAST)))
    assert result.measurements["param.THR_MDL_FAC"] == THR_MDL_FAC
    assert "hover.thr_mdl_fac" not in result.measurements
    bare = _analyze(replace(FAST, params={"THR_MDL_FAC": None})).measurements
    assert "param.THR_MDL_FAC" not in bare


def test_cell_count_is_the_first_positive_value():
    m = _analyze(replace(FAST, cell_counts=(0.0, 0.0, 6.0, 4.0))).measurements
    assert m["cell_count"] == 6.0


@pytest.mark.parametrize("counts", [None, (0.0, 0.0)])
def test_cell_count_absent_without_error(counts):
    result = _analyze(replace(FAST, cell_counts=counts))
    assert "cell_count" not in result.measurements
    assert result.errors == []


# ---------------------------------------------------------------------------
# All keys, purity, gap rule
# ---------------------------------------------------------------------------


def test_every_measurement_is_finite():
    m = _analyze().measurements
    assert all(np.isfinite(v) for v in m.values())
    assert {"hover.command", "motor_lag_s", "inflight_gyro_noise.roll",
            "rate.torque_setpoint_hz", "cell_count"} <= set(m)


def test_new_measurements_do_not_change_the_log():
    datasets = quad_datasets(FAST)
    before = {t: {k: v.copy() for k, v in d.items()} for t, d in datasets.items()}
    _analyze(FAST, datasets)
    assert all(np.array_equal(before[t][k], datasets[t][k]) for t in before for k in before[t])


def _jittered(datasets: dict, seed: int = 4) -> dict:
    """The same topics with stamps off the round 2.5 ms grid (+-0.4 ms), still increasing."""
    rng = np.random.default_rng(seed)
    out = {}
    for topic, data in datasets.items():
        if topic not in ("vehicle_torque_setpoint", "vehicle_angular_velocity",
                         "vehicle_rates_setpoint"):
            out[topic] = data
            continue
        jitter = np.round(rng.uniform(-400.0, 400.0, data["timestamp"].size))
        out[topic] = {k: (v + jitter if k.startswith("timestamp") else v)
                      for k, v in data.items()}
    return out


def _drop_pairs(datasets: dict) -> dict:
    out = dict(datasets)
    for topic in ("vehicle_torque_setpoint", "vehicle_angular_velocity",
                  "vehicle_rates_setpoint"):
        data = datasets[topic]
        drop = np.zeros(data["timestamp"].size, dtype=bool)
        for index in (1500, 1501, 2000, 2001, 2500, 2501, 3000, 3001, 3500, 3501, 4000, 4001):
            drop[index] = True
        out[topic] = {k: v[~drop] for k, v in data.items()}
    return out


def test_two_dropped_samples_are_interpolated():
    # Two consecutive dropped samples at 400 Hz leave a gap of about 3 intervals; with
    # timestamp jitter it is above or below 3 by chance. The limit sits above it, so every
    # such gap is bridged and the used share equals that of the log without drops.
    full = _jittered(quad_datasets(FAST))
    base = _analyze(FAST, full).measurements
    m = _analyze(FAST, _drop_pairs(full)).measurements
    # A fit-grid step differs by a microsecond with the median interval: under a sample.
    for axis in ("roll", "pitch", "yaw"):
        assert m[f"used_fraction.{axis}"] == pytest.approx(base[f"used_fraction.{axis}"], abs=5e-4)


def test_gap_limit_sits_above_three_intervals():
    assert MAX_GAP_INTERVALS == 3.5


def test_a_gap_of_four_dropped_samples_is_left_empty():
    full = _jittered(quad_datasets(FAST))
    base = _analyze(FAST, full).measurements
    data = full["vehicle_rates_setpoint"]
    drop = np.zeros(data["timestamp"].size, dtype=bool)
    drop[2000:2004] = True
    cut = dict(full)
    cut["vehicle_rates_setpoint"] = {k: v[~drop] for k, v in data.items()}
    m = _analyze(FAST, cut).measurements
    assert m["used_fraction.roll"] < base["used_fraction.roll"] - 1e-4


# ---------------------------------------------------------------------------
# Guards: one failing group does not take the others with it
# ---------------------------------------------------------------------------


def test_effectiveness_failure_keeps_the_other_groups(monkeypatch, capsys):
    import log_validation.response as response

    def broken(*_args, **_kwargs):
        raise RuntimeError("fit exploded at /home/someone/x")

    monkeypatch.setattr(response, "fit_first_order_plant_runs", broken)
    result = _analyze()
    assert {"hover.command", "motor_lag_s", "inflight_gyro_noise.roll",
            "rate.torque_setpoint_hz", "param.IMU_GYRO_RATEMAX", "cell_count"} \
        <= set(result.measurements)
    assert result.errors == ["effectiveness failed: RuntimeError"]
    assert "/home/someone/x" in capsys.readouterr().err


@pytest.mark.parametrize("name, group, other", [
    ("hover_measurements", "hover", "inflight_gyro_noise.roll"),
    ("inflight_noise_measurements", "inflight noise", "hover.command"),
])
def test_hover_and_noise_fail_separately(monkeypatch, name, group, other):
    import log_validation.response_measures as measures

    def broken(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(measures, name, broken)
    result = _analyze()
    assert other in result.measurements
    assert result.errors == [f"{group} failed: RuntimeError"]


def test_battery_scaling_and_params_failures_are_contained(monkeypatch):
    import log_validation.response as response

    def broken(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(response, "_battery_scaling", broken)
    monkeypatch.setattr(response, "param_measurements", broken)
    result = _analyze()
    assert "hover.command" in result.measurements and "effectiveness.roll" in result.measurements
    assert sorted(result.errors) == ["battery scaling failed: RuntimeError",
                                     "parameters failed: RuntimeError"]


# ---------------------------------------------------------------------------
# Motor lag gates and other small cases
# ---------------------------------------------------------------------------


def test_motor_lag_has_its_own_r2_gate():
    assert ResponseGates().min_motor_r2 == 0.5
    result = _analyze(FAST, None, replace(GATES, min_motor_r2=0.99999))
    assert "motor_lag_s" not in result.measurements
    assert any("ESC RPM" in e and "low fit quality" in e for e in result.errors)
    assert "motor_lag_s" in _analyze(FAST, None, replace(GATES, min_r2=0.99999)).measurements


def test_motor_lag_at_the_grid_edge_is_refused():
    result = _analyze(replace(FAST, motor_lag_s=0.25))
    assert "motor_lag_s" not in result.measurements
    assert any("motor lag at grid edge" in e for e in result.errors)


def test_thr_mdl_fac_zero_is_a_valid_linear_model():
    result = _analyze(replace(FAST, params={"THR_MDL_FAC": 0.0}))
    assert result.measurements["hover.command"] == pytest.approx(HOVER_THRUST)
    assert result.measurements["hover.thr_mdl_fac"] == 0.0
    assert result.errors == []


def test_thrust_above_one_gets_no_command():
    result = _analyze(replace(FAST, hover_thrust=1.2))
    assert result.measurements["hover.thrust"] == pytest.approx(1.2)
    assert "hover.command" not in result.measurements
    assert any("hover command refused" in e for e in result.errors)


def test_nan_valid_flag_is_invalid():
    datasets = quad_datasets(FAST)
    est = dict(datasets["hover_thrust_estimate"])
    t = est["timestamp"] * 1e-6
    est["valid"] = np.where(t < 15.0, np.nan, 1.0)
    est["hover_thrust"] = np.where(t < 15.0, 0.9, 0.4)
    datasets["hover_thrust_estimate"] = est
    assert _analyze(FAST, datasets).measurements["hover.estimate"] == pytest.approx(0.4)


def test_hover_segment_shorter_than_the_noise_window():
    # 20 ms of hover at 400 Hz is 8 samples; the 20 Hz window needs 20.
    measurements, errors = inflight_noise_measurements(quad_log(FAST), [(10.0, 10.02)], 20.0, 3.5)
    assert not any(k.startswith("inflight_gyro_noise") for k in measurements)
    assert len(errors) == 1 and "too short" in errors[0]
