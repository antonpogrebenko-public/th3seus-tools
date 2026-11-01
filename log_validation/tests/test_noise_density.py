"""Noise density uses each sample's own integration interval, not the logging spacing."""

from __future__ import annotations

import math

import numpy as np

from log_validation import report
from log_validation.tests.fakes import FakeULog, disarmed_topic, ts_us

GYRO_DENSITY = 0.0003  # rad/s/sqrt(Hz)
ACCEL_DENSITY = 0.002  # m/s^2/sqrt(Hz)
INTERVAL_US = 4000.0
DURATION_S = 400


def _combined(n, rate_hz, seed=3, interval_us=INTERVAL_US, with_gyro_dt=True, zero_first=0,
              with_accel_dt=True):
    """sensor_combined whose samples average white noise over interval_us."""
    rng = np.random.default_rng(seed)
    root = math.sqrt(interval_us * 1e-6)
    d = {"timestamp": ts_us(n, rate_hz)}
    for i in range(3):
        d[f"gyro_rad[{i}]"] = rng.normal(0, GYRO_DENSITY / root, n)
        d[f"accelerometer_m_s2[{i}]"] = (9.81 if i == 2 else 0.0) + rng.normal(0, ACCEL_DENSITY / root, n)
    if with_gyro_dt:
        gdt = np.full(n, interval_us)
        gdt[:zero_first] = 0.0
        d["gyro_integral_dt"] = gdt
    if with_accel_dt:
        d["accelerometer_integral_dt"] = np.full(n, interval_us)
    return d


def _log(combined):
    return FakeULog({"sensor_combined": combined, "actuator_armed": disarmed_topic()})


def _row(rows, sensor, param):
    for r in rows:
        if r.sensor == sensor and r.param == param:
            return r
    raise AssertionError(f"row {sensor}.{param} not found")


def measured(rows, sensor, param):
    return _row(rows, sensor, param).measured


def test_subsampled_stream_density():
    rows = report.analyze(_log(_combined(DURATION_S, 1.0)))
    assert abs(measured(rows, "gyro", "gyro_noise_density") - GYRO_DENSITY) / GYRO_DENSITY < 0.10
    assert abs(measured(rows, "accel", "accel_noise_density") - ACCEL_DENSITY) / ACCEL_DENSITY < 0.10
    assert abs(measured(rows, "gyro", "integration_interval_s") - 0.004) < 1e-9


def test_same_stream_at_two_logging_rates():
    full = _combined(DURATION_S * 250, 250.0)
    keep = np.arange(0, DURATION_S * 250, 250)
    sub = {k: v[keep] for k, v in full.items()}
    sub["timestamp"] = ts_us(DURATION_S, 1.0)
    d_full = measured(report.analyze(_log(full)), "gyro", "gyro_noise_density")
    d_sub = measured(report.analyze(_log(sub)), "gyro", "gyro_noise_density")
    assert abs(d_full - d_sub) / d_full < 0.10


def test_no_integration_interval():
    rows = report.analyze(_log(_combined(DURATION_S, 1.0, with_gyro_dt=False)))
    row = _row(rows, "gyro", "gyro_noise_density")
    assert row.measured is None and "gyro_integral_dt" in row.note


def test_no_accelerometer_integration_interval():
    rows = report.analyze(_log(_combined(DURATION_S, 1.0, with_accel_dt=False)))
    for param in ("accel_noise_density", "integration_interval_s"):
        row = _row(rows, "accel", param)
        assert row.measured is None and "accelerometer_integral_dt" in row.note
    assert measured(rows, "gyro", "gyro_noise_density") is not None


def test_log_without_sensor_combined_names_the_missing_interval():
    rng = np.random.default_rng(5)
    t = ts_us(DURATION_S, 1.0)
    gyro = {"timestamp": t, **{a: rng.normal(0, 0.0047, DURATION_S) for a in "xyz"}}
    rows = report.analyze(FakeULog({"sensor_gyro": gyro, "actuator_armed": disarmed_topic()}))
    for sensor, dt_field in (("gyro", "gyro_integral_dt"), ("accel", "accelerometer_integral_dt")):
        density = _row(rows, sensor, f"{sensor}_noise_density")
        assert density.measured is None
        assert density.note == (f"no integration interval: topic 'sensor_combined' (which "
                                f"carries {dt_field}) not in log")
        assert _row(rows, sensor, "integration_interval_s").note == density.note


def test_zero_intervals_are_ignored():
    rows = report.analyze(_log(_combined(DURATION_S, 1.0, zero_first=10)))
    assert abs(measured(rows, "gyro", "gyro_noise_density") - GYRO_DENSITY) / GYRO_DENSITY < 0.10
    rows = report.analyze(_log(_combined(DURATION_S, 1.0, zero_first=DURATION_S)))
    assert measured(rows, "gyro", "gyro_noise_density") is None


def test_integration_interval_s():
    assert report.integration_interval_s(np.array([0.0, 4000.0, 6000.0, 4000.0])) == 0.004
    assert report.integration_interval_s(np.zeros(3)) is None


def test_legacy_row_reads_logged_spacing_of_sensor_gyro():
    combined = _combined(DURATION_S, 1.0)
    rng = np.random.default_rng(5)
    gyro = {"timestamp": combined["timestamp"]}
    for a in "xyz":
        gyro[a] = rng.normal(0, 0.0047, DURATION_S)
    rows = report.analyze(FakeULog(
        {"sensor_combined": combined, "sensor_gyro": gyro, "actuator_armed": disarmed_topic()}
    ))
    legacy = measured(rows, "gyro", "legacy_noise_density")
    assert abs(legacy - 0.0047) / 0.0047 < 0.10  # 1 s spacing: sigma * sqrt(1 s)
    assert measured(rows, "accel", "legacy_noise_density") is None
