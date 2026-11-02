"""The at-rest values are measured on the disarmed still window, and only there."""

from __future__ import annotations

import math

import numpy as np
import pytest

from log_validation import report
from log_validation.batch_analyze import batch_analyze
from log_validation.tests.fakes import FakeULog, ts_us
from log_validation.tests.part_fakes import device_id

RATE_HZ = 100.0
INTERVAL_US = 4000.0
SAMPLE_ROOT = math.sqrt(INTERVAL_US * 1e-6)
GPS_RATE_HZ = 5.0
GPS_EPH_M = 1.2
AT_REST_KEYS = ("gyro.gyro_noise_density", "accel.accel_noise_density",
                "gyro.legacy_noise_density", "accel.legacy_noise_density",
                "gyro.gyro_bias_sigma", "baro.noise_sigma", "baro.chip_noise_sigma",
                "mag.noise_sigma_gauss", "mag.rate_hz", "gps.horizontal_noise_sigma",
                "gps.altitude_noise_sigma", "gps.velocity_noise_sigma",
                "gps.rest_spread_horizontal_m", "mag.allan_slope", "mag.resolution_gauss",
                "baro.chip_rate_hz", "baro.chip_allan_slope", "baro.chip_resolution_m",
                "gps.rest_spread_altitude_m", "gps.rest_window_s")
BARO_MAG_RATE_HZ = 50.0
RAW_BARO_PA_PER_M = 11.4


def _armed(times_s, values):
    return {"actuator_armed": {"timestamp": np.asarray(times_s, float) * 1e6,
                               "armed": np.asarray(values)}}


def _status(times_s, states):
    return {"vehicle_status": {"timestamp": np.asarray(times_s, float) * 1e6,
                               "arming_state": np.asarray(states, float)}}


def _noise(rng, segments, scale=1.0):
    """White noise with the per-sample std density / sqrt(0.004) of each (seconds, density)."""
    parts = [rng.normal(0, scale * density / SAMPLE_ROOT, int(seconds * RATE_HZ))
             for seconds, density in segments]
    return np.concatenate(parts)


def _gps(rng, n):
    h_sigma = GPS_EPH_M
    deg_per_m = 180.0 / (math.pi * 6371000.0)
    return {
        "timestamp": ts_us(n, GPS_RATE_HZ),
        "lat": (40.015 + rng.normal(0, h_sigma * deg_per_m, n)) / 1e-7,
        "lon": (-105.27 + rng.normal(0, h_sigma * deg_per_m, n)) / 1e-7,
        "alt": (1655.0 + rng.normal(0, 2.5, n)) / 1e-3,
        "vel_n_m_s": rng.normal(0, 0.08, n),
        "vel_e_m_s": rng.normal(0, 0.08, n),
        "vel_d_m_s": rng.normal(0, 0.08, n),
        "fix_type": np.full(n, 3),
        "eph": np.full(n, GPS_EPH_M),
        "epv": np.full(n, 1.5),
        "s_variance_m_s": np.full(n, 0.2),
        "device_id": np.full(n, device_id(0xA8)),
    }


def _baro_mag(rng, seconds):
    n = int(seconds * BARO_MAG_RATE_HZ)
    ts = ts_us(n, BARO_MAG_RATE_HZ)
    return {
        "vehicle_air_data": {"timestamp": ts,
                             "baro_alt_meter": 1655.0 + rng.normal(0, 0.2, n)},
        "sensor_baro": {"timestamp": ts,
                        "pressure": 95000.0 + rng.normal(0, 0.2 * RAW_BARO_PA_PER_M, n),
                        "device_id": np.full(n, device_id(0x3D))},
        "sensor_mag": {"timestamp": ts,
                       "x": 0.21 + rng.normal(0, 0.004, n),
                       "y": 0.03 + rng.normal(0, 0.004, n),
                       "z": 0.49 + rng.normal(0, 0.004, n),
                       "device_id": np.full(n, device_id(0x06))},
    }


def _log(segments, arming, gps=True, **kwargs):
    rng = np.random.default_rng(11)
    n = int(sum(seconds for seconds, _ in segments) * RATE_HZ)
    combined = {"timestamp": ts_us(n, RATE_HZ),
                "gyro_integral_dt": np.full(n, INTERVAL_US),
                "accelerometer_integral_dt": np.full(n, INTERVAL_US)}
    legacy_gyro = {"timestamp": combined["timestamp"]}
    legacy_accel = {"timestamp": combined["timestamp"]}
    for i, axis in enumerate("xyz"):
        combined[f"gyro_rad[{i}]"] = legacy_gyro[axis] = _noise(rng, segments)
        combined[f"accelerometer_m_s2[{i}]"] = legacy_accel[axis] = (
            9.81 if i == 2 else 0.0) + _noise(rng, segments, scale=10.0)
    datasets = {"sensor_combined": combined, "sensor_gyro": legacy_gyro,
                "sensor_accel": legacy_accel, **_baro_mag(rng, n / RATE_HZ), **arming}
    if gps:
        datasets["vehicle_gps_position"] = _gps(rng, int(n / RATE_HZ * GPS_RATE_HZ))
    return FakeULog(datasets, **kwargs)


def _row(rows, key):
    sensor, param = key.split(".")
    for r in rows:
        if r.sensor == sensor and r.param == param:
            return r
    return None


def _value(rows, key):
    row = _row(rows, key)
    return None if row is None else row.measured


def _note(rows, key):
    row = _row(rows, key)
    return None if row is None else row.note


def test_hover_longer_than_the_rest_before_arming():
    rows = report.analyze(_log([(90, 1e-4), (300, 1.5e-3)], _armed([0, 90], [0, 1])))
    assert abs(_value(rows, "gyro.gyro_noise_density") - 1e-4) / 1e-4 < 0.10
    assert abs(_value(rows, "gps.rest_window_s") - 90.0) <= 1.0
    assert all(_value(rows, key) is not None for key in AT_REST_KEYS)


def test_rest_right_after_landing():
    segments = [(60, 1e-4), (5, 1.5e-3), (115, 1e-4)]
    log = _log(segments, _armed([0, 60], [1, 0]))
    rows = report.analyze(log)
    assert abs(_value(rows, "gyro.gyro_noise_density") - 1e-4) / 1e-4 < 0.10
    assert _value(rows, "gps.rest_window_s") <= 110.0


def test_logged_from_arming_on():
    rows = report.analyze(_log([(120, 1e-4)], _armed([0], [1])))
    assert _note(rows, "imu.stationary_window") == "no disarmed quiet >=3s run"
    assert all(_value(rows, key) is None for key in AT_REST_KEYS)
    assert _value(rows, "gps.eph_median_m") == pytest.approx(GPS_EPH_M)


def test_arming_state_from_vehicle_status():
    rows = report.analyze(_log([(30, 1e-4), (30, 5e-4)], _status([0, 30], [1, 2])),
                          gps_rest_min_s=10.0)
    assert _value(rows, "gps.rest_window_s") <= 30.0
    assert abs(_value(rows, "gyro.gyro_noise_density") - 1e-4) / 1e-4 < 0.10


def test_error_arming_states_count_as_armed():
    rows = report.analyze(_log([(60, 1e-4)], _status([0], [3])))
    assert all(_value(rows, key) is None for key in AT_REST_KEYS)
    assert _note(rows, "imu.stationary_window") == "no disarmed quiet >=3s run"


def test_no_arming_information():
    rows = report.analyze(_log([(60, 1e-4)], {}))
    note = _note(rows, "imu.stationary_window")
    assert "actuator_armed" in note and "vehicle_status" in note
    assert all(_value(rows, key) is None for key in AT_REST_KEYS)
    assert _value(rows, "gps.eph_median_m") == pytest.approx(GPS_EPH_M)


def test_gyro_only_log_uses_disarmed_samples():
    rng = np.random.default_rng(5)
    segments = [(30, 1e-4), (30, 5e-4)]
    n = int(60 * RATE_HZ)
    gyro = {"timestamp": ts_us(n, RATE_HZ), **{a: _noise(rng, segments) for a in "xyz"}}
    log = FakeULog({"sensor_gyro": gyro, **_armed([0, 30], [0, 1])})
    rows = report.analyze(log)
    expected = 1e-4 / SAMPLE_ROOT * math.sqrt(1.0 / RATE_HZ)
    assert abs(_value(rows, "gyro.legacy_noise_density") - expected) / expected < 0.10


def _entry(tmp_path, log):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "a1.ulg").write_bytes(b"")
    return batch_analyze(log_dir, None, loader=lambda p: log)[0]


def test_entry_error_through_batch_analyze(tmp_path):
    log = _log([(120, 1e-4)], _armed([0], [1]),
               params={"CAL_GYRO0_ID": 0x3C000A, "SYS_AUTOSTART": 4001,
                       "IMU_GYRO_RATEMAX": 400},
               info={"ver_hw": "PX4_FMU_V5"})
    entry = _entry(tmp_path, log)
    assert "imu.stationary_window: no disarmed quiet >=3s run" in entry.errors
    assert "gps.fix_type_mode" in entry.measurements
    assert "gps.eph_median_m" in entry.measurements
    assert entry.measurements["param.IMU_GYRO_RATEMAX"] == 400.0


def test_entry_error_without_arming_topics_through_batch_analyze(tmp_path):
    log = _log([(120, 1e-4)], {}, params={"CAL_GYRO0_ID": 0x3C000A, "SYS_AUTOSTART": 4001},
               info={"ver_hw": "PX4_FMU_V5"})
    entry = _entry(tmp_path, log)
    note = next(e for e in entry.errors if e.startswith("imu.stationary_window:"))
    assert "actuator_armed" in note and "vehicle_status" in note
    assert "gps.eph_median_m" in entry.measurements


def test_log_of_50_ms(tmp_path):
    rng = np.random.default_rng(13)
    combined = {"timestamp": ts_us(50, 1000.0),
                "gyro_integral_dt": np.full(50, 1000.0),
                "accelerometer_integral_dt": np.full(50, 1000.0)}
    for i in range(3):
        combined[f"gyro_rad[{i}]"] = rng.normal(0, 1e-3, 50)
        combined[f"accelerometer_m_s2[{i}]"] = np.full(50, 9.81 if i == 2 else 0.0)
    log = FakeULog({"sensor_combined": combined, **_armed([0], [0]),
                    "vehicle_gps_position": _gps(np.random.default_rng(13), 10)},
                   params={"CAL_GYRO0_ID": 0x3C000A, "SYS_AUTOSTART": 4001,
                           "IMU_GYRO_RATEMAX": 400},
                   info={"ver_hw": "PX4_FMU_V5"})
    entry = _entry(tmp_path, log)
    assert "imu.stationary_window: no disarmed quiet >=3s run" in entry.errors
    assert not any(e.startswith("analysis failed") for e in entry.errors)
    assert entry.measurements["gps.eph_median_m"] == pytest.approx(GPS_EPH_M)
    assert not any(key in entry.measurements for key in AT_REST_KEYS)
