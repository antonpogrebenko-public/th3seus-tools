"""End-to-end pipeline test against a synthetic ULog with known noise.

Builds a duck-typed ULog (matching the pyulog surface report.py uses:
`data_list`, `get_dataset(name, multi_instance)` -> object with `.data` dict of
numpy arrays including 'timestamp' in microseconds). Injects known noise levels
and asserts the report recovers them. No real log or pyulog read required.
"""

from __future__ import annotations

import math

import numpy as np

from log_validation import report
from log_validation.tests.fakes import disarmed_topic
from log_validation.tests.part_fakes import device_id


class _Dataset:
    def __init__(self, data: dict):
        self.data = data


class _NamedData:
    def __init__(self, name, data):
        self.name = name
        self.data = data


class FakeULog:
    def __init__(self, datasets: dict[str, dict]):
        self._datasets = {name: _Dataset(d) for name, d in datasets.items()}
        self.data_list = [_NamedData(name, d) for name, d in datasets.items()]

    def get_dataset(self, name, multi_instance=0):
        return self._datasets[name]  # raises KeyError if absent (loader wraps it)


# Pressure change per meter of altitude near 95000 Pa (ISA), about 11.4 Pa/m.
RAW_BARO_PA_PER_M = 11.4


def _ts_us(n: int, rate_hz: float) -> np.ndarray:
    dt_us = 1e6 / rate_hz
    return (np.arange(n) * dt_us).astype(np.float64)


def _build_log() -> FakeULog:
    rng = np.random.default_rng(7)

    # IMU at 200 Hz, 70 s, stationary (true rate 0, gravity on z).
    rate = 200.0
    n = int(70 * rate)
    dt = 1.0 / rate
    gyro_density = 0.001  # rad/s/sqrt(Hz)
    accel_density = 0.008  # m/s^2/sqrt(Hz)
    g_sigma = gyro_density / math.sqrt(dt)
    a_sigma = accel_density / math.sqrt(dt)

    gyro = {
        "timestamp": _ts_us(n, rate),
        "x": rng.normal(0, g_sigma, n),
        "y": rng.normal(0, g_sigma, n),
        "z": rng.normal(0, g_sigma, n),
    }
    accel = {
        "timestamp": _ts_us(n, rate),
        "x": rng.normal(0, a_sigma, n),
        "y": rng.normal(0, a_sigma, n),
        "z": 9.81 + rng.normal(0, a_sigma, n),
    }

    # sensor_combined at 200 Hz: same noise, each sample integrated over 5000 us.
    combined = {
        "timestamp": _ts_us(n, rate),
        "gyro_integral_dt": np.full(n, 5000.0),
        "accelerometer_integral_dt": np.full(n, 5000.0),
    }
    for i in range(3):
        combined[f"gyro_rad[{i}]"] = gyro["xyz"[i]]
        combined[f"accelerometer_m_s2[{i}]"] = accel["xyz"[i]]

    # Baro at 50 Hz: 0.2 m noise on a constant altitude.
    bn = int(70 * 50)
    baro = {
        "timestamp": _ts_us(bn, 50.0),
        "baro_alt_meter": 1655.0 + rng.normal(0, 0.2, bn),
    }

    # Raw barometer at 50 Hz: 95000 Pa plus 0.2 m equivalent white noise (about 2.3 Pa).
    baro_raw = {
        "timestamp": _ts_us(bn, 50.0),
        "pressure": 95000.0 + rng.normal(0, 0.2 * RAW_BARO_PA_PER_M, bn),
        "device_id": np.full(bn, device_id(0x3D)),  # MS5611
    }

    # Mag at 50 Hz: 0.004 Gauss noise per axis on a constant field.
    mag = {
        "timestamp": _ts_us(bn, 50.0),
        "x": 0.21 + rng.normal(0, 0.004, bn),
        "y": 0.03 + rng.normal(0, 0.004, bn),
        "z": 0.49 + rng.normal(0, 0.004, bn),
        "device_id": np.full(bn, device_id(0x06)),  # IST8310
    }

    # GPS at 5 Hz: constant position with 1.2 m horizontal noise.
    gn = 70 * 5
    h_sigma = 1.2
    deg_per_m = 180.0 / (math.pi * 6371000.0)
    gps = {
        "timestamp": _ts_us(gn, 5.0),
        "lat": (40.015 + rng.normal(0, h_sigma * deg_per_m, gn)) / 1e-7,
        "lon": (-105.27 + rng.normal(0, h_sigma * deg_per_m, gn)) / 1e-7,
        "alt": (1655.0 + rng.normal(0, 2.5, gn)) / 1e-3,
        "vel_n_m_s": rng.normal(0, 0.08, gn),
        "vel_e_m_s": rng.normal(0, 0.08, gn),
        "vel_d_m_s": rng.normal(0, 0.08, gn),
        "fix_type": np.full(gn, 3),
        "eph": np.full(gn, 1.0),
        "epv": np.full(gn, 1.5),
        "s_variance_m_s": np.full(gn, 0.2),
        "device_id": np.full(gn, device_id(0xA8)),  # UBX_8
    }
    status_imu = {
        "timestamp": _ts_us(10, 1.0),
        "gyro_device_id_primary": np.full(10, 0x3C000A),  # ICM20689 on SPI
    }

    return FakeULog(
        {
            "sensor_gyro": gyro,
            "sensor_accel": accel,
            "sensor_combined": combined,
            "vehicle_air_data": baro,
            "sensor_baro": baro_raw,
            "sensor_mag": mag,
            "vehicle_gps_position": gps,
            "sensors_status_imu": status_imu,
            "actuator_armed": disarmed_topic(),
        }
    )


def _measured(rows, sensor, param):
    for r in rows:
        if r.sensor == sensor and r.param == param:
            return r.measured
    raise AssertionError(f"row {sensor}.{param} not found")


def test_pipeline_recovers_injected_noise():
    rows = report.analyze(_build_log())

    assert abs(_measured(rows, "gyro", "gyro_noise_density") - 0.001) / 0.001 < 0.1
    assert abs(_measured(rows, "accel", "accel_noise_density") - 0.008) / 0.008 < 0.1
    assert abs(_measured(rows, "baro", "noise_sigma") - 0.2) / 0.2 < 0.15
    assert abs(_measured(rows, "baro", "chip_noise_sigma") - 0.2) / 0.2 < 0.15
    assert abs(_measured(rows, "mag", "noise_sigma_gauss") - 0.004) / 0.004 < 0.15
    assert abs(_measured(rows, "gps", "horizontal_noise_sigma") - 1.2) / 1.2 < 0.25
    assert abs(_measured(rows, "gps", "update_rate_hz") - 5.0) < 0.5
    assert _measured(rows, "gps", "eph_median_m") == 1.0


def test_pipeline_without_sensor_combined_falls_back_to_sensor_gyro():
    full = _build_log()
    log = FakeULog({d.name: d.data for d in full.data_list if d.name != "sensor_combined"})
    rows = report.analyze(log)
    # The still window comes from sensor_gyro; the densities need sensor_combined's
    # integration intervals, so only the legacy densities and the other sensors remain.
    assert _measured(rows, "gyro", "gyro_noise_density") is None
    assert _measured(rows, "accel", "accel_noise_density") is None
    assert abs(_measured(rows, "gyro", "legacy_noise_density") - 0.001) / 0.001 < 0.1
    assert abs(_measured(rows, "accel", "legacy_noise_density") - 0.008) / 0.008 < 0.1
    assert _measured(rows, "gyro", "gyro_bias_sigma") is not None
    assert abs(_measured(rows, "baro", "noise_sigma") - 0.2) / 0.2 < 0.15
    assert abs(_measured(rows, "gps", "update_rate_hz") - 5.0) < 0.5


def test_pipeline_verdicts_present():
    rows = report.analyze(_build_log())
    verdicts = {r.verdict for r in rows}
    assert verdicts.issubset({"ok", "FLAG", "n/a", "info"})
    assert any(r.verdict == "ok" for r in rows)
