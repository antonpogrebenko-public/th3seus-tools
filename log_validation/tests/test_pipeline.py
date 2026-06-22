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


def _ts_us(n: int, rate_hz: float) -> np.ndarray:
    dt_us = 1e6 / rate_hz
    return (np.arange(n) * dt_us).astype(np.float64)


def _build_log() -> FakeULog:
    rng = np.random.default_rng(7)

    # IMU at 200 Hz, 60 s, stationary (true rate 0, gravity on z).
    rate = 200.0
    n = int(60 * rate)
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

    # Baro at 50 Hz: 0.2 m noise on a constant altitude.
    bn = int(60 * 50)
    baro = {
        "timestamp": _ts_us(bn, 50.0),
        "baro_alt_meter": 1655.0 + rng.normal(0, 0.2, bn),
    }

    # Mag at 50 Hz: 0.004 Gauss noise per axis on a constant field.
    mag = {
        "timestamp": _ts_us(bn, 50.0),
        "x": 0.21 + rng.normal(0, 0.004, bn),
        "y": 0.03 + rng.normal(0, 0.004, bn),
        "z": 0.49 + rng.normal(0, 0.004, bn),
    }

    # GPS at 5 Hz: constant position with 1.2 m horizontal noise.
    gn = 60 * 5
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
    }

    return FakeULog(
        {
            "sensor_gyro": gyro,
            "sensor_accel": accel,
            "vehicle_air_data": baro,
            "sensor_mag": mag,
            "vehicle_gps_position": gps,
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
    assert abs(_measured(rows, "mag", "noise_sigma_gauss") - 0.004) / 0.004 < 0.15
    assert abs(_measured(rows, "gps", "horizontal_noise_sigma") - 1.2) / 1.2 < 0.25
    assert abs(_measured(rows, "gps", "update_rate_hz") - 5.0) < 0.5


def test_pipeline_verdicts_present():
    rows = report.analyze(_build_log())
    verdicts = {r.verdict for r in rows}
    assert verdicts.issubset({"ok", "FLAG", "n/a", "info"})
    assert any(r.verdict == "ok" for r in rows)
