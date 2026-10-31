"""Tests for the per-log part measurements: barometer chip noise, magnetometer evidence, gyro rate
parameters. Synthetic logs only."""

from __future__ import annotations

import warnings

import numpy as np
import pytest

from log_validation.aggregate import aggregate_profiles, profiled_measurements
from log_validation.batch_analyze import analyze_log
from log_validation.calibration_ranges import contributes_to_noise
from log_validation.part_measures import (
    GYRO_RATE_PARAMS,
    baro_chip_values,
    gyro_rate_params,
    isa_altitude_m,
    mag_evidence_values,
)
from log_validation.stationary import Window
from log_validation.tests.fakes import FakeULog, ts_us
from log_validation.tests.test_batch_analyze import _fake
from log_validation.tests.test_pipeline import _build_log

WINDOW_60S = Window(0, 2999, 0.0, 60.0)
BASE_PA = 95000.0


def values(vs):
    return {v.key: v.value for v in vs if v.value is not None}


def _baro_log(sigma_m, rate_hz=50.0, seconds=60.0, scale=1.0, repeat=1, pressure=None):
    """sensor_baro at rate_hz: 95000 Pa plus white noise of sigma_m altitude per reading,
    each reading logged `repeat` times, pressure multiplied by `scale` (0.01 for hPa)."""
    n = int(rate_hz * seconds)
    dh_dp = abs(isa_altitude_m(np.array([BASE_PA + 1.0])) - isa_altitude_m(np.array([BASE_PA - 1.0]))) / 2.0
    sigma_p = sigma_m / float(dh_dp[0])
    readings = BASE_PA + np.random.default_rng(11).normal(0, sigma_p, n // repeat)
    p = np.repeat(readings, repeat) * scale if pressure is None else pressure
    return FakeULog({"sensor_baro": {"timestamp": ts_us(n, rate_hz), "pressure": p}})


def _mag_log(sigma, rate_hz=50.0, seconds=60.0, quantum=None):
    n = int(rate_hz * seconds)
    rng = np.random.default_rng(12)
    data = {"timestamp": ts_us(n, rate_hz)}
    for axis, base in zip("xyz", (0.21, 0.03, 0.49)):
        x = base + rng.normal(0, sigma, n)
        data[axis] = x if quantum is None else np.round(x / quantum) * quantum
    return FakeULog({"sensor_mag": data})


def test_raw_barometer_at_50_hz():
    v = values(baro_chip_values(_baro_log(0.1), WINDOW_60S, 0))
    assert abs(v["baro.chip_noise_sigma"] - 0.1) / 0.1 < 0.10
    assert abs(v["baro.chip_rate_hz"] - 50.0) / 50.0 < 0.01
    assert abs(v["baro.chip_allan_slope"] + 0.5) <= 0.15 and v["baro.chip_resolution_m"] > 0


def test_pressure_in_hpa_gives_the_same_noise():
    v = values(baro_chip_values(_baro_log(0.1, scale=0.01), WINDOW_60S, 0))
    assert abs(v["baro.chip_noise_sigma"] - 0.1) / 0.1 < 0.10


def test_repeated_readings_are_not_new_samples():
    v = values(baro_chip_values(_baro_log(0.1, rate_hz=100.0, repeat=2), WINDOW_60S, 0))
    assert abs(v["baro.chip_rate_hz"] - 50.0) / 50.0 < 0.01
    assert abs(v["baro.chip_noise_sigma"] - 0.1) / 0.1 < 0.10


def test_bad_pressure_samples_are_ignored():
    log = _baro_log(0.1)
    p = log.get_dataset("sensor_baro").data["pressure"].copy()
    p[10], p[20] = 0.0, np.nan
    bad = FakeULog({"sensor_baro": {"timestamp": ts_us(p.size, 50.0), "pressure": p}})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        out = baro_chip_values(bad, WINDOW_60S, 0)
    v = values(out)
    assert len(out) == 4 and all(np.isfinite(x) for x in v.values())
    assert abs(v["baro.chip_noise_sigma"] - 0.1) / 0.1 < 0.15


def test_constant_baro_gives_an_error_not_a_value():
    out = baro_chip_values(_baro_log(0.1, pressure=np.full(3000, BASE_PA)), WINDOW_60S, 0)
    assert len(out) == 1 and out[0].key == "baro.chip_noise_sigma"
    assert out[0].value is None and out[0].unit == "m" and out[0].note


def test_missing_baro_topic_gives_one_error_value():
    out = baro_chip_values(FakeULog({"sensor_mag": {"timestamp": ts_us(5, 1.0)}}), WINDOW_60S, 0)
    assert len(out) == 1 and out[0].value is None and "sensor_baro" in out[0].note


def test_no_raw_barometer_topic():
    base = _fake()
    log = FakeULog({d.name: d.data for d in base.data_list if d.name != "sensor_baro"},
                   params=dict(base.initial_parameters), info=dict(base.msg_info_dict))
    result = analyze_log(_stub_path("nobaro"), loader=lambda p: log)
    m, errors = result.measurements, result.errors
    assert "baro.noise_sigma" in m and not any(k.startswith("baro.chip_") for k in m)
    assert any(e.startswith("baro.chip_noise_sigma:") and "sensor_baro" in e for e in errors)


def _stub_path(stem):
    from pathlib import Path
    return Path(f"{stem}.ulg")


def test_magnetometer_at_100_hz():
    v = values(mag_evidence_values(_mag_log(0.004, rate_hz=100.0), WINDOW_60S, 0))
    assert abs(v["mag.rate_hz"] - 100.0) / 100.0 < 0.01
    assert abs(v["mag.allan_slope"] + 0.5) <= 0.15


def test_quantized_magnetometer_resolution():
    v = values(mag_evidence_values(_mag_log(0.004, quantum=0.001), WINDOW_60S, 0))
    assert v["mag.resolution_gauss"] == pytest.approx(0.001)


def test_missing_mag_topic_gives_one_error_value():
    out = mag_evidence_values(_baro_log(0.1), WINDOW_60S, 0)
    assert len(out) == 1 and out[0].key == "mag.rate_hz" and out[0].value is None
    assert out[0].unit == "Hz" and out[0].note


def test_gyro_rate_parameters_recorded():
    log = _fake(IMU_GYRO_RATEMAX=800, IMU_GYRO_CUTOFF=40)
    m = analyze_log(_stub_path("g1"), loader=lambda p: log).measurements
    assert m["param.IMU_GYRO_RATEMAX"] == 800 and m["param.IMU_GYRO_CUTOFF"] == 40


def test_absent_gyro_rate_parameters_give_no_values():
    assert GYRO_RATE_PARAMS == ("IMU_GYRO_RATEMAX", "IMU_GYRO_CUTOFF")
    assert gyro_rate_params({}) == {}
    assert gyro_rate_params({"IMU_GYRO_RATEMAX": float("nan"), "IMU_GYRO_CUTOFF": 30}) == {
        "param.IMU_GYRO_CUTOFF": 30.0}


def test_log_without_gyro_rate_parameters_does_not_raise():
    m = analyze_log(_stub_path("g2"), loader=lambda p: _fake()).measurements
    assert not any(k.startswith("param.") for k in m)


def test_parameters_alone_do_not_make_a_noise_contribution():
    entry = {"log_id": "a", "measurements": {"param.IMU_GYRO_RATEMAX": 800.0}}
    assert not contributes_to_noise(entry)
    assert aggregate_profiles([entry] * 3) == []


def test_profiled_measurements_drop_parameters_and_the_fix_type_mode():
    m = {"param.IMU_GYRO_CUTOFF": 40.0, "gps.fix_type_mode": 3.0, "baro.noise_sigma": 0.2,
         "mag.rate_hz": float("nan")}
    assert profiled_measurements(m) == {"baro.noise_sigma": 0.2}
