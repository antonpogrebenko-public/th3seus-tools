"""Tests for the per-log part measurements: barometer chip noise, magnetometer evidence, gyro rate
parameters. Synthetic logs only."""

from __future__ import annotations

import math
import warnings

import numpy as np
import pytest

from log_validation.aggregate import aggregate_profiles, profiled_measurements
from log_validation.batch_analyze import analyze_log
from log_validation.calibration_ranges import contributes_to_noise
from log_validation.part_measures import (
    GYRO_RATE_PARAMS,
    baro_chip_values,
    gps_accuracy_values,
    gps_position,
    gps_rest_spread_values,
    gyro_rate_params,
    isa_altitude_m,
    mag_evidence_values,
)
from log_validation.stationary import Window
from log_validation.tests.part_fakes import InstanceULog, device_id, topic
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


DEG_PER_M = 180.0 / (math.pi * 6371000.0)
GPS = "vehicle_gps_position"


def _gps_log(topic_name=GPS, n=100, rate_hz=5.0, **fields):
    return InstanceULog({topic_name: topic(n, rate_hz, **fields)})


def _wander_log(seconds=120.0, rate_hz=5.0, white_m=0.3, wander_m=1.0, extra=None):
    """New-format GPS at rest: white noise plus a slow sinusoidal wander on north and east."""
    n = int(seconds * rate_hz)
    t = np.arange(n) / rate_hz
    rng = np.random.default_rng(21)
    wander = wander_m * np.sin(2 * np.pi * t / seconds)
    north = rng.normal(0, white_m, n) + wander
    east = rng.normal(0, white_m, n) + wander
    fields = {
        "latitude_deg": 40.0 + north * DEG_PER_M,
        "longitude_deg": -105.0 + east * DEG_PER_M / math.cos(math.radians(40.0)),
        "altitude_msl_m": 1600.0 + rng.normal(0, 0.5, n),
        "fix_type": 3,
    }
    fields.update(extra or {})
    return _gps_log(n=n, rate_hz=rate_hz, **fields)


def test_rtk_log():
    fix = [6] * 90 + [3] * 10
    eph = [0.02] * 90 + [1.5] * 10
    log = _gps_log(fix_type=fix, eph=eph, epv=0.03, s_variance_m_s=0.01)
    v = values(gps_accuracy_values(log, GPS))
    assert v["gps.fix_type_mode"] == 6 and v["gps.eph_median_m"] == pytest.approx(0.02)
    assert v["gps.epv_median_m"] == pytest.approx(0.03)
    assert v["gps.speed_accuracy_median_m_s"] == pytest.approx(0.01)


def test_only_the_newer_gps_topic():
    log = InstanceULog({"sensor_gps": topic(
        100, 5, fix_type=3, eph=1.2, epv=2.0, s_variance_m_s=0.3, device_id=device_id(0xA8))})
    entry = analyze_log(_stub_path("newgps"), loader=lambda p: log)
    assert entry.measurements["gps.eph_median_m"] == pytest.approx(1.2)
    assert entry.parts["gps"]["source"] == "sensor_gps"


def test_no_3d_fix():
    vs = gps_accuracy_values(_gps_log(fix_type=2, eph=1.0), GPS)
    assert [v.value for v in vs] == [None] and "no 3D fix" in vs[0].note


def test_no_gps_topic():
    vs = gps_accuracy_values(_baro_log(0.1), None)
    assert [v.key for v in vs] == ["gps.eph_median_m"] and vs[0].value is None
    assert "no GPS topic" in vs[0].note


def test_extrapolated_samples_do_not_count():
    log = _gps_log(fix_type=[8] * 60 + [3] * 40, eph=1.0, epv=1.0, s_variance_m_s=0.1)
    assert values(gps_accuracy_values(log, GPS))["gps.fix_type_mode"] == 3


def test_fix_type_mode_tie_takes_the_larger():
    log = _gps_log(fix_type=[3] * 50 + [4] * 50, eph=1.0, epv=1.0, s_variance_m_s=0.1)
    assert values(gps_accuracy_values(log, GPS))["gps.fix_type_mode"] == 4


def test_a_missing_accuracy_field_gives_that_keys_error():
    vs = gps_accuracy_values(_gps_log(fix_type=3, eph=1.0, epv=2.0), GPS)
    assert values(vs)["gps.epv_median_m"] == pytest.approx(2.0)
    missing = [v for v in vs if v.value is None]
    assert [v.key for v in missing] == ["gps.speed_accuracy_median_m_s"] and missing[0].note


def test_accuracy_without_a_stationary_window():
    base = _fake()
    moving = {d.name: dict(d.data) for d in base.data_list}
    n = len(moving["sensor_gyro"]["timestamp"])
    for axis in "xyz":
        moving["sensor_gyro"][axis] = np.full(n, 0.5)
        moving["sensor_combined"][f"gyro_rad[{'xyz'.index(axis)}]"] = np.full(n, 0.5)
    log = FakeULog(moving, params=dict(base.initial_parameters), info=dict(base.msg_info_dict))
    result = analyze_log(_stub_path("moving"), loader=lambda p: log)
    assert result.measurements["gps.eph_median_m"] == pytest.approx(1.0)
    assert "gps.rest_spread_horizontal_m" not in result.measurements


def test_slow_wander_counts():
    log = _wander_log()
    v = values(gps_rest_spread_values(log, GPS, Window(0, 599, 0.0, 120.0), 60.0))
    assert abs(v["gps.rest_spread_horizontal_m"] - math.sqrt(0.3**2 + 0.5)) / 0.768 < 0.10
    assert v["gps.rest_window_s"] == pytest.approx(120.0)
    assert abs(v["gps.rest_spread_altitude_m"] - 0.5) / 0.5 < 0.15


def test_rest_window_too_short():
    vs = gps_rest_spread_values(_wander_log(seconds=30.0), GPS, Window(0, 149, 0.0, 30.0), 60.0)
    assert [v.value for v in vs] == [None] and "30 s" in vs[0].note


def test_rest_spread_ignores_samples_without_a_fix():
    window = Window(0, 599, 0.0, 120.0)
    clean = values(gps_rest_spread_values(_wander_log(), GPS, window, 60.0))
    fix = np.full(600, 3)
    fix[100:105] = 0
    log = _wander_log(extra={"fix_type": fix})
    data = log.get_dataset(GPS).data
    data["latitude_deg"] = data["latitude_deg"].copy()
    data["longitude_deg"] = data["longitude_deg"].copy()
    data["latitude_deg"][100:105] = 0.0
    data["longitude_deg"][100:105] = 0.0
    got = values(gps_rest_spread_values(log, GPS, window, 60.0))
    assert got["gps.rest_spread_horizontal_m"] == pytest.approx(
        clean["gps.rest_spread_horizontal_m"], rel=0.01)


def test_rest_spread_without_a_topic():
    vs = gps_rest_spread_values(_baro_log(0.1), None, WINDOW_60S, 60.0)
    assert [v.value for v in vs] == [None] and vs[0].note


def test_gps_position_reads_old_and_new_field_names():
    old = _gps_log(lat=400000000, lon=-1050000000, alt=1600000)
    new = _gps_log(latitude_deg=40.0, longitude_deg=-105.0, altitude_msl_m=1600.0)
    for log in (old, new):
        t, lat, lon, alt = gps_position(log, GPS)
        assert lat[0] == pytest.approx(40.0) and lon[0] == pytest.approx(-105.0)
        assert alt[0] == pytest.approx(1600.0) and t.size == 100
    with pytest.raises(KeyError):
        gps_position(_gps_log(fix_type=3), GPS)


def test_existing_gps_keys_read_from_sensor_gps():
    base = _fake()
    data = {d.name: d.data for d in base.data_list if d.name != GPS}
    old = base.get_dataset(GPS).data
    data["sensor_gps"] = {
        "timestamp": old["timestamp"],
        "latitude_deg": old["lat"] * 1e-7,
        "longitude_deg": old["lon"] * 1e-7,
        "altitude_msl_m": old["alt"] * 1e-3,
        "vel_n_m_s": old["vel_n_m_s"], "vel_e_m_s": old["vel_e_m_s"],
        "vel_d_m_s": old["vel_d_m_s"], "device_id": old["device_id"],
    }
    log = FakeULog(data, params=dict(base.initial_parameters), info=dict(base.msg_info_dict))
    m = analyze_log(_stub_path("sg"), loader=lambda p: log).measurements
    assert abs(m["gps.horizontal_noise_sigma"] - 1.2) / 1.2 < 0.25
    assert abs(m["gps.update_rate_hz"] - 5.0) < 0.5


def _short_rest_log():
    """The default test log, but the gyro moves after 30 s, so the still window is 30 s."""
    base = _fake()
    data = {d.name: {k: np.array(v) for k, v in d.data.items()} for d in base.data_list}
    still = 30 * 200
    for axis in "xyz":
        data["sensor_gyro"][axis][still:] = 0.5
    for i in range(3):
        data["sensor_combined"][f"gyro_rad[{i}]"][still:] = 0.5
    return FakeULog(data, params=dict(base.initial_parameters), info=dict(base.msg_info_dict))


def test_gps_rest_min_s_option(tmp_path, monkeypatch):
    import json

    from log_validation import batch_analyze as ba

    log = _short_rest_log()
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "a1.ulg").touch()
    monkeypatch.setattr(ba, "ULog", lambda *a, **k: log)
    default_out, short_out = tmp_path / "d.json", tmp_path / "s.json"
    ba.main([str(tmp_path / "logs"), "-o", str(default_out)])
    ba.main([str(tmp_path / "logs"), "-o", str(short_out), "--gps-rest-min-s", "20"])
    default = json.loads(default_out.read_text())[0]
    short = json.loads(short_out.read_text())[0]
    assert "gps.rest_spread_horizontal_m" not in default["measurements"]
    assert any("shorter than 60 s" in e for e in default["errors"])
    assert short["measurements"]["gps.rest_window_s"] == pytest.approx(30.0, abs=0.1)
    with pytest.raises(SystemExit):
        ba.main([str(tmp_path / "logs"), "--gps-rest-min-s", "-1"])
