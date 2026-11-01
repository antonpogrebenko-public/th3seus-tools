"""Tests for the per-log part measurements: barometer chip noise, magnetometer evidence, gyro rate
parameters. Synthetic logs only."""

from __future__ import annotations

import math
import warnings
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pytest

from log_validation.aggregate import aggregate_profiles, profiled_measurements
from log_validation.batch_analyze import analyze_log
from log_validation.calibration_ranges import contributes_to_noise, log_id_lists
from log_validation.part_measures import (
    MIN_DISTINCT_SAMPLES,
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
    above, below = (isa_altitude_m(np.array([BASE_PA + d])) for d in (1.0, -1.0))
    dh_dp = abs(above - below) / 2.0
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
    assert out[0].value is None and out[0].unit == "m" and "constant" in out[0].note


def _steps(levels: np.ndarray, n: int = 3000) -> np.ndarray:
    """n readings that hold each level in turn: len(levels) distinct samples."""
    return np.repeat(levels, n // levels.size + 1)[:n]


@pytest.mark.parametrize("steps", [2, MIN_DISTINCT_SAMPLES - 1])
def test_few_distinct_baro_samples_give_an_error_and_no_warning(steps):
    pressure = _steps(BASE_PA + np.arange(steps, dtype=float))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        out = baro_chip_values(_baro_log(0.1, pressure=pressure), WINDOW_60S, 0)
    assert [(v.key, v.value) for v in out] == [("baro.chip_noise_sigma", None)]
    assert out[0].note == (f"only {steps} distinct barometer samples in the still window "
                           f"(need {MIN_DISTINCT_SAMPLES})")


def test_the_minimum_of_distinct_baro_samples_gives_values():
    levels = BASE_PA + np.random.default_rng(3).normal(0, 1.0, MIN_DISTINCT_SAMPLES)
    out = baro_chip_values(_baro_log(0.1, pressure=_steps(levels)), WINDOW_60S, 0)
    assert len(values(out)) == 4


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


def test_constant_mag_axis_keeps_the_rate():
    log = _mag_log(0.004)
    data = {k: np.array(v) for k, v in log.get_dataset("sensor_mag").data.items()}
    data["z"] = np.full(data["z"].size, 0.49)
    out = mag_evidence_values(FakeULog({"sensor_mag": data}), WINDOW_60S, 0)
    assert abs(values(out)["mag.rate_hz"] - 50.0) / 50.0 < 0.01
    assert [(v.key, v.value, v.note) for v in out if v.value is None] == [
        ("mag.allan_slope", None, "magnetometer axis z is constant in the still window")]


def test_few_distinct_mag_samples_keep_the_rate_and_give_no_warning():
    n = 3000
    data = {"timestamp": ts_us(n, 50.0), "x": _steps(np.array([0.2, 0.21, 0.22, 0.23]), n),
            "y": np.full(n, 0.03), "z": np.full(n, 0.49)}
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        out = mag_evidence_values(FakeULog({"sensor_mag": data}), WINDOW_60S, 0)
    assert "mag.rate_hz" in values(out)
    assert [(v.key, v.note) for v in out if v.value is None] == [(
        "mag.allan_slope",
        f"only 4 distinct magnetometer samples in the still window (need {MIN_DISTINCT_SAMPLES})")]


def test_one_distinct_mag_sample_is_constant():
    n = 3000
    data = {"timestamp": ts_us(n, 50.0), **{a: np.full(n, 0.2) for a in "xyz"}}
    out = mag_evidence_values(FakeULog({"sensor_mag": data}), WINDOW_60S, 0)
    assert [(v.key, v.value) for v in out] == [("mag.rate_hz", None), ("mag.allan_slope", None)]
    assert all("constant magnetometer reading" in v.note for v in out)


def test_gyro_rate_parameters_recorded():
    log = _fake(IMU_GYRO_RATEMAX=800, IMU_GYRO_CUTOFF=40)
    m = analyze_log(_stub_path("g1"), loader=lambda p: log).measurements
    assert m["param.IMU_GYRO_RATEMAX"] == 800 and m["param.IMU_GYRO_CUTOFF"] == 40


def test_absent_gyro_rate_parameters_give_no_values():
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


NOT_NOISE = ("gps.eph_median_m", "gps.epv_median_m", "gps.speed_accuracy_median_m_s",
             "baro.chip_rate_hz", "baro.chip_allan_slope", "baro.chip_resolution_m",
             "mag.rate_hz", "mag.allan_slope", "mag.resolution_gauss", "gps.rest_window_s")
STILL_NOISE = ("baro.chip_noise_sigma", "gps.rest_spread_horizontal_m",
               "gps.rest_spread_altitude_m", "gps.update_rate_hz", "gps.horizontal_noise_sigma",
               "mag.noise_sigma_gauss")


def test_profiled_measurements_drop_the_listed_keys_only():
    m = {k: 1.0 for k in NOT_NOISE + STILL_NOISE}
    assert set(profiled_measurements(m)) == set(STILL_NOISE)


def test_whole_log_gps_values_alone_do_not_contribute_to_noise():
    entry = asdict(analyze_log(_stub_path("moving"), loader=lambda p: _moving_log()))
    m = entry["measurements"]
    assert {"gps.eph_median_m", "gps.epv_median_m", "gps.speed_accuracy_median_m_s"} <= set(m)
    assert not contributes_to_noise(entry)
    assert log_id_lists([entry], {}, [])["noise"] == []
    assert aggregate_profiles([{**entry, "log_id": f"m{i}"} for i in range(3)]) == []


def test_a_log_with_a_still_window_still_contributes_to_noise():
    entry = asdict(analyze_log(_stub_path("still"), loader=lambda p: _fake()))
    assert contributes_to_noise(entry) and log_id_lists([entry], {}, [])["noise"] == ["still"]
    assert aggregate_profiles([{**entry, "log_id": f"s{i}"} for i in range(3)]) != []


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


@pytest.mark.parametrize("key, field, label", [
    ("gps.eph_median_m", "eph", "horizontal"),
    ("gps.epv_median_m", "epv", "vertical"),
    ("gps.speed_accuracy_median_m_s", "s_variance_m_s", "speed")])
def test_receiver_without_an_accuracy_estimate(key, field, label):
    fields = {"eph": 1.0, "epv": 2.0, "s_variance_m_s": 0.3, field: [0.0] * 90 + [-1.0] * 10}
    vs = gps_accuracy_values(_gps_log(fix_type=3, **fields), GPS)
    assert key not in values(vs) and len(values(vs)) == 3
    missing = [v for v in vs if v.value is None]
    assert [(v.key, v.note) for v in missing] == [(key, f"receiver reports no {label} accuracy")]


def test_receiver_without_a_speed_accuracy():
    log = InstanceULog({"sensor_gps": topic(
        100, 5, fix_type=3, eph=1.2, epv=2.0, s_variance_m_s=0.0, device_id=device_id(0xA8))})
    entry = analyze_log(_stub_path("nospeed"), loader=lambda p: log)
    assert "gps.speed_accuracy_median_m_s" not in entry.measurements
    assert entry.measurements["gps.eph_median_m"] == pytest.approx(1.2)
    assert ("gps.speed_accuracy_median_m_s: receiver reports no speed accuracy"
            in entry.errors)


def test_zero_accuracy_samples_do_not_count_toward_the_median():
    eph = [0.0] * 60 + [1.0] * 20 + [2.0] * 20
    log = _gps_log(fix_type=3, eph=eph, epv=2.0, s_variance_m_s=0.3)
    assert values(gps_accuracy_values(log, GPS))["gps.eph_median_m"] == pytest.approx(1.5)


def test_accuracy_samples_of_another_fix_type_do_not_count():
    # The zeros are on the RTK float samples; the mode (3) has positive estimates.
    log = _gps_log(fix_type=[3] * 60 + [5] * 40, eph=[1.0] * 60 + [0.0] * 40, epv=2.0,
                   s_variance_m_s=0.3)
    assert values(gps_accuracy_values(log, GPS))["gps.eph_median_m"] == pytest.approx(1.0)


def _moving_log() -> FakeULog:
    """The default test log, but the gyro never rests, so there is no still window."""
    base = _fake()
    moving = {d.name: dict(d.data) for d in base.data_list}
    n = len(moving["sensor_gyro"]["timestamp"])
    for axis in "xyz":
        moving["sensor_gyro"][axis] = np.full(n, 0.5)
        moving["sensor_combined"][f"gyro_rad[{'xyz'.index(axis)}]"] = np.full(n, 0.5)
    return FakeULog(moving, params=dict(base.initial_parameters), info=dict(base.msg_info_dict))


def test_accuracy_without_a_stationary_window():
    result = analyze_log(_stub_path("moving"), loader=lambda p: _moving_log())
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


REST_120S = Window(0, 599, 0.0, 120.0)


def _wander_with(fill: dict[str, float], fix: np.ndarray | None = None) -> InstanceULog:
    """The default wander log with samples 100-104 of each named field set to its value."""
    base = _wander_log(extra=None if fix is None else {"fix_type": fix})
    data = {k: np.array(v, dtype=float) for k, v in base.get_dataset(GPS).data.items()}
    for name, value in fill.items():
        data[name][100:105] = value
    return InstanceULog({GPS: data})


def test_rest_spread_ignores_samples_without_a_fix():
    clean = values(gps_rest_spread_values(_wander_log(), GPS, REST_120S, 60.0))
    fix = np.full(600, 3)
    fix[100:105] = 0
    log = _wander_with({"latitude_deg": 0.0, "longitude_deg": 0.0}, fix)
    got = values(gps_rest_spread_values(log, GPS, REST_120S, 60.0))
    assert got["gps.rest_spread_horizontal_m"] == pytest.approx(
        clean["gps.rest_spread_horizontal_m"], rel=0.01)


@pytest.mark.parametrize("field", ["latitude_deg", "longitude_deg", "altitude_msl_m"])
def test_rest_spread_ignores_non_finite_positions(field):
    clean = values(gps_rest_spread_values(_wander_log(), GPS, REST_120S, 60.0))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        got = values(gps_rest_spread_values(_wander_with({field: np.nan}), GPS, REST_120S, 60.0))
    for key in ("gps.rest_spread_horizontal_m", "gps.rest_spread_altitude_m"):
        assert math.isfinite(got[key]) and got[key] == pytest.approx(clean[key], rel=0.02)


def test_rest_spread_needs_four_samples_with_a_fix():
    fix = np.zeros(600)
    fix[:3] = 3
    vs = gps_rest_spread_values(_wander_log(extra={"fix_type": fix}), GPS, REST_120S, 60.0)
    assert [v.value for v in vs] == [None]
    assert vs[0].note == "too few GPS samples with a fix in the at-rest window"


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
