"""Part identification: which IMU, barometer, magnetometer and GPS receiver made a log."""

from __future__ import annotations

import json

import numpy as np

from log_validation import report
from log_validation.batch_analyze import batch_analyze
from log_validation.part_ids import empty_parts_json, identify_parts
from log_validation.response import ResponseGates, analyze_response
from log_validation.tests.part_fakes import InstanceULog, device_id, topic
from log_validation.tests.response_fakes import quad_log
from log_validation.tests.test_batch_analyze import _fake, _touch, load_by_stem
from log_validation.tests.test_pipeline import _build_log

BMI088, ICM20602, MPU6000, ICM42688P = 0x66, 0x38, 0x21, 0x26
MS5611, RM3100, IST8310, UBX_F9P = 0x3D, 0x07, 0x06, 0xAA


def test_entry_carries_the_part_records():
    log = InstanceULog({"sensors_status_imu": topic(5, 1, gyro_device_id_primary=device_id(BMI088)),
                        "sensor_baro": topic(5, 1, device_id=device_id(MS5611)),
                        "sensor_mag": topic(5, 1, device_id=device_id(RM3100)),
                        "vehicle_gps_position": topic(5, 1, device_id=device_id(UBX_F9P))})
    parts = identify_parts(log, {}).to_json()
    assert set(parts) == {"imu", "baro", "mag", "gps"}
    assert all(set(r) == {"name", "devtype", "device_id", "source"} for r in parts.values())


def test_primary_gyro_wins_over_the_first_calibration_slot():
    log = InstanceULog({"sensors_status_imu": topic(5, 1, gyro_device_id_primary=device_id(BMI088))})
    imu = identify_parts(log, {"CAL_GYRO0_ID": device_id(ICM20602)}).imu
    assert (imu.name, imu.source) == ("BMI088", "sensors_status_imu")


def test_first_vehicle_imu_without_a_sensor_selection():
    log = InstanceULog({("vehicle_imu", 1): topic(5, 1, gyro_device_id=device_id(MPU6000)),
                        ("vehicle_imu", 0): topic(5, 1, gyro_device_id=device_id(ICM42688P))})
    imu = identify_parts(log, {"CAL_GYRO0_ID": device_id(ICM20602)}).imu
    assert (imu.name, imu.source) == ("ICM42688P", "vehicle_imu")


def test_only_calibration_slots():
    imu = identify_parts(InstanceULog({}),
                         {"CAL_GYRO0_ID": 0, "CAL_GYRO1_ID": device_id(MPU6000)}).imu
    assert (imu.name, imu.source) == ("MPU6000", "CAL_GYRO1_ID")


def test_imu_id_in_the_accelerometer_table_only():
    # Any devtype of ACC_DEVTYPES that is not in IMU_DEVTYPES decodes through the second table.
    from log_validation.hardware import ACC_DEVTYPES, IMU_DEVTYPES
    devtype = next(t for t in ACC_DEVTYPES if t not in IMU_DEVTYPES)
    imu = identify_parts(InstanceULog({}), {"CAL_GYRO0_ID": device_id(devtype)}).imu
    assert (imu.name, imu.devtype) == (ACC_DEVTYPES[devtype], devtype)


def test_unknown_imu_type_stays_unknown():
    imu = identify_parts(InstanceULog({}), {"CAL_GYRO0_ID": device_id(0x99)}).imu
    assert (imu.name, imu.devtype) == ("UNKNOWN_0x99", 0x99)


def test_barometer_on_current_firmware():
    baro = identify_parts(InstanceULog({"sensor_baro": topic(5, 1, device_id=device_id(MS5611))}),
                          {}).baro
    assert (baro.name, baro.devtype, baro.source) == ("MS5611", 0x3D, "sensor_baro")


def test_magnetometer_from_its_topic():
    log = InstanceULog({"sensor_mag": topic(5, 1, device_id=device_id(RM3100))})
    assert identify_parts(log, {}).mag.name == "RM3100"


def _two_mags(primary: dict | None) -> InstanceULog:
    datasets = {("sensor_mag", 0): topic(5, 1, device_id=device_id(IST8310)),
                ("sensor_mag", 1): topic(5, 1, device_id=device_id(RM3100))}
    if primary is not None:
        datasets["sensors_status_mag"] = primary
    return InstanceULog(datasets)


def test_measured_magnetometer_is_the_primary_instance():
    parts = identify_parts(
        _two_mags(topic(5, 1, device_id_primary=device_id(RM3100))), {})
    assert (parts.mag.name, parts.mag_instance) == ("RM3100", 1)


def test_primary_not_among_the_instances_falls_back_to_instance_0():
    parts = identify_parts(
        _two_mags(topic(5, 1, device_id_primary=device_id(MS5611))), {})
    assert (parts.mag.name, parts.mag_instance) == ("IST8310", 0)


def test_lowest_logged_instance_without_a_primary():
    log = InstanceULog({("sensor_mag", 2): topic(5, 1, device_id=device_id(RM3100)),
                        ("sensor_mag", 1): topic(5, 1, device_id=device_id(IST8310))})
    parts = identify_parts(log, {})
    assert (parts.mag.name, parts.mag_instance) == ("IST8310", 1)


def test_most_common_primary_after_a_failover():
    ids = [device_id(IST8310)] * 3 + [device_id(RM3100)] * 7
    parts = identify_parts(_two_mags(topic(10, 1, device_id_primary=ids)), {})
    assert (parts.mag.name, parts.mag_instance) == ("RM3100", 1)


def test_tie_goes_to_the_value_logged_first():
    ids = [device_id(RM3100)] * 2 + [device_id(IST8310)] * 2
    parts = identify_parts(_two_mags(topic(4, 1, device_id_primary=ids)), {})
    assert parts.mag.name == "RM3100"


def test_zero_device_ids_are_ignored():
    ids = [0, 0, 0, device_id(RM3100)]
    log = InstanceULog({"sensor_mag": topic(4, 1, device_id=ids)})
    assert identify_parts(log, {}).mag.name == "RM3100"


def test_instance_without_a_device_id_uses_its_calibration_parameter():
    baro = identify_parts(InstanceULog({"sensor_baro": topic(5, 1, pressure=95000.0)}),
                          {"CAL_BARO0_ID": device_id(MS5611)}).baro
    assert (baro.name, baro.source) == ("MS5611", "CAL_BARO0_ID")


def test_ublox_f9p():
    log = InstanceULog({"vehicle_gps_position": topic(5, 1, device_id=device_id(UBX_F9P))})
    gps = identify_parts(log, {}).gps
    assert (gps.name, gps.source) == ("UBX_F9P", "vehicle_gps_position")


def test_gps_topic_without_a_device_id():
    parts = identify_parts(InstanceULog({"vehicle_gps_position": topic(5, 1, fix_type=3)}), {})
    assert (parts.gps.name, parts.gps.devtype, parts.gps_topic) == (None, None,
                                                                    "vehicle_gps_position")
    assert parts.gps.source == "vehicle_gps_position"


def test_sensor_gps_when_vehicle_gps_position_is_absent():
    log = InstanceULog({"sensor_gps": topic(5, 1, fix_type=3)})
    assert identify_parts(log, {}).gps_topic == "sensor_gps"


def test_unknown_barometer_type():
    log = InstanceULog({"sensor_baro": topic(5, 1, device_id=device_id(0x99))})
    baro = identify_parts(log, {}).baro
    assert (baro.devtype, baro.name) == (0x99, "UNKNOWN_0x99")


def test_log_without_part_data():
    parts = identify_parts(InstanceULog({}), {})
    assert all(r["name"] is None for r in parts.to_json().values()) and parts.gps_topic is None
    assert parts.to_json() == empty_parts_json()


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------


def test_noise_entry_has_parts(tmp_path):
    out = tmp_path / "out" / "r.json"
    batch_analyze(_touch(tmp_path, ["a1"]), out, loader=load_by_stem)
    entry = json.loads(out.read_text())[0]
    assert entry["parts"]["imu"] == {"name": "ICM20689", "devtype": 0x3C,
                                     "device_id": 0x3C000A, "source": "sensors_status_imu"}


def test_failed_load_entry_has_empty_parts(tmp_path):
    out = tmp_path / "out" / "r.json"
    batch_analyze(_touch(tmp_path, ["bad"]), out, loader=load_by_stem)
    assert json.loads(out.read_text())[0]["parts"] == empty_parts_json()


def test_response_entry_has_parts():
    result = analyze_response(quad_log(), "r1", None, ResponseGates())
    assert set(result.parts) == {"imu", "baro", "mag", "gps"}


def _measured(rows, sensor, param):
    return next(r.measured for r in rows if r.sensor == sensor and r.param == param)


def test_mag_noise_is_measured_on_the_primary_instance():
    base = _build_log()
    datasets = {d.name: d.data for d in base.data_list}
    n = len(datasets["sensor_mag"]["timestamp"])
    rng = np.random.default_rng(3)

    def mag(sigma):
        return {"timestamp": datasets["sensor_mag"]["timestamp"],
                "device_id": np.full(n, device_id(IST8310)),
                **{a: 0.2 + rng.normal(0, sigma, n) for a in "xyz"}}

    noisy, quiet = mag(0.02), mag(0.004)
    quiet["device_id"] = np.full(n, device_id(RM3100))
    datasets = {k: v for k, v in datasets.items() if k != "sensor_mag"}
    log = InstanceULog({**datasets, ("sensor_mag", 0): noisy, ("sensor_mag", 1): quiet,
                        "sensors_status_mag": topic(n, 50, device_id_primary=device_id(RM3100))})
    assert abs(_measured(report.analyze(log), "mag", "noise_sigma_gauss") - 0.004) / 0.004 < 0.15


def test_part_identification_failure_is_an_entry_error(tmp_path, monkeypatch):
    import log_validation.batch_analyze as module

    def broken(_ulog, _params):
        raise RuntimeError("secret detail at /some/private/path")

    monkeypatch.setattr(module, "identify_parts", broken)
    out = tmp_path / "out" / "r.json"
    batch_analyze(_touch(tmp_path, ["a1"]), out, loader=load_by_stem)
    entry = json.loads(out.read_text())[0]
    assert "part identification failed: RuntimeError" in entry["errors"]
    assert entry["parts"] == empty_parts_json()
    assert entry["measurements"]  # the noise analysis still ran


def test_no_header_results_do_not_share_a_parts_dict():
    from log_validation.response import _no_header

    assert _no_header().parts is not _no_header().parts
