"""Tests for the pinned mirror of TH3SEUS's part tables (hitl-physics 0.13.1
src/build.rs) and name parsers (hitl-daemon 0.27.0 build_config.rs)."""

from __future__ import annotations

from pathlib import Path

import pytest

from log_validation import th3seus_parts
from log_validation.th3seus_parts import (
    HITL_DAEMON_VERSION,
    HITL_PHYSICS_VERSION,
    PARTS,
    Classified,
    candidate_variant_name,
    classify_log_part,
    current_literal,
    current_value,
    daemon_parse,
    fix_class,
    variant,
)


def _imu(gyro, accel, sigma, tau):
    return {
        "gyro_noise_density": gyro,
        "accel_noise_density": accel,
        "gyro_bias_sigma": sigma,
        "gyro_bias_tau": tau,
    }


def _gps(h, alt, vel, drift, tau, rate, delay, cold, warm, hot, sats):
    return {
        "horizontal_noise_sigma_m": h,
        "altitude_noise_sigma_m": alt,
        "velocity_noise_sigma_mps": vel,
        "position_drift_sigma": drift,
        "position_drift_tau": tau,
        "update_rate_hz": rate,
        "delay_ms": delay,
        "ttff_cold_s": cold,
        "ttff_warm_s": warm,
        "ttff_hot_s": hot,
        "max_satellites": sats,
    }


# Copied from hitl-physics 0.13.1 src/build.rs; each Other is written out as the literals it
# resolves to (ImuChip::Other = Mpu6000, GpsChipset::Other = UbloxM8N).
_MPU6000 = _imu("8.726646e-4", "6.37e-3", "1e-4", "100.0")
_M8N = _gps("1.5", "3.0", "0.10", "0.06", "30.0", "5.0", "120.0", "26.0", "5.0", "1.0", "22")
BUILD_RS = {
    ("imu", "Mpu6000"): _MPU6000,
    ("imu", "Mpu6500"): _imu("9.4e-4", "6.86e-3", "1e-4", "100.0"),
    ("imu", "Icm20689"): _imu("8.73e-5", "7.85e-4", "5e-5", "200.0"),
    ("imu", "Icm42688p"): _imu("4.88e-5", "6.86e-4", "5e-5", "200.0"),
    ("imu", "Bmi270"): _imu("1.40e-4", "2.45e-3", "8e-5", "150.0"),
    ("imu", "Lsm6dso"): _imu("6.11e-5", "5.88e-4", "6e-5", "180.0"),
    ("imu", "Other"): _MPU6000,
    ("mag", "Hmc5883"): {"noise_sigma_gauss": "0.0006"},
    ("mag", "Qmc5883"): {"noise_sigma_gauss": "0.0005"},
    ("mag", "Ist8310"): {"noise_sigma_gauss": "0.0003"},
    ("mag", "Rm3100"): {"noise_sigma_gauss": "0.000013"},
    ("mag", "Lis3mdl"): {"noise_sigma_gauss": "0.00032"},
    ("mag", "Other"): {"noise_sigma_gauss": "0.0006"},
    ("baro", "Bmp280"): {"noise_sigma_m": "0.16"},
    ("baro", "Bmp388"): {"noise_sigma_m": "0.08"},
    ("baro", "Dps310"): {"noise_sigma_m": "0.05"},
    ("baro", "Spl06"): {"noise_sigma_m": "0.10"},
    ("baro", "Ms5611"): {"noise_sigma_m": "0.10"},
    ("baro", "Other"): {"noise_sigma_m": "0.15"},
    ("gps", "UbloxM8N"): _M8N,
    ("gps", "UbloxM9N"): _gps("0.9", "1.5", "0.05", "0.04", "40.0", "10.0", "80.0", "24.0", "2.0", "1.0", "33"),
    ("gps", "UbloxM10"): _gps("1.0", "1.8", "0.06", "0.04", "40.0", "10.0", "90.0", "24.0", "2.0", "1.0", "32"),
    ("gps", "UbloxF9P"): _gps("0.014", "0.02", "0.015", "0.005", "60.0", "8.0", "60.0", "25.0", "2.0", "1.0", "60"),
    ("gps", "SeptentrioMosaic"): _gps("0.006", "0.012", "0.010", "0.003", "60.0", "100.0", "40.0", "45.0", "20.0", "5.0", "80"),
    ("gps", "Here3"): _gps("0.9", "1.5", "0.05", "0.04", "40.0", "8.0", "100.0", "24.0", "2.0", "1.0", "33"),
    ("gps", "Other"): _M8N,
}


def test_versions():
    assert (HITL_PHYSICS_VERSION, HITL_DAEMON_VERSION) == ("0.13.1", "0.27.0")


def test_values_pinned_to_build_rs():
    for (part, name), fields in BUILD_RS.items():
        found = variant(part, name)
        assert found is not None, (part, name)
        assert dict(found.fields) == fields


def test_variant_order_is_build_rs_order():
    assert [v.name for v in PARTS["imu"].variants] == [
        "Mpu6000", "Mpu6500", "Icm20689", "Icm42688p", "Bmi270", "Lsm6dso", "Other"]
    assert [v.name for v in PARTS["mag"].variants] == [
        "Hmc5883", "Qmc5883", "Ist8310", "Rm3100", "Lis3mdl", "Other"]
    assert [v.name for v in PARTS["baro"].variants] == [
        "Bmp280", "Bmp388", "Dps310", "Spl06", "Ms5611", "Other"]
    assert [v.name for v in PARTS["gps"].variants] == [
        "UbloxM8N", "UbloxM9N", "UbloxM10", "UbloxF9P", "SeptentrioMosaic", "Here3", "Other"]


def test_part_records():
    assert [(p.enum, p.profile, p.profile_fn, p.parser) for p in PARTS.values()] == [
        ("ImuChip", "ImuProfile", "imu_profile", "parse_imu_chip"),
        ("BaroChip", "BaroProfile", "baro_profile", "parse_baro_chip"),
        ("MagChip", "MagProfile", "mag_profile", "parse_mag_chip"),
        ("GpsChipset", "GpsProfile", "gps_profile", "parse_gps_chipset"),
    ]
    assert [p.optional for p in PARTS.values()] == [False, False, True, False]
    assert PARTS["mag"].no_profile_variants == ("None",)
    assert all(p.no_profile_variants == () for k, p in PARTS.items() if k != "mag")
    assert PARTS["gps"].measured[-1] == ("update_rate_hz", "gps.update_rate_hz")


def test_variant_is_none_for_a_name_that_is_not_a_variant():
    assert variant("imu", "Bmi088") is None
    assert variant("mag", "None") is None
    assert variant("imu", "Other").name == "Other"


def test_other_delegates_only_where_build_rs_delegates():
    assert variant("imu", "Other").delegate == "ImuChip::Mpu6000.imu_profile()"
    assert variant("gps", "Other").delegate == "GpsChipset::UbloxM8N.gps_profile()"
    for part, spec in PARTS.items():
        for item in spec.variants:
            if (part, item.name) not in (("imu", "Other"), ("gps", "Other")):
                assert item.delegate == "", (part, item.name)


def test_arm_comments_are_kept():
    assert "best in class" in variant("baro", "Dps310").comment
    assert "CAN bus latency" in variant("gps", "Here3").comment


def test_value_parses_the_rust_literal():
    assert variant("imu", "Mpu6000").value("gyro_bias_sigma") == 1e-4
    assert variant("gps", "UbloxF9P").value("max_satellites") == 60.0
    with pytest.raises(KeyError):
        variant("imu", "Mpu6000").value("nope")


@pytest.mark.parametrize("part,text,expected", [  # hitl-daemon 0.27.0 build_config.rs, every arm
    ("imu", "MPU6000", "Mpu6000"), ("imu", "mpu-6000p", "Mpu6000"), ("imu", "MPU6500", "Mpu6500"),
    ("imu", "mpu6500p", "Mpu6500"), ("imu", "ICM-20689", "Icm20689"), ("imu", "icm20689p", "Icm20689"),
    ("imu", "ICM-42688-P", "Icm42688p"), ("imu", "icm42688", "Icm42688p"), ("imu", "BMI270", "Bmi270"),
    ("imu", "LSM6DSO", "Lsm6dso"), ("imu", "lsm6dso32", "Lsm6dso"), ("imu", "BMI088", "Other"),
    ("baro", "BMP280", "Bmp280"), ("baro", "BMP388", "Bmp388"), ("baro", "BMP390", "Bmp388"),
    ("baro", "DPS310", "Dps310"), ("baro", "SPL06", "Spl06"), ("baro", "spl06-001", "Spl06"),
    ("baro", "MS5611", "Ms5611"), ("baro", "ms5611-01ba03", "Ms5611"), ("baro", "MS5607", "Other"),
    ("mag", "HMC5883", "Hmc5883"), ("mag", "hmc5883l", "Hmc5883"), ("mag", "QMC5883", "Qmc5883"),
    ("mag", "qmc5883l", "Qmc5883"), ("mag", "IST8310", "Ist8310"), ("mag", "RM3100", "Rm3100"),
    ("mag", "LIS3MDL", "Lis3mdl"), ("mag", "None", "None"), ("mag", "", "None"), ("mag", "BMM150", "Other"),
    ("gps", "u-blox ZED-F9P", "UbloxF9P"), ("gps", "NEO-M10", "UbloxM10"), ("gps", "NEO-M9N", "UbloxM9N"),
    ("gps", "NEO-M8N", "UbloxM8N"), ("gps", "Septentrio mosaic-X5", "SeptentrioMosaic"),
    ("gps", "mosaic-X5", "SeptentrioMosaic"), ("gps", "Septentrio", "SeptentrioMosaic"),
    ("gps", "Here3", "Here3"), ("gps", "unknown", "Other")])
def test_parsers_pinned_to_build_config_rs(part, text, expected):
    assert daemon_parse(part, text) == expected


def test_two_names_one_entry():
    assert classify_log_part("baro", "BMP388") == classify_log_part("baro", "BMP390") == Classified("existing", "Bmp388")


def test_part_without_an_entry():
    assert classify_log_part("imu", "BMI088") == Classified("candidate", "Bmi088")
    assert current_value("imu", "Bmi088", "gyro_noise_density") == 8.726646e-4
    assert current_literal("gps", "Ubx20", "max_satellites") == "22"
    assert current_literal("gps", "Here3", "update_rate_hz") == "8.0"


def test_px4_gps_names_where_the_substring_rules_do_not_apply():
    assert [classify_log_part("gps", n).entry for n in ("UBX_8", "UBX_9", "UBX_10", "UBX_F9P", "SBF")] == \
        ["UbloxM8N", "UbloxM9N", "UbloxM10", "UbloxF9P", "SeptentrioMosaic"]


@pytest.mark.parametrize("part,name", [("gps", "UBX"), ("gps", "NMEA"), ("gps", "UAVCAN"),
    ("imu", "UAVCAN"), ("baro", "UAVCAN"), ("mag", "UAVCAN"), ("baro", "UNKNOWN_0x99"), ("mag", None),
    ("imu", ""), ("mag", "None")])
def test_generic_unknown_and_unidentified_count_only_toward_other(part, name):
    assert classify_log_part(part, name) == Classified("other_only", "Other")


@pytest.mark.parametrize("part,name", [("imu", "SIM"), ("baro", "BAROSIM"), ("mag", "MAGSIM"), ("gps", "SIM")])
def test_simulated_devices_are_excluded(part, name):
    assert classify_log_part(part, name).kind == "excluded"


def test_fix_classes():
    assert fix_class("UbloxF9P") == fix_class("SeptentrioMosaic") == "rtk_fixed"
    assert {fix_class(e) for e in ("UbloxM8N", "Here3", "Ubx20", "Other")} == {"standalone_3d"}
    assert th3seus_parts.FIX_TYPE_OF_CLASS == {"standalone_3d": 3, "rtk_fixed": 6}
    assert th3seus_parts.FIX_CLASS_RTK_FIXED == "rtk_fixed"
    assert th3seus_parts.FIX_CLASS_STANDALONE_3D == "standalone_3d"


def test_gps_variants_carry_their_fix_class():
    assert {v.name: v.fix_class for v in PARTS["gps"].variants} == {
        "UbloxM8N": "standalone_3d", "UbloxM9N": "standalone_3d", "UbloxM10": "standalone_3d",
        "UbloxF9P": "rtk_fixed", "SeptentrioMosaic": "rtk_fixed", "Here3": "standalone_3d",
        "Other": "standalone_3d"}
    assert all(v.fix_class is None for v in PARTS["imu"].variants)


def test_candidate_variant_names():
    assert [candidate_variant_name(n) for n in ("BMI088", "EMLID_REACH", "LSM9DS1_M")] == ["Bmi088", "EmlidReach", "Lsm9ds1M"]


def test_mirror_text_is_ascii():
    for spec in PARTS.values():
        for line in spec.doc:
            assert line.isascii(), line
        for item in spec.variants:
            assert item.comment.isascii(), item.comment
            assert item.delegate.isascii()
    assert Path(th3seus_parts.__file__).read_text(encoding="utf-8").isascii()
