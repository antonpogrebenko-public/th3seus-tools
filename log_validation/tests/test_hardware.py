"""Tests for PX4 device-id decoding.

Device-id vectors are taken from real downloaded logs and verified against the
known hardware (e.g. a Pixhawk 4 / FMU_V5 ships an ICM20689). These guard the
packed-bitfield layout (bus_type:3, bus:5, address:8, devtype:8) — an earlier
version read devtype from the low byte and mis-identified ~94% of logs.
"""

from __future__ import annotations

from log_validation.hardware import (
    IMU_DEVTYPES,
    HardwareInfo,
    decode_device_id,
    extract_hardware_info,
    is_simulation,
)


def _hw(**kw) -> HardwareInfo:
    base = dict(
        fc_board="PX4_FMU_V5",
        fc_board_subtype=None,
        mcu=None,
        px4_version=None,
        imu_name="ICM20689",
        imu_devtype=0x3C,
        mag_name=None,
        baro_name=None,
        accel_ids=(),
        gyro_ids=(),
        mag_ids=(),
        baro_id=None,
    )
    base.update(kw)
    return HardwareInfo(**base)


def test_decode_devtype_is_high_byte():
    # FMU_V5 CAL_GYRO0_ID = 3932170 = 0x3C000A -> devtype 0x3C = ICM20689.
    d = decode_device_id(0x3C000A, IMU_DEVTYPES)
    assert d is not None
    assert d.devtype == 0x3C
    assert d.name == "ICM20689"


def test_decode_bus_fields():
    # 0x3C000A: bus_type = 0x0A & 0x07 = 2 (SPI), bus = (0x0A >> 3) & 0x1F = 1.
    d = decode_device_id(0x3C000A, IMU_DEVTYPES)
    assert d.bus_type == "SPI"
    assert d.bus_index == 1
    assert d.address == 0x00


def test_decode_fxas21002_gyro():
    # NXP FMUK66 CAL_GYRO0_ID = 5505298 = 0x540112 -> devtype 0x54 = FXAS21002C.
    d = decode_device_id(0x540112, IMU_DEVTYPES)
    assert d.name == "FXAS21002C"


def test_decode_sim_devtype():
    # A SIM IMU = 0x14010C -> devtype 0x14 = SIM (must be identifiable so it
    # can be excluded from real-data noise profiles).
    d = decode_device_id(0x14010C, IMU_DEVTYPES)
    assert d.devtype == 0x14
    assert d.name == "SIM"


def test_decode_zero_is_none():
    assert decode_device_id(0, IMU_DEVTYPES) is None


def test_is_simulation_by_devtype():
    assert is_simulation(_hw(imu_devtype=0x14, imu_name="SIM"))


def test_is_simulation_by_board_name():
    # SITL board with a non-SIM devtype value (firmware-dependent) still caught.
    assert is_simulation(_hw(fc_board="PX4_SITL", imu_devtype=0x23))


def test_is_simulation_by_bus_type():
    # gyro_id low 3 bits = 4 -> SIMULATION bus, regardless of devtype.
    sim_id = (0x3C << 16) | 0x04
    assert is_simulation(_hw(fc_board="HOLYBRO_X", gyro_ids=(sim_id,)))


def test_is_simulation_false_for_real_hardware():
    real_id = (0x3C << 16) | 0x02  # SPI bus
    assert not is_simulation(_hw(gyro_ids=(real_id,)))


def test_extract_hardware_primary_imu_from_gyro_id():
    params = {"CAL_GYRO0_ID": 0x3C000A, "CAL_ACC0_ID": 0x3C000A}
    msg_info = {"ver_hw": "PX4_FMU_V5", "sys_mcu": "STM32F76xxx, rev. Z"}
    hw = extract_hardware_info(msg_info, params)
    assert hw.fc_board == "PX4_FMU_V5"
    assert hw.imu_name == "ICM20689"
    assert hw.imu_devtype == 0x3C
