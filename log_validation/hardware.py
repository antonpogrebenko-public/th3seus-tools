"""PX4 hardware identification from ulog metadata.

Decodes device IDs and extracts flight controller, IMU, mag, baro, GPS info.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# PX4 device type definitions from src/drivers/drv_sensor.h
# Updated 2026-06 from PX4-Autopilot main branch
IMU_DEVTYPES = {
    0x11: "LSM303D",
    0x12: "LSM6DSV",
    0x13: "LSM6DSV32X",
    0x14: "SIM",
    0x17: "LSM6DSK320X",
    0x21: "MPU6000",
    0x22: "L3GD20",
    0x24: "MPU9250",
    0x25: "ICM20649",
    0x26: "ICM42688P",
    0x27: "ICM40609D",
    0x28: "ICM20948",
    0x29: "ICM42605",
    0x2A: "ICM42670P",
    0x2B: "IIM42652",
    0x2C: "IAM20680HP",
    0x2D: "ICM42686P",
    0x2E: "IIM42653",
    0x33: "MPU6050",
    0x34: "ICM45686",
    0x36: "MPU6500",
    0x37: "BMI270",
    0x38: "ICM20602",
    0x3A: "ICM20608G",
    0x3C: "ICM20689",
    0x41: "BMI055",
    0x42: "BMI055",
    0x44: "LSM9DS1",
    0x54: "FXAS21002C",
    0x66: "BMI088",
    0x57: "ADIS16448",
    0x58: "ADIS16470",
    0x59: "ADIS16477",
    0x5A: "ADIS16507",
    0x5B: "SCH16T",
    0x5C: "ADIS16607",
    0x5D: "ADIS1650X",
    0x5E: "ADIS1657X",
    0x63: "ADIS16497",
    0x6A: "BMI088",
    0x6C: "BMI085",
    0x87: "UAVCAN",
}

ACC_DEVTYPES = {
    0x33: "MPU6050",
    0x41: "BMI055",
    0x52: "FXOS8701C",
    0x61: "LSM303AGR",
    0x6A: "BMI088",
    0x6C: "BMI085",
    0x80: "UAVCAN",
}

MAG_DEVTYPES = {
    0x01: "HMC5883",
    0x03: "MAGSIM",
    0x04: "AK8963",
    0x05: "LIS3MDL",
    0x06: "IST8310",
    0x07: "RM3100",
    0x08: "QMC5883L",
    0x09: "AK09916",
    0x0A: "VCM1193L",
    0x0B: "IST8308",
    0x0D: "MMC5983MA",
    0x0E: "IIS2MDC",
    0x0F: "QMC5883P",
    0x10: "AF9838",
    0x43: "BMM150",
    0x45: "LSM9DS1_M",
    0x62: "LSM303AGR",
    0x88: "UAVCAN",
    0xE5: "BMM350",
    0xF2: "AK09940A",
}

BARO_DEVTYPES = {
    0x3D: "MS5611",
    0x3E: "MS5607",
    0x3F: "BMP280",
    0x40: "LPS25H",
    0x4D: "TCBP001TA",
    0x4E: "MS5837",
    0x4F: "SPL06",
    0x50: "LPS33HW",
    0x51: "MPL3115A2",
    0x5F: "MPC2520",
    0x60: "LPS22HB",
    0x65: "BAROSIM",
    0x67: "BMP388",
    0x68: "DPS310",
    0x6E: "BMP390",
    0x6F: "BMP581",
    0x81: "UAVCAN",
    0xB7: "ICP101XX",
    0xB8: "ICP201XX",
    0xE7: "AUAV",
    0xE8: "SPA06",
}

GPS_DEVTYPES = {
    0x85: "UAVCAN",
    0xA0: "ASHTECH",
    0xA1: "EMLID_REACH",
    0xA2: "FEMTOMES",
    0xA3: "MTK",
    0xA4: "SBF",
    0xA5: "UBX",
    0xA6: "UBX_6",
    0xA7: "UBX_7",
    0xA8: "UBX_8",
    0xA9: "UBX_9",
    0xAA: "UBX_F9P",
    0xAB: "NMEA",
    0xAC: "UBX_10",
    0xAD: "UBX_20",
    0xAF: "SIM",
}

# PX4 device::Device::DeviceBusType enum (src/lib/drivers/device/Device.hpp).
BUS_TYPES = {
    0: "UNKNOWN",
    1: "I2C",
    2: "SPI",
    3: "UAVCAN",
    4: "SIMULATION",
    5: "SERIAL",
    6: "MAVLINK",
}


@dataclass(frozen=True)
class DecodedDevice:
    """Decoded PX4 device ID."""

    name: str
    devtype: int
    bus_type: str
    bus_index: int
    address: int

    def __str__(self) -> str:
        return f"{self.name}({self.bus_type}{self.bus_index})"


def decode_device_id(
    dev_id: int, devtype_table: dict[int, str]
) -> DecodedDevice | None:
    """Decode a PX4 device ID into its components."""
    if dev_id == 0:
        return None

    # PX4 device id is a packed little-endian bitfield
    # (src/lib/drivers/device/Device.hpp DeviceStructure):
    #   bus_type : 3, bus : 5, address : 8, devtype : 8
    # The devtype is therefore bits 16-23, NOT the low byte.
    bus_type_num = dev_id & 0x07
    bus_idx = (dev_id >> 3) & 0x1F
    addr = (dev_id >> 8) & 0xFF
    devtype = (dev_id >> 16) & 0xFF

    name = devtype_table.get(devtype, f"UNKNOWN_0x{devtype:02X}")
    bus_type = BUS_TYPES.get(bus_type_num, f"BUS{bus_type_num}")

    return DecodedDevice(
        name=name,
        devtype=devtype,
        bus_type=bus_type,
        bus_index=bus_idx,
        address=addr,
    )


@dataclass(frozen=True)
class HardwareInfo:
    """Hardware identification from a PX4 log."""

    # Flight controller
    fc_board: str
    fc_board_subtype: str | None
    mcu: str | None
    px4_version: str | None

    # Primary sensors (first valid device)
    imu_name: str | None
    imu_devtype: int | None
    mag_name: str | None
    baro_name: str | None

    # Raw device IDs for detailed analysis
    accel_ids: tuple[int, ...]
    gyro_ids: tuple[int, ...]
    mag_ids: tuple[int, ...]
    baro_id: int | None


def extract_hardware_info(
    msg_info: dict[str, Any], params: dict[str, Any]
) -> HardwareInfo:
    """Extract hardware info from ulog msg_info_dict and initial_parameters."""

    # Flight controller identification
    fc_board = msg_info.get("ver_hw", "UNKNOWN")
    fc_subtype = msg_info.get("ver_hw_subtype")
    mcu = msg_info.get("sys_mcu")
    px4_ver = msg_info.get("ver_sw")

    # Collect device IDs from CAL parameters
    accel_ids = []
    gyro_ids = []
    mag_ids = []
    baro_id = None

    for i in range(4):
        acc_id = params.get(f"CAL_ACC{i}_ID", 0)
        if acc_id:
            accel_ids.append(int(acc_id))

        gyro_id = params.get(f"CAL_GYRO{i}_ID", 0)
        if gyro_id:
            gyro_ids.append(int(gyro_id))

        mag_id = params.get(f"CAL_MAG{i}_ID", 0)
        if mag_id:
            mag_ids.append(int(mag_id))

    # Baro doesn't have numbered CAL params in all versions
    baro_id_val = params.get("CAL_BARO_PRIME", params.get("SENS_BARO_PRIME", 0))
    if baro_id_val:
        baro_id = int(baro_id_val)

    # Decode primary IMU (prefer gyro ID, fall back to accel)
    imu_name = None
    imu_devtype = None

    primary_imu_id = gyro_ids[0] if gyro_ids else (accel_ids[0] if accel_ids else 0)
    if primary_imu_id:
        # Try IMU table first, then ACC table
        decoded = decode_device_id(primary_imu_id, IMU_DEVTYPES)
        if decoded and "UNKNOWN" not in decoded.name:
            imu_name = decoded.name
            imu_devtype = decoded.devtype
        else:
            decoded = decode_device_id(primary_imu_id, ACC_DEVTYPES)
            if decoded and "UNKNOWN" not in decoded.name:
                imu_name = decoded.name
                imu_devtype = decoded.devtype
            elif decoded:
                imu_name = decoded.name
                imu_devtype = decoded.devtype

    # Decode primary mag
    mag_name = None
    if mag_ids:
        decoded = decode_device_id(mag_ids[0], MAG_DEVTYPES)
        if decoded:
            mag_name = decoded.name

    # Decode baro (if we have the ID)
    baro_name = None
    if baro_id:
        decoded = decode_device_id(baro_id, BARO_DEVTYPES)
        if decoded:
            baro_name = decoded.name

    return HardwareInfo(
        fc_board=fc_board,
        fc_board_subtype=fc_subtype,
        mcu=mcu,
        px4_version=px4_ver,
        imu_name=imu_name,
        imu_devtype=imu_devtype,
        mag_name=mag_name,
        baro_name=baro_name,
        accel_ids=tuple(accel_ids),
        gyro_ids=tuple(gyro_ids),
        mag_ids=tuple(mag_ids),
        baro_id=baro_id,
    )


# PX4 DRV_IMU_DEVTYPE_SIM on current firmware. The devtype value can be
# reassigned across firmware versions, so it is only one of several signals.
SIM_IMU_DEVTYPE = 0x14


def is_simulation(hw: HardwareInfo) -> bool:
    """True if the log comes from SITL/HIL rather than real hardware.

    Firmware versions differ in the exact SIM devtype value, so this checks
    three version-robust signals, any of which is conclusive:
      * primary IMU devtype == the known SIM devtype,
      * the FC board name is a simulation target (PX4_SITL / SIH),
      * the primary gyro's bus type decodes to SIMULATION.
    """
    if hw.imu_devtype == SIM_IMU_DEVTYPE:
        return True

    board = (hw.fc_board or "").upper()
    if "SITL" in board or "SIH" in board:
        return True

    if hw.gyro_ids:
        bus_type = hw.gyro_ids[0] & 0x07
        if BUS_TYPES.get(bus_type) == "SIMULATION":
            return True

    return False


def normalize_fc_board(board: str) -> str:
    """Normalize FC board name for grouping.

    PX4_FMU_V4 and PX4_FMU_V4_PRO -> PX4_FMU_V4
    CUAV_X7PRO_V1 -> CUAV_X7PRO
    """
    # Remove version suffixes
    normalized = board.upper()

    # Common patterns
    if "_PRO" in normalized and not normalized.endswith("_PRO"):
        # Keep PRO but remove trailing version
        pass
    elif normalized.endswith("_V1") or normalized.endswith("_V2"):
        normalized = normalized[:-3]

    return normalized
