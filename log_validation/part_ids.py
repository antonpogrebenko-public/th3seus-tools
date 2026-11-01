"""Which IMU, barometer, magnetometer and GPS receiver produced a log's measurements.

PX4 packs the part type into every device ID (see `hardware.decode_device_id`). The IDs come
from the logged topics when they carry one, and from the calibration parameters otherwise. A
device type that no table names stays `UNKNOWN_0x..`; it is never mapped to a known part.
"""

from __future__ import annotations

import math
import numbers
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

from . import ulog_loader as ul
from .hardware import (
    BARO_DEVTYPES,
    GPS_DEVTYPES,
    MAG_DEVTYPES,
    DecodedDevice,
    decode_device_id,
    decode_imu_id,
)

TOPIC_STATUS_IMU = "sensors_status_imu"
TOPIC_VEHICLE_IMU = "vehicle_imu"
TOPIC_BARO = "sensor_baro"
TOPIC_STATUS_BARO = "sensors_status_baro"
TOPIC_MAG = "sensor_mag"
TOPIC_STATUS_MAG = "sensors_status_mag"
TOPIC_GPS_POSITION = "vehicle_gps_position"
TOPIC_SENSOR_GPS = "sensor_gps"
PART_TOPICS = (TOPIC_STATUS_IMU, TOPIC_VEHICLE_IMU, TOPIC_BARO, TOPIC_STATUS_BARO, TOPIC_MAG,
               TOPIC_STATUS_MAG, TOPIC_GPS_POSITION, TOPIC_SENSOR_GPS)
PART_KEYS = ("imu", "baro", "mag", "gps")

FIELD_DEVICE_ID = "device_id"
FIELD_PRIMARY_DEVICE_ID = "device_id_primary"
FIELD_STATUS_GYRO_ID = "gyro_device_id_primary"
FIELD_VEHICLE_GYRO_ID = "gyro_device_id"
CAL_SLOTS = 4  # CAL_GYRO0_ID .. CAL_GYRO3_ID


@dataclass(frozen=True)
class PartRecord:
    """One part: its name, device type and ID, and where the ID was read."""

    name: str | None
    devtype: int | None
    device_id: int | None
    source: str | None


@dataclass(frozen=True)
class LogParts:
    """The four parts of a log and the topic instances the measurements read."""

    imu: PartRecord
    baro: PartRecord
    mag: PartRecord
    gps: PartRecord
    baro_instance: int
    mag_instance: int
    gps_topic: str | None
    # Why a calibration parameter that was read gave no part, such as a malformed ID.
    notes: tuple[str, ...] = ()

    def to_json(self) -> dict[str, dict[str, Any]]:
        return {key: asdict(getattr(self, key)) for key in PART_KEYS}


_EMPTY = PartRecord(None, None, None, None)


def empty_parts() -> LogParts:
    """Four records with every field None: a log that could not be identified."""
    return LogParts(_EMPTY, _EMPTY, _EMPTY, _EMPTY, 0, 0, None)


def empty_parts_json() -> dict[str, dict[str, Any]]:
    """The JSON of `empty_parts()`: the entry of a log that could not be identified."""
    return empty_parts().to_json()


def _common_id(ulog: Any, topic: str, field: str, instance: int = 0) -> int | None:
    """The most common non-zero value of a field (ties: logged first); None when there is none."""
    try:
        values = ul.series(ulog, topic, field, instance).values
    except KeyError:
        return None
    counts = Counter(int(v) for v in values if math.isfinite(v) and v != 0)
    return counts.most_common(1)[0][0] if counts else None


def _param_id(params: Mapping[str, Any], name: str) -> tuple[int | None, str | None]:
    """(device ID, None) from a calibration parameter; (None, None) when it is absent or 0;
    (None, note) when it is not a whole number above 0, which no device ID is."""
    value = params.get(name)
    number = isinstance(value, numbers.Real) and not isinstance(value, bool)
    if value is None or (number and value == 0):
        return None, None
    if not number or not math.isfinite(value) or value < 0 or value != int(value):
        return None, f"{name} is not a device ID"
    return int(value), None


def _record(decoded: DecodedDevice | None, device_id: int | None, source: str) -> PartRecord:
    if decoded is None or device_id is None:
        return _EMPTY
    return PartRecord(decoded.name, decoded.devtype, device_id, source)


def _from_params(params: Mapping[str, Any], names: Sequence[str],
                 decode: Callable[[int], DecodedDevice | None],
                 ) -> tuple[PartRecord, tuple[str, ...]]:
    """The first of the calibration parameters that holds a device ID, and a note for each
    malformed one read before it."""
    notes: list[str] = []
    for name in names:
        device_id, note = _param_id(params, name)
        if note is not None:
            notes.append(note)
        elif device_id is not None:
            return _record(decode(device_id), device_id, name), tuple(notes)
    return _EMPTY, tuple(notes)


def _imu(ulog: Any, params: Mapping[str, Any]) -> tuple[PartRecord, tuple[str, ...]]:
    candidates: list[tuple[int | None, str]] = [
        (_common_id(ulog, TOPIC_STATUS_IMU, FIELD_STATUS_GYRO_ID), TOPIC_STATUS_IMU)]
    imu_instances = ul.instances(ulog, TOPIC_VEHICLE_IMU)
    if imu_instances:
        candidates.append((_common_id(ulog, TOPIC_VEHICLE_IMU, FIELD_VEHICLE_GYRO_ID,
                                      imu_instances[0]), TOPIC_VEHICLE_IMU))
    for device_id, source in candidates:
        if device_id is not None:
            return _record(decode_imu_id(device_id), device_id, source), ()
    return _from_params(params, [f"CAL_GYRO{i}_ID" for i in range(CAL_SLOTS)], decode_imu_id)


def _instance_id(ulog: Any, params: Mapping[str, Any], topic: str, instance: int,
                 cal_prefix: str, table: dict[int, str]) -> tuple[PartRecord, tuple[str, ...]]:
    device_id = _common_id(ulog, topic, FIELD_DEVICE_ID, instance)
    if device_id is not None:
        return _record(decode_device_id(device_id, table), device_id, topic), ()
    return _from_params(params, [f"{cal_prefix}{instance}_ID"],
                        lambda d: decode_device_id(d, table))


def _measured_instance(ulog: Any, topic: str, status_topic: str) -> int:
    """The instance named by the selection topic, else the lowest logged instance, else 0."""
    logged = ul.instances(ulog, topic)
    primary = _common_id(ulog, status_topic, FIELD_PRIMARY_DEVICE_ID)
    if primary is not None:
        for instance in logged:
            if _common_id(ulog, topic, FIELD_DEVICE_ID, instance) == primary:
                return instance
    return logged[0] if logged else 0


def gps_topic(ulog: Any) -> str | None:
    """The GPS topic the measurements read: `vehicle_gps_position`, else `sensor_gps`, else
    None. Needs no part identification, so the GPS values survive a failed one."""
    return next((t for t in (TOPIC_GPS_POSITION, TOPIC_SENSOR_GPS) if ul.instances(ulog, t)),
                None)


def _gps(ulog: Any) -> tuple[PartRecord, str | None]:
    """The receiver of the GPS topic the measurements read, named from its lowest logged
    instance: with two receivers on `sensor_gps`, that is instance 0, the one they read."""
    topic = gps_topic(ulog)
    if topic is None:
        return _EMPTY, None
    device_id = _common_id(ulog, topic, FIELD_DEVICE_ID, ul.instances(ulog, topic)[0])
    if device_id is None:
        return PartRecord(None, None, None, topic), topic
    return _record(decode_device_id(device_id, GPS_DEVTYPES), device_id, topic), topic


def identify_parts(ulog: Any, params: Mapping[str, Any]) -> LogParts:
    """The IMU, barometer, magnetometer and GPS receiver of a parsed log."""
    baro_instance = _measured_instance(ulog, TOPIC_BARO, TOPIC_STATUS_BARO)
    mag_instance = _measured_instance(ulog, TOPIC_MAG, TOPIC_STATUS_MAG)
    gps, gps_topic = _gps(ulog)
    imu, imu_notes = _imu(ulog, params)
    baro, baro_notes = _instance_id(ulog, params, TOPIC_BARO, baro_instance, "CAL_BARO",
                                    BARO_DEVTYPES)
    mag, mag_notes = _instance_id(ulog, params, TOPIC_MAG, mag_instance, "CAL_MAG", MAG_DEVTYPES)
    return LogParts(
        imu=imu,
        baro=baro,
        mag=mag,
        gps=gps,
        baro_instance=baro_instance,
        mag_instance=mag_instance,
        gps_topic=gps_topic,
        notes=imu_notes + baro_notes + mag_notes,
    )
