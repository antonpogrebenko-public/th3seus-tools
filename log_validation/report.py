"""Compare measured noise from a real log against hitl-sensors model defaults.

Produces a flat list of comparison rows per sensor. Topics/fields are named as
module constants so a PX4 rename is a one-line change and the loader raises a
clear error rather than silently misreading.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from . import noise_stats as ns
from . import reference as ref
from . import ulog_loader as ul
from .part_ids import TOPIC_GPS_POSITION, TOPIC_MAG, LogParts, gps_topic, identify_parts
from .part_measures import (
    DEFAULT_GPS_REST_MIN_S,
    Value,
    baro_chip_values,
    gps_accuracy_values,
    gps_position,
    gps_rest_spread_values,
    mag_evidence_values,
)
from .stationary import find_stationary_window, slice_to_window

# Topic names (modern PX4). Override here if a log uses different names.
TOPIC_ACCEL = "sensor_accel"
TOPIC_GYRO = "sensor_gyro"
TOPIC_COMBINED = "sensor_combined"
TOPIC_AIR_DATA = "vehicle_air_data"

# A measured/model ratio inside this band counts as agreement. The band is wide
# because real airframes, mounts, and GPS receivers vary a lot; the goal is
# order-of-magnitude validation of the model, not an exact match.
RATIO_OK_LOW = 1.0 / 3.0
RATIO_OK_HIGH = 3.0


@dataclass(frozen=True)
class Row:
    sensor: str
    param: str
    model: float
    measured: float | None
    unit: str
    verdict: str
    note: str = ""

    @property
    def ratio(self) -> float | None:
        if self.measured is None or self.model == 0:
            return None
        return self.measured / self.model


def _verdict(measured: float | None, model: float) -> str:
    if measured is None:
        return "n/a"
    if model == 0:
        return "info"
    r = measured / model
    return "ok" if RATIO_OK_LOW <= r <= RATIO_OK_HIGH else "FLAG"


def _row(sensor, param_key, table, measured, note=""):
    p = table[param_key]
    return Row(
        sensor=sensor,
        param=p.name,
        model=p.value,
        measured=measured,
        unit=p.unit,
        verdict=_verdict(measured, p.value),
        note=note or p.note,
    )


def integration_interval_s(dt_us: np.ndarray) -> float | None:
    """Median of the positive integration intervals in seconds, None when none is positive."""
    dt = np.asarray(dt_us, dtype=float)
    positive = dt[dt > 0]
    if positive.size == 0:
        return None
    return float(np.median(positive)) * ul.US_TO_S


def _info_row(sensor: str, param: str, measured: float | None, unit: str, note: str = "") -> Row:
    return Row(sensor, param, 0.0, measured, unit, _verdict(measured, 0.0), note)


def _value_rows(values: list[Value]) -> list[Row]:
    """Info rows of part measurement values; the key is `<sensor>.<param>`."""
    rows = []
    for v in values:
        sensor, param = v.key.split(".", 1)
        rows.append(_info_row(sensor, param, v.value, v.unit, v.note))
    return rows


def _imu_source(ulog) -> tuple[np.ndarray, np.ndarray, str] | None:
    """Gyro stream used for the still window: sensor_combined, else sensor_gyro."""
    try:
        t, xyz = ul.vec3(ulog, TOPIC_COMBINED, "gyro_rad")
        return t, xyz, TOPIC_COMBINED
    except KeyError:
        pass
    try:
        t, xyz = ul.xyz(ulog, TOPIC_GYRO)
        return t, xyz, TOPIC_GYRO
    except KeyError:
        return None


def analyze(
    ulog: ul.ULog,
    parts: LogParts | None = None,
    gps_rest_min_s: float = DEFAULT_GPS_REST_MIN_S,
) -> list[Row]:
    """Run all sensor comparisons that the log supports.

    `parts` names the magnetometer instance and the GPS topic to measure; it is identified
    from the log when not given. Parts without a GPS topic (a failed identification gives
    empty parts) read the log's own GPS topic. The receiver's accuracy estimates cover the
    whole log, so they are recorded even when there is no still window."""
    parts = parts or identify_parts(ulog, getattr(ulog, "initial_parameters", None) or {})
    gps = parts.gps_topic or gps_topic(ulog)
    whole_log = _value_rows(gps_accuracy_values(ulog, gps))
    source = _imu_source(ulog)
    if source is None:
        note = f"neither '{TOPIC_COMBINED}' nor '{TOPIC_GYRO}' in log"
        return [Row("imu", "stationary_window", 0.0, None, "", "n/a", note), *whole_log]
    gt, gxyz, _topic = source

    window = find_stationary_window(gt, gxyz)
    if window is None:
        no_window = Row("imu", "stationary_window", 0.0, None, "s", "n/a", "no quiet >=3s run")
        return [no_window, *whole_log]

    rows: list[Row] = []
    rows.extend(_imu_rows(ulog, window, gt, gxyz))
    rows.extend(_baro_rows(ulog, window))
    rows.extend(_mag_rows(ulog, window, parts.mag_instance))
    rows.extend(_value_rows(baro_chip_values(ulog, window, parts.baro_instance)))
    rows.extend(_value_rows(mag_evidence_values(ulog, window, parts.mag_instance)))
    rows.extend(_gps_rows(ulog, window, gps or TOPIC_GPS_POSITION))
    rows.extend(whole_log)
    rows.extend(_value_rows(gps_rest_spread_values(ulog, gps, window, gps_rest_min_s)))
    return rows


def _density_rows(ulog, window, sensor, param_key, field, dt_field) -> list[Row]:
    """Noise density and integration interval from one sensor_combined vector field."""
    interval_param = "integration_interval_s"
    if TOPIC_COMBINED not in ul.topic_names(ulog):
        # sensor_gyro alone can give the still window, but not the integration interval.
        note = (f"no integration interval: topic '{TOPIC_COMBINED}' (which carries "
                f"{dt_field}) not in log")
        return [_row(sensor, param_key, ref.IMU, None, note),
                _info_row(sensor, interval_param, None, "s", note)]
    try:
        t, xyz = ul.vec3(ulog, TOPIC_COMBINED, field)
        dt_us = ul.series(ulog, TOPIC_COMBINED, dt_field).values
        mask = slice_to_window(t, window)
        interval_s = integration_interval_s(dt_us[mask])
        if interval_s is None:
            raise ValueError(f"no positive {dt_field} in the still window")
        w = xyz[mask]
        density = float(np.mean([ns.noise_density(w[:, a], interval_s) for a in range(3)]))
    except (KeyError, ValueError) as exc:
        return [
            _row(sensor, param_key, ref.IMU, None, str(exc)),
            _info_row(sensor, interval_param, None, "s", str(exc)),
        ]
    return [
        _row(sensor, param_key, ref.IMU, density),
        _info_row(sensor, interval_param, interval_s, "s"),
    ]


def _legacy_row(ulog, window, sensor, param_key, topic) -> Row:
    """Previous formula: logged-sample spacing of the raw sensor topic as the interval."""
    p = ref.IMU[param_key]
    note = f"legacy: {topic} at its logged spacing"
    try:
        t, xyz = ul.xyz(ulog, topic)
        mask = slice_to_window(t, window)
        dt = ns.median_dt(t[mask])
        w = xyz[mask]
        measured = float(np.mean([ns.noise_density(w[:, a], dt) for a in range(3)]))
    except (KeyError, ValueError) as exc:
        measured, note = None, str(exc)
    return Row(sensor, "legacy_noise_density", p.value, measured, p.unit, _verdict(measured, p.value), note)


def _imu_rows(ulog, window, gt, gxyz) -> list[Row]:
    rows: list[Row] = []
    rows.extend(_density_rows(ulog, window, "gyro", "gyro_noise_density", "gyro_rad", "gyro_integral_dt"))
    rows.extend(
        _density_rows(
            ulog, window, "accel", "accel_noise_density", "accelerometer_m_s2", "accelerometer_integral_dt"
        )
    )
    rows.append(_legacy_row(ulog, window, "gyro", "gyro_noise_density", TOPIC_GYRO))
    rows.append(_legacy_row(ulog, window, "accel", "accel_noise_density", TOPIC_ACCEL))

    # Bias instability via Allan deviation (approximate, needs a long window).
    gw = gxyz[window.i0 : window.i1 + 1]
    dt_gyro = ns.median_dt(gt[window.i0 : window.i1 + 1])
    note = "approx; short window" if window.duration_s < 20 else "approx (Allan floor)"
    try:
        gyro_bias = float(
            np.mean([ns.allan_deviation(gw[:, a], dt_gyro).bias_instability for a in range(3)])
        )
        rows.append(_row("gyro", "gyro_bias_sigma", ref.IMU, gyro_bias, note))
    except ValueError as exc:
        rows.append(_row("gyro", "gyro_bias_sigma", ref.IMU, None, str(exc)))
    return rows


def _baro_rows(ulog, window) -> list[Row]:
    try:
        s = ul.series(ulog, TOPIC_AIR_DATA, "baro_alt_meter")
        mask = slice_to_window(s.t_s, window)
        measured = ns.white_noise_std(s.values[mask])
        return [_row("baro", "noise_sigma", ref.BARO, measured)]
    except (KeyError, ValueError) as exc:
        return [_row("baro", "noise_sigma", ref.BARO, None, str(exc))]


def _mag_rows(ulog, window, instance: int) -> list[Row]:
    try:
        mt, mxyz = ul.xyz(ulog, TOPIC_MAG, instance)
        mask = slice_to_window(mt, window)
        mw = mxyz[mask]
        measured = float(np.mean([ns.white_noise_std(mw[:, a]) for a in range(3)]))
        return [_row("mag", "noise_sigma_gauss", ref.MAG, measured)]
    except (KeyError, ValueError) as exc:
        return [_row("mag", "noise_sigma_gauss", ref.MAG, None, str(exc))]


def _gps_rows(ulog, window, topic: str) -> list[Row]:
    rows: list[Row] = []

    try:
        t_s, lat_deg, lon_deg, alt_m = gps_position(ulog, topic)
    except KeyError as exc:
        return [_row("gps", "horizontal_noise_sigma", ref.GPS, None, str(exc))]

    mask = slice_to_window(t_s, window)
    if mask.sum() < 4:
        return [_row("gps", "horizontal_noise_sigma", ref.GPS, None, "too few GPS samples on ground")]

    lat_deg, lon_deg, alt_m = lat_deg[mask], lon_deg[mask], alt_m[mask]
    north, east = ns.latlon_to_local_m(
        lat_deg, lon_deg, float(np.mean(lat_deg)), float(np.mean(lon_deg))
    )

    rows.append(
        _row("gps", "horizontal_noise_sigma", ref.GPS, ns.horizontal_position_noise(north, east))
    )
    rows.append(_row("gps", "altitude_noise_sigma", ref.GPS, ns.white_noise_std(alt_m)))
    rows.append(_row("gps", "update_rate_hz", ref.GPS, ns.sample_rate_hz(t_s)))

    try:
        vn = ul.series(ulog, topic, "vel_n_m_s").values[mask]
        ve = ul.series(ulog, topic, "vel_e_m_s").values[mask]
        vd = ul.series(ulog, topic, "vel_d_m_s").values[mask]
        vel = float(np.mean([ns.white_noise_std(v) for v in (vn, ve, vd)]))
        rows.append(_row("gps", "velocity_noise_sigma", ref.GPS, vel))
    except (KeyError, ValueError) as exc:
        rows.append(_row("gps", "velocity_noise_sigma", ref.GPS, None, str(exc)))
    return rows
