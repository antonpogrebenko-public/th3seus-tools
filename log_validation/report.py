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
from .stationary import find_stationary_window, slice_to_window

# Topic names (modern PX4). Override here if a log uses different names.
TOPIC_ACCEL = "sensor_accel"
TOPIC_GYRO = "sensor_gyro"
TOPIC_MAG = "sensor_mag"
TOPIC_AIR_DATA = "vehicle_air_data"
TOPIC_GPS = "vehicle_gps_position"

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


def analyze(ulog: ul.ULog) -> list[Row]:
    """Run all sensor comparisons that the log supports."""
    rows: list[Row] = []

    # Stationary window from the gyro (cleanest noise floor).
    try:
        gt, gxyz = ul.xyz(ulog, TOPIC_GYRO)
    except KeyError as exc:
        return [Row("imu", "stationary_window", 0.0, None, "", "n/a", str(exc))]

    window = find_stationary_window(gt, gxyz)
    if window is None:
        return [Row("imu", "stationary_window", 0.0, None, "s", "n/a", "no quiet >=3s run")]

    dt_gyro = ns.median_dt(gt[window.i0 : window.i1 + 1])
    rows.extend(_imu_rows(ulog, window, dt_gyro, gxyz))
    rows.extend(_baro_rows(ulog, window))
    rows.extend(_mag_rows(ulog, window))
    rows.extend(_gps_rows(ulog, window))
    return rows


def _imu_rows(ulog, window, dt_gyro, gxyz) -> list[Row]:
    rows: list[Row] = []
    gw = gxyz[window.i0 : window.i1 + 1]
    gyro_density = float(np.mean([ns.noise_density(gw[:, a], dt_gyro) for a in range(3)]))
    rows.append(_row("gyro", "gyro_noise_density", ref.IMU, gyro_density))

    try:
        at, axyz = ul.xyz(ulog, TOPIC_ACCEL)
        mask = slice_to_window(at, window)
        aw = axyz[mask]
        dt_acc = ns.median_dt(at[mask])
        acc_density = float(np.mean([ns.noise_density(aw[:, a], dt_acc) for a in range(3)]))
        rows.append(_row("accel", "accel_noise_density", ref.IMU, acc_density))
    except (KeyError, ValueError) as exc:
        rows.append(_row("accel", "accel_noise_density", ref.IMU, None, str(exc)))

    # Bias instability via Allan deviation (approximate, needs a long window).
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


def _mag_rows(ulog, window) -> list[Row]:
    try:
        mt, mxyz = ul.xyz(ulog, TOPIC_MAG)
        mask = slice_to_window(mt, window)
        mw = mxyz[mask]
        measured = float(np.mean([ns.white_noise_std(mw[:, a]) for a in range(3)]))
        return [_row("mag", "noise_sigma_gauss", ref.MAG, measured)]
    except (KeyError, ValueError) as exc:
        return [_row("mag", "noise_sigma_gauss", ref.MAG, None, str(exc))]


def _gps_rows(ulog, window) -> list[Row]:
    rows: list[Row] = []

    # PX4 changed GPS field names across versions:
    #   Old (pre-v1.14): lat/lon (int32, 1e-7 deg), alt (int32, mm)
    #   New (v1.14+):    latitude_deg/longitude_deg (float64 deg), altitude_msl_m (float m)
    try:
        lat = ul.series(ulog, TOPIC_GPS, "lat")
        lon = ul.series(ulog, TOPIC_GPS, "lon")
        alt = ul.series(ulog, TOPIC_GPS, "alt")
        is_new_format = False
    except KeyError:
        try:
            lat = ul.series(ulog, TOPIC_GPS, "latitude_deg")
            lon = ul.series(ulog, TOPIC_GPS, "longitude_deg")
            alt = ul.series(ulog, TOPIC_GPS, "altitude_msl_m")
            is_new_format = True
        except KeyError as exc:
            return [_row("gps", "horizontal_noise_sigma", ref.GPS, None, str(exc))]

    mask = slice_to_window(lat.t_s, window)
    if mask.sum() < 4:
        return [_row("gps", "horizontal_noise_sigma", ref.GPS, None, "too few GPS samples on ground")]

    if is_new_format:
        lat_deg = lat.values[mask]
        lon_deg = lon.values[mask]
        alt_m = alt.values[mask]
    else:
        lat_deg = lat.values[mask] * ul.GPS_LATLON_SCALE
        lon_deg = lon.values[mask] * ul.GPS_LATLON_SCALE
        alt_m = alt.values[mask] * ul.GPS_ALT_SCALE
    north, east = ns.latlon_to_local_m(
        lat_deg, lon_deg, float(np.mean(lat_deg)), float(np.mean(lon_deg))
    )

    rows.append(
        _row("gps", "horizontal_noise_sigma", ref.GPS, ns.horizontal_position_noise(north, east))
    )
    rows.append(_row("gps", "altitude_noise_sigma", ref.GPS, ns.white_noise_std(alt_m)))
    rows.append(_row("gps", "update_rate_hz", ref.GPS, ns.sample_rate_hz(lat.t_s)))

    try:
        vn = ul.series(ulog, TOPIC_GPS, "vel_n_m_s").values[mask]
        ve = ul.series(ulog, TOPIC_GPS, "vel_e_m_s").values[mask]
        vd = ul.series(ulog, TOPIC_GPS, "vel_d_m_s").values[mask]
        vel = float(np.mean([ns.white_noise_std(v) for v in (vn, ve, vd)]))
        rows.append(_row("gps", "velocity_noise_sigma", ref.GPS, vel))
    except (KeyError, ValueError) as exc:
        rows.append(_row("gps", "velocity_noise_sigma", ref.GPS, None, str(exc)))
    return rows
