"""Per-log measurements that identify a part: the barometer chip's own noise, the magnetometer's
rate and white-noise evidence, and the gyro rate parameters.

The barometer is measured on `sensor_baro` (the chip's reading), not on `vehicle_air_data`
(which is already filtered). Each function returns `Value`s; `report` turns them into rows.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np

from . import noise_stats as ns
from . import ulog_loader as ul
from .part_ids import TOPIC_BARO, TOPIC_MAG
from .reference_airframes import is_finite_number
from .stationary import Window, slice_to_window

# International standard atmosphere, sea level.
ISA_SEA_LEVEL_K = 288.15
ISA_LAPSE_K_PER_M = 0.0065
ISA_SEA_LEVEL_PA = 101325.0
ISA_EXPONENT = 0.190263
# A median pressure below this is hPa (the lowest real pressure is far above it in Pa).
HPA_BELOW_PA = 2000.0
PA_PER_HPA = 100.0

GYRO_RATE_PARAMS = ("IMU_GYRO_RATEMAX", "IMU_GYRO_CUTOFF")
PARAM_PREFIX = "param."

# GPS fix types (the PX4 `fix_type` field).
FIX_3D = 3
FIX_RTK_FIXED = 6
FIX_EXTRAPOLATED = 8
DEFAULT_GPS_REST_MIN_S = 60.0
MIN_GPS_REST_SAMPLES = 4
TOPIC_GPS_HINT = "vehicle_gps_position or sensor_gps"
GPS_FIELDS_OLD = ("lat", "lon", "alt")
GPS_FIELDS_NEW = ("latitude_deg", "longitude_deg", "altitude_msl_m")
# (key, topic field, unit, label) of the receiver's own accuracy estimates. A receiver that
# does not report an estimate sends 0, so samples of 0 or less do not count.
GPS_ACCURACY_FIELDS = (
    ("gps.eph_median_m", "eph", "m", "horizontal"),
    ("gps.epv_median_m", "epv", "m", "vertical"),
    ("gps.speed_accuracy_median_m_s", "s_variance_m_s", "m/s", "speed"),
)
AXES = 3


@dataclass(frozen=True)
class Value:
    """One measurement of a log; `key` is `<sensor>.<param>`, `value` None when it failed."""

    key: str
    value: float | None
    unit: str
    note: str = ""


def isa_altitude_m(pressure_pa: np.ndarray) -> np.ndarray:
    """Altitude (m) of a pressure (Pa) in the standard atmosphere."""
    ratio = np.asarray(pressure_pa, dtype=float) / ISA_SEA_LEVEL_PA
    return ISA_SEA_LEVEL_K / ISA_LAPSE_K_PER_M * (1.0 - ratio**ISA_EXPONENT)


def _baro_series(ulog: Any, window: Window, instance: int) -> tuple[np.ndarray, np.ndarray]:
    """Time and altitude (m about the window's mean pressure) of the distinct good readings."""
    s = ul.series(ulog, TOPIC_BARO, "pressure", instance)
    mask = slice_to_window(s.t_s, window)
    t, p = s.t_s[mask], s.values[mask]
    good = np.isfinite(p) & (p > 0)
    t, p = t[good], p[good]
    if p.size == 0:
        raise ValueError("no valid pressure in the still window")
    if np.median(p) < HPA_BELOW_PA:
        p = p * PA_PER_HPA
    t, p = ns.distinct_samples(t, p)
    return t, isa_altitude_m(p) - isa_altitude_m(np.mean(p))


def baro_chip_values(ulog: Any, window: Window, instance: int) -> list[Value]:
    """Noise, rate, Allan slope and resolution of the barometer chip in the still window."""
    try:
        t, alt = _baro_series(ulog, window, instance)
        return [
            Value("baro.chip_noise_sigma", ns.white_noise_std(alt), "m"),
            Value("baro.chip_rate_hz", ns.sample_rate_hz(t), "Hz"),
            Value("baro.chip_allan_slope", ns.allan_slope(alt), ""),
            Value("baro.chip_resolution_m", ns.resolution(alt), "m"),
        ]
    except (KeyError, ValueError) as exc:
        return [Value("baro.chip_noise_sigma", None, "m", str(exc))]


def mag_evidence_values(ulog: Any, window: Window, instance: int) -> list[Value]:
    """Rate, white-noise Allan slope and resolution of the magnetometer in the still window."""
    try:
        t, xyz = ul.xyz(ulog, TOPIC_MAG, instance)
        mask = slice_to_window(t, window)
        t, xyz = ns.distinct_samples(t[mask], xyz[mask])
        slope = float(np.mean([ns.allan_slope(xyz[:, a]) for a in range(AXES)]))
        resolution = float(min(ns.resolution(xyz[:, a]) for a in range(AXES)))
        return [
            Value("mag.rate_hz", ns.sample_rate_hz(t), "Hz"),
            Value("mag.allan_slope", slope, ""),
            Value("mag.resolution_gauss", resolution, "gauss"),
        ]
    except (KeyError, ValueError) as exc:
        return [Value("mag.rate_hz", None, "Hz", str(exc))]


def gyro_rate_params(params: Mapping[str, Any]) -> dict[str, float]:
    """The finite gyro rate parameters of the log header, keyed `param.<name>`."""
    return {
        f"{PARAM_PREFIX}{name}": float(params[name])
        for name in GYRO_RATE_PARAMS
        if is_finite_number(params.get(name))
    }


def gps_position(ulog: Any, topic: str) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Time (s), latitude (deg), longitude (deg) and altitude (m) of a GPS topic.

    Reads the older `lat`/`lon`/`alt` fields (1e-7 deg, mm) or the newer `latitude_deg`/
    `longitude_deg`/`altitude_msl_m`; raises KeyError when the topic has neither."""
    try:
        lat, lon, alt = (ul.series(ulog, topic, f) for f in GPS_FIELDS_OLD)
        return (
            lat.t_s,
            lat.values * ul.GPS_LATLON_SCALE,
            lon.values * ul.GPS_LATLON_SCALE,
            alt.values * ul.GPS_ALT_SCALE,
        )
    except KeyError:
        lat, lon, alt = (ul.series(ulog, topic, f) for f in GPS_FIELDS_NEW)
        return lat.t_s, lat.values, lon.values, alt.values


def _valid_fix_mask(fix: np.ndarray) -> np.ndarray:
    """Samples with a 3D fix or better, not extrapolated."""
    return (fix >= FIX_3D) & (fix != FIX_EXTRAPOLATED)


def _fix_mode(fix: np.ndarray) -> int:
    """The most common fix type; a tie goes to the larger."""
    kinds, counts = np.unique(fix.astype(int), return_counts=True)
    return int(kinds[counts == counts.max()].max())


def _median_of(ulog: Any, topic: str, field: str, mask: np.ndarray, label: str) -> float:
    """Median of the positive estimates; a receiver that reports none sends 0."""
    v = ul.series(ulog, topic, field).values[mask]
    v = v[np.isfinite(v)]
    if v.size == 0:
        raise ValueError(f"no finite '{field}' in {topic}")
    v = v[v > 0]
    if v.size == 0:
        raise ValueError(f"receiver reports no {label} accuracy")
    return float(np.median(v))


def gps_accuracy_values(ulog: Any, topic: str | None) -> list[Value]:
    """The receiver's fix type and accuracy estimates over the samples with a 3D fix."""
    if topic is None:
        return [Value("gps.eph_median_m", None, "m", f"no GPS topic ({TOPIC_GPS_HINT})")]
    try:
        fix = ul.series(ulog, topic, "fix_type").values
    except KeyError as exc:
        return [Value("gps.eph_median_m", None, "m", str(exc))]
    valid = _valid_fix_mask(fix)
    if not valid.any():
        return [Value("gps.eph_median_m", None, "m", f"no 3D fix in {topic}")]
    mode = _fix_mode(fix[valid])
    mask = fix == mode
    out = [Value("gps.fix_type_mode", float(mode), "")]
    for key, field, unit, label in GPS_ACCURACY_FIELDS:
        try:
            out.append(Value(key, _median_of(ulog, topic, field, mask, label), unit))
        except (KeyError, ValueError) as exc:
            out.append(Value(key, None, unit, str(exc)))
    return out


def gps_rest_spread_values(
    ulog: Any, topic: str | None, window: Window, min_window_s: float
) -> list[Value]:
    """Total standard deviation of horizontal position and of altitude in the still window.

    Horizontal is sqrt(0.5 * (var north + var east)), so slow wander counts as spread."""
    key = "gps.rest_spread_horizontal_m"
    if window.duration_s < min_window_s:
        note = f"at-rest window {window.duration_s:.0f} s is shorter than {min_window_s:g} s"
        return [Value(key, None, "m", note)]
    if topic is None:
        return [Value(key, None, "m", f"no GPS topic ({TOPIC_GPS_HINT})")]
    try:
        t, lat, lon, alt = gps_position(ulog, topic)
        mask = slice_to_window(t, window)
        try:
            mask &= _valid_fix_mask(ul.series(ulog, topic, "fix_type").values)
        except KeyError:
            pass
        lat, lon, alt = lat[mask], lon[mask], alt[mask]
        if lat.size < MIN_GPS_REST_SAMPLES:
            raise ValueError("too few GPS samples with a fix in the at-rest window")
        north, east = ns.latlon_to_local_m(lat, lon, float(np.mean(lat)), float(np.mean(lon)))
        horizontal = float(np.sqrt(0.5 * (np.var(north, ddof=1) + np.var(east, ddof=1))))
        return [
            Value(key, horizontal, "m"),
            Value("gps.rest_spread_altitude_m", float(np.std(alt, ddof=1)), "m"),
            Value("gps.rest_window_s", float(window.duration_s), "s"),
        ]
    except (KeyError, ValueError) as exc:
        return [Value(key, None, "m", str(exc))]
