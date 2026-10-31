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
