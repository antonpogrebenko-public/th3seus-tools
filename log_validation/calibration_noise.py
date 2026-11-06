"""Noise before and after the estimator fix, and the in-flight against at-rest gyro noise."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from .aggregate import ALL_KEY, mcu_family
from .reference_airframes import finite_number
from .th3seus_model import AXES

TICK_HZ = 400.0
# (legacy key, new key) per sensor: the current TH3SEUS estimator and the fixed one.
PAIRED_DENSITIES = {
    "gyro": ("gyro.legacy_noise_density", "gyro.gyro_noise_density"),
    "accel": ("accel.legacy_noise_density", "accel.accel_noise_density"),
}
AT_REST_GYRO = "gyro.gyro_noise_density"
INFLIGHT_RATE = "inflight_gyro_rate_hz"


def _per_sample(density: float | None) -> float | None:
    return None if density is None else density * math.sqrt(TICK_HZ)


def _pairs(entries: Sequence[Mapping[str, Any]], sensor: str) -> list[tuple[float, float]]:
    """(legacy, new) of each entry that has both as finite numbers."""
    legacy_key, new_key = PAIRED_DENSITIES[sensor]
    values = [(finite_number(m, legacy_key), finite_number(m, new_key))
              for m in (e.get("measurements") or {} for e in entries)]
    return [(old, new) for old, new in values if old is not None and new is not None]


def _sensor_cells(entries: Sequence[Mapping[str, Any]], sensor: str,
                  min_logs: int) -> dict[str, Any]:
    pairs = _pairs(entries, sensor)
    enough = len(pairs) >= min_logs
    legacy = float(np.median([p[0] for p in pairs])) if enough else None
    new = float(np.median([p[1] for p in pairs])) if enough else None
    return {f"{sensor}_n_logs": len(pairs),
            f"legacy_{sensor}_density": legacy, f"new_{sensor}_density": new,
            f"legacy_{sensor}_per_sample_400hz": _per_sample(legacy),
            f"new_{sensor}_per_sample_400hz": _per_sample(new)}


def _row(key: str, entries: Sequence[Mapping[str, Any]], min_logs: int) -> dict[str, Any]:
    row: dict[str, Any] = {"mcu_family": key}
    for sensor in PAIRED_DENSITIES:
        row.update(_sensor_cells(entries, sensor, min_logs))
    return row


def noise_before_after(noise: Sequence[dict], min_logs: int = 3) -> list[dict[str, Any]]:
    """One row per MCU family, then `ALL`: paired median densities and the noise per 400 Hz sample.

    "legacy" is the density of TH3SEUS's current estimator, "new" the fixed one.
    Per sensor, both medians are taken over the same logs, those with both
    values, and `<sensor>_n_logs` is their number; with fewer than `min_logs`
    such logs the sensor's densities are None. A family with no sensor at
    `min_logs` has no row. Per-sample noise is density * sqrt(400)."""
    by_family: dict[str, list[dict]] = defaultdict(list)
    for entry in noise:
        family = mcu_family((entry.get("hardware") or {}).get("mcu"))
        if family:
            by_family[family].append(entry)
    rows = [_row(k, by_family[k], min_logs) for k in sorted(by_family)]
    rows.append(_row(ALL_KEY, noise, min_logs))
    return [r for r in rows if any(r[f"{s}_n_logs"] >= min_logs for s in PAIRED_DENSITIES)]


@dataclass(frozen=True)
class VibrationEvidence:
    """In-flight gyro noise on the density basis against the at-rest density."""

    at_rest_density: float
    inflight_density: float
    ratio: float
    n_at_rest: int
    n_inflight: int
    median_inflight_std: float
    median_rate_hz: float


def _inflight_log(measurements: Mapping[str, Any]) -> tuple[float, float, float] | None:
    """(density, std, rate_hz) of one log: mean of the axes' std, std / sqrt(rate)."""
    stds = [finite_number(measurements, f"inflight_gyro_noise.{a}") for a in AXES]
    rate = finite_number(measurements, INFLIGHT_RATE)
    if rate is None or rate <= 0 or any(s is None for s in stds):
        return None
    std = float(np.mean(stds))
    return std / math.sqrt(rate), std, rate


def vibration_evidence(noise: Sequence[dict], response: Sequence[dict]) -> VibrationEvidence | None:
    """Median in-flight gyro noise over median at-rest density, or None without both.

    Conversion: the in-flight value is a per-sample std (rad/s) of the logged
    angular velocity minus its moving average, at the logged rate f; its density
    is std * sqrt(1 / f), the same basis as the at-rest density (rad/s/sqrt(Hz)).
    This treats the in-flight signal as white up to f / 2, which it is not: PX4
    low-passes the logged angular velocity at IMU_GYRO_CUTOFF, usually well below
    f / 2, so its noise power lies in a narrower band than the sqrt(f) divisor
    assumes. The in-flight density, and so the ratio, is biased low."""
    rest = [v for v in (finite_number(e.get("measurements") or {}, AT_REST_GYRO) for e in noise)
            if v is not None]
    flights = [x for x in (_inflight_log(e.get("measurements") or {}) for e in response)
               if x is not None]
    if not rest or not flights:
        return None
    rest_density = float(np.median(rest))
    if rest_density <= 0:
        return None
    density = float(np.median([f[0] for f in flights]))
    return VibrationEvidence(rest_density, density, density / rest_density, len(rest),
                             len(flights), float(np.median([f[1] for f in flights])),
                             float(np.median([f[2] for f in flights])))
