"""Noise before and after the estimator fix, and the in-flight against at-rest gyro noise."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from .aggregate import ALL_KEY, ProfileGroup, aggregate_profiles
from .reference_airframes import finite_number
from .th3seus_model import AXES

TICK_HZ = 400.0
SENSORS = {
    "legacy_gyro": "gyro.legacy_noise_density",
    "new_gyro": "gyro.gyro_noise_density",
    "legacy_accel": "accel.legacy_noise_density",
    "new_accel": "accel.accel_noise_density",
}
AT_REST_GYRO = "gyro.gyro_noise_density"
INFLIGHT_RATE = "inflight_gyro_rate_hz"


def _median_of(group: ProfileGroup, param: str) -> float | None:
    stats = group.sensors.get(param)
    return stats.median if stats is not None else None


def _row(key: str, group: ProfileGroup) -> dict[str, Any]:
    row: dict[str, Any] = {"mcu_family": key, "n_logs": group.sample_count}
    for name, param in SENSORS.items():
        density = _median_of(group, param)
        row[f"{name}_density"] = density
        row[f"{name}_per_sample_400hz"] = None if density is None else density * math.sqrt(TICK_HZ)
    return row


def noise_before_after(noise: Sequence[dict], min_logs: int = 3) -> list[dict[str, Any]]:
    """One row per MCU family, then `ALL`: median densities and the noise per 400 Hz sample.

    "legacy" is the density of TH3SEUS's current estimator, "new" the fixed one.
    A family with fewer than `min_logs` logs has no row; a density the group
    lacks is None. Per-sample noise is density * sqrt(400)."""
    groups = {g.key: g for g in aggregate_profiles(list(noise), min_logs)
              if g.kind == "mcu_family"}
    rows = [_row(k, groups[k]) for k in sorted(groups) if k != ALL_KEY]
    if ALL_KEY in groups:
        rows.append(_row(ALL_KEY, groups[ALL_KEY]))
    return rows


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
    angular velocity above the cut-off, at the logged rate f; its density is
    std * sqrt(1 / f), the same basis as the at-rest density (rad/s/sqrt(Hz)).
    This treats the in-flight signal as white, which it is not exactly."""
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
