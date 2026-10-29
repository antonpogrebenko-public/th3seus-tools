"""Aggregate batch results into grouped noise profiles.

Computes statistics per MCU family, board and IMU group with IQR outlier filtering.

Usage:
    python -m log_validation.aggregate results.json -o aggregated.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from . import reference as ref


@dataclass(frozen=True)
class SensorStats:
    """Aggregated statistics for a sensor parameter."""

    param: str
    count: int
    median: float
    p5: float
    p25: float
    p75: float
    p95: float
    mean: float
    std: float
    model_value: float
    model_unit: str


@dataclass(frozen=True)
class ProfileGroup:
    """Statistics for one group: an MCU family, a board or an IMU chip."""

    kind: str
    key: str
    sample_count: int
    sensors: dict[str, SensorStats]


ALL_KEY = "ALL"


def mcu_family(mcu: str | None) -> str | None:
    """Map an MCU string to the family name used by the TH3SEUS seeding script."""
    if not mcu:
        return None
    upper = mcu.upper()
    for family in ("STM32F4", "STM32F7", "STM32H7"):
        if family in upper:
            return family
    if "KINETIS" in upper or "K66" in upper:
        return "NXP_K66"
    return None


def iqr_filter(values: np.ndarray, k: float = 1.5) -> np.ndarray:
    """Remove outliers outside k * IQR from Q1/Q3."""
    if len(values) < 4:
        return values
    q1, q3 = np.percentile(values, [25, 75])
    iqr = q3 - q1
    lower = q1 - k * iqr
    upper = q3 + k * iqr
    return values[(values >= lower) & (values <= upper)]


def get_model_ref(param: str) -> tuple[float, str]:
    """Get model reference value and unit for a parameter."""
    param_tables = {
        "gyro.gyro_noise_density": (ref.IMU, "gyro_noise_density"),
        "gyro.gyro_bias_sigma": (ref.IMU, "gyro_bias_sigma"),
        "accel.accel_noise_density": (ref.IMU, "accel_noise_density"),
        "baro.noise_sigma": (ref.BARO, "noise_sigma"),
        "mag.noise_sigma_gauss": (ref.MAG, "noise_sigma_gauss"),
        "gps.horizontal_noise_sigma": (ref.GPS, "horizontal_noise_sigma"),
        "gps.altitude_noise_sigma": (ref.GPS, "altitude_noise_sigma"),
        "gps.velocity_noise_sigma": (ref.GPS, "velocity_noise_sigma"),
        "gps.update_rate_hz": (ref.GPS, "update_rate_hz"),
    }
    if param in param_tables:
        table, key = param_tables[param]
        p = table[key]
        return p.value, p.unit
    return 0.0, ""


def _compute_sensors(
    measurements_list: list[dict[str, Any]], min_logs: int
) -> dict[str, SensorStats]:
    """Compute outlier-filtered statistics per parameter."""
    all_params: set[str] = set()
    for m in measurements_list:
        all_params.update(m.keys())

    sensors: dict[str, SensorStats] = {}
    for param in sorted(all_params):
        values = [m[param] for m in measurements_list if m.get(param) is not None]
        if len(values) < min_logs:
            continue
        filtered = iqr_filter(np.array(values, dtype=float))
        if len(filtered) < 2:
            continue
        p5, p25, p75, p95 = np.percentile(filtered, [5, 25, 75, 95])
        model_val, model_unit = get_model_ref(param)
        sensors[param] = SensorStats(
            param=param,
            count=len(filtered),
            median=float(np.median(filtered)),
            p5=float(p5),
            p25=float(p25),
            p75=float(p75),
            p95=float(p95),
            mean=float(np.mean(filtered)),
            std=float(np.std(filtered, ddof=1)),
            model_value=model_val,
            model_unit=model_unit,
        )
    return sensors


def aggregate_profiles(
    results: list[dict[str, Any]], min_logs: int = 3
) -> list[ProfileGroup]:
    """Group results by MCU family (plus ALL), board and IMU chip."""
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for r in results:
        measurements = r.get("measurements")
        if not measurements:
            continue
        hw = r.get("hardware") or {}
        buckets[("mcu_family", ALL_KEY)].append(measurements)
        family = mcu_family(hw.get("mcu"))
        if family:
            buckets[("mcu_family", family)].append(measurements)
        if hw.get("fc_board"):
            buckets[("board", str(hw["fc_board"]))].append(measurements)
        if hw.get("imu_name"):
            buckets[("imu", str(hw["imu_name"]))].append(measurements)

    output: list[ProfileGroup] = []
    for (kind, key), measurements_list in buckets.items():
        if len(measurements_list) < min_logs:
            continue
        sensors = _compute_sensors(measurements_list, min_logs)
        if sensors:
            output.append(ProfileGroup(kind, key, len(measurements_list), sensors))

    output.sort(key=lambda g: (g.kind, -g.sample_count, g.key))
    return output


def to_json(groups: list[ProfileGroup]) -> list[dict[str, Any]]:
    """Convert to JSON-serializable format."""
    out = []
    for g in groups:
        out.append({
            "kind": g.kind,
            "key": g.key,
            "sample_count": g.sample_count,
            "sensors": {
                k: {
                    "param": v.param,
                    "count": v.count,
                    "median": v.median,
                    "p5": v.p5,
                    "p25": v.p25,
                    "p75": v.p75,
                    "p95": v.p95,
                    "mean": v.mean,
                    "std": v.std,
                    "model_value": v.model_value,
                    "model_unit": v.model_unit,
                    "ratio_to_model": v.median / v.model_value if v.model_value else None,
                }
                for k, v in g.sensors.items()
            },
        })
    return out


def print_summary(groups: list[ProfileGroup]) -> None:
    """Print human-readable summary."""
    print(f"\n{'='*80}")
    print(f"Aggregated {len(groups)} profile groups")
    print(f"{'='*80}\n")

    for g in groups[:10]:  # Top 10
        print(f"{g.kind}: {g.key} | n={g.sample_count}")
        for param, s in sorted(g.sensors.items()):
            ratio = s.median / s.model_value if s.model_value else 0
            verdict = "ok" if 0.33 <= ratio <= 3.0 else "FLAG"
            print(f"  {param:35} {s.median:10.6f} (model: {s.model_value:.6f}) ratio={ratio:.2f}x [{verdict}]")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate batch analysis results")
    parser.add_argument("input", type=Path, help="Input JSON from batch_analyze")
    parser.add_argument("-o", "--output", type=Path, help="Output aggregated JSON")
    parser.add_argument("--summary", action="store_true", help="Print summary to stdout")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: {args.input} not found", file=sys.stderr)
        sys.exit(1)

    with open(args.input) as f:
        results = json.load(f)

    groups = aggregate_profiles(results)

    if args.summary or not args.output:
        print_summary(groups)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(to_json(groups), f, indent=2)
        print(f"Wrote {len(groups)} groups to {args.output}")


if __name__ == "__main__":
    main()
