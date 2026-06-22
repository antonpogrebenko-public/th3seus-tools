"""Aggregate batch results by hardware configuration.

Computes statistics per hardware group with IQR outlier filtering.

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
    p25: float
    p75: float
    mean: float
    std: float
    model_value: float
    model_unit: str


@dataclass(frozen=True)
class HardwareGroup:
    """Statistics grouped by hardware configuration."""

    fc_board: str
    imu_name: str | None
    sample_count: int
    sensors: dict[str, SensorStats]


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


def aggregate_by_hardware(
    results: list[dict[str, Any]],
    group_by: list[str] = ["fc_board", "imu_name"],
) -> list[HardwareGroup]:
    """Group results by hardware and compute filtered statistics."""

    # Group measurements by hardware key
    groups: dict[tuple, list[dict[str, float]]] = defaultdict(list)

    for r in results:
        hw = r.get("hardware", {})
        if not hw:
            continue

        # Build group key
        key_parts = []
        for field in group_by:
            val = hw.get(field)
            key_parts.append(str(val) if val else "UNKNOWN")
        key = tuple(key_parts)

        # Collect valid measurements
        measurements = r.get("measurements", {})
        if measurements:
            groups[key].append(measurements)

    # Compute statistics per group
    output: list[HardwareGroup] = []

    for key, measurements_list in groups.items():
        if len(measurements_list) < 3:
            continue

        # Collect all params across this group
        all_params: set[str] = set()
        for m in measurements_list:
            all_params.update(m.keys())

        sensors: dict[str, SensorStats] = {}

        for param in sorted(all_params):
            values = []
            for m in measurements_list:
                if param in m and m[param] is not None:
                    values.append(m[param])

            if len(values) < 3:
                continue

            arr = np.array(values)
            filtered = iqr_filter(arr)

            if len(filtered) < 2:
                continue

            model_val, model_unit = get_model_ref(param)

            sensors[param] = SensorStats(
                param=param,
                count=len(filtered),
                median=float(np.median(filtered)),
                p25=float(np.percentile(filtered, 25)),
                p75=float(np.percentile(filtered, 75)),
                mean=float(np.mean(filtered)),
                std=float(np.std(filtered, ddof=1)),
                model_value=model_val,
                model_unit=model_unit,
            )

        if sensors:
            output.append(HardwareGroup(
                fc_board=key[0] if len(key) > 0 else "UNKNOWN",
                imu_name=key[1] if len(key) > 1 and key[1] != "UNKNOWN" else None,
                sample_count=len(measurements_list),
                sensors=sensors,
            ))

    # Sort by sample count descending
    output.sort(key=lambda g: g.sample_count, reverse=True)
    return output


def to_json(groups: list[HardwareGroup]) -> list[dict[str, Any]]:
    """Convert to JSON-serializable format."""
    out = []
    for g in groups:
        out.append({
            "fc_board": g.fc_board,
            "imu_name": g.imu_name,
            "sample_count": g.sample_count,
            "sensors": {
                k: {
                    "param": v.param,
                    "count": v.count,
                    "median": v.median,
                    "p25": v.p25,
                    "p75": v.p75,
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


def print_summary(groups: list[HardwareGroup]) -> None:
    """Print human-readable summary."""
    print(f"\n{'='*80}")
    print(f"Aggregated {len(groups)} hardware configurations")
    print(f"{'='*80}\n")

    for g in groups[:10]:  # Top 10
        print(f"FC: {g.fc_board} | IMU: {g.imu_name or 'N/A'} | n={g.sample_count}")
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

    groups = aggregate_by_hardware(results)

    if args.summary or not args.output:
        print_summary(groups)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(to_json(groups), f, indent=2)
        print(f"Wrote {len(groups)} groups to {args.output}")


if __name__ == "__main__":
    main()
