"""Batch analyze logs and output structured JSON with hardware metadata.

Usage:
    python -m log_validation.batch_analyze data/downloaded/ -o results.json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
from pyulog import ULog

from . import report
from .hardware import HardwareInfo, extract_hardware_info, is_simulation


@dataclass
class LogResult:
    """Analysis result for a single log file."""

    log_id: str
    hardware: dict[str, Any]
    measurements: dict[str, float | None]
    errors: list[str]


def analyze_log(path: Path) -> LogResult | None:
    """Analyze a single log file, return structured result."""
    log_id = path.stem
    errors: list[str] = []
    measurements: dict[str, float | None] = {}

    try:
        ulog = ULog(str(path))
    except Exception as e:
        return LogResult(
            log_id=log_id,
            hardware={},
            measurements={},
            errors=[f"load failed: {e}"],
        )

    # Extract hardware info
    hw = None
    try:
        hw = extract_hardware_info(ulog.msg_info_dict, ulog.initial_parameters)
        hw_dict = asdict(hw)
    except Exception as e:
        hw_dict = {}
        errors.append(f"hardware extraction failed: {e}")

    # Skip simulation logs: SITL/HIL report synthetic noise that does not
    # represent real hardware, so they must not pollute the noise profiles.
    # Detection is firmware-version-robust (devtype, board name, or bus type).
    if hw is not None and is_simulation(hw):
        return LogResult(
            log_id=log_id,
            hardware=hw_dict,
            measurements={},
            errors=["simulation log skipped (SITL/HIL)"],
        )

    # Run noise analysis
    try:
        rows = report.analyze(ulog)
        for row in rows:
            key = f"{row.sensor}.{row.param}"
            if row.measured is not None:
                measurements[key] = row.measured
            elif row.note:
                errors.append(f"{key}: {row.note}")
    except Exception as e:
        errors.append(f"analysis failed: {e}")

    return LogResult(
        log_id=log_id,
        hardware=hw_dict,
        measurements=measurements,
        errors=errors,
    )


def batch_analyze(
    log_dir: Path, output_path: Path | None = None, limit: int | None = None
) -> list[LogResult]:
    """Analyze all logs in directory."""
    paths = sorted(log_dir.glob("*.ulg"))
    if limit:
        paths = paths[:limit]

    results: list[LogResult] = []
    total = len(paths)

    for i, path in enumerate(paths, 1):
        print(f"\r[{i}/{total}] {path.name[:40]:<40}", end="", flush=True)
        result = analyze_log(path)
        if result:
            results.append(result)

    print()  # newline after progress

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(
                [asdict(r) for r in results],
                f,
                indent=2,
                default=_json_default,
            )
        print(f"Wrote {len(results)} results to {output_path}")

    return results


def _json_default(obj: Any) -> Any:
    """JSON serializer for numpy types."""
    if isinstance(obj, (np.integer, np.floating)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch analyze PX4 logs")
    parser.add_argument("log_dir", type=Path, help="Directory containing .ulg files")
    parser.add_argument("-o", "--output", type=Path, help="Output JSON file")
    parser.add_argument("-n", "--limit", type=int, help="Limit number of logs")
    args = parser.parse_args()

    if not args.log_dir.is_dir():
        print(f"Error: {args.log_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    batch_analyze(args.log_dir, args.output, args.limit)


if __name__ == "__main__":
    main()
