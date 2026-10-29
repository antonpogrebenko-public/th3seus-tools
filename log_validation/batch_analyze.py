"""Batch analyze logs and output structured JSON with hardware metadata.

Usage:
    python -m log_validation.batch_analyze data/downloaded/ -o results.json
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pyulog import ULog

from . import report
from .hardware import extract_hardware_info, is_simulation
from .manifest import (
    DEFAULT_MAX_PER_VEHICLE,
    ManifestError,
    add_selection_args,
    select_logs,
)
from .parallel import run_per_log
from .outputs import PrivateDataError, forbidden_strings, write_json
from .report import (
    TOPIC_ACCEL,
    TOPIC_AIR_DATA,
    TOPIC_COMBINED,
    TOPIC_GPS,
    TOPIC_GYRO,
    TOPIC_MAG,
)

# The only topics any of the measurements below read. Kept next to the parse so
# adding a measurement that needs another topic fails loudly here rather than
# silently returning nothing.
REQUIRED_TOPICS = (
    TOPIC_ACCEL,
    TOPIC_GYRO,
    TOPIC_COMBINED,
    TOPIC_MAG,
    TOPIC_AIR_DATA,
    TOPIC_GPS,
)


@dataclass(frozen=True)
class LogResult:
    """Analysis result for a single log file."""

    log_id: str
    hardware: dict[str, Any]
    measurements: dict[str, float | None]
    errors: list[str]
    vehicle_id: str | None = None
    airframe_id: int | None = None


def load_filtered(path: Path) -> ULog:
    """Parse only the topics the analysis reads.

    `ULog(path)` decodes all 59 topics in a log to reach the few in `report.py`;
    measured at 5.51 s against 1.04 s filtered on a 4.2 MB log, a 5.3x
    difference. `msg_info_dict` and `initial_parameters`, which
    `extract_hardware_info` needs, come from the log header and are unaffected
    by the filter.
    """
    return ULog(str(path), message_name_filter_list=list(REQUIRED_TOPICS))


def _airframe_id(params: dict[str, Any]) -> int | None:
    """SYS_AUTOSTART as an int, or None when absent or 0."""
    try:
        value = int(params.get("SYS_AUTOSTART", 0) or 0)
    except (TypeError, ValueError):
        return None
    return value or None


def analyze_log(
    path: Path,
    vehicle_id: str | None = None,
    loader: Callable[[Path], Any] = load_filtered,
) -> LogResult:
    """Analyze a single log file, return structured result."""
    log_id = path.stem
    errors: list[str] = []
    measurements: dict[str, float | None] = {}

    try:
        ulog = loader(path)
    except Exception as e:
        return LogResult(
            log_id=log_id,
            hardware={},
            measurements={},
            errors=[f"load failed: {e}"],
            vehicle_id=vehicle_id,
        )

    try:
        airframe_id = _airframe_id(ulog.initial_parameters)
    except Exception as e:
        airframe_id = None
        errors.append(f"parameters unavailable: {e}")

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
            vehicle_id=vehicle_id,
            airframe_id=airframe_id,
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
        print(f"{log_id}: analysis failed: {type(e).__name__}: {e}", file=sys.stderr)
        errors.append(f"analysis failed: {type(e).__name__}")

    return LogResult(
        log_id=log_id,
        hardware=hw_dict,
        measurements=measurements,
        errors=errors,
        vehicle_id=vehicle_id,
        airframe_id=airframe_id,
    )


def batch_analyze(
    log_dir: Path,
    output_path: Path | None = None,
    limit: int | None = None,
    manifest_path: Path | None = None,
    max_per_vehicle: int = DEFAULT_MAX_PER_VEHICLE,
    jobs: int = 1,
    loader: Callable[[Path], Any] = load_filtered,
) -> list[LogResult]:
    """Analyze the selected logs of a directory; results are sorted by log ID."""
    selection = select_logs(log_dir, manifest_path, max_per_vehicle)
    paths = list(selection.paths)
    if limit:
        paths = paths[:limit]
    vehicles = [selection.vehicle_of.get(p.stem) for p in paths]

    tasks = [(p, v, loader) for p, v in zip(paths, vehicles)]
    results = run_per_log(_analyze_task, tasks, jobs, _died_result)
    for i, path in enumerate(paths, 1):
        _progress(i, len(paths), path)

    print()  # newline after progress
    results.sort(key=lambda r: r.log_id)

    if output_path:
        write_json(output_path, [asdict(r) for r in results], forbidden_strings(log_dir))
        print(f"Wrote {len(results)} results to {output_path}")

    return results


def _analyze_task(task: tuple[Path, str | None, Callable[[Path], Any]]) -> LogResult:
    path, vehicle_id, loader = task
    return analyze_log(path, vehicle_id, loader)


def _died_result(task: tuple[Path, str | None, Callable[[Path], Any]]) -> LogResult:
    path, vehicle_id, _ = task
    return LogResult(
        log_id=path.stem,
        hardware={},
        measurements={},
        errors=["worker process died"],
        vehicle_id=vehicle_id,
    )


def _progress(done: int, total: int, path: Path) -> None:
    print(f"\r[{done}/{total}] {path.name[:40]:<40}", end="", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch analyze PX4 logs")
    parser.add_argument("log_dir", type=Path, help="Directory containing .ulg files")
    parser.add_argument("-o", "--output", type=Path, help="Output JSON file")
    parser.add_argument("-n", "--limit", type=int, help="Limit number of logs")
    add_selection_args(parser)
    args = parser.parse_args()

    if not args.log_dir.is_dir():
        print(f"Error: {args.log_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    try:
        batch_analyze(
            args.log_dir,
            args.output,
            args.limit,
            manifest_path=args.manifest,
            max_per_vehicle=args.max_per_vehicle,
            jobs=args.jobs,
        )
    except (ManifestError, PrivateDataError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
