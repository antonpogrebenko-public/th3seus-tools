"""Batch analyze logs and output structured JSON with hardware metadata.

Usage:
    python -m log_validation.batch_analyze data/downloaded/ -o results.json
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pyulog import ULog

from . import report
from .hardware import extract_hardware_info, is_simulation
from .manifest import (
    DEFAULT_MAX_PER_VEHICLE,
    ManifestError,
    add_selection_args,
    format_counts,
    select_logs,
    selection_counts,
)
from .parallel import run_per_log
from .outputs import PrivateDataError, forbidden_strings, write_json
from .part_ids import (
    PART_TOPICS,
    TOPIC_GPS_POSITION,
    TOPIC_MAG,
    empty_parts,
    empty_parts_json,
    identify_parts,
)
from .part_measures import DEFAULT_GPS_REST_MIN_S, gyro_rate_params
from .report import TOPIC_ACCEL, TOPIC_AIR_DATA, TOPIC_COMBINED, TOPIC_GYRO

SIMULATION_ERROR = "simulation log skipped (SITL/HIL)"

# The only topics any of the measurements below read. Kept next to the parse so
# adding a measurement that needs another topic fails loudly here rather than
# silently returning nothing.
REQUIRED_TOPICS = tuple(dict.fromkeys((
    TOPIC_ACCEL,
    TOPIC_GYRO,
    TOPIC_COMBINED,
    TOPIC_MAG,
    TOPIC_AIR_DATA,
    TOPIC_GPS_POSITION,
    *PART_TOPICS,
)))


@dataclass(frozen=True)
class LogResult:
    """Analysis result for a single log file."""

    log_id: str
    hardware: dict[str, Any]
    measurements: dict[str, float | None]
    errors: list[str]
    vehicle_id: str | None = None
    airframe_id: int | None = None
    parts: dict[str, dict[str, Any]] = field(default_factory=empty_parts_json)


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


def _failure(log_id: str, what: str, error: Exception) -> str:
    """The entry text for a failure: the exception type only, as the message can hold a path.

    The full message goes to stderr with the log ID."""
    name = type(error).__name__
    print(f"{log_id}: {what}: {name}: {error}", file=sys.stderr)
    return f"{what}: {name}"


def analyze_log(
    path: Path,
    vehicle_id: str | None = None,
    loader: Callable[[Path], Any] = load_filtered,
    gps_rest_min_s: float = DEFAULT_GPS_REST_MIN_S,
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
            errors=[_failure(log_id, "load failed", e)],
            vehicle_id=vehicle_id,
        )

    try:
        airframe_id = _airframe_id(ulog.initial_parameters)
    except Exception as e:
        airframe_id = None
        errors.append(_failure(log_id, "parameters unavailable", e))

    # Extract hardware info
    hw = None
    try:
        hw = extract_hardware_info(ulog.msg_info_dict, ulog.initial_parameters)
        hw_dict = asdict(hw)
    except Exception as e:
        hw_dict = {}
        errors.append(_failure(log_id, "hardware extraction failed", e))

    params = getattr(ulog, "initial_parameters", None) or {}
    try:
        parts = identify_parts(ulog, params)
    except Exception as e:
        parts = empty_parts()
        errors.append(_failure(log_id, "part identification failed", e))

    # Skip simulation logs: SITL/HIL report synthetic noise that does not
    # represent real hardware, so they must not pollute the noise profiles.
    # Detection is firmware-version-robust (devtype, board name, or bus type).
    if hw is not None and is_simulation(hw):
        return LogResult(
            log_id=log_id,
            hardware=hw_dict,
            measurements={},
            errors=[SIMULATION_ERROR],
            vehicle_id=vehicle_id,
            airframe_id=airframe_id,
            parts=parts.to_json(),
        )

    # Run noise analysis
    try:
        rows = report.analyze(ulog, parts, gps_rest_min_s)
        for row in rows:
            key = f"{row.sensor}.{row.param}"
            if row.measured is not None:
                measurements[key] = row.measured
            elif row.note:
                errors.append(f"{key}: {row.note}")
    except Exception as e:
        errors.append(_failure(log_id, "analysis failed", e))

    measurements.update(gyro_rate_params(params))

    return LogResult(
        log_id=log_id,
        hardware=hw_dict,
        measurements=measurements,
        errors=errors,
        vehicle_id=vehicle_id,
        airframe_id=airframe_id,
        parts=parts.to_json(),
    )


def batch_analyze(
    log_dir: Path,
    output_path: Path | None = None,
    limit: int | None = None,
    manifest_path: Path | None = None,
    max_per_vehicle: int = DEFAULT_MAX_PER_VEHICLE,
    jobs: int = 1,
    loader: Callable[[Path], Any] = load_filtered,
    gps_rest_min_s: float = DEFAULT_GPS_REST_MIN_S,
) -> list[LogResult]:
    """Analyze the selected logs of a directory; results are sorted by log ID."""
    selection = select_logs(log_dir, manifest_path, max_per_vehicle)
    print(f"Selection: {format_counts(selection_counts(selection))}", file=sys.stderr)
    paths = list(selection.paths)
    if limit:
        paths = paths[:limit]
    vehicles = [selection.vehicle_of.get(p.stem) for p in paths]

    tasks = [(p, v, loader, gps_rest_min_s) for p, v in zip(paths, vehicles)]
    results = run_per_log(_analyze_task, tasks, jobs, _died_result)
    for i, path in enumerate(paths, 1):
        _progress(i, len(paths), path)

    print()  # newline after progress
    results.sort(key=lambda r: r.log_id)

    if output_path:
        write_json(output_path, [asdict(r) for r in results], forbidden_strings(log_dir))
        print(f"Wrote {len(results)} results to {output_path}")

    return results


_Task = tuple[Path, str | None, Callable[[Path], Any], float]


def _analyze_task(task: _Task) -> LogResult:
    path, vehicle_id, loader, gps_rest_min_s = task
    return analyze_log(path, vehicle_id, loader, gps_rest_min_s)


def _died_result(task: _Task) -> LogResult:
    path, vehicle_id, _, _ = task
    return LogResult(
        log_id=path.stem,
        hardware={},
        measurements={},
        errors=["worker process died"],
        vehicle_id=vehicle_id,
    )


def _progress(done: int, total: int, path: Path) -> None:
    print(f"\r[{done}/{total}] {path.name[:40]:<40}", end="", flush=True)


def _non_negative(text: str) -> float:
    value = float(text)
    if not value >= 0:
        raise argparse.ArgumentTypeError("must not be negative")
    return value


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Batch analyze PX4 logs")
    parser.add_argument("log_dir", type=Path, help="Directory containing .ulg files")
    parser.add_argument("-o", "--output", type=Path, help="Output JSON file")
    parser.add_argument("-n", "--limit", type=int, help="Limit number of logs")
    parser.add_argument(
        "--gps-rest-min-s",
        type=_non_negative,
        default=DEFAULT_GPS_REST_MIN_S,
        metavar="S",
        help="shortest at-rest window (s) that gets a GPS spread (default %(default)s)",
    )
    add_selection_args(parser)
    args = parser.parse_args(argv)

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
            gps_rest_min_s=args.gps_rest_min_s,
        )
    except (ManifestError, PrivateDataError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
