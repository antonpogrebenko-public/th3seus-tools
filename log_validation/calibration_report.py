"""Write the calibration directory: report, ranges, log-ID lists and normalized inputs.

Usage:
    python -m log_validation.calibration_report --noise noise.json --battery battery.json \\
        --response response.json --airframes airframes.json --run-info run-info.json \\
        --out-dir calibration [--forbid STR ...]

Everything is written through `outputs` with the forbidden strings (home, user, host,
the folders of the input files and each `--forbid` value), into a staging folder first,
so a refused write leaves the output folder as it was. The Run date line of report.md
is the `date` of run-info.json, so the same inputs give the same files.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from .aggregate import aggregate_profiles, to_json
from .calibration_noise import noise_before_after
from .calibration_ranges import (
    DEFAULT_MIN_VEHICLES,
    RangeRow,
    log_id_lists,
    ranges_csv,
    ranges_table,
    vehicle_percentiles,
)
from .calibration_render import render_report
from .outputs import (
    PrivateDataError,
    check_out_dir,
    input_forbidden,
    write_json,
    write_staged,
    write_text,
)

__all__ = ["RangeRow", "log_id_lists", "noise_before_after", "ranges_csv", "ranges_table",
           "render_report", "vehicle_percentiles", "write_calibration_dir", "main"]

RUN_INFO_KEYS = {"date": str, "tool_commit": str, "parameters": dict, "commands": list}
AIRFRAME_KEYS = ("min_vehicles", "comparisons", "recommendations")


def _load(path: Path, kind: type, what: str) -> Any:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"{what} is not valid JSON: {e.msg} (line {e.lineno})") from e
    if not isinstance(value, kind):
        raise ValueError(f"{what} must hold a JSON {kind.__name__}")
    return value


def _check_run_info(run_info: dict) -> None:
    for key, kind in RUN_INFO_KEYS.items():
        if not isinstance(run_info.get(key), kind):
            raise ValueError(f"run-info needs {key!r} as a {kind.__name__}")
    inputs = run_info.get("inputs")
    if inputs is None:
        return
    counts_ok = isinstance(inputs, dict) and all(
        isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in inputs.values())
    if not counts_ok:
        raise ValueError("run-info 'inputs', when given, must map names to counts of 0 or more")


def _check_airframes(airframes: dict) -> None:
    missing = [k for k in AIRFRAME_KEYS if k not in airframes]
    if missing:
        raise ValueError(f"airframes JSON lacks {', '.join(missing)}")


def _by_log_id(entries: list) -> list[dict]:
    if not all(isinstance(e, dict) and isinstance(e.get("log_id"), str) for e in entries):
        raise ValueError("per-log results must be objects with a log_id")
    return sorted(entries, key=lambda e: e["log_id"])


def _stage(stage: Path, forbidden: Sequence[str], noise: list[dict], battery: dict,
           response: list[dict], airframes: dict, run_info: dict) -> None:
    ranges = ranges_table(noise, battery, response)
    write_text(stage / "report.md",
               render_report(noise, battery, response, airframes, ranges, run_info), forbidden)
    write_text(stage / "ranges.csv", ranges_csv(ranges), forbidden)
    for name, ids in log_id_lists(noise, battery, response).items():
        write_text(stage / "log-ids" / f"{name}.txt", "".join(f"{i}\n" for i in ids), forbidden)
    write_json(stage / "noise-results.json", noise, forbidden)
    write_json(stage / "noise-profiles.json", to_json(aggregate_profiles(noise)), forbidden)
    write_json(stage / "battery-fit.json", battery, forbidden)
    write_json(stage / "response-results.json", response, forbidden)
    write_json(stage / "airframes.json", airframes, forbidden)
    write_json(stage / "run-info.json", run_info, forbidden)


def write_calibration_dir(
    out_dir: Path, noise_path: Path, battery_path: Path, response_path: Path,
    airframes_path: Path, run_info_path: Path, extra_forbidden: Sequence[str] = (),
) -> None:
    """Write report.md, ranges.csv, log-ids/{noise,battery,response}.txt and the normalized inputs.

    Raises ValueError for an input of the wrong shape (a nested row included) or an
    `out_dir` that is not empty. Raises PrivateDataError when any output would hold
    a private string; then nothing is written to `out_dir`."""
    check_out_dir(out_dir)
    inputs = (noise_path, battery_path, response_path, airframes_path, run_info_path)
    noise = _by_log_id(_load(noise_path, list, "noise results"))
    battery = _load(battery_path, dict, "battery fit")
    response = _by_log_id(_load(response_path, list, "response results"))
    airframes = _load(airframes_path, dict, "airframes")
    run_info = _load(run_info_path, dict, "run-info")
    _check_airframes(airframes)
    _check_run_info(run_info)
    forbidden = input_forbidden(inputs, extra_forbidden)

    def stage_files(stage: Path) -> None:
        try:
            _stage(stage, forbidden, noise, battery, response, airframes, run_info)
        except (KeyError, TypeError, AttributeError) as e:
            # The top-level shapes are checked above; a nested row of the wrong shape
            # shows up only while rendering. Nothing has reached out_dir yet.
            raise ValueError(f"an input holds a malformed row ({type(e).__name__}: {e})") from e

    write_staged(out_dir, stage_files)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m log_validation.calibration_report",
        description="Write the calibration directory from the pass outputs.",
        epilog="Exit status: 0 done, 2 error or private data refused.")
    for name, what in (("noise", "noise pass results JSON (batch_analyze)"),
                       ("battery", "battery fit JSON (battery_fit --out-json)"),
                       ("response", "response pass results JSON"),
                       ("airframes", "reference-airframe comparison JSON"),
                       ("run-info", "run info JSON: date, tool_commit, parameters, commands, "
                                    "and optionally inputs (counts)")):
        parser.add_argument(f"--{name}", type=Path, required=True, help=what)
    parser.add_argument("--out-dir", type=Path, required=True, help="calibration directory")
    parser.add_argument("--forbid", action="append", default=[], metavar="STR",
                        help="a string no output may contain (repeatable), such as an account ID")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        write_calibration_dir(args.out_dir, args.noise, args.battery, args.response,
                              args.airframes, args.run_info, args.forbid)
    except (PrivateDataError, ValueError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
