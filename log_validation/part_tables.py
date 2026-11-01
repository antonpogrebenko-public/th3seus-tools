"""Per-part calibration tables from the merged noise results.

Usage:
    python -m log_validation.part_tables NOISE_JSON --out-dir DIR --run-date YYYY-MM-DD \\
        [--min-vehicles 10] [--allan-slope-tolerance 0.15] [--sim-rate-baro 50] \\
        [--sim-rate-mag 50] [--catalog-spellings FILE]

NOISE_JSON is the merged per-log noise results (a JSON list of entries). FILE maps a part
type to {catalog string: log part name}; those spellings are emitted next to the log names
of new variants.

Outputs, in DIR (which must be new or empty): part-tables.json, board-table.json and
rust/imu_chip.rs, rust/baro_chip.rs, rust/mag_chip.rs, rust/gps_chipset.rs and
rust/daemon_aliases.rs. Every input and option is checked and every output is built in
memory first; the files are then written through the private-string guard into a staging
folder and copied into DIR, so an error leaves DIR as it was. The same inputs give
byte-identical files.

Exit status: 0 done, 2 error (bad option, unreadable or malformed input, private data
refused, non-empty DIR).
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .outputs import (
    PrivateDataError, check_out_dir, input_forbidden, read_json, write_json, write_staged,
    write_text)
from .part_boards import board_table
from .part_rows import (
    DEFAULT_ALLAN_SLOPE_TOLERANCE, DEFAULT_MIN_VEHICLES, DEFAULT_SIM_RATE_HZ, PartTables,
    RowRules, build_part_tables)
from .part_rust import emit_rust_files, is_plain
from .th3seus_parts import HITL_DAEMON_VERSION, HITL_PHYSICS_VERSION, PART_TYPES

PART_TABLES_FILE = "part-tables.json"
BOARD_TABLE_FILE = "board-table.json"
RUST_DIR = "rust"
RUN_DATE_PATTERN = r"\d{4}-\d{2}-\d{2}"


def _check_entry(entry: Any) -> None:
    if not isinstance(entry, dict) or not isinstance(entry.get("log_id"), str):
        raise ValueError("each noise entry must be an object with a string log_id")
    if not isinstance(entry.get("measurements"), dict):
        raise ValueError(f"noise entry {entry['log_id']!r}: measurements must be an object")
    for key, kind in (("vehicle_id", (str, type(None))), ("hardware", dict), ("errors", list)):
        if key in entry and not isinstance(entry[key], kind):
            raise ValueError(f"noise entry {entry['log_id']!r}: {key} has the wrong type")
    _check_parts(entry["log_id"], entry.get("parts"), "parts" in entry)


def _check_parts(log_id: str, parts: Any, present: bool) -> None:
    if not present:
        return
    if not isinstance(parts, dict):
        raise ValueError(f"noise entry {log_id!r}: parts must be an object")
    for part_type, part in parts.items():
        if not isinstance(part, dict):
            raise ValueError(f"noise entry {log_id!r}: parts.{part_type} must be an object")
        if not isinstance(part.get("name"), (str, type(None))):
            raise ValueError(f"noise entry {log_id!r}: parts.{part_type}.name must be a string "
                             "or null")


def check_noise_entries(value: Any) -> list[dict]:
    """The entries when `value` is a list of well-formed noise entries; ValueError otherwise."""
    if not isinstance(value, list):
        raise ValueError("the noise results must be a JSON list of entries")
    for entry in value:
        _check_entry(entry)
    return value


def load_catalog_spellings(path: Path) -> dict[str, dict[str, str]]:
    """{part type: {catalog string: log part name}}; ValueError for any other shape, and for
    a string that is not printable ASCII or holds `"` or `\\` (the strings are written into
    the Rust text, as aliases and in comments)."""
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"the catalog spellings are not valid JSON: {e.msg} "
                         f"(line {e.lineno})") from e
    if not isinstance(value, dict):
        raise ValueError("the catalog spellings must be a JSON object")
    for part_type, spellings in value.items():
        if part_type not in PART_TYPES:
            raise ValueError(f"catalog spellings: unknown part type {part_type!r}")
        if not isinstance(spellings, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in spellings.items()):
            raise ValueError(f"catalog spellings for {part_type!r} must map strings to strings")
        for text in (*spellings, *spellings.values()):
            if not is_plain(text):
                raise ValueError(f"catalog spellings for {part_type!r}: {ascii(text)} is not "
                                 "printable ASCII without '\"' and '\\'")
    return value


def part_tables_document(tables: PartTables, rules: RowRules, run_date: str) -> dict:
    return {
        "hitl_physics_version": HITL_PHYSICS_VERSION,
        "hitl_daemon_version": HITL_DAEMON_VERSION,
        "run_date": run_date,
        "parameters": asdict(rules),
        "rows": [asdict(row) for row in tables.rows],
        "log_ids": list(tables.log_ids),
    }


def _read_json(path: Path, what: str) -> Any:
    try:
        return read_json(path)
    except ValueError as e:
        raise ValueError(f"{what}: {e}") from e


def write_part_tables(noise_path: Path, out_dir: Path, run_date: str, rules: RowRules,
                      catalog_path: Path | None = None) -> None:
    """Build every output in memory, then write them all into `out_dir` or none.

    Raises ValueError for a malformed input or a non-empty `out_dir`, OSError for an
    unreadable one and PrivateDataError when an output would hold a private string."""
    check_out_dir(out_dir)
    entries = check_noise_entries(_read_json(noise_path, "the noise results"))
    catalog = load_catalog_spellings(catalog_path) if catalog_path else {}
    tables = build_part_tables(entries, rules)
    documents = {PART_TABLES_FILE: part_tables_document(tables, rules, run_date),
                 BOARD_TABLE_FILE: board_table(entries)}
    sources = emit_rust_files(tables, run_date, catalog)
    forbidden = input_forbidden([p for p in (noise_path, catalog_path) if p])

    def stage_files(stage: Path) -> None:
        for name, document in documents.items():
            write_json(stage / name, document, forbidden)
        for name, text in sources.items():
            write_text(stage / RUST_DIR / name, text, forbidden)

    write_staged(out_dir, stage_files)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m log_validation.part_tables",
        description="Write the per-part tables, the board table and the Rust text.",
        epilog="Exit status: 0 done, 2 error or private data refused.")
    parser.add_argument("noise", type=Path, metavar="NOISE_JSON",
                        help="merged per-log noise results (a JSON list)")
    parser.add_argument("--out-dir", type=Path, required=True, help="new or empty folder")
    parser.add_argument("--run-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--min-vehicles", type=int, default=DEFAULT_MIN_VEHICLES)
    parser.add_argument("--allan-slope-tolerance", type=float,
                        default=DEFAULT_ALLAN_SLOPE_TOLERANCE)
    parser.add_argument("--sim-rate-baro", type=float, default=DEFAULT_SIM_RATE_HZ, metavar="HZ")
    parser.add_argument("--sim-rate-mag", type=float, default=DEFAULT_SIM_RATE_HZ, metavar="HZ")
    parser.add_argument("--catalog-spellings", type=Path, default=None, metavar="FILE",
                        help="JSON {part type: {catalog string: log part name}}")
    return parser


def _rules_from(args: argparse.Namespace) -> RowRules:
    """The row rules, or ValueError for an option value out of range."""
    if args.min_vehicles < 1:
        raise ValueError("--min-vehicles must be 1 or more")
    for flag, value in (("--allan-slope-tolerance", args.allan_slope_tolerance),
                        ("--sim-rate-baro", args.sim_rate_baro),
                        ("--sim-rate-mag", args.sim_rate_mag)):
        if not (math.isfinite(value) and value > 0):
            raise ValueError(f"{flag} must be a finite number above 0")
    if not re.fullmatch(RUN_DATE_PATTERN, args.run_date, re.ASCII):
        raise ValueError("--run-date must look like YYYY-MM-DD")
    return RowRules(min_vehicles=args.min_vehicles,
                    allan_slope_tolerance=args.allan_slope_tolerance,
                    sim_rate_baro_hz=args.sim_rate_baro, sim_rate_mag_hz=args.sim_rate_mag)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        rules = _rules_from(args)
        write_part_tables(args.noise, args.out_dir, args.run_date, rules, args.catalog_spellings)
    except (PrivateDataError, ValueError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
