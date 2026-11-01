"""The part tables in the calibration report, and the part files of the calibration directory.

`load_part_outputs` reads and checks what the `part_tables` pass wrote; `render_part_tables`
turns it into the report section; `write_part_files` writes the parts files, the Rust text
and the log-ID list into the staging folder through the private-string guard.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .calibration_markdown import NA, _num, _table
from .outputs import write_json, write_text
from .part_rows import REASON_WHITE_NOISE
from .part_rust import ALIASES_FILE, RUST_FILES
from .part_tables import BOARD_TABLE_FILE, PART_TABLES_FILE, RUST_DIR

__all__ = ["BATTERY_RUST_FILE", "PART_TABLES_SECTION", "PartOutputs", "TOP_BOARDS",
           "load_part_outputs", "render_part_tables", "write_part_files"]

PART_TABLES_SECTION = "Part tables"
TOP_BOARDS = 10
BATTERY_RUST_FILE = "battery_tables.rs"
PARTS_LOG_IDS_FILE = "parts.txt"
FLAG_TEXT = "yes (beyond 3x)"
RATE_TYPES = ("baro", "mag")
PART_HEADINGS = (("imu", "IMU"), ("baro", "Barometer"), ("mag", "Magnetometer"),
                 ("gps", "GPS receiver"))
ENTRY_LABELS = {"existing": "existing", "candidate": "candidate (new)", "other": "Other"}
ROW_HEADERS = ("Part", "Parameter", "Entry", "Current", "Proposed", "Logs", "Vehicles", "p25",
               "p75", "No proposal because")
RATE_HEADERS = ("Logged rate (Hz)", "Factor", "Flag")
BOARD_HEADERS = ("Board", "Logs", "Vehicles", "IMU", "Baro", "Mag", "IMU_GYRO_RATEMAX",
                 "IMU_GYRO_CUTOFF")


@dataclass(frozen=True)
class PartOutputs:
    """What the part_tables pass wrote: both JSON documents and the Rust text by file name."""

    tables: dict
    boards: dict
    rust: dict[str, str]


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise ValueError(f"the parts folder lacks {path.name}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"{path.name} is not valid JSON: {e.msg} (line {e.lineno})") from e
    except UnicodeDecodeError as e:
        raise ValueError(f"{path.name} is not valid UTF-8 text") from e


def _check_tables(tables: Any) -> dict:
    if not isinstance(tables, dict):
        raise ValueError(f"{PART_TABLES_FILE} must hold a JSON object")
    rows, log_ids = tables.get("rows"), tables.get("log_ids")
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise ValueError(f"{PART_TABLES_FILE} needs 'rows' as a list of objects")
    if not isinstance(log_ids, list) or not all(isinstance(i, str) for i in log_ids):
        raise ValueError(f"{PART_TABLES_FILE} needs 'log_ids' as a list of strings")
    return tables


def _check_boards(boards: Any) -> dict:
    if not isinstance(boards, dict) or not isinstance(boards.get("boards"), dict):
        raise ValueError(f"{BOARD_TABLE_FILE} must be an object with a 'boards' object")
    return boards


def _read_rust(path: Path) -> str:
    try:
        return path.read_bytes().decode("ascii")
    except FileNotFoundError as e:
        raise ValueError(f"the parts folder lacks {RUST_DIR}/{path.name}") from e
    except UnicodeDecodeError as e:
        raise ValueError(f"{RUST_DIR}/{path.name} is not ASCII text") from e


def load_part_outputs(parts_dir: Path) -> PartOutputs:
    """Read the part_tables outputs in `parts_dir`; ValueError when one is missing or malformed."""
    parts_dir = Path(parts_dir)
    tables = _check_tables(_read_json(parts_dir / PART_TABLES_FILE))
    boards = _check_boards(_read_json(parts_dir / BOARD_TABLE_FILE))
    names = (*RUST_FILES.values(), ALIASES_FILE)
    rust = {name: _read_rust(parts_dir / RUST_DIR / name) for name in names}
    return PartOutputs(tables=tables, boards=boards, rust=rust)


def write_part_files(stage: Path, forbidden: Sequence[str], parts: PartOutputs) -> None:
    """Write the parts files and `log-ids/parts.txt` (sorted, no duplicates) into `stage`."""
    write_json(stage / PART_TABLES_FILE, parts.tables, forbidden)
    write_json(stage / BOARD_TABLE_FILE, parts.boards, forbidden)
    for name, text in parts.rust.items():
        write_text(stage / RUST_DIR / name, text, forbidden)
    ids = sorted(set(parts.tables["log_ids"]))
    write_text(stage / "log-ids" / PARTS_LOG_IDS_FILE, "".join(f"{i}\n" for i in ids), forbidden)


def _parameters_line(tables: Mapping[str, Any]) -> str:
    p = tables.get("parameters") or {}
    return (f"Rules: at least {p.get('min_vehicles', NA)} vehicles per proposed value; Allan "
            f"slope within {p.get('allan_slope_tolerance', NA)} of -0.5; simulator rates "
            f"{p.get('sim_rate_baro_hz', NA)} Hz (barometer) and {p.get('sim_rate_mag_hz', NA)} "
            f"Hz (magnetometer). Rust text for hitl-physics {tables.get('hitl_physics_version', NA)}"
            f" and hitl-daemon {tables.get('hitl_daemon_version', NA)}.")


def _part_cell(row: Mapping[str, Any]) -> str:
    names = ", ".join(str(n) for n in row.get("log_names") or [])
    return f"{row.get('variant', NA)} ({names})" if names else str(row.get("variant", NA))


def _reason_cell(row: Mapping[str, Any]) -> str:
    reason = row.get("reason")
    if not reason:
        return ""
    if row.get("part") in RATE_TYPES and reason == REASON_WHITE_NOISE:
        return (f"{reason} ({row.get('failed_slope', NA)} failed the slope, "
                f"{row.get('failed_resolution', NA)} the resolution, "
                f"of {row.get('checked', NA)} checked)")
    return str(reason)


def _row_cells(row: Mapping[str, Any]) -> list[str]:
    parameter = str(row.get("parameter", NA))
    if row.get("fix_class"):
        parameter = f"{parameter} ({row['fix_class']})"
    entry = str(row.get("status", NA))
    cells = [_part_cell(row), parameter, ENTRY_LABELS.get(entry, entry),
             _num(row.get("current")), _num(row.get("proposed")), str(row.get("logs", NA)),
             str(row.get("vehicles", NA)), _num(row.get("p25")), _num(row.get("p75")),
             _reason_cell(row)]
    if row.get("part") in RATE_TYPES:
        cells += [_num(row.get("median_rate_hz")), _num(row.get("median_factor")),
                  FLAG_TEXT if row.get("factor_flagged") else ""]
    return cells


def _part_type_lines(part_type: str, heading: str, rows: Sequence[Mapping[str, Any]]) -> list[str]:
    lines = [f"### {heading}", ""]
    mine = [r for r in rows if r.get("part") == part_type]
    if not mine:
        return lines + ["No rows.", ""]
    headers = ROW_HEADERS + (RATE_HEADERS if part_type in RATE_TYPES else ())
    return lines + _table(headers, [_row_cells(r) for r in mine])


def _shares(shares: Any) -> str:
    if not isinstance(shares, Mapping) or not shares:
        return NA
    ranked = sorted(shares.items(), key=lambda kv: (-kv[1], kv[0]))
    return ", ".join(f"{chip} {_num(share, '.2f')}" for chip, share in ranked)


def _board_cells(name: str, board: Mapping[str, Any]) -> list[str]:
    return [name, str(board.get("logs", NA)), str(board.get("vehicles", NA)),
            _shares(board.get("imu")), _shares(board.get("baro")), _shares(board.get("mag")),
            _num(board.get("imu_gyro_ratemax_median")), _num(board.get("imu_gyro_cutoff_median"))]


def _board_lines(boards: Mapping[str, Any]) -> list[str]:
    table = boards.get("boards") or {}
    top = sorted(table.items(), key=lambda kv: (-kv[1].get("vehicles", 0), kv[0]))[:TOP_BOARDS]
    lines = ["### Boards", "",
             f"The {TOP_BOARDS} boards with the most vehicles, with the share of vehicles whose "
             "most common chip is each one.", ""]
    if top:
        lines += _table(BOARD_HEADERS, [_board_cells(name, b) for name, b in top])
    else:
        lines += ["No boards.", ""]
    return lines + [f"The full board table is in `{BOARD_TABLE_FILE}`.", ""]


def render_part_tables(tables: Mapping[str, Any], boards: Mapping[str, Any]) -> list[str]:
    """The body of the part-tables section: parameters, one table per part type, the boards."""
    rows = tables.get("rows") or []
    lines = [_parameters_line(tables), ""]
    for part_type, heading in PART_HEADINGS:
        lines += _part_type_lines(part_type, heading, rows)
    return lines + _board_lines(boards)
