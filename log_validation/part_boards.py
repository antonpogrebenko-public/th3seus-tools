"""Board table: per flight-controller board, the chips its vehicles carry and the gyro rate
parameter medians. Shares are over vehicles (each vehicle's most common chip per part type)."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from .batch_analyze import SIMULATION_ERROR
from .calibration_ranges import vehicle_percentiles
from .reference_airframes import finite_number, vehicle_key

BOARD_PART_TYPES = ("imu", "baro", "mag")
PARAM_RATEMAX = "param.IMU_GYRO_RATEMAX"
PARAM_CUTOFF = "param.IMU_GYRO_CUTOFF"


def _board_of(entry: Mapping[str, Any]) -> str | None:
    hardware = entry.get("hardware")
    board = hardware.get("fc_board") if isinstance(hardware, Mapping) else None
    return board if isinstance(board, str) and board else None


def _counts(entry: Mapping[str, Any]) -> bool:
    """A readable board and not a simulation log."""
    return _board_of(entry) is not None and SIMULATION_ERROR not in (entry.get("errors") or ())


def _chip_name(entry: Mapping[str, Any], part_type: str) -> str | None:
    parts = entry.get("parts")
    part = parts.get(part_type) if isinstance(parts, Mapping) else None
    name = part.get("name") if isinstance(part, Mapping) else None
    return name if isinstance(name, str) and name else None


def _most_common(names: Sequence[str]) -> str | None:
    if not names:
        return None
    counts = Counter(names)
    return min(counts, key=lambda n: (-counts[n], n))


def _chip_shares(vehicles: Mapping[Any, Sequence[Mapping[str, Any]]], part_type: str,
                 ) -> dict[str, float]:
    chips = Counter(
        chip for logs in vehicles.values()
        if (chip := _most_common([n for e in logs if (n := _chip_name(e, part_type))])))
    return {chip: chips[chip] / len(vehicles) for chip in sorted(chips)}


def _median_over_vehicles(logs: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = []
    for entry in logs:
        value = finite_number(entry.get("measurements") or {}, key)
        if value is not None:
            values.append((str(entry.get("log_id")), entry.get("vehicle_id"), value))
    median = vehicle_percentiles(values)[2][50]
    return None if math.isnan(median) else median


def _board_row(logs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    vehicles: dict[Any, list[Mapping[str, Any]]] = defaultdict(list)
    for entry in logs:
        vehicles[vehicle_key(entry)].append(entry)
    row: dict[str, Any] = {"logs": len(logs), "vehicles": len(vehicles)}
    for part_type in BOARD_PART_TYPES:
        row[part_type] = _chip_shares(vehicles, part_type)
    row["imu_gyro_ratemax_median"] = _median_over_vehicles(logs, PARAM_RATEMAX)
    row["imu_gyro_cutoff_median"] = _median_over_vehicles(logs, PARAM_CUTOFF)
    return row


def board_table(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """{"boards": {board: row}} over the readable, non-simulation entries, boards sorted."""
    by_board: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for entry in entries:
        if _counts(entry):
            by_board[_board_of(entry)].append(entry)
    return {"boards": {board: _board_row(by_board[board]) for board in sorted(by_board)}}
