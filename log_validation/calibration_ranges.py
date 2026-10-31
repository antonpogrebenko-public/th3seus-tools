"""Randomization ranges and log-ID lists for the calibration report.

Turns the per-log noise entries, the battery fit result and the response entries
into the ranges TH3SEUS randomizes over, and into the lists of Flight Review log
IDs that contributed to each analysis.

Ranges rules:

* Each log gives at most one value per quantity. Within a class, a vehicle's
  logs are reduced to the vehicle's median first, and the p5, p25, p50, p75 and
  p95 are taken over those vehicle medians with numpy's default linear
  interpolation, so a vehicle with many logs counts once.
* A log without a vehicle ID is its own vehicle, keyed so that it cannot collide
  with a real vehicle ID.
* Non-finite values never enter a percentile.
* A class is omitted when fewer than `min_vehicles` vehicles contributed. The
  "all" row of a quantity is written whenever at least one vehicle contributed,
  with its vehicle count, so a reader can judge it.
* The lag quantity is `lag_minus_filter_s.<axis>`, not `lag_s.<axis>`: the
  filter delay depends on each log's PX4 parameters, and a simulator running
  PX4's own filters would count it twice.
* Effectiveness uses the same values as the reference-airframe comparison,
  converted to motor-command space (`reference_airframes.log_effectiveness`).
  Logs with `bat_scale_en` == 1 are kept: PX4 scales the torque setpoint before
  publishing it, so the published torque is the allocator input.
* Classes: the battery cell count and the airframe ID. A response entry's cell
  count is `measurements["cell_count"]` and its airframe ID is top level; a
  battery row carries its own cell count (and an airframe ID if it has one); a
  noise entry has an airframe ID only. Class values are written as integers
  ("4", "4019").
* Rows are sorted by quantity, class kind, class value (plain string order).
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from .aggregate import profiled_measurements
from .reference_airframes import finite_number, is_finite_number, log_effectiveness, vehicle_key
from .th3seus_model import AXES

DEFAULT_MIN_VEHICLES = 5
PERCENTILE_POINTS = (5, 25, 50, 75, 95)
CSV_FLOAT_FORMAT = ".6g"
CSV_HEADER = "quantity,class_kind,class_value,n_logs,n_vehicles,p5,p25,p50,p75,p95"

ALL = "all"
CELL_COUNT = "cell_count"
AIRFRAME = "airframe"

NOISE_DENSITY_KEYS = ("gyro.gyro_noise_density", "accel.accel_noise_density")
BATTERY_QUANTITY = "battery.r_mohm_per_cell"
PLAIN_RESPONSE_KEYS = ("hover.command", "motor_lag_s", "rate.torque_setpoint_hz",
                       *(f"inflight_gyro_noise.{a}" for a in AXES))


@dataclass(frozen=True)
class RangeRow:
    quantity: str
    class_kind: str  # "all", "cell_count" or "airframe"
    class_value: str
    n_logs: int
    n_vehicles: int
    p5: float
    p25: float
    p50: float
    p75: float
    p95: float


@dataclass(frozen=True)
class _Sample:
    log_id: str
    vehicle: tuple[str, Any]
    value: float
    classes: tuple[tuple[str, str], ...]


def vehicle_percentiles(
    values: Sequence[tuple[str, str | None, float]],
) -> tuple[int, int, dict[int, float]]:
    """(n_logs, n_vehicles, {5, 25, 50, 75, 95: value}) over vehicle medians.

    Input is (log_id, vehicle_id, value). A missing vehicle ID makes the log its
    own vehicle. Non-finite values are ignored. With no finite value the
    percentiles are NaN."""
    by_vehicle: dict[tuple[str, Any], list[float]] = defaultdict(list)
    n_logs = 0
    for log_id, vehicle_id, value in values:
        if not is_finite_number(value):
            continue
        n_logs += 1
        by_vehicle[vehicle_key({"log_id": log_id, "vehicle_id": vehicle_id})].append(float(value))
    medians = [float(np.median(v)) for v in by_vehicle.values()]
    if not medians:
        return 0, 0, {p: math.nan for p in PERCENTILE_POINTS}
    points = np.percentile(medians, PERCENTILE_POINTS)
    return n_logs, len(medians), {p: float(x) for p, x in zip(PERCENTILE_POINTS, points)}


def ranges_table(noise: Sequence[dict], battery: Mapping[str, Any], response: Sequence[dict],
                 min_vehicles: int = DEFAULT_MIN_VEHICLES) -> list[RangeRow]:
    """Percentile rows for every quantity, overall and per class, sorted."""
    samples: dict[str, list[_Sample]] = defaultdict(list)
    for entry in noise:
        _add_noise(samples, entry)
    for row in battery.get("per_log_r") or []:
        _add_battery(samples, row)
    for entry in response:
        _add_response(samples, entry)
    rows: list[RangeRow] = []
    for quantity, quantity_samples in samples.items():
        rows.extend(_quantity_rows(quantity, quantity_samples, min_vehicles))
    return sorted(rows, key=lambda r: (r.quantity, r.class_kind, r.class_value))


def ranges_csv(rows: Sequence[RangeRow]) -> str:
    """Header plus one line per row, sorted; floats use a fixed `.6g` format."""
    ordered = sorted(rows, key=lambda r: (r.quantity, r.class_kind, r.class_value))
    lines = [CSV_HEADER]
    for r in ordered:
        floats = ",".join(format(x, CSV_FLOAT_FORMAT) for x in (r.p5, r.p25, r.p50, r.p75, r.p95))
        lines.append(f"{r.quantity},{r.class_kind},{r.class_value},{r.n_logs},{r.n_vehicles},{floats}")
    return "\n".join(lines) + "\n"


def log_id_lists(noise: Sequence[dict], battery: Mapping[str, Any],
                 response: Sequence[dict]) -> dict[str, list[str]]:
    """Sorted log IDs that contributed, under `noise`, `battery` and `response`.

    Noise: entries for which `contributes_to_noise` holds. Battery: the fit log
    IDs plus the scored holdout log IDs. Response: entries that give at least
    one value to a ranges-table quantity after the table's filters."""
    noise_ids = {e["log_id"] for e in noise if contributes_to_noise(e)}
    scratch: dict[str, list[_Sample]] = defaultdict(list)
    for entry in response:
        _add_response(scratch, entry)
    response_ids = {s.log_id for group in scratch.values() for s in group}
    battery_ids = set(battery.get("fit_log_ids") or []) | set(
        battery.get("holdout_scored_log_ids") or [])
    return {"noise": sorted(noise_ids), "battery": sorted(battery_ids),
            "response": sorted(response_ids)}


def contributes_to_noise(entry: Mapping[str, Any]) -> bool:
    """True when the entry gives a finite value to a published noise number.

    The published noise numbers are the new densities (the ranges), the legacy
    and new densities (the report's before and after table) and the
    noise-profile parameters (`noise-profiles.json`). The profiles take every
    finite noise measurement of an entry (`aggregate.profiled_measurements`: the
    logged parameters and the GPS fix type are not noise numbers), so an entry contributes
    when it has at least one."""
    return bool(profiled_measurements(entry.get("measurements") or {}))


# ---- samples ----

def _class_text(value: Any) -> str | None:
    """Stable text for a class value: integers without a decimal point; None if unusable."""
    if not is_finite_number(value) or value <= 0 or float(value) != int(value):
        return None
    return str(int(value))


def _classes(cell_count: Any, airframe_id: Any) -> tuple[tuple[str, str], ...]:
    out = []
    for kind, raw in ((CELL_COUNT, cell_count), (AIRFRAME, airframe_id)):
        text = _class_text(raw)
        if text is not None:
            out.append((kind, text))
    return tuple(out)


def _add_noise(samples: dict[str, list[_Sample]], entry: Mapping[str, Any]) -> None:
    measurements = entry.get("measurements") or {}
    classes = _classes(None, entry.get("airframe_id"))
    for key in NOISE_DENSITY_KEYS:
        value = finite_number(measurements, key)
        if value is not None:
            samples[key].append(_Sample(entry["log_id"], vehicle_key(entry), value, classes))


def _add_battery(samples: dict[str, list[_Sample]], row: Mapping[str, Any]) -> None:
    value = row.get("median_mohm_per_cell")
    if is_finite_number(value):
        classes = _classes(row.get("cell_count"), row.get("airframe_id"))
        samples[BATTERY_QUANTITY].append(
            _Sample(row["log_id"], vehicle_key(row), float(value), classes))


def _add_response(samples: dict[str, list[_Sample]], entry: Mapping[str, Any]) -> None:
    measurements = entry.get("measurements") or {}
    classes = _classes(measurements.get("cell_count"), entry.get("airframe_id"))
    values: dict[str, float | None] = {k: finite_number(measurements, k) for k in PLAIN_RESPONSE_KEYS}
    for axis in AXES:
        values[f"effectiveness.{axis}"] = log_effectiveness(measurements, axis)
        values[f"lag_minus_filter_s.{axis}"] = finite_number(
            measurements, f"lag_minus_filter_s.{axis}")
    for quantity, value in values.items():
        if value is not None:
            samples[quantity].append(_Sample(entry["log_id"], vehicle_key(entry), value, classes))


# ---- rows ----

def _quantity_rows(quantity: str, samples: Sequence[_Sample], min_vehicles: int) -> list[RangeRow]:
    groups: dict[tuple[str, str], list[_Sample]] = {(ALL, ALL): list(samples)}
    for sample in samples:
        for cls in sample.classes:
            groups.setdefault(cls, []).append(sample)
    rows = []
    for (kind, value), group in groups.items():
        n_logs, n_vehicles, p = vehicle_percentiles(
            [(s.log_id, _vehicle_id(s), s.value) for s in group])
        if n_vehicles >= (1 if kind == ALL else min_vehicles):
            rows.append(RangeRow(quantity, kind, value, n_logs, n_vehicles,
                                 p[5], p[25], p[50], p[75], p[95]))
    return rows


def _vehicle_id(sample: _Sample) -> str | None:
    kind, ident = sample.vehicle
    return str(ident) if kind == "vehicle" else None
