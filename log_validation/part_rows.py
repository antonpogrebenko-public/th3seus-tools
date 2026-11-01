"""Per-part rows over vehicles: statistics, the vehicle threshold, Other and GPS fix classes."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .calibration_ranges import vehicle_percentiles
from .reference_airframes import finite_number
from .th3seus_parts import (
    FIX_CLASS_STANDALONE_3D, FIX_TYPE_OF_CLASS, KIND_CANDIDATE, KIND_EXCLUDED, KIND_EXISTING,
    OTHER, PARTS, PartType, classify_log_part, current_value, fix_class)

# calibration_ranges.DEFAULT_MIN_VEHICLES is 5 and means something else (ranges); proposing a
# part value needs 10 vehicles.
DEFAULT_MIN_VEHICLES = 10
DEFAULT_ALLAN_SLOPE_TOLERANCE = 0.15
# hitl-daemon 0.27.0 crates/simulation/src/loop_runner.rs: SENSOR_50HZ_PERIOD_US = 20_000,
# so the sensors run at 1e6 / 20_000 = 50 Hz.
DEFAULT_SIM_RATE_HZ = 50.0

STATUS_EXISTING = "existing"
STATUS_CANDIDATE = "candidate"
STATUS_OTHER = "other"
REASON_BELOW_THRESHOLD = "below the vehicle threshold"
REASON_WHITE_NOISE = "too few logs pass the white-noise check"
REASON_NO_LOGS = "no log measured it"

GPS = "gps"
FIX_MODE_KEY = "gps.fix_type_mode"
STANDALONE_FIX_TYPE = FIX_TYPE_OF_CLASS[FIX_CLASS_STANDALONE_3D]

Sample = tuple[str, str | None, float]


@dataclass(frozen=True)
class RowRules:
    min_vehicles: int = DEFAULT_MIN_VEHICLES
    allan_slope_tolerance: float = DEFAULT_ALLAN_SLOPE_TOLERANCE
    sim_rate_baro_hz: float = DEFAULT_SIM_RATE_HZ
    sim_rate_mag_hz: float = DEFAULT_SIM_RATE_HZ


@dataclass(frozen=True)
class PartRow:
    part: str
    variant: str
    status: str
    log_names: tuple[str, ...]
    parameter: str
    measurement: str
    fix_class: str | None
    logs: int
    vehicles: int
    median: float | None
    p25: float | None
    p75: float | None
    current: float
    proposed: float | None
    reason: str | None
    checked: int
    failed_slope: int
    failed_resolution: int
    median_rate_hz: float | None
    median_factor: float | None
    factor_flagged: bool


@dataclass(frozen=True)
class PartTables:
    rows: tuple[PartRow, ...]
    log_ids: tuple[str, ...]


@dataclass(frozen=True)
class _Pool:
    """The samples of one row: (log, vehicle, value) plus the log names behind them."""
    samples: tuple[Sample, ...] = ()
    names: frozenset[str] = frozenset()


def _log_name(entry: Mapping[str, Any], part_type: str) -> str | None:
    part = (entry.get("parts") or {}).get(part_type) or {}
    return part.get("name")


def _counts_toward_variant(part_type: str, entry_name: str, measurements: Mapping[str, Any]) -> bool:
    if part_type != GPS:
        return True
    mode = finite_number(measurements, FIX_MODE_KEY)
    return mode is not None and mode == FIX_TYPE_OF_CLASS[fix_class(entry_name)]


def _counts_toward_other(part_type: str, measurements: Mapping[str, Any]) -> bool:
    if part_type != GPS:
        return True
    mode = finite_number(measurements, FIX_MODE_KEY)
    return mode is not None and mode == STANDALONE_FIX_TYPE


def _targets(part_type: str, entry: Mapping[str, Any]) -> tuple[tuple[str, str] | None, bool, str | None]:
    """(own row as (variant, status) or None, counts toward Other, log name)."""
    name = _log_name(entry, part_type)
    classified = classify_log_part(part_type, name)
    if classified.kind == KIND_EXCLUDED:
        return None, False, name
    measurements = entry.get("measurements") or {}
    to_other = _counts_toward_other(part_type, measurements)
    if classified.kind not in (KIND_EXISTING, KIND_CANDIDATE):
        return None, to_other, name
    if not _counts_toward_variant(part_type, classified.entry, measurements):
        return None, to_other, name
    status = STATUS_EXISTING if classified.kind == KIND_EXISTING else STATUS_CANDIDATE
    return (classified.entry, status), to_other, name


def _collect(part_type: str, measurement: str, entries: Sequence[Mapping[str, Any]]
             ) -> tuple[dict[tuple[str, str], _Pool], _Pool]:
    own: dict[tuple[str, str], list[Sample]] = defaultdict(list)
    own_names: dict[tuple[str, str], set[str]] = defaultdict(set)
    other: list[Sample] = []
    for entry in entries:
        value = finite_number(entry.get("measurements") or {}, measurement)
        if value is None:
            continue
        target, to_other, name = _targets(part_type, entry)
        sample = (entry["log_id"], entry.get("vehicle_id"), value)
        if target is not None:
            own[target].append(sample)
            own_names[target].add(name)
        if to_other:
            other.append(sample)
    pools = {k: _Pool(tuple(v), frozenset(own_names[k])) for k, v in own.items()}
    return pools, _Pool(tuple(other))


def _none_if_nan(value: float) -> float | None:
    return None if math.isnan(value) else value


def _row_fix_class(part_type: str, variant: str, status: str) -> str | None:
    if part_type != GPS:
        return None
    return FIX_CLASS_STANDALONE_3D if status == STATUS_OTHER else fix_class(variant)


def _make_row(part_type: str, variant: str, status: str, parameter: str, measurement: str,
              pool: _Pool, rules: RowRules) -> PartRow:
    logs, vehicles, pct = vehicle_percentiles(pool.samples)
    median, p25, p75 = (_none_if_nan(pct[p]) for p in (50, 25, 75))
    enough = median is not None and vehicles >= rules.min_vehicles
    reason = None if enough else (REASON_BELOW_THRESHOLD if median is not None else REASON_NO_LOGS)
    return PartRow(
        part=part_type, variant=variant, status=status,
        log_names=tuple(sorted(pool.names)) if status != STATUS_OTHER else (),
        parameter=parameter, measurement=measurement,
        fix_class=_row_fix_class(part_type, variant, status),
        logs=logs, vehicles=vehicles, median=median, p25=p25, p75=p75,
        current=current_value(part_type, variant, parameter),
        proposed=median if enough else None, reason=reason,
        checked=0, failed_slope=0, failed_resolution=0,
        median_rate_hz=None, median_factor=None, factor_flagged=False,
    )


def _variant_order(spec: PartType, pools: Mapping[tuple[str, str], _Pool]) -> list[tuple[str, str]]:
    existing = [(v.name, STATUS_EXISTING) for v in spec.variants if v.name != OTHER]
    known = {name for name, _ in existing}
    candidates = sorted({(n, s) for n, s in pools if n not in known})
    return existing + candidates + [(OTHER, STATUS_OTHER)]


def _row_with_ids(part_type: str, name: str, status: str, parameter: str, measurement: str,
                  pool: _Pool, rules: RowRules) -> tuple[str, str, PartRow, tuple[str, ...]]:
    row = _make_row(part_type, name, status, parameter, measurement, pool, rules)
    ids = tuple(sorted({s[0] for s in pool.samples})) if row.proposed is not None else ()
    return name, status, row, ids


def _part_rows(part_type: str, entries: Sequence[Mapping[str, Any]], rules: RowRules
               ) -> tuple[list[tuple[str, str, PartRow, tuple[str, ...]]], ...]:
    """Per parameter, the (variant, status, row, log IDs) list in table order."""
    spec = PARTS[part_type]
    collected = {m: _collect(part_type, m[1], entries) for m in spec.measured}
    merged: dict[tuple[str, str], _Pool] = {}
    for pools, _ in collected.values():
        merged.update(pools)
    order = _variant_order(spec, merged)
    result = []
    for parameter, measurement in spec.measured:
        pools, other = collected[(parameter, measurement)]
        result.append([
            _row_with_ids(part_type, name, status, parameter, measurement,
                          other if status == STATUS_OTHER else pools.get((name, status), _Pool()),
                          rules)
            for name, status in order])
    return tuple(result)


def _ordered_rows(part_type: str, entries: Sequence[Mapping[str, Any]], rules: RowRules
                  ) -> list[tuple[PartRow, tuple[str, ...]]]:
    """(row, log IDs behind its proposed value) in table order."""
    per_parameter = _part_rows(part_type, entries, rules)
    count = len(per_parameter[0])
    return [per_parameter[p][i][2:] for i in range(count) for p in range(len(per_parameter))]


def build_part_tables(entries: Sequence[Mapping[str, Any]], rules: RowRules = RowRules()
                      ) -> PartTables:
    ordered = [pair for part_type in PARTS for pair in _ordered_rows(part_type, entries, rules)]
    ids = {i for _, row_ids in ordered for i in row_ids}
    return PartTables(rows=tuple(r for r, _ in ordered), log_ids=tuple(sorted(ids)))
