"""Per-part rows over vehicles: statistics, the vehicle threshold, Other and GPS fix classes."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import cached_property
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
# No log is eligible: none has a finite value, or (barometer and magnetometer) none has
# it with all three white-noise evidence values and a rate above 0.
REASON_NO_LOGS = "no eligible log"

GPS = "gps"
FIX_MODE_KEY = "gps.fix_type_mode"
STANDALONE_FIX_TYPE = FIX_TYPE_OF_CLASS[FIX_CLASS_STANDALONE_3D]

WHITE_NOISE_SLOPE = -0.5
FACTOR_FLAG = 3.0
# Per part type: the per-log evidence for the white-noise check (Allan slope, resolution, rate).
EVIDENCE_KEYS = {
    "baro": ("baro.chip_allan_slope", "baro.chip_resolution_m", "baro.chip_rate_hz"),
    "mag": ("mag.allan_slope", "mag.resolution_gauss", "mag.rate_hz"),
}

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


def white_noise_failures(slope: float, value: float, resolution: float, tolerance: float
                         ) -> tuple[bool, bool]:
    """(slope is off -1/2 by more than the tolerance, value is below the resolution)."""
    return abs(slope - WHITE_NOISE_SLOPE) > tolerance, value < resolution


def rate_factor(log_rate_hz: float, sim_rate_hz: float) -> float:
    """sqrt(sim / log) when the log is at least the simulator rate, else 1.

    The PX4 logger keeps one sample per logging interval instead of averaging, so a series
    logged below the simulator rate holds samples at the driver's own, unknown rate.
    """
    if log_rate_hz < sim_rate_hz:
        return 1.0
    return math.sqrt(sim_rate_hz / log_rate_hz)


@dataclass(frozen=True)
class _Obs:
    """One eligible log: the outcome of the check and, for a passing log, its value at the
    simulator rate (a failing log counts nowhere, so it keeps its logged value and no rate)."""
    log_id: str
    vehicle_id: str | None
    value: float
    failed_slope: bool = False
    failed_resolution: bool = False
    rate_hz: float | None = None
    factor: float | None = None

    @property
    def passed(self) -> bool:
        return not (self.failed_slope or self.failed_resolution)

    @property
    def sample(self) -> Sample:
        return self.log_id, self.vehicle_id, self.value


@dataclass(frozen=True)
class _Pool:
    """The eligible logs of one row plus the log names behind them."""
    obs: tuple[_Obs, ...] = ()
    names: frozenset[str] = frozenset()

    @cached_property
    def samples(self) -> tuple[Sample, ...]:
        """The samples that count toward the row: the logs that pass the check."""
        return tuple(o.sample for o in self.obs if o.passed)


def _sim_rate(part_type: str, rules: RowRules) -> float:
    return rules.sim_rate_baro_hz if part_type == "baro" else rules.sim_rate_mag_hz


def _observe(part_type: str, entry: Mapping[str, Any], value: float, rules: RowRules) -> _Obs | None:
    """None when the log is not eligible (it lacks the evidence for the check)."""
    log_id, vehicle_id = entry["log_id"], entry.get("vehicle_id")
    if part_type not in EVIDENCE_KEYS:
        return _Obs(log_id, vehicle_id, value)
    measurements = entry.get("measurements") or {}
    evidence = [finite_number(measurements, key) for key in EVIDENCE_KEYS[part_type]]
    if any(v is None for v in evidence) or evidence[2] <= 0:
        return None
    slope, res, rate = evidence
    failed_slope, failed_res = white_noise_failures(slope, value, res, rules.allan_slope_tolerance)
    if failed_slope or failed_res:
        return _Obs(log_id, vehicle_id, value, failed_slope, failed_res)
    factor = rate_factor(rate, _sim_rate(part_type, rules))
    return _Obs(log_id, vehicle_id, value * factor, rate_hz=rate, factor=factor)


def _log_name(entry: Mapping[str, Any], part_type: str) -> str | None:
    """The part's log name without surrounding whitespace, the string `classify_log_part`
    classifies; None when the entry has no readable one (unnamed)."""
    parts = entry.get("parts")
    part = parts.get(part_type) if isinstance(parts, Mapping) else None
    name = part.get("name") if isinstance(part, Mapping) else None
    return name.strip() if isinstance(name, str) else None


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


def _collect(part_type: str, measurement: str, entries: Sequence[Mapping[str, Any]],
             rules: RowRules) -> tuple[dict[tuple[str, str], _Pool], _Pool]:
    own: dict[tuple[str, str], list[_Obs]] = defaultdict(list)
    own_names: dict[tuple[str, str], set[str]] = defaultdict(set)
    other: list[_Obs] = []
    for entry in entries:
        value = finite_number(entry.get("measurements") or {}, measurement)
        obs = None if value is None else _observe(part_type, entry, value, rules)
        if obs is None:
            continue
        target, to_other, name = _targets(part_type, entry)
        if target is not None:
            own[target].append(obs)
            own_names[target].add(name)
        if to_other:
            other.append(obs)
    pools = {k: _Pool(tuple(v), frozenset(own_names[k])) for k, v in own.items()}
    return pools, _Pool(tuple(other))


def _none_if_nan(value: float) -> float | None:
    return None if math.isnan(value) else value


def _row_fix_class(part_type: str, variant: str, status: str) -> str | None:
    if part_type != GPS:
        return None
    return FIX_CLASS_STANDALONE_3D if status == STATUS_OTHER else fix_class(variant)


def _median_of(values: Sequence[float]) -> float | None:
    return statistics.median(values) if values else None


def _no_proposal_reason(pool: _Pool, rules: RowRules) -> str:
    """Why a row has no proposed value: no eligible log, white-noise failures, or too few."""
    if not pool.obs:
        return REASON_NO_LOGS
    _, eligible_vehicles, _ = vehicle_percentiles([o.sample for o in pool.obs])
    return REASON_WHITE_NOISE if eligible_vehicles >= rules.min_vehicles else REASON_BELOW_THRESHOLD


def _make_row(part_type: str, variant: str, status: str, parameter: str, measurement: str,
              pool: _Pool, rules: RowRules) -> PartRow:
    logs, vehicles, pct = vehicle_percentiles(pool.samples)
    median, p25, p75 = (_none_if_nan(pct[p]) for p in (50, 25, 75))
    enough = median is not None and vehicles >= rules.min_vehicles
    checked = len(pool.obs) if part_type in EVIDENCE_KEYS else 0
    passing = [o for o in pool.obs if o.passed and o.rate_hz is not None]
    factor = _median_of([o.factor for o in passing])
    return PartRow(
        part=part_type, variant=variant, status=status,
        log_names=tuple(sorted(pool.names)) if status != STATUS_OTHER else (),
        parameter=parameter, measurement=measurement,
        fix_class=_row_fix_class(part_type, variant, status),
        logs=logs, vehicles=vehicles, median=median, p25=p25, p75=p75,
        current=current_value(part_type, variant, parameter),
        proposed=median if enough else None,
        reason=None if enough else _no_proposal_reason(pool, rules),
        checked=checked,
        failed_slope=sum(o.failed_slope for o in pool.obs),
        failed_resolution=sum(o.failed_resolution for o in pool.obs),
        median_rate_hz=_median_of([o.rate_hz for o in passing]), median_factor=factor,
        factor_flagged=factor is not None and (factor > FACTOR_FLAG or factor < 1 / FACTOR_FLAG),
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
    collected = {m: _collect(part_type, m[1], entries, rules) for m in spec.measured}
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
