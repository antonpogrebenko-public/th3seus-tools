"""Reference-airframe comparison: TH3SEUS predictions against real logs of public kits.

For each commercial kit that has its own PX4 airframe ID, the kit table gives the
build values TH3SEUS needs, each with a public source. This module compares
TH3SEUS's predicted hover motor command, motor lag and control effectiveness
(`th3seus_model.predict`) with what the response pass measured in real logs of
that airframe, and recommends reviewing a TH3SEUS formula only when the evidence
is consistent across kits.

Known limitations, inherited from the Task 9 mirror (`th3seus_model`): the
predictions are static (nominal pack voltage, no voltage sag or thermal
effects) and assume a symmetric quad X.

Comparison rules:

* A log belongs to a kit when its top-level `airframe_id` equals the kit's.
* Each log gives one value per quantity; the median over a vehicle's logs is
  taken first and the percentiles are taken over vehicles, so a vehicle with
  many logs counts once. A log without `vehicle_id` is its own vehicle.
* `lag` is the per-log mean of `lag_minus_filter_s.roll` and `.pitch` (the one
  present if only one is), compared with TH3SEUS's `tau_motor_s`.
* Effectiveness from a log with `bat_scale_en` == 1 is left out: PX4 then
  publishes torque scaled by battery voltage, which is not comparable.
* TH3SEUS's effectiveness is in motor-command space (THR_MDL_FAC = 0), so each
  measured value is first converted with `to_command_space`.
* A quantity gets a row only when at least `min_vehicles` vehicles contributed.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from .outputs import PrivateDataError, forbidden_strings, write_json
from .th3seus_model import AXES, Build, Prediction, predict

DEFAULT_MIN_VEHICLES = 5
DEFAULT_KITS_PATH = Path(__file__).with_name("reference_kits.json")

HOVER = "hover.command"
LAG = "lag"
EFFECTIVENESS = "effectiveness"

FORMULA_OF = {
    "hover.command": "thrust basis K_CT (reported, never changed)",
    "lag": "motor lag tau = 0.02 + 15/KV",
    "effectiveness": "inertia and arm-length formulas",
}

PERCENTILES = (25.0, 50.0, 75.0)
BUILD_FIELDS = tuple(f.name for f in dataclasses.fields(Build))
REQUIRED_BUILD_FIELDS = tuple(
    f.name for f in dataclasses.fields(Build)
    if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
)


@dataclass(frozen=True)
class Kit:
    name: str
    airframe_id: int
    build: Build | None
    sources: dict[str, str]
    missing_sources: tuple[str, ...]


@dataclass(frozen=True)
class QuantityRow:
    quantity: str
    predicted: float
    real_median: float
    real_p25: float
    real_p75: float
    n_vehicles: int
    ratio: float  # predicted / real median


@dataclass(frozen=True)
class KitComparison:
    kit: str
    airframe_id: int
    n_vehicles: int
    compared: bool
    rows: tuple[QuantityRow, ...]
    note: str


@dataclass(frozen=True)
class Recommendation:
    quantity: str
    formula: str
    recommended: bool
    kits: tuple[str, ...]
    note: str


# ---- kit table ----

def load_kits(path: Path) -> list[Kit]:
    """Kits from a JSON list of {name, airframe_id, build, sources}.

    Raises ValueError, naming the kit and field, for malformed JSON, a non-list,
    an unknown or missing Build field, or a value Build rejects. A build field
    with no source is listed in `missing_sources` and the kit's `build` is None."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"kits file is not valid JSON: {e}") from e
    if not isinstance(data, list):
        raise ValueError("kits file must hold a JSON list of kits")
    kits = [_parse_kit(index, item) for index, item in enumerate(data)]
    _refuse_shared(kits)
    return kits


def _refuse_shared(kits: Sequence[Kit]) -> None:
    """Two kits on one airframe share vehicles and would count twice as evidence."""
    for field in ("airframe_id", "name"):
        first: dict[Any, str] = {}
        for kit in kits:
            value = getattr(kit, field)
            if value in first:
                raise ValueError(f"kits {first[value]} and {kit.name} share {field} {value!r}")
            first[value] = kit.name


def _parse_kit(index: int, item: Any) -> Kit:
    if not isinstance(item, dict):
        raise ValueError(f"kit #{index} must be a JSON object")
    name = item.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError(f"kit #{index}: name must be a non-empty string")
    airframe_id = item.get("airframe_id")
    if isinstance(airframe_id, bool) or not isinstance(airframe_id, int):
        raise ValueError(f"kit {name}: airframe_id must be an integer, got {airframe_id!r}")
    values, sources = item.get("build"), item.get("sources", {})
    if not isinstance(values, dict) or not isinstance(sources, dict):
        raise ValueError(f"kit {name}: build and sources must be JSON objects")
    build = _build_of(name, values)
    for field in sources:
        if field not in BUILD_FIELDS:
            raise ValueError(f"kit {name}: source for unknown build field {field!r}")
    missing = tuple(f for f in values if not sources.get(f))
    return Kit(name=name, airframe_id=airframe_id, build=None if missing else build,
               sources=dict(sources), missing_sources=missing)


def _build_of(name: str, values: Mapping[str, Any]) -> Build:
    for field in values:
        if field not in BUILD_FIELDS:
            raise ValueError(f"kit {name}: unknown build field {field!r}")
    for field in REQUIRED_BUILD_FIELDS:
        if field not in values:
            raise ValueError(f"kit {name}: build field {field!r} is missing")
    for field, value in values.items():
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
            raise ValueError(f"kit {name}: build field {field!r} must be a number, got {value!r}")
    try:
        return Build(**values)
    except ValueError as e:
        raise ValueError(f"kit {name}: {e}") from e


# ---- measured values per log ----

def to_command_space(effectiveness: float, thr_mdl_fac: float | None,
                     hover_command: float | None) -> float | None:
    """Measured effectiveness converted to motor-command space (THR_MDL_FAC = 0).

    PX4 applies the thrust-model inverse after allocation, so a log flown with
    f = THR_MDL_FAC > 0 shows the effectiveness divided by dT/du at hover. The
    value is multiplied by (1 - f) + 2 f u_hover. None when f or u_hover is
    missing, not finite, or the hover command is not positive."""
    if not _finite(thr_mdl_fac) or not _finite(hover_command) or hover_command <= 0.0:
        return None
    return effectiveness * ((1.0 - thr_mdl_fac) + 2.0 * thr_mdl_fac * hover_command)


def _finite(value: Any) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value))


def finite_number(measurements: Mapping[str, Any], key: str) -> float | None:
    value = measurements.get(key)
    return float(value) if _finite(value) else None


def _log_lag(measurements: Mapping[str, Any]) -> float | None:
    values = [v for v in (finite_number(measurements, f"lag_minus_filter_s.{a}")
                          for a in ("roll", "pitch")) if v is not None]
    return sum(values) / len(values) if values else None


def log_effectiveness(measurements: Mapping[str, Any], axis: str) -> float | None:
    if measurements.get("bat_scale_en") == 1:
        return None
    raw = finite_number(measurements, f"effectiveness.{axis}")
    if raw is None or raw <= 0.0:
        return None
    converted = to_command_space(raw, finite_number(measurements, "hover.thr_mdl_fac"),
                                 finite_number(measurements, HOVER))
    return converted if converted is not None and converted > 0.0 else None


def _log_values(measurements: Mapping[str, Any]) -> dict[str, float]:
    values = {HOVER: finite_number(measurements, HOVER), LAG: _log_lag(measurements)}
    for axis in AXES:
        values[f"{EFFECTIVENESS}.{axis}"] = log_effectiveness(measurements, axis)
    return {q: v for q, v in values.items() if v is not None}


def vehicle_key(log: Mapping[str, Any]) -> tuple[str, Any]:
    vehicle_id = log.get("vehicle_id")
    return ("log", log.get("log_id")) if vehicle_id is None else ("vehicle", vehicle_id)


def _predicted(prediction: Prediction) -> dict[str, float]:
    return {
        HOVER: prediction.hover_command,
        LAG: prediction.tau_motor_s,
        f"{EFFECTIVENESS}.roll": prediction.effectiveness_roll,
        f"{EFFECTIVENESS}.pitch": prediction.effectiveness_pitch,
        f"{EFFECTIVENESS}.yaw": prediction.effectiveness_yaw,
    }


# ---- comparison ----

def compare_kits(kits: Sequence[Kit], response: Sequence[dict],
                 min_vehicles: int = DEFAULT_MIN_VEHICLES) -> list[KitComparison]:
    """One KitComparison per kit, in kit order."""
    return [_compare_kit(k, response, min_vehicles) for k in kits]


def _compare_kit(kit: Kit, response: Sequence[dict], min_vehicles: int) -> KitComparison:
    logs = [log for log in response if log.get("airframe_id") == kit.airframe_id]
    vehicle_values: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for log in logs:
        per_vehicle = vehicle_values[vehicle_key(log)]
        for quantity, value in _log_values(log.get("measurements") or {}).items():
            per_vehicle[quantity].append(value)
    n_vehicles = len(vehicle_values)
    reasons = [f"no source for {field}" for field in kit.missing_sources]
    if n_vehicles < min_vehicles:
        reasons.append(f"{n_vehicles} vehicles, fewer than {min_vehicles}")
    if reasons or kit.build is None:
        return KitComparison(kit.name, kit.airframe_id, n_vehicles, False, (),
                             "not compared: " + "; ".join(reasons))
    rows = _rows(_predicted(predict(kit.build)), vehicle_values, min_vehicles)
    return KitComparison(kit.name, kit.airframe_id, n_vehicles, True, rows, "compared")


def _rows(predicted: Mapping[str, float], vehicle_values: Mapping[tuple, Mapping[str, list[float]]],
          min_vehicles: int) -> tuple[QuantityRow, ...]:
    rows = []
    for quantity, value in predicted.items():
        medians = [float(np.median(v[quantity])) for v in vehicle_values.values() if v.get(quantity)]
        if len(medians) < min_vehicles:
            continue
        p25, p50, p75 = (float(x) for x in np.percentile(medians, PERCENTILES))
        ratio = value / p50 if p50 != 0.0 else math.nan
        rows.append(QuantityRow(quantity, value, p50, p25, p75, len(medians), ratio))
    return tuple(rows)


# ---- recommendations ----

def recommendations(comparisons: Sequence[KitComparison]) -> list[Recommendation]:
    """One Recommendation per formula in FORMULA_OF.

    A formula is recommended for review only when at least 2 kits have data for
    it and in every such kit the prediction lies outside [p25, p75] on the same
    side, for every quantity of the formula. Kits that were not compared, or
    that have no data for the formula, are not evidence. The hover command is
    never recommended."""
    return [_recommend(formula, comparisons) for formula in FORMULA_OF]


def _formula_of_quantity(quantity: str) -> str:
    return quantity.split(".")[0] if quantity.startswith(EFFECTIVENESS) else quantity


def _side(row: QuantityRow) -> int:
    if row.predicted > row.real_p75:
        return 1
    return -1 if row.predicted < row.real_p25 else 0


def _kit_side(comparison: KitComparison, formula: str) -> tuple[bool, int]:
    """(has data, common side of the kit's rows); side 0 when inside or mixed."""
    sides = {_side(r) for r in comparison.rows if _formula_of_quantity(r.quantity) == formula}
    if not sides:
        return False, 0
    return True, sides.pop() if len(sides) == 1 else 0


def _recommend(formula: str, comparisons: Sequence[KitComparison]) -> Recommendation:
    evidence = [(c.kit, _kit_side(c, formula)) for c in comparisons if c.compared]
    with_data = [(name, side) for name, (has, side) in evidence if has]
    without = tuple(name for name, (has, _) in evidence if not has)
    names = tuple(name for name, _ in with_data)
    consistent = (len(with_data) >= 2 and len({side for _, side in with_data}) == 1
                  and with_data[0][1] != 0)
    tail = _without_data_note(without)
    label = FORMULA_OF[formula]
    if formula == HOVER:
        note = ("hover throttle discrepancy is reported for the thrust basis K_CT, "
                "which is never recommended for change")
        if consistent:
            note += f"; the prediction is off the same way in all {len(names)} kits"
        return Recommendation(formula, label, False, names, note + tail)
    if consistent:
        direction = "above" if with_data[0][1] > 0 else "below"
        note = (f"review the formula: the prediction is {direction} the real p25 to p75 "
                f"in all {len(names)} compared kits")
        return Recommendation(formula, label, True, names, note + tail)
    return Recommendation(formula, label, False, names, _no_change_note(len(names)) + tail)


def _without_data_note(without: Sequence[str]) -> str:
    if not without:
        return ""
    return "; compared kits without data for this formula: " + ", ".join(without)


def _no_change_note(n_kits: int) -> str:
    if n_kits < 2:
        verb = "has" if n_kits == 1 else "have"
        kits = "kit" if n_kits == 1 else "kits"
        return (f"no change recommended: {n_kits} compared {kits} {verb} data, "
                "at least 2 are needed")
    return "no change recommended: the evidence is not consistent across the compared kits"


# ---- CLI ----

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m log_validation.reference_airframes",
        description="Compare TH3SEUS predictions with real logs of reference kits.")
    parser.add_argument("response_json", type=Path, help="response pass output JSON")
    parser.add_argument("--kits", type=Path, default=DEFAULT_KITS_PATH,
                        help="kit table JSON (default: the bundled table)")
    parser.add_argument("-o", "--output", type=Path, required=True, help="output JSON file")
    parser.add_argument("--min-vehicles", type=int, default=DEFAULT_MIN_VEHICLES,
                        help=f"vehicles a kit needs to be compared (default {DEFAULT_MIN_VEHICLES})")
    return parser


def _run(args: argparse.Namespace) -> None:
    if args.min_vehicles < 1:
        raise ValueError(f"--min-vehicles must be at least 1, got {args.min_vehicles}")
    response = json.loads(args.response_json.read_text(encoding="utf-8"))
    if not isinstance(response, list):
        raise ValueError("response JSON must hold a list of per-log results")
    kits = load_kits(args.kits)
    comparisons = compare_kits(kits, response, args.min_vehicles)
    document = {
        "min_vehicles": args.min_vehicles,
        "comparisons": [asdict(c) for c in comparisons],
        "recommendations": [asdict(r) for r in recommendations(comparisons)],
    }
    write_json(args.output, document, forbidden_strings(args.response_json, args.kits))


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        _run(args)
    except (PrivateDataError, ValueError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
