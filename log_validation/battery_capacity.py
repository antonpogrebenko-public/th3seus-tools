"""Battery internal resistance per cell count and pack-capacity class.

The pack capacity is the log's battery capacity parameter.  Rows give the
spread of the per-cell resistance of the step events; the Rust constants stay
per cell count and do not read them.
"""

from __future__ import annotations

import argparse
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np

DEFAULT_CAPACITY_BOUNDS_MAH: tuple[float, ...] = (1000.0, 2500.0, 5000.0, 10000.0, 20000.0)
UNKNOWN_CAPACITY = "unknown capacity"
MIN_CLASS_VEHICLES = 5
CAPACITY_PARAMS = ("BAT1_CAPACITY", "BAT_CAPACITY")


@dataclass(frozen=True)
class CapacityClassRFit:
    """Resistance spread of one cell count in one pack-capacity class."""

    cell_count: int
    capacity_class: str
    n_steps: int
    n_vehicles: int
    median_mohm_per_cell: float
    p10_mohm_per_cell: float
    p90_mohm_per_cell: float


def pack_capacity_mah(params: Mapping[str, Any]) -> float | None:
    """BAT1_CAPACITY, else BAT_CAPACITY, as logged (-1 included); None when absent."""
    for name in CAPACITY_PARAMS:
        value = _finite_float(params.get(name))
        if value is not None:
            return value
    return None


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _class_index(capacity_mah: float | None, bounds: Sequence[float]) -> int | None:
    """Index of the class (0 = below the first bound), None for an unknown capacity."""
    if capacity_mah is None or not math.isfinite(capacity_mah) or capacity_mah <= 0:
        return None
    return sum(1 for b in bounds if capacity_mah >= b)


def capacity_class(capacity_mah: float | None, bounds: Sequence[float]) -> str:
    """Class name of a capacity: "<1000", "1000-2500", ..., ">=20000" (lower bound inclusive)."""
    index = _class_index(capacity_mah, bounds)
    return UNKNOWN_CAPACITY if index is None else _class_name(index, bounds)


def _class_name(index: int, bounds: Sequence[float]) -> str:
    if index == 0:
        return f"<{bounds[0]:g}"
    if index == len(bounds):
        return f">={bounds[-1]:g}"
    return f"{bounds[index - 1]:g}-{bounds[index]:g}"


def parse_bounds(text: str) -> tuple[float, ...]:
    """A comma list of positive, strictly increasing finite numbers."""
    try:
        bounds = tuple(float(part) for part in text.split(","))
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a comma list of numbers: {text!r}") from None
    if not all(math.isfinite(b) and b > 0 for b in bounds):
        raise argparse.ArgumentTypeError("capacity bounds must be positive numbers")
    if any(a >= b for a, b in zip(bounds, bounds[1:])):
        raise argparse.ArgumentTypeError("capacity bounds must be strictly increasing")
    return bounds


def add_capacity_args(parser: argparse.ArgumentParser) -> None:
    default = ",".join(f"{b:g}" for b in DEFAULT_CAPACITY_BOUNDS_MAH)
    parser.add_argument(
        "--capacity-bounds", type=parse_bounds, default=DEFAULT_CAPACITY_BOUNDS_MAH,
        metavar="MAH,MAH,...",
        help=f"pack-capacity class bounds in mAh, increasing (default: {default})",
    )


def _vehicle_of_log(log_id: str, vehicle_of: Mapping[str, str]) -> tuple[str, str]:
    return ("vehicle", vehicle_of[log_id]) if log_id in vehicle_of else ("log", log_id)


def capacity_class_fits(
    observations: Sequence[Any],
    vehicle_of: Mapping[str, str],
    bounds: Sequence[float] = DEFAULT_CAPACITY_BOUNDS_MAH,
    min_vehicles: int = MIN_CLASS_VEHICLES,
) -> tuple[CapacityClassRFit, ...]:
    """One row per cell count and capacity class with at least ``min_vehicles`` vehicles.

    Observations carry ``log_id``, ``cell_count``, ``capacity_mah`` and
    ``r_obs``.  Percentiles run over the step events; a log without a vehicle
    counts as its own.  Rows sort by cell count, then class in bound order,
    unknown capacity last.
    """
    unknown = len(bounds) + 1
    groups: dict[tuple[int, int], list[Any]] = {}
    for o in observations:
        index = _class_index(o.capacity_mah, bounds)
        key = (o.cell_count, unknown if index is None else index)
        groups.setdefault(key, []).append(o)
    rows = [_class_row(c, i, group, vehicle_of, bounds) for (c, i), group in sorted(groups.items())]
    return tuple(r for r in rows if r is not None and r.n_vehicles >= min_vehicles)


def _class_row(
    cells: int, index: int, group: Sequence[Any], vehicle_of: Mapping[str, str],
    bounds: Sequence[float],
) -> CapacityClassRFit | None:
    stepped = [o for o in group if any(math.isfinite(r) for r in o.r_obs)]
    values = np.array([r for o in stepped for r in o.r_obs if math.isfinite(r)], dtype=float)
    if values.size == 0:
        return None
    name = UNKNOWN_CAPACITY if index == len(bounds) + 1 else _class_name(index, bounds)
    return CapacityClassRFit(
        cell_count=cells,
        capacity_class=name,
        n_steps=int(values.size),
        n_vehicles=len({_vehicle_of_log(o.log_id, vehicle_of) for o in stepped}),
        median_mohm_per_cell=float(np.median(values)),
        p10_mohm_per_cell=float(np.percentile(values, 10)),
        p90_mohm_per_cell=float(np.percentile(values, 90)),
    )
